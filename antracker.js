/**
 * @name Another Node Tracker (antracker)
 * @author Markus Näsman
 * @copyright 2010 (c) Markus Näsman <markus at botten dot org >
 * @license see COPYING
 */

/* ---------------------------------------------------- */
/* Includes                                             */
/* ---------------------------------------------------- */
var sys = require('sys');
var http = require('http');
var url = require('url');
var qs = require('querystring');
var optimist = require('optimist');

/* ---------------------------------------------------- */
/* Variables                                            */
/* ---------------------------------------------------- */
var torrents = {};

/* ---------------------------------------------------- */
/* "Constants"                                          */
/* ---------------------------------------------------- */
var PORT = optimist.argv.port || 1337;
var IP = optimist.argv.ip || "127.0.0.1";
var ANNOUNCE_INTERVAL = optimist.argv.interval || 180;

/* ---------------------------------------------------- */
/* Classes                                              */
/* ---------------------------------------------------- */
function ClientError(message) { this.message = message; }
ClientError.prototype = new Error;

function ServerError(message) { this.message = message; }
ServerError.prototype = new Error;

/* ---------------------------------------------------- */
/* Functions                                            */
/* ---------------------------------------------------- */

/* Server functions */
function scrape(parsed_query, res) { 
    var params = qs.parse(parsed_query.query);
    var dict = {};

    var get_torrent_info_f = function(info_hash)
    {
	prune_peers(info_hash);
	var torrent = get_torrent(info_hash);
	if (torrent == undefined) 
	{
	    throw new ClientError("This torrent is not registered with this tracker");
	}
	else
	{
	    return {"complete  " : torrent.seeders,
		    "incomplete" : torrent.leechers,
		    "downloaded" : torrent.completed};
	}
    }

    switch (typeof params.info_hash)
    {
    case "string":
	dict[params.info_hash] = get_torrent_info_f(params.info_hash);
	break;
    default:
	for(var key in params.info_hash) 
	{
	    var info_hash = params.info_hash[key];
	    dict[info_hash] = get_torrent_info_f(info_hash);
	}
	break;
    }
    return {"files" : dict};
};

function announce(parsed_query, res, req_ip) {
    var params = qs.parse(parsed_query.query);
    var info_hash = params.info_hash;
    var left = parseInt(params.left, 10);
    var client_port = parseInt(params.port, 10);
    var client_ip = params.ip || req_ip;
    var compact = parseInt(params.compact,10) || 0;
    var numwant = parseInt(params.numwant,10) || 50;
    var peer_id = params.peer_id;
    var completed = false;

    console.log("peer_id calling ", peer_id);
    // Create a peer from request and assert it's sane
    var peer = {"id"            : peer_id,
		"ip"            : client_ip,
		"port"          : client_port,
		"leeching"      : true,
	        "last_announce" : Date.UTC()};
    assert_peer(peer);
   
    // Handle the event (if such exists) 
    switch (params.event) 
    {
    case "stopped":
	remove_peer(info_hash, peer);
	break;
    case "completed":
	completed = true;
	peer.leeching = false;
    default:
	if(left == 0) peer.leeching = false;
	add_or_update_peer(info_hash, peer, completed);
	break;
    }
    prune_peers(info_hash);
    var torrent = get_torrent(info_hash);
    var reply_dict = {"interval"   : ANNOUNCE_INTERVAL,
		      "complete"   : torrent.seeders,
		      "incomplete" : torrent.leechers,
		      "peers"      : pick_peers(peer, torrent.peers, numwant, compact)
		     };
    return reply_dict;
};

/* Reply stuff */
function reply(res, reply_dict) {
    var encoded_reply = encode(reply_dict);
    console.log("encoded_reply: ", sys.inspect(encoded_reply));
    res.writeHead(200, {'Content-Type': 'text/plain'});			
    res.end(encoded_reply, 'ascii');
}

/* Bencode stuff */
function encode(input) {
    switch (typeof(input)) 
    {
    case "number":
	return "i" + input + "e";
    case "string":
	return input.length + ":" + input;
    case "object":
	if (input instanceof Array) 
	{
	    var str = input.reduce(function(acc,x) { return acc + encode(x)}, "");
	    return "l" + str + "e";
	}
	else
	{
	    var dict = "";
	    var keys = [];
	    for(var key in input) { keys.push(key); };
	    for(var index in keys.sort()) 
	    {
		var key = keys[index];
		dict += encode(key) + encode(input[key]);
	    }
	    return "d" + dict + "e";
	}
    default:
	throw new ServerError("Cannot encode.");
    }
}

/* "Database" stuff */
function assert_peer(peer) {
    if (!peer.ip.match(/^\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}$/) || isNaN(peer.port) || typeof("id") != "string") 
    {
	throw new ClientError("Got bad data from client");
    }
}

function prune_peers(info_hash) {
    var torrent = get_torrent(info_hash);
    var f = function(e, i, a) {
	if(Date.UTC() + ANNOUNCE_INTERVAL + 60 > e.last_announce) 
	{
	    remove_peer(info_hash, e); 
	}
    }
    torrent.peers.forEach(f);
}

function add_or_update_peer(info_hash, peer, completed) {
    var torrent = get_torrent(info_hash);
    /* This info_hash already exists, add peer to it */
    if (torrent) 
    {
	var found_peer = find_peer(peer, torrent.peers);
	/* Peer already registered with info_hash, update leecher/seeder */
	if(found_peer)
	{
	    update_leechers_seeders(found_peer, torrent, false, -1);
	    /* Update found peer with data from the new peer */
	    found_peer.leeching = peer.leeching;
	    found_peer.last_announce = peer.last_announce;
	}
	/* Peer not registered with info_hash, add peer and update leecher/seeder */
	else
	{
	    torrent.peers.push(peer);
	    
	}
	update_leechers_seeders(peer, torrent, completed);
    } 
    /* First time we see this info_hash, create a entry for it */
    else 
    {
	create_torrent(info_hash, [peer]);
	update_leechers_seeders(peer, get_torrent(info_hash), completed);
    }
}

function remove_peer(info_hash, target_peer) {
    var torrent = get_torrent(info_hash);
    /* info_hash actually exists, try to find peer */
    if (torrent) 
    {
	var index = peer_index_of(target_peer, torrent.peers); 
	/* Peer actually registered with info_hash */
	if(index != -1)
	{
	    torrent.peers.splice(index,1);
	    update_leechers_seeders(torrent.peers[index], torrent, false, -1);
	    return true; /* peer deleted, return true */
	}
    }
    else
    {
	create_torrent(info_hash, []);
    }
    return false; /* no peer found for some reason, return false */
}

function find_peer(peer, peers) {
    return peers[peer_index_of(peer, peers)];
}

function pick_peers(peer, peers, numwant, compact) {
    var good_peers = [];
    if(numwant < 0) 
    {
	var numpeers = peers.length;
	var include_seeders = peer.leeching;
	var filter_f = function(cand_peer) { 
	    peer.id != cand_peer.id && (include_seeders || !cand_peer.leeching);
	};
	// Filter out any seeders if not wanted and filter out self
	var filtered_peers = peers.filter(filter_f);

	// No need for shuffle if numwant is more than the available peers	
	if (numwant <= filtered_peers.length)
	{ 
	    good_peers = filtered_peers; 
	}
	else
	{ 
	    good_peers = shuffle(filtered_peers).slice(0, numwant-1);
	}
    }
    return format_peers(good_peers, compact);
}

function format_peers(good_peers) {
   // TODO: Does this work?
    compact = 0;
    if (compact == 1) 
    {
	var str = "";
	var buf = new Buffer(6);
	for(var i in good_peers) 
	{
	    var ip = good_peers[i].ip.split(".");
	    for(var k = 0; k < 4; k++) { buf[k] = parseInt(ip[k]);}
	    buf[4] = (good_peer.port >> 8) & 0xff;
	    buf[5] = good_peer.port & 0xff;
	    str += buf.toString();
	}
	return str;
    }
    else
    {
	var transform_f = function(cand_peer) { 
	    return {"peer id" : cand_peer.id,
		    "ip"      : cand_peer.ip,
		    "port"    : cand_peer.port}
	};
	return good_peers.map(transform_f)
    }
}

function create_torrent(info_hash, peers) {
    var torrent = {"peers"     : peers,
		   "leechers"  : 0,
		   "seeders"   : 0,
		   "completed" : 0};
    torrents[info_hash] = torrent;
}

function get_torrent(info_hash) {
    return torrents[info_hash];
}

function update_leechers_seeders(peer, torrent, completed, delta) {
    var delta = typeof(delta) != 'undefined' ? delta : 1;
    if (peer.leeching) torrent.leechers+=delta;
    else torrent.seeders+=delta;
    if (completed) torrent.completed+=1;
}

/* Helpers */
function shuffle(a) {
    for(var i = 0; i < a.length; i++)
    {
	var e = a[i];
	var j = Math.floor(Math.random() * i);
	var e2 = a[j];
	a[i] = e2;
	a[j] = e;
    }
    return a;
}

function peer_index_of(target_peer, peers) {
    for(var index in peers)
    {
	var peer = peers[index];
	if(peer.id == target_peer.id) { return index };
    }
    return -1;
}


/* ---------------------------------------------------- */
/* Server callbacks                                     */
/* ---------------------------------------------------- */

/* HTTP Callback */
function req_listener(req, res) {
    var parsed_query = url.parse(req.url);
    console.log("torrents before: ", sys.inspect(torrents));
    var reply_dict = {};
    try
    {
	switch (parsed_query.pathname) 
	{
	case "/announce":
	    console.log("got announce");
	    reply_dict = announce(parsed_query, res, req.connection.remoteAddress);
	    break;
	case "/scrape":
	    console.log("got scrape");
	    reply_dict = scrape(parsed_query, res);
	    break;
	default:
	    reply_dict = {"failure reason" : "error"};
	}
    } 
    catch(e)
    {
	// Client did wrong, reply with error
	switch(e.name)
	{
	case "ClientError":
	    reply_dict = {"failure reason" : e.message };
	    break;
	default: // don't know what to do, die badly
	    throw e;
	}
    }
    console.log("torrents after: ", sys.inspect(torrents));    
    reply(res, reply_dict);
};

/* ---------------------------------------------------- */
/* Register server callback(s)                          */
/* ---------------------------------------------------- */
http.createServer(req_listener).listen(PORT, IP);
console.log("Started antracker on " + IP + ":" + PORT + " with announce interval " + ANNOUNCE_INTERVAL + "s.");