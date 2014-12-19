#!/usr/bin/env node

var $ = require('./lib/dollar').$,
	BSON = require('bson').BSONPure,
	MongoOplog = require('mongo-oplog'),
	streamProcess = require('./lib/streamProcess'),
	debug = require('debug')('mongo-graph-connector'),
	argv = require('optimist')
	.usage('connector for mongo and graph db.\nUsage: $0')
	.alias('t', 'timestamp')
	.describe('t', 'timestamp: the timestamp of the last run')
	.default('t', './timestamp.json')
	.argv;

/*
 * load common components
 */
require('./lib/allLoader').loadDollar();

var timestamp = null;

var lastTimestamp = require(argv.t).ts || null;
var currentTimestamp = Date.now();
//replay oplogs since last run
MongoOplog.prototype.init = function init(fn) {
	debug('Connected to oplog database');
	var oplog = this;
	oplog.conn.ready(function ready(err, db) {

		if (err) return oplog.onerror(err);

		var time, since, query = {},
			coll = db.collection(oplog.coll),
			options = {
				oplogReplay: true,
				timeout: false,
				numberOfRetries: -1
			};
		oplog.running = true;
		time = {
			$lte: BSON.Timestamp(0, currentTimestamp / 1000)
		};
		query.ts = time;
		oplog.stream = coll.find(query, options).sort({
			ts: 1
		}).stream();
		oplog.bind();
		oplog.stream.on('close', function() {
			console.log("stop");
		});
		if (fn) fn(null, oplog.stream);
	});
	return this;
};
var mongoConn = 'mongodb://' + $('config').MONGO_HOST + ':' + $('config').MONGO_PORT + '/local';
console.log(mongoConn);
var mongoOplog = MongoOplog(mongoConn);
var realtimeOplog = mongoOplog.tail(
	function() {
		console.log('start syncing mongo db and graph db');
	}
);
var initOplog = mongoOplog.init(
	function() {
		console.log('start initilizing mongo db and graph db');
	}
);

//replay oplogs since last run
initOplog.on('op', streamProcess);

//run realtime oplogs
realtimeOplog.on('op', streamProcess);

//console.log('save timestamp:'+timestamp);
//save last timestamp 

realtimeOplog.stop(function() {
	console.log('server stopped');
});