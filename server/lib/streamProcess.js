var $ = require('./dollar').$,
	_ = require('underscore'),
	grex = require('grex');

require('./allLoader').loadDollar();

var db = $('config').DB;
var gremlin = grex.gremlin;
var g = grex.g;
var client = grex.createClient({
	host: $('config').GRAPH_HOST,
	port: $('config').GRAPH_PORT,
	graph: $('config').GRAPH
});
var edgesNs = $('config').EDGES;
var verticesNs = $('config').VERTICES;
var vertexTypeMap = $('config').VERTEX_TYPE_MAPS;
var vertexFieldMask = $('config').VERTEX_FIELD_MASKS;

var objectToGroovy = function(object) {
	return JSON.stringify(object).replace('{', '[').replace('}', ']');
};

var process = module.exports = function(data) {
	var op = data.op;
	timestamp = data.ts.toString();
	ns = data.ns;
	//check start with "<db_name>."
	if (ns.lastIndexOf(db + '.', 0) !== 0)
		return;
	collection = ns.substr(db.length + 1);
	if (_.contains(edgesNs, collection)) {
		processingEdge(data, collection);
	}
	if (_.contains(verticesNs, collection)) {
		processingVertex(data, collection);
	}
};

/*
 * add/update/delete edge into graph db
 */
var processingEdge = function(data, collection) {
	console.log('proc edge');
	var query = gremlin();
	var properties = {};
	var item = data.o;
	//upsert (should be repeatable)
	if (data.op === 'i') {
		if (item.src && item.dest) {
			query("src=g.V('_uid','" + item.src.toString() + "').next()");
			query("dest=g.V('_uid','" + item.dest.toString() + "').next()");
			//find edge between src and dest
			query("edgePipe=src.bothE.as('x').bothV.retain([dest]).back('x')");
			properties = {
				'_etype': item.type,
				'_uid': item._id
			};
			if (item.prop) {
				properties = _.extend(item.prop, properties);
			}
			if (item.type) {
				properties = _.extend({
					'_etype': item.type
				}, properties);
			}
			var groovyProperties = objectToGroovy(properties);
			var upsertQuery = [
				"if(edgePipe.count()){",
				groovyProperties + ".each {edgePipe.next().setProperty(it.key, it.value)}",
				"} else {",
				"g.addEdge(src,dest,'" + item.type + "'," + groovyProperties + ")",
				"}"
			].join('\n');
			query(upsertQuery);
			client.execute(query, function(err, response) {
				console.log(err);
			});
		}
	}
	if (data.op === 'u') {
		if (item['$set'])
			item = item['$set'];
		item._uid = data.o2._id.toString();
		query("edge=g.E.has('_uid','" + item._uid + "').next()");
		properties = {
			'_uid': item._id
		};
		if (item.prop) {
			properties = _.extend(item.prop, properties);
		}
		query(objectToGroovy(properties) + ".each{edge.setProperty(it.key, it.value)}");
		client.execute(query, function(err, response) {
			console.log(err);
		});
	}
	if (data.op === 'd') {
		query("edge=g.E.has('_uid','" + item._id.toString() + "').remove()");
		client.execute(query, function(err, response) {
			console.log(err);
		});
	}
};
/*
 * add/update/delete vertex into graph db
 */
var processingVertex = function(data, collection) {
	console.log('proc vertex');
	var item = data.o;
	var query = gremlin();
	var groovyItem = {};
	var mask = [];

	//upsert (should be repeatable)
	if (data.op === 'i') {
		//"_id" is used in titan graph db
		if (item._id) {
			item._uid = item._id.toString();
			delete item._id;
		}
		//apply mask
		if (vertexFieldMask[collection]) {
			mask = _.union(vertexFieldMask[collection], ['_uid']);
			item = _.pick(item, mask);
		}
		if (vertexTypeMap[collection]) {
			item._vtype = vertexTypeMap[collection];
		}
		//find vertex by id
		query("vertexPipe=g.V.has('_uid','" + item._uid + "')");
		groovyItem = objectToGroovy(item);
		var upsertQuery = [
			"if(vertexPipe.count()){",
			groovyItem + ".each {vertexPipe.next().setProperty(it.key, it.value)}",
			"} else {",
			"g.addVertex(" + groovyItem + ")",
			"}"
		].join('\n');
		query(upsertQuery);
		client.execute(query, function(err, response) {
			console.log(err);
		});
	}
	if (data.op === 'u') {
		if (item['$set'])
			item = item['$set'];
		item._uid = data.o2._id.toString();
		//apply mask
		if (vertexFieldMask[collection]) {
			mask = _.union(vertexFieldMask[collection], ['_uid']);
			item = _.pick(item, mask);
		}
		if (vertexTypeMap[collection]) {
			item._vtype = vertexTypeMap[collection];
		}
		//find vertex by id
		query("vertexPipe=g.V.has('_uid','" + item._uid + "')");
		groovyItem = objectToGroovy(item);
		query(groovyItem + ".each {vertexPipe.next().setProperty(it.key, it.value)}");
		client.execute(query, function(err, response) {
			console.log(err);
		});
	}
	if (data.op === 'd') {
		query("g.V.has('_uid','" + item._id.toString() + "').remove()");
		client.execute(query, function(err, response) {
			console.log(err);
		});
	}
};