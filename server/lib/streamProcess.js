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

var process = module.exports = function(data) {
	var op = data.op;
	timestamp = data.ts.toString();
	ns = data.ns;
	//check start with "<db_name>."
	if (ns.lastIndexOf(db + '.', 0) !== 0)
		return;
	collection = ns.substr(db.length + 1);
	if (_.contains(edgesNs, collection)) {
		processingEdge(data);
	}
	if (_.contains(verticesNs, collection)) {
		processingVertex(data);
	}
};

/*
 * add edge into graph db
 */
var processingEdge = function(data) {
	console.log('proc edge');
};
/*
 * add vertex into graph db
 */
var processingVertex = function(data) {
	console.log('proc vertex');
	var item = data.o;
	var query = gremlin();
	if (data.op === 'i') {

		if (item._id) {
			item.uid = item._id.toString();
			delete item._id;
		}
		query(g.addVertex(item));
		client.execute(query, function(err, response) {
			console.log(response);
		});
	};
	if (data.op === 'u') {

		if (item._id) {
			item.oid = item._id.toString();
			delete item._id;
		}
		query(g.addVertex(item));
		client.execute(query, function(err, response) {
			console.log(response);
		});
	}
};