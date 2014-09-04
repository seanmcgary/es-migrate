var _ = require('lodash');
var q = require('q');
var elasticsearch = require('elasticsearch');
var optimist = require('optimist');
var logwrangler = require('logwrangler');
var logger = logwrangler.create();
var fs = require('fs');
var path = require('path');


var argv = optimist.argv;

console.log(argv);

var host = argv.host || 'localhost';
var port = argv.port || 9200;
var fromIndex = argv.from;
var toIndex = argv.to;
var indexAlias = argv.alias;
var apiVersion = argv.apiVersion || '1.1';
var batchSize = argv.batchSize || 100;
var newMapping = argv.newMapping;

if(!fromIndex || !toIndex || !indexAlias){
	console.log('useage: --alias, --to, and --from are required');
	return process.exit();
}

var hostname = [host, port].join(':');

var client = elasticsearch.Client({
	host: hostname,
	apiVersion: apiVersion
});

var batchDeferred = q.defer();
var getBatch = function(limit, offset){
	logger.info({
		message: 'getting batch',
		data: {
			limit: limit,
			offset: offset
		}
	});
	client.search({
		index: fromIndex,
		from: offset,
		size: limit,
		query: {
			sort: '$ts:asc',
			match_all: {}
		}
	})
	.then(function(results){
		if(results && results.hits && results.hits.hits){
			var totalDocs = results.hits.hits.length;

			var docs = results.hits.hits;

			insertBatch(docs)
			.then(function(){
				if(totalDocs == limit){
					getBatch(limit, offset + limit);
				} else {
					batchDeferred.resolve();
				}
			});
		}
	});
};

var insertBatch = function(docs){
	
	var batch = _.flatten(_.map(docs, function(doc){
		var ops = [
			{ index: { _index: toIndex, _type: doc._type, _id: doc._id }},
			doc._source
		];
		return ops;
	}));
	logger.info({
		message: 'processing batch'
	});
	return client.bulk({
		body: batch
	})
	.then(function(){
		logger.info({
			message: 'batch inserted'
		});
	}, function(err){
		logger.info({
			message: 'error inserting batch',
			data: {
				error: err
			}
		});
	});
};

var updateAliases = function(){
	logger.info({
		message: 'updating aliases'
	});
	return client.indices.updateAliases({
		body: {
			actions: [
				{ remove: { index: fromIndex, alias: indexAlias }}, 
				{ add: { index: toIndex, alias: indexAlias }}
			]
		}
	})
	.then(function(){
		logger.info({
			message: 'indexes updated'
		});
	}, function(err){
		logger.info({
			message: 'index update error',
			data: {
				error: err
			}
		});
	});
};

var setupNewIndex = function(){
	if(!newMapping){
		return q.resolve();
	}

	var deferred = q.defer();
	fs.readFile(newMapping, function(err, data){
		console.log(err);
		data = data.toString();
		var json;
		try {
			json = JSON.parse(data);
		} catch(e){
			logger.error({
				message: 'invalid json',
				data: {
					error: e
				}
			});
			process.exit();
		}

		client.indices.exists({
			index: toIndex
		})
		.then(function(exists){
			if(exists){
				return q.resolve();
			}

			return client.indices.create({
				index: toIndex
			});
		})
		.then(function(){

			return client.indices.close({
				index: toIndex
			})
			.then(function(){
				logger.info({
					message: 'indexed closed'
				});
				return client.indices.putSettings({
					index: toIndex,
					body: json
				})
				.then(function(){
					logger.info({
						message: 'mapping updated'
					});
				});
			})
			.then(function(){
				return client.indices.open({
					index: toIndex
				})
				.then(function(){
					logger.info({
						message: 'indexed re-opened'
					});
					return deferred.resolve(json);
				});
			});
		})
		.then(undefined, function(err){
			logger.error({
				message: 'failed to update mapping',
				data: {
					error: err
				}
			});
			process.exit();
		})
	});

	return deferred.promise;
};

setupNewIndex()
.then(function(){

	getBatch(batchSize, 0);
	batchDeferred.promise
	.then(function(){
		logger.info({
			message: 'migration complete'
		});

		updateAliases()
		.then(function(){
			console.log('done');
			process.exit();
		}, function(){
			console.log(arguments);
		});
	}, function(){
		console.log(arguments);
	});
});
