
var Client = require('./lib/client');
var cql = require('cql-protocol');

/**
 * Create client
 * @param {object} option - optional parameters
 * @param {array} hosts - seed hosts to connect to get peers. ex) [127.0.0.1:9402,127.0.0.2:9042,.. ]
 */
exports.createClient = function(option) {
  return new Client(option);
};

// exports libraries

exports.Client = require('./lib/client');
exports.Connection = require('./lib/connection');

exports.opcodes = require('./lib/opcodes');
exports.types = require('./lib/types');

exports.messages = cql.messages;

exports.CONSISTENCY_LEVEL = cql.CL;
exports.CL = cql.CL;

