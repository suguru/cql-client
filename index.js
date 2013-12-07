
var Client = require('./lib/client');
var protocol = require('./lib/protocol/protocol');

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
exports.types = require('./lib/protocol/types');

exports.messages = protocol.messages;

exports.CONSISTENCY_LEVEL = protocol.CL;
exports.CL = protocol.CL;

