
var cql = require('cql-protocol');
var net = require('net');
var events = require('events');
var util = require('util');
var opcodes = require('./opcodes');

/**
 * cql.CL = {
 *   ANY,
 *   ONE,
 *   TWO,
 *   THREE,
 *   QUORUM,
 *   ALL,
 *   LOCAL_QUORUM,
 *   EACH_QUORUM
 * }
 *
 * cql.messages = {
 *   Error,
 *   Startup,
 *   Ready,
 *   Authenticate,
 *   Credentials,
 *   Options,
 *   Supported,
 *   Query,
 *   Result,
 *   Prepare,
 *   Execute,
 *   Register,
 *   Event,
 *   Batch,
 *   AuthChallenge,
 *   AuthResponse,
 *   AuthSuccess
 * }
 *
 */

/**
 * Create a connection
 * @param {object} option - optional parameters
 * @param {string} option.host - host address
 * @param {string} option.port - port number
 * @param {string=} option.keyspace - default keyspace
 * @param {number=} option.protocolVersion - CQL procotol version
 */
function Connection(option) {

  option = option || {};

  var host = option.host || '127.0.0.1:9042';
  var port = option.port || 9042;

  if (host.indexOf(':') > 0) {
    host = host.split(':');
    port = Number(host[1]);
    host = host[0];
  }

  this._host = host;
  this._port = port;
  this._keyspace = option.keyspace;
  this._protocolVersion = option.protocolVersion || 2;
  this._streamHandlers = {};
  this._streamIds = [];
  for (var i = 127; i >= 0; i--) {
    this._streamIds.push(i);
  }
  this._streamWaitQueue = [];
  this._consistencyLevel = option.consistencyLevel || option.cl || cql.CL.QUORUM;
  this._maxWaitPerConns = option.maxWaitPerConns || 100;
  this._preparedMap = {};
  this._reconnectInterval = option.reconnectInterval || 5000;
  this._connectTimeout = option.connectTimeout || 1000;
  this._autoReconnect = option.autoReconnect === true;
}

/**
 * @callback errorCallback
 * @param {object} err - error object
 */

util.inherits(Connection, events.EventEmitter);

/**
 * Connect to the server.
 * @param {errorCallback} cb 
 */
Connection.prototype.connect = function(cb) {

  var protocol = new cql.NativeProtocol(this._protocolVersion);
  var socket = net.connect({ host: this._host, port: this._port });
  var self = this;

  this._active = false;
  this._shutdown = false;

  socket.pipe(protocol).pipe(socket);
  socket.on('close', this.emit.bind(this, 'close'));
  socket.on('close', function() {
    var error = new Error('Connection closed from cassandara');
    self._active = false;
    // reset wait queue
    self._preparedMap = {};
    // fail wait queue
    self._streamWaitQueue.forEach(function(callback) {
      try {
        callback(error);
      } catch (e) {
        self.emit('error', e);
      }
    });
    self._streamWaitQueue = [];
    // fail stream handlers
    for (var id in self._streamHandlers) {
      if (self._streamHandlers.hasOwnProperty(id)) {
        var handler = self._streamHandlers[id];
        try {
          handler(error);
        } catch (e) {
          self.emit('error', e);
        }
      }
    }
    self._streamHandlers = {};
    self._streamIds = [];
    for (var i = 127; i >= 0; i--) {
      self._streamIds.push(i);
    }
    // auto reconnect
    if (!self._shutdown && self._autoReconnect) {
      self.emit('reconnecting');
      setTimeout(
        self.connect.bind(self, cb),
        self._reconnectInterval
      );
    }
  });
  socket.on('timeout', function() {
    socket.destroy();
  });
  socket.on('error', this.emit.bind(this, 'error'));
  socket.on('connect', function() {

    self.sendMessage(new cql.messages.Startup(), function(err) {

      var fail = function(err) {
        self.emit('error', err);
        self._active = false;
        if (cb) {
          cb(err);
          cb = undefined;
        }
      };

      var success = function() {
        self._active = true;
        self.emit('connect');
        clearTimeout(self._connectTimeoutId);
        if (cb) {
          cb();
          cb = undefined;
        }
      };

      if (err) {
        fail(err);
      } else {
        if (self._keyspace) {
          self.query('USE ' + self._keyspace, function(err) {
            if (err) {
              fail(err);
              // disconnect to invoke retrying
              self.close();
            } else {
              success();
            }
          });
        } else {
          success();
        }
      }
    });
  });
  protocol.on('message', this.receiveMessage.bind(this));

  this._protocol = protocol;
  this._socket = socket;

  // Connect  time out
  this._connectTimeoutId = setTimeout(function() {
    socket.end();
    cb(new Error('Connect timeout'));
  }, this._connectTimeout);

};

/**
 * Get available stream Id
 * @param {getAvailableStreamIdCallback} cb
 */
Connection.prototype.getAvailableStreamId = function(callback) {
  if (this._streamIds.length > 0) {
    return callback(null, this._streamIds.pop());
  }
  // wait until stream Id will be available
  if (this._streamWaitQueue.length < this._maxWaitPerConns) {
    this._streamWaitQueue.push(callback);
  } else {
    // queue is full
    callback(new Error('Too many requests. Increase connection size to execute more concurrent queries.'));
  }
};

/**
 * Release stream ID
 * @param {number} streamId - StreamID
 */
Connection.prototype.releaseStreamId = function(streamId) {
  delete this._streamHandlers[streamId];
  if (this._streamWaitQueue.length > 0) {
    // execute next stream queue
    this._streamWaitQueue.shift()(null, streamId);
  } else {
    // push back to available slots
    this._streamIds.push(streamId);
  }
};

/**
 * Send message
 * @param {object} message - CQL protocol message
 * @param {function} callback
 */
Connection.prototype.sendMessage = function(message, callback) {
  var self = this;
  this.getAvailableStreamId(function(err, streamId) {
    if (err) {
      return callback(err);
    }
    try {
      self._streamHandlers[streamId] = callback;
      message.streamId = streamId;
      self._protocol.sendMessage(message);
    } catch (e) {
      self.releaseStreamId(streamId);
      throw e;
    }
  });
};

/**
 * Handle CQL protocol message
 * @param {object} message - message chunk
 */
Connection.prototype.receiveMessage = function(message) {

  var opcode = message.opcode;

  if (opcode === opcodes.event) {
    this.emit('event', message.event);
    return;
  }

  var streamId = message.streamId;
  if (streamId >= 0 && streamId < 128) {
    try {
      var streamHandler = this._streamHandlers[streamId];
      if (streamHandler) {
        if (opcode === opcodes.error) {
          var error = new Error(message.errorMessage);
          error.code = message.errorCode;
          error.details = message.details;
          streamHandler.call(this, error);
        } else {
          streamHandler.call(this, null, message);
        }
      }
    } catch (e) {
      throw e;
    } finally {
      this.releaseStreamId(streamId);
    }
  }

};

/**
 * Execute query
 */
Connection.prototype.query = function(query, values, options, callback) {

  if (arguments.length === 2) {
    callback = values;
    values = null;
    options = {};
  } else if (arguments.length === 3) {
    callback = options;
    if (Array.isArray(values)) {
      options = {};
    } else {
      options = values;
      values = null;
    }
  }

  var msg = new cql.messages.Query();
  msg.query = query;
  if (values) {
    msg.values = values;
  }
  var opts = msg.options;

  opts.consistencyLevel = options.consistencyLevel || options.cl || this._consistencyLevel;

  // page_size
  if (options.pageSize) {
    opts.pageSize = options.pageSize;
  }

  // paging_state
  if (options.pagingState) {
    opts.pagingState = options.pagingState;
  }

  if (options.serialConsistency) {
    opts.serialConsistency = options.serialConsistency;
  }

  this.sendMessage(msg, function(err, reply) {
    if (err) {
      callback(err);
    } else {
      if (reply.resultSet) {
        callback(null, reply.resultSet);
      } else if (reply.schema) {
        callback(null, reply.schema);
      } else {
        callback(null);
      }
    }
  });
};

/**
 * Prepare the query
 * @param {string} query - query
 * @param {function} callback
 */
Connection.prototype.prepare = function(query, callback) {

  var preparedMap = this._preparedMap;

  if (query in preparedMap) {
    // return stored info
    return callback(null, preparedMap[query]);
  }

  var msg = new cql.messages.Prepare();
  msg.query = query;

  this.sendMessage(msg, function(err, reply) {
    if (err) {
      return callback(err);
    }
    var prepared = reply.prepared;
    // store prepare info
    preparedMap[query] = prepared;
    callback(null, prepared);
  });

};

/**
 * Unprepare the query to expire
 * @param {string} query - query
 */
Connection.prototype.unprepare = function(query) {
  delete this._preparedMap[query];
};

/**
 * Execute prepared query
 * @param {object} queryId - Query ID
 * @param {array=} values - values to be bound
 * @param {object=} options - options
 * @param {function} callback
 */
Connection.prototype.execute = function(queryId, values, options, callback) {

  if (arguments.length === 2) {
    callback = values;
    values = null;
    options = {};
  } else if (arguments.length === 3) {
    callback = options;
    if (Array.isArray(values)) {
      options = {};
    } else {
      options = values;
      values = null;
    }
  }

  var msg = new cql.messages.Execute(queryId);

  var opts = msg.options;

  if (values) {
    opts.setValues(values);
  }

  opts.consistencyLevel = options.consistencyLevel || options.cl || this._consistencyLevel;

  // page_size
  if (options.pageSize) {
    opts.pageSize = options.pageSize;
  }

  // paging_state
  if (options.pagingState) {
    opts.pagingState = options.pagingState;
  }

  // serial consistency
  if (options.serialConsistency) {
    opts.serialConsistency = options.serialConsistency;
  }

  this.sendMessage(msg, function(err, reply) {
    if (err) {
      callback(err);
    } else {
      if (reply.resultSet) {
        callback(null, reply.resultSet);
      } else if (reply.schema) {
        callback(null, reply.schema);
      } else {
        callback(null);
      }
    }
  });
};

/**
 * Register event type
 * @param {array} event - list of event <TOPOLOGY_CHANGE, STATUS_CHANGE, SCHEMA_CHANGE>
 * @param {function} callback
 */
Connection.prototype.register = function(events, callback) {
  var msg = new cql.messages.Register();
  if (typeof events === 'string') {
    events = [events];
  }
  // save to reuse in reconnection
  this._events = events;
  msg.events = events;
  this.sendMessage(msg, callback);
};

/**
 * Execute batch statement
 * @param {array} list - array of query and value pair. list items should be [query, values] or { query: query, values: values }
 * @param {options=} object - option
 */
Connection.prototype.batch = function(list, options, callback) {

  if (typeof options === 'function') {
    callback = options;
    options = {};
  } else {
    options = options || {};
  }

  var msg = new cql.messages.Batch(options.type || 0);

  msg.consistencyLevel = options.consistencyLevel || options.cl || this._consistencyLevel;

  for (var i = 0; i < list.length; i++) {
    var item = list[i];
    if (Array.isArray(item)) {
      msg.add(item[0], item[1]);
    } else if (typeof item === 'object') {
      msg.add(item.query, item.values);
    } else {
      msg.add(item);
    }
  }

  this.sendMessage(msg, callback);

};

/**
 * Close the connection
 * @param {errorCallback} cb
 */
Connection.prototype.close = function(cb) {
  this._shtudown = true;
  if (this.socket) {
    if (cb) {
      this.socket.once('close', function(hasError) {
        if (hasError) {
          cb(new Error('Connection termination failed'));
        } else {
          cb();
        }
      });
    }
    this.protocol.shutdown();
  } else {
    if (cb) {
      cb();
    }
  }
};

module.exports = Connection;
