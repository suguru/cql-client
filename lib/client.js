
var async = require('async');
var Connection = require('./connection');
var ResultSet = require('./resultset');

var protocol = require('./protocol/protocol');
var types = require('./protocol/types');
var events = require('events');
var util = require('util');
var errorCodes = require('./errorcodes');

/**
 * Create client
 * @constructor
 * @param {object} option - optional parameters
 * @param {array} option.hosts - seed hosts to connect to get peers. ex) [127.0.0.1:9402,127.0.0.2:9042,.. ]
 * @param {string=} option.consistencyLevel - default consistency level (one,two,three,quorum,local_quorum,each_quorum,all,any). default is quorum.
 * @param {number=} option.connectTimeout - milli-seconds until timeout
 * @param {number=} option.connsPerHost - number of connections to be used per hosts. default is 2
 * @param {number=} option.reconnectInterval - interval milliseconds for waiting next attemp to reconnect
 * @param {number=} option.protocolVersion - native protocol version. default is 2
 * @param {number=} option.maxWaitPerConns - maximum number of request to be accepted per connection
 * @param {boolean} optino.autoDetect - detect peers automatically
 *
 */
function Client(option) {

  option = option || {};

  // seed hosts
  var hosts = option.hosts || option.host;
  if (!hosts || hosts.length === 0) {
    throw new Error('`hosts` required to create client');
  }

  if (typeof hosts === 'string') {
    hosts = [hosts];
  }

  this._seeds = hosts;
  this._conns = [];
  this._connsPerHost = option.connsPerHost || 2;
  this._connectRoundRobin = 0;
  this._connectTimeout = option.connectTimeout || 5000;
  this._connectWaitQueue = [];
  this._reconnectInterval = option.reconnectInterval || 5000;
  this._autoDetect = !!option.autoDetect;
  this._keyspace = option.keyspace;

  this._protocolVersion = option.protocolVersion || 2;
  // set default consistency level
  var cl = (option.cl || option.consistencyLevel || 'quorum').toUpperCase();
  if (cl in protocol.CL) {
    this._consistencyLevel = protocol.CL[cl];
  } else {
    throw new Error('Invalid consistency level ' + cl);
  }
  this._maxWaitPerConns = option.maxWaitPerConns || 100;

  // auto connect to seed
  setImmediate(this.connect.bind(this));
}

util.inherits(Client, events.EventEmitter);

/**
 * Connect to seed host
 */
Client.prototype.connect = function(cb) {

  var seedHosts = this._seeds.slice();
  var seedConn = null;
  var self = this;

  if (this._autoDetect) {
    // trying to connect to seed host
    async.whilst(function() {
      return seedConn === null;
    }, function(callback) {
      if (seedHosts.length === 0) {
        return callback(new Error('Unavailable seed hosts'));
      }
      var host = seedHosts.shift();
      self.emit('log', 'info', 'Connecting to seed host ' + host);

      var conn = self.createConnection(host, false);
      conn.connect(function(err) {
        if (err) {
          self.emit('log', 'error', 'Failed to connect to ' + host);
          callback();
        } else {
          seedConn = conn;
          // mark auto reconnect
          conn.setAutoReconnect(true);
          callback();
        }
      });
    }, function(err) {
      if (err) {
        if (cb) {
          cb(err);
        } else {
          self.emit('error', err);
        }
      } else {
        // try to connect peers
        self.connectPeers(seedConn, cb);
      }
    });
  } else {
    // trying to connect all hosts
    this._conns = [];
    this._seeds.forEach(function(host) {
      var conn = self.createConnection(host, true);
      conn.once('connect', function() {
        if (cb) {
          cb();
          cb = null;
        }
        if (!self._ready) {
          self._ready = true;
          self.emit('connect');
          self._connectWaitQueue.forEach(function(callback) {
            try {
              self.getAvailableConnection(callback);
            } catch (e) {
              self.emit('error', e);
            }
          });
          self._connectWaitQueue = [];
        }
      });
      self._conns.push(conn);
      conn.connect();
    });
  }
};

/**
 * Get peers and create connections to peers
 * @param {object} seedConn - seed connection
 * @param {function} cb - connect callbacks
 */
Client.prototype.connectPeers = function(seedConn, cb) {
  var self = this;
  this.emit('log', 'debug', 'Get peers to connect');
  seedConn.query('SELECT peer FROM system.peers', function(err, rs) {
    if (err) {
      self.emit('error', err);
      if (cb) {
        cb(err);
        cb = null;
      }
      return;
    }
    var connsPerHost = self._connsPerHost;
    var connections = [seedConn];
    var rows = rs.rows, i;
    for (i = 0; i < connsPerHost - 1; i++) {
      rows.push({ peer: seedConn._host });
    }
    for (i = 0; i < rows.length; i++) {
      var row = rows[i];
      for (var j = 0; j < connsPerHost; j++) {
        var conn = self.createConnection(row.peer, true);
        connections.push(conn);
        conn.connect();
      }
    }
    self._conns = connections;

    setImmediate(function() {
    });
  });
};

Client.prototype._connectReady = function() {
  this._ready = true;
  this.emit('connect');
  self._connectWaitQueue.forEach(function(callback) {
    try {
      self.getAvailableConnection(callback);
    } catch (e) {
      self.emit('error', e);
    }
  });
  self._connectWaitQueue = [];
};

/**
 * Close all connections
 */
Client.prototype.close = function() {
  // close active connecitons
  this._conns.forEach(function(conn) {
    if (conn._active && !conn._shutdown) {
      conn.close();
    }
  });
};

/**
 * Create connection with default parameters
 * @param {string} host - host name or address
 */
Client.prototype.createConnection = function(host, autoReconnect) {
  var conn = new Connection({
    host: host,
    consistencyLevel: this._consistencyLevel,
    maxWaitPerConns: this._maxWaitPerConns,
    keyspace: this._keyspace,
    autoReconnect: autoReconnect,
    connectTimeout: this._connectTimeout
  });
  // bind events
  conn.on('close', this.emit.bind(this, 'log', 'info', 'cassandra connection closed from ' + host));
  conn.on('connect', this.emit.bind(this, 'log', 'debug', 'connected to cassandra ' + host));
  conn.on('reconnecting', this.emit.bind(this, 'log', 'debug', 'reconnecting to host ' + host));
  conn.on('error', this.emit.bind(this, 'error'));
  return conn;
};

/**
 * Get available connection to execute queries.
 */
Client.prototype.getAvailableConnection = function(callback) {
  if (!this._ready) {
    this._connectWaitQueue.push(callback);
    return;
  }
  var left = this._conns.length;
  while (left > 0) {
    var next = this._connectRoundRobin++;
    if (next >= this._conns.length) {
      next = 0;
      this._connectRoundRobin = 0;
    }
    var conn = this._conns[next];
    if (conn.available()) {
      callback(null, conn);
      return;
    }
  }
  callback(new Error('No active connections'));
};

/**
 * Execute query. Client will use prepared query if values specified.
 * @param {string} query - query
 * @param {array=} values - bound values
 * @param {object=} options - optional parameters
 * @param {function} callback - call back query result
 */
Client.prototype.execute = function(query, values, options, callback) {

  // adjust arguments
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

  var self = this;

  this.getAvailableConnection(function(err, conn) {
    if (err) {
      return callback(err);
    }
    if (values) {
      conn.prepare(query, function(err, prepared) {

        if (err && err._closed) {
          // retry query with closed error
          return setImmediate(self.execute.bind(self, query, values, options, callback));
        } else if (err) {
          callback(err);
          return;
        }

        var cassValues = toCassandraValues(prepared.metadata, values);

        conn.execute(prepared.id, cassValues, options, function(err, result) {
          if (err && err.code === errorCodes.unprepared) {
            // if query expired, unprepare and retry
            conn.unprepare(query);
            // retry execution
            setImmediate(self.execute.bind(self, query, values, options, callback));
          } else if (err && err._closed) {
            // retry query with closed error
            setImmediate(self.execute.bind(self, query, values, options, callback));
          } else if (err) {
            callback(err);
          } else if (result.resultSet) {
            callback(null, new ResultSet(
              conn,
              prepared.id,
              cassValues,
              options,
              result.resultSet));
          } else if (result.schema) {
            callback(null, result.schema);
          } else {
            callback(null, null);
          }
        });
      });
    } else {
      conn.query(query, options, function(err, result) {
        if (err && err._closed) {
          setImmediate(self.execute.bind(self, query, null, options, callback));
        } else if (err) {
          callback(err);
        } else if (result.resultSet) {
          callback(null, new ResultSet(conn, query, null, options, result.resultSet));
        } else if (result.schema) {
          callback(null, result.schema);
        } else {
          callback(null, null);
        }
      });
    }
  });
};

/**
 * Create batch instance.
 */
Client.prototype.batch = function() {
  var self = this;
  var list = [];
  var opts = null;
  return {
    add: function(query, values) {
      list.push({ query: query, values: values });
      return this;
    },
    option: function(options) {
      opts = options;
      return this;
    },
    commit: function(callback) {
      self.getAvailableConnection(function(err, conn) {
        if (err) {
          return callback(err);
        }
        async.map(list, function(item, done) {
          conn.prepare(item.query, done);
        }, function(err, prepareds) {

          if (err && err._closed) {
            // retry query with closed error
            return setImmediate(self.commit.bind(self, callback));
          } else if (err) {
            return callback(err);
          }

          var commits = [];
          for (var i = 0; i < prepareds.length; i++) {
            var prepared = prepareds[i];
            var source = list[i];
            var values = toCassandraValues(prepared.metadata, source.values);
            commits.push({ query: prepared.id, values: values });
          }
          conn.batch(commits, opts, function(err) {
            if (err && err._closed) {
              // retry failed connection
              setImmediate(self.commit.bind(self, callback));
            } else if (err && err.code === errorCodes.unprepared) {
              // unprepare all queries
              list.forEach(function(item) {
                conn.unprepare(item.query);
              });
              // retry unprepared query
              setImmediate(self.commit.bind(self, callback));
            } else if (err) {
              callback(err);
            } else {
              callback();
            }
          });
        });
      });
    }
  };
};

/**
 * Convert values to binaries to be set for cassandra
 * @param {object} metadata - metadata
 * @param {array} values - value list
 */
function toCassandraValues(metadata, values) {
  if (values === null || values === undefined) {
    return null;
  }
  var specs = metadata.columnSpecs;
  var list = [];
  for (var i = 0; i < specs.length; i++) {
    var value = values[i];
    if (value !== null) {
      var spec = specs[i];
      var type = types.fromType(spec.type);
      list.push(type.serialize(value));
    } else {
      list.push(null);
    }
  }
  return list;
}

module.exports = Client;
