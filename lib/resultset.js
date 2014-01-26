var events = require('events');
var util = require('util');

/**
 * Result set wrapper
 *
 */
function ResultSet(conn, query, values, options, rs) {
  this.query = query;
  this.metadata = rs.metadata;
  this.rows = rs.rows;
  this._conn = conn;
  this._values = values;
  this._options = options;
}

ResultSet.prototype.hasNext = function() {
  return !!this.metadata.pagingState;
};

/**
 * Get next result set
 */
ResultSet.prototype.next = function(callback) {
  var opts = {
    pagingState: this.metadata.pagingState,
    pageSize: this._options.pageSize,
    consistencyLevel: this._options.consistencyLevel,
    serialConsistency: this._options.serialConsistency
  };
  var self = this;
  if (typeof this.query === 'string') {
    this._conn.query(this.query, opts, function(err, result) {
      if (err) {
        callback(err);
      } else {
        callback(null, new ResultSet(self._conn, self.query, null, opts, result.resultSet));
      }
    });
  } else {
    this._conn.execute(this.query, this._values, opts, function(err, result) {
      if (err) {
        callback(err);
      } else {
        callback(null, new ResultSet(self._conn, self.query, self._values, opts, result.resultSet));
      }
    });
  }
};

/**
 * Create result set cursor
 */
ResultSet.prototype.cursor = function() {
  return new Cursor(this);
};

function Cursor(rs) {
  this.rs = rs;
  setImmediate(this.start.bind(this));
}

util.inherits(Cursor, events.EventEmitter);

Cursor.prototype.start = function() {
  var self = this;
  var rs = this.rs;
  var rows = rs.rows;
  // each fetch event
  self.emit('fetch', rs);
  for (var i = 0; i < rows.length; i++) {
    if (self._aborted) {
      self.emit('end');
      return;
    }
    var row = rows[i];
    // each row event
    self.emit('row', row);
  }
  if (rs.hasNext()) {
    // get next result
    rs.next(function(err, next) {
      if (err) {
        // work-around for cassandra bug
        // which throw IllegalArgumentException
        // when next result set is empty 
        if (/Illegal\sCapacity/.test(err.message)) {
          self.emit('end');
        } else {
          self.emit('error', err);
        }
      } else {
        // swap result set to new one
        self.rs = next;
        setImmediate(self.start.bind(self));
      }
    });
  } else {
    // emit end
    self.emit('end');
  }
};

Cursor.prototype.abort = function() {
  this._aborted = true;
};

module.exports = ResultSet;

