/**
 * Module related for result returned from Cassandra.
 *
 * @module cql-protocol/resultSet
 */
var types = require('./types');

/**
 * A row.
 *
 * @constructor
 */
function Row(columnSpecs, row) {
  var i, col, type, val, cols = [];
  for (i = 0; i < columnSpecs.length; i++) {
    col = columnSpecs[i];
    type = types.fromType(col.type);
    if (col.subtype) {
      type = type(col.subtype);
    } else if (col.keytype) {
      type = type(col.keytype, col.valuetype);
    }
    val = type.deserialize(row[i]);
    this[col.name] = val;
    cols.push(val);
  }
  Object.defineProperty(this, 'columns', {
    value: cols,
    enumerable: false
  });
}

/**
 * ResultSet
 *
 * @constructor
 * @param {Metadata} metadaata - Metadata
 * @property {Metadata} metadata - Metadata that describes the contents of this ResultSet
 */
function ResultSet(metadata, rows) {
  this.rows = [];
  this.metadata = metadata;
  for (var i = 0; i < rows.length; i++) {
    this.rows.push(new Row(metadata.columnSpecs, rows[i]));
  }
}
ResultSet.prototype = Object.create(
  {},
  {constructor: {value: ResultSet}}
);
ResultSet.decode = function(input) {
  var rows = [],
      cols,
      i, j;

  var metadata = Metadata.decode(input);
  var rowsCount = input.readInt();

  for (i = 0; i < rowsCount; i++) {
    cols = [];
    for (j = 0; j < metadata.columnSpecs.length; j++) {
      cols.push(input.readBytes());
    }
    rows.push(cols);
  }
  return new ResultSet(metadata, rows);
};

/**
 * ResultSet Metadata
 *
 * @constructor
 * @property {number} flags - 
 * @property {number} columnCount - number of columns
 */
function Metadata() {
  this.flags = 0;
  this.names = [];
  this.columnCount = 0;
}

Metadata.prototype = Object.create(
  {},
  {constructor: {value: Metadata}}
);

Object.defineProperties(Metadata.prototype, {
  'pagingState': {
    set: function(value) {
      if (value) {
        this.flags |= (1 << 0x0002);
        this._pagingState = value;
      }
    },
    get: function() { return this._pagingState; }
  },
  'columnSpecs': {
    get: function() { return this.names.slice(); }
  }
});

Metadata.prototype.skipMetadata = function() {
  this.flags |= 0x0004;
};

Metadata.prototype.addColumnSpec = function(columnSpec) {
  this.names.push(columnSpec);
};

// TODO
Metadata.prototype.encode = function(out) {
  var flags = this.flags;
  out.writeInt(flags);
  out.writeInt(this.columnCount);

  // has more pages (since v2)
  if (flags & 0x0002) {
    out.writeBytes(this.pagingState);
  }
  // No metadata (since v2)
  if (flags & 0x0004) {
    return;
  }
};
Metadata.decode = function(input) {
  var metadata = new Metadata(),
      i, keyspace, table,
      hasGlobalTableSpec,
      columnSpec;

  var flags = input.readInt();
  metadata.columnCount = input.readInt();

  // has more pages (since v2)
  if (flags & 0x0002) {
    metadata.pagingState = input.readBytes();
  }
  // No metadata (since v2)
  if (flags & 0x0004) {
    return metadata;
  }
  // is global_table_spec set?
  hasGlobalTableSpec = flags & 0x0001;
  if (hasGlobalTableSpec) {
    keyspace = input.readString();
    table = input.readString();
  }

  for (i = 0; i < metadata.columnCount; i++) {
    if (!hasGlobalTableSpec) {
      keyspace = input.readString();
      table = input.readString();
    }
    columnSpec = {
      keyspace: keyspace,
      table: table,
      name: input.readString(),
      type: input.readShort()
    };
    if (columnSpec.type === 0x0000) {
      // custom type
      columnSpec.customType = input.readString();
    } else if (columnSpec.type === 0x0020 || columnSpec.type === 0x0022) {
      // list, set
      columnSpec.subtype = input.readShort();
    } else if (columnSpec.type === 0x0021) {
      // map
      columnSpec.keytype = input.readShort();
      columnSpec.valuetype = input.readShort();
    }
    metadata.addColumnSpec(columnSpec);
  }
  return metadata;
};

module.exports.ResultSet = ResultSet;
module.exports.Metadata = Metadata;

