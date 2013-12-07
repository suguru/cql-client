/**
 * Module related for result returned from Cassandra.
 *
 * @module cql-protocol/resultSet
 */
var uuid = require('node-uuid'),
    types = require('./types');

/**
 * A row.
 *
 * @constructor
 */
function Row(columnSpecs, row) {
  var _byIndex = [],
      i, col, val,
      type;

  Object.defineProperty(this, 'byIndex', {
    get: function() { return _byIndex.slice(); }
  });

  for (i = 0; i < columnSpecs.length; i++) {
    col = columnSpecs[i];
    type = types.fromType(col.type);
    if (col.subtype) {
      type = type(col.subtype);
    } else if (col.keytype) {
      type = type(col.keytype, col.valuetype);
    }
    val = type.deserialize(row[i]);

    _byIndex[i] = val;
    Object.defineProperty(this, col.name, {
      value: val,
      enumerable: true
    });
  }
}

/**
 * ResultSet
 *
 * @constructor
 * @param {Metadata} metadaata - Metadata
 * @property {Metadata} metadata - Metadata that describes the contents of this ResultSet
 */
function ResultSet(metadata, rows) {
  var _rows = [],
      i;

  for (i = 0; i < rows.length; i++) {
    _rows.push(new Row(metadata.columnSpecs, rows[i]));
  }

  Object.defineProperties(this, {
    'metadata': {
      value: metadata,
      enumerable: true
    },
    'rows': {
      get: function() { return _rows.slice(); },
      enumerable: true
    }
  });
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
}

/**
 * ResultSet Metadata
 *
 * @constructor
 * @property {number} flags - 
 * @property {number} columnCount - number of columns
 */
function Metadata() {
  var _flags = 0,
      _names = [],
      _columnCount = 0,
      _pagingState;

  Object.defineProperties(this, {
    'flags': {
      get: function() { return _flags; },
    },
    'columnCount': {
      set: function(value) { _columnCount = value; },
      get: function() { return _columnCount; }
    },
    'pagingState': {
      set: function(value) {
             if (value) {
               _flags |= (1 << 0x0002);
               _pagingState = value;
             }
           },
      get: function() { return _pagingState; }
    },
    'columnSpecs': {
      get: function() { return _names.slice(); }
    }
  });

  this.skipMetadata = function() {
    _flags |= 0x0004;
  };

  this.addColumnSpec = function(columnSpec) {
    _names.push(columnSpec);
  };
}
Metadata.prototype = Object.create(
  {},
  {constructor: {value: Metadata}}
);

// TODO
Metadata.prototype.encode = function(out, protocolVersion) {
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
    } else if (columnSpec.type == 0x0021) {
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

