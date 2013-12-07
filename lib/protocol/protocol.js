/**
 * @module cql-protocol/protocol
 */
var stream = require('stream'),
    Buf = require('./buffer').Buf,
    resultSet = require('./resultset');

var DEFAULT_PROTOCOL_VERSION = 2;
var DEFAULT_CQL_VERSION = '3.1.0';

var CONSISTENCY_LEVEL = Object.freeze({
  ANY: 0x0000,
  ONE: 0x0001,
  TWO: 0x0002,
  THREE: 0x0003,
  QUORUM: 0x0004,
  ALL: 0x0005,
  LOCAL_QUORUM: 0x0006,
  EACH_QUORUM: 0x0007
});

/**
 * NativeProtocol class.
 *
 * Protocol is implemented as node.js' stream.Duplex, so piping with
 * socket like:
 *
 * <code>
 *   var conn = net.connect(..);
 *   conn.pipe(protocol).pipe(conn);
 * </code>
 *
 * to hook up to network connection.
 *
 * @constructor
 * @param {object} options - options to configure protocol
 */
function NativeProtocol(protocolVersion) {
  var _protocolVersion = protocolVersion || DEFAULT_PROTOCOL_VERSION;

  if (!(this instanceof NativeProtocol))
    return new NativeProtocol(options);

  stream.Duplex.call(this);

  Object.defineProperties(this, {
    'protocolVersion': {
      set: function(val) {
             if (typeof val !== 'number' || val < 1 || val > DEFAULT_PROTOCOL_VERSION) {
               throw new Error('Invalid protocol version is set: ' + val);
             }
             _protocolVersion = val;
           },
      get: function() { return _protocolVersion; }
    }
  });
  this._messageQueue = [];

  this._readBuf = new Buffer(8); // for header
  this.lastRead = 0;
}
NativeProtocol.prototype = Object.create(
  stream.Duplex.prototype,
  {constructor: {value: NativeProtocol}}
);

/**
 * Queue message to be sent.
 *
 * @param {Message} message - Message to send
 */
NativeProtocol.prototype.sendMessage = function(message) {
  // TODO verify message object
  this._messageQueue.push(message);
  this.emit('readable');
};

/**
 * Gracefully shutdown protocol.
 *
 * All messages queued are expected to be sent before shutdown.
 */
NativeProtocol.prototype.shutdown = function() {
  this.sendMessage(null);
};

/**
 * Sending messages is implemented as readable stream.
 *
 * @private
 */
NativeProtocol.prototype._read = function() {
  var message = this._messageQueue.shift();
  if (typeof message === 'undefined') {
    // empty message. wait for next message
    this.push('');
  } else if (message === null) {
    // shutdown
    this.push(null);
  } else {
    this.push(Frame.wrap(message, this.protocolVersion).encode());
  }
};

/**
 * Receiving messages is implemented as writable stream.
 *
 * @private
 */
NativeProtocol.prototype._write = function(chunk, encoding, done) {
  var toRead;

  while (chunk.length > 0) {
    // header: _readBuf should be set up to have 8 bytes
    //   body: _readBuf should have size of body length in header
    toRead = Math.min(this._readBuf.length - this.lastRead, chunk.length);
    // copy to current read buffer from received chunks
    chunk.copy(this._readBuf, this.lastRead, 0, toRead);
    this.lastRead += toRead;
    if (this.lastRead === this._readBuf.length) {
      if (this.currentFrame) {
        if (this.currentFrame.body.length === this.lastRead) {
          this.emit('message', this.currentFrame.decode());
          // reset to read next header
          this.currentFrame = void 0;
          this._readBuf = new Buffer(8); // to read next frame header
          this.lastRead = 0;
        }
      } else {
        this.currentFrame = Frame.create(this._readBuf);
        if (this.currentFrame.body.length === 0) {
          // this is empty body message
          this.emit('message', this.currentFrame.decode());
          // reset to read next header
          this.currentFrame = void 0;
        } else {
          this._readBuf = this.currentFrame.body;
        }
        this.lastRead = 0;
      }
    }
    chunk = chunk.slice(toRead, chunk.length);
  }
  done();
};

/** @namespace */
var messages = Object.freeze({
  Error: ErrorMessage,
  Startup: StartupMessage,
  Ready: ReadyMessage,
  Authenticate: AuthenticateMessage,
  Credentials: CredentialsMessage,
  Options: OptionsMessage,
  Supported: SupportedMessage,
  Query: QueryMessage,
  Result: ResultMessage,
  Prepare: PrepareMessage,
  Execute: ExecuteMessage,
  Register: RegisterMessage,
  Event: EventMessage,
  Batch: BatchMessage,
  AuthChallenge: AuthChallengeMessage,
  AuthResponse: AuthResponseMessage,
  AuthSuccess: AuthSuccessMessage
});
function messageFromOpcode(opcode) {
  return messages[Object.keys(messages)[opcode]];
}

/**
 * Frame
 *
 * @constructor
 */
function Frame(values) {
  var _values = values || {},
      _rawVersion = _values.rawVersion,
      _isRequest = (_rawVersion & 0x80) === 0,
      _version = _rawVersion & 0x7F,
      _flags = _values.flags || 0,
      _streamId = _values.streamId || 0,
      _opcode = _values.opcode,
      _body;

  if (_values.body instanceof Buffer) {
    _body = _values.body;
  } else if (typeof _values.bodyLength === 'number') {
    _body = new Buffer(_values.bodyLength);
  }
  Object.defineProperties(this, {
    'isRequest': {
      get: function() { return _isRequest; },
      enumerable: true
    },
    'version': {
      set: function(val) { _version = val; },
      get: function() { return _version; },
      enumerable: true
    },
    'flags': {
      get: function() { return _flags; },
      enumerable: true
    },
    'isTracing': {
      get: function() { return _flags & 0x02; },
      enumerable: true
    },
    'streamId': {
      get: function() { return _streamId; },
      enumerable: true
    },
    'opcode': {
      get: function() { return _opcode; },
      enumerable: true
    },
    'body': {
      get: function() { return _body; },
      enumerable: true
    }
  });
}
Frame.prototype.encode = function() {
  var bodylen = this.body.length,
      buf = new Buffer(8 + bodylen);
  // header
  buf.writeUInt8(this.version, 0);
  buf.writeUInt8(this.flags, 1);
  buf.writeInt8(this.streamId, 2);
  buf.writeUInt8(this.opcode, 3);
  // body length
  buf.writeUInt32BE(bodylen, 4);
  // body
  this.body.copy(buf, 8);
  return buf;
};
Frame.prototype.decode = function() {
  var input = new Buf(this.body),
      message, traceId;
  // TODO handle compression
  if (this.flags & 0x01) {
    console.warn("compression not supported yet.");
  }
  // handle tracing for response message
  if (this.isTracing && !this.isRequest) {
    traceId = input.readUUID();
  }
  message = messageFromOpcode(this.opcode).decode(input, this.version);
  message.flags = this.flags;
  message.streamId = this.streamId;

  if (!message.isReqeust) {
    message.traceId = traceId;
  }
  return message;
};
Frame.wrap = function(message, protocolVersion) {
  return new Frame({
    rawVersion: message.isRequest ? protocolVersion : (protocolVersion | 0x80),
    flags: message.flags,
    streamId: message.streamId,
    opcode: message.opcode,
    body: message.encode(protocolVersion)
  });
};
Frame.create = function(buffer) {
  return new Frame({
    rawVersion: buffer.readUInt8(0),
    flags: buffer.readUInt8(1),
    streamId: buffer.readInt8(2),
    opcode: buffer.readUInt8(3),
    bodyLength: buffer.readUInt32BE(4)
  });
};

/**
 * Abstract base class of all messages
 *
 * @abstract
 * @constructor
 */
function Message(isRequest, opcode) {
  var _flags = 0,
      _streamId = 0;

  Object.defineProperties(this, {
    'isRequest': {
      value: isRequest,
      enumerable: true
    },
    'flags': {
      set: function(val) { _flags = val; },
      get: function() { return _flags; },
      enumerable: true
    },
    'isTracing': {
      get: function() { return _flags & 0x02; },
      enumerable: true
    },
    'streamId': {
      set: function(val) {
             if (typeof val !== 'number' || Math.abs(val) > 128) {
               throw new Error('streamId is not a number or is out of range: ' + val);
             }
             _streamId = val;
           },
      get: function() { return _streamId; },
      enumerable: true
    },
    'opcode': {
      value: opcode,
      enumerable: true
    }
  });

  /**
   * Enable tracing on this message.
   */
  this.enableTracing = function() {
    _flags |= 0x02;
  };
}
Message.prototype = Object.create(
  {},
  {constructor: {value: Message}}
);
Message.prototype.encode = function(protocolVersion) {
  throw new Error('not implemented');
};

function Request(opcode) {
  Message.call(this, true, opcode);
}
Request.prototype = Object.create(
  Message.prototype,
  {constructor: {value: Request}}
);

function Response(opcode) {
  Message.call(this, false, opcode);
}
Response.prototype = Object.create(
  Message.prototype,
  {constructor: {value: Response}}
);

/**
 * ERROR message.
 *
 * @constructor
 */
function ErrorMessage(errorCode, errorMessage, details) {
  Response.call(this, 0x00);

  Object.defineProperties(this, {
    'errorCode': {
      value: errorCode || 0,
      enumerable: true
    },
    'errorMessage': {
      value: errorMessage || '',
      enumerable: true
    },
    'details': {
      value: details || {},
      enumerable: true
    }
  });
}
ErrorMessage.prototype = Object.create(
  Response.prototype,
  {constructor: {value: ErrorMessage}}
);
ErrorMessage.prototype.encode = function() {
  var body = new Buf();
  body.writeInt(this.errorCode);
  body.writeString(this.errorMessage);
  // additional info if any
  switch(this.errorCode) {
    case 0x1000: // Unavailable exception
      body.writeShort(this.details.consistencyLevel);
      body.writeInt(this.details.required);
      body.writeInt(this.details.alive);
      break;
    case 0x1100: // Write_timeout:
      body.writeShort(this.details.consistencyLevel);
      body.writeInt(this.details.received);
      body.writeInt(this.details.blockFor);
      body.writeString(this.details.writeType);
      break;
    case 0x1200: // Read_timeout
      body.writeShort(this.details.consistencyLevel);
      body.writeInt(this.details.received);
      body.writeInt(this.details.blockFor);
      body.writeByte(this.details.dataPresent ? 0 : 1);
      break;
    case 0x2400: // Already_exists
      body.writeString(this.details.keyspace);
      body.writeString(this.details.table);
      break;
    case 0x2500: // Unprepared
      body.writeShortBytes(this.details.id);
      break;
  }
  return body.toBuffer();
};
ErrorMessage.decode = function(input) {
  var errorCode = input.readInt(),
      errorMessage = input.readString(),
      details = {};
  // additional info if any
  switch(errorCode) {
    case 0x1000: // Unavailable exception
      details.consistencyLevel = input.readShort();
      details.required = input.readInt();
      details.alive = input.readInt();
      break;
    case 0x1100: // Write_timeout:
      details.consistencyLevel = input.readShort();
      details.received = input.readInt();
      details.blockFor = input.readInt();
      details.writeType = input.readString();
      break;
    case 0x1200: // Read_timeout
      details.consistencyLevel = input.readShort();
      details.received = input.readInt();
      details.blockFor = input.readInt();
      details.dataPresent = input.readByte() === 0;
      break;
    case 0x2400: // Already_exists
      details.keyspace = input.readString();
      details.table = input.readString();
      break;
    case 0x2500: // Unprepared
      details.id = input.readShortBytes();
      break;
  }
  return new ErrorMessage(errorCode, errorMessage, details);
};

/**
 * STARTUP message
 * @constructor
 */
function StartupMessage() {
  Request.call(this, 0x01);

  this.options = {
    CQL_VERSION: DEFAULT_CQL_VERSION
  };
}
StartupMessage.prototype = Object.create(
  Request.prototype,
  {constructor: {value: StartupMessage}}
);
StartupMessage.prototype.encode = function() {
  return new Buf().writeStringMap(this.options).toBuffer();
};
StartupMessage.decode = function(input) {
  var message = new StartupMessage();
  message.options = input.readStringMap();
  return message;
};

/**
 * READY message
 *
 * @constructor
 */
function ReadyMessage() {
  Response.call(this, 0x02);
}
ReadyMessage.prototype = Object.create(
  Response.prototype,
  {constructor: {value: ReadyMessage}}
);
ReadyMessage.prototype.encode = function() {
  return new Buffer(0);
};
ReadyMessage.decode = function() {
  return new ReadyMessage();
};

/**
 * AUTHENTICATE message
 *
 * @constructor
 */
function AuthenticateMessage() {
  Response.call(this, 0x03);
}
AuthenticateMessage.prototype = Object.create(
  Response.prototype,
  {constructor: {value: AuthenticateMessage}}
);
AuthenticateMessage.prototype.encode = function() {
  return new Buf().writeString(this.authenticator).toBuffer();
};
AuthenticateMessage.decode = function(input) {
  var message = new AuthenticateMessage();
  message.authenticator = input.readString();
  return message;
};

/**
 * CREDENTIALS message.
 * (v1 only)
 *
 * @constructor
 */
function CredentialsMessage() {
  Request.call(this, 0x04);
}
CredentialsMessage.prototype = Object.create(
  Response.prototype,
  {constructor: {value: CredentialsMessage}}
);
CredentialsMessage.prototype.encode = function() {
  return new Buf().writeStringMap(this.credentials).toBuffer();
};
CredentialsMessage.decode = function(input) {
  var message = new CredentialsMessage();
  message.credentials = input.readStringMap();
  return message;
};

/**
 * OPTIONS message
 *
 * @constructor
 */
function OptionsMessage() {
  Request.call(this, 0x05);
}
OptionsMessage.prototype = Object.create(
  Request.prototype,
  {constructor: {value: OptionsMessage}}
);
OptionsMessage.prototype.encode = function() {
  return new Buffer(0);
};
OptionsMessage.decode = function() {
  return new OptionsMessage();
};

/**
 * SUPPORTED message
 *
 * @constructor
 */
function SupportedMessage() {
  Response.call(this, 0x06);
}
SupportedMessage.prototype = Object.create(
  Response.prototype,
  {constructor: {value: SupportedMessage}}
);
SupportedMessage.prototype.encode = function() {
  return new Buf().writeStringMultimap(this.options).toBuffer();
};
SupportedMessage.decode = function(input) {
  var message = new SupportedMessage();
  message.options = input.readStringMultimap();
  return message;
};

/**
 * Query options used by QUERY/EXECUTE message.
 *
 * Introduced in protocol v2, to hold bind values, paging state, etc.
 *
 * @constructor
 */
function QueryOptions() {
  var _flags = 0,
      _consistencyLevel = CONSISTENCY_LEVEL.ONE,
      _values = [],
      _pageSize = 0,
      _pagingState = null,
      _serialConsistency;

  Object.defineProperties(this, {
    'flags': {
      get: function() { return _flags; }
    },
    'consistencyLevel': {
      set: function(cl) { _consistencyLevel = cl; },
      get: function() { return _consistencyLevel; }
    },
    'values': {
      get: function() { return _values.slice(); }
    },
    'pageSize': {
      set: function(pageSize) {
             _flags |= (1 << 0x02);
             _pageSize = pageSize;
           },
      get: function() { return _pageSize; }
    },
    'pagingState': {
      set: function(pagingState) {
             _flags |= (1 << 0x03);
             _pagingState = pagingState;
           },
      get: function() { return _pagingState; }
    },
    'serialConsistency': {
      set: function(cl) {
             _flags |= (1 << 0x04);
             _serialConsistency = cl;
           },
      get: function() { return _serialConsistency; }
    }
  });

  /**
   * Set binding values.
   *
   * @param {array} values Array of bytes for binding values.
   */
  this.setValues = function() {
    var i, args;
    _flags |= (1 << 0x00);
    args = Array.prototype.reduce.call(arguments, function(a, b) {
      return a.concat(b);
    }, []);
    for (i = 0; i < args.length; i++) {
      // TODO verify if it is Buffer
      _values.push(args[i] || null);
    }
  };

  this.skipMetadata = function() {
    _flags |= (1 << 0x01);
  };
}
QueryOptions.prototype = Object.create(
  {},
  {constructor: {value: QueryOptions}}
);
QueryOptions.prototype.encode = function(out, protocolVersion) {
  var i, values;
  if (protocolVersion > 1) {
    out.writeShort(this.consistencyLevel);
    // flags
    out.writeByte(this.flags);
    // values
    if ((this.flags & (1 << 0x00)) !== 0) {
      values = this.values;
      out.writeShort(values.length);
      for (i = 0; i < values.length; i++) {
        out.writeBytes(values[i]);
      }
    }
    // page size
    if ((this.flags & (1 << 0x02)) !== 0) {
      out.writeInt(this.pageSize);
    }
    // paging state
    if ((this.flags & (1 << 0x03)) !== 0) {
      out.writeBytes(this.pagingState);
    }
    // serial consistency
    if ((this.flags & (1 << 0x04)) !== 0) {
      out.writeShort(this.serialConsistency);
    }
  } else {
    // for v1
    values = this.values;
    out.writeShort(values.length);
    for (i = 0; i < values.length; i++) {
      out.writeBytes(values);
    }
    out.writeShort(this.consistencyLevel);
  }
};
QueryOptions.decode = function(input, protocolVersion) {
  var options = new QueryOptions();
  var flags, i, values = [], valueslen;
  if (protocolVersion > 1) {
    options.consistencyLevel = input.readShort();
    flags = input.readByte();
    // values
    if ((flags & (1 << 0x00)) !== 0) {
      valueslen = input.readShort();
      for (i = 0; i < valueslen; i++) {
        values[i] = input.readBytes();
      }
      options.setValues(values);
    }
    // skip metadata
    if ((flags & (1 << 0x01)) !== 0) {
      options.skipMetadata();
    }
    // page size
    if ((flags & (1 << 0x02)) !== 0) {
      options.pageSize = input.readInt();
    }
    // paging state
    if ((flags & (1 << 0x03)) !== 0) {
      options.pagingState = input.readBytes();
    }
    // serial consistency
    if ((flags & (1 << 0x04)) !== 0) {
      options.serialConsistency = input.readShort();
    }
  } else {
    options.values = [];
    valueslen = input.readShort();
    for (i = 0; i < valueslen; i++) {
      options.values[i] = input.readBytes();
    }
    options.consistencyLevel = input.readShort();
  }
  return options;
}

/**
 * QUERY message.
 *
 * @constructor
 */
function QueryMessage() {
  Request.call(this, 0x07);

  this.options = new QueryOptions();
}
QueryMessage.prototype = Object.create(
  Request.prototype,
  {constructor: {value: QueryMessage}}
);
QueryMessage.prototype.encode = function(protocolVersion) {
  var body = new Buf();
  body.writeLongString(this.query);
  this.options.encode(body, protocolVersion);
  return body.toBuffer();
};
QueryMessage.decode = function(input, protocolVersion) {
  var message = new QueryMessage();
  message.query = input.readLongString();
  message.options = QueryOptions.decode(input, protocolVersion);
  return message;
};

/**
 * RESULT message.
 *
 * @constructor
 */
function ResultMessage() {
  Response.call(this, 0x08);
}
ResultMessage.prototype = Object.create(
  Response.prototype,
  {constructor: {value: ResultMessage}}
);
ResultMessage.prototype.encode = function(protocolVersion) {
  // TODO
};
ResultMessage.decode = function(input, protocolVersion) {
  var kind = input.readInt(),
      message = new ResultMessage();
  message.kind = kind;
  switch(kind) {
    case 0x0001: // Void
      break;
    case 0x0002: //Rows
      message.resultSet = resultSet.ResultSet.decode(input);
      break;
    case 0x0003: //Set_keyspace
      message.keyspace = input.readString();
      break;
    case 0x0004: //Prepared
      message.prepared = {
        id: input.readShortBytes(),
        metadata: resultSet.Metadata.decode(input),
      };
      if (protocolVersion > 1) {
        message.prepared.resultMetadata = resultSet.Metadata.decode(input);
      }
      break;
    case 0x0005: //Schema_change
      message.schema = {
        change: input.readString(),
        keyspace: input.readString(),
        table: input.readString()
      };
      break;
  }
  return message;
};

/**
 * PREPARE message.
 *
 * @constructor
 */
function PrepareMessage() {
  Request.call(this, 0x09);
}
PrepareMessage.prototype = Object.create(
  Request.prototype,
  {constructor: {value: PrepareMessage}}
);
PrepareMessage.prototype.encode = function() {
  return new Buf().writeLongString(this.query).toBuffer();
};
PrepareMessage.decode = function(input) {
  var message = new PrepareMessage();
  message.query = input.readLongString();
  return message;
};

/**
 * EXECUTE message.
 *
 * @constructor
 */
function ExecuteMessage(queryId) {
  Request.call(this, 0x0A);

  Object.defineProperties(this, {
    'id': {
      value: queryId,
      enumerable: true
    }
  });
  this.options = new QueryOptions();
}
ExecuteMessage.prototype = Object.create(
  Request.prototype,
  {constructor: {value: ExecuteMessage}}
);
ExecuteMessage.prototype.encode = function(protocolVersion) {
  var i, body = new Buf();
  body.writeShortBytes(this.id);
  this.options.encode(body, protocolVersion);
  return body.toBuffer();
};
ExecuteMessage.decode = function(input, protocolVersion) {
  var message = new ExecuteMessage(input.readShortBytes());
  message.options = QueryOptions.decode(input, protocolVersion);
  return message;
};

/**
 * REGISTER message
 *
 * @constructor
 */
function RegisterMessage() {
  Request.call(this, 0x0B);
}
RegisterMessage.prototype = Object.create(
  Request.prototype,
  {constructor: {value: RegisterMessage}}
);
RegisterMessage.prototype.encode = function() {
  return new Buf().writeStringList(this.events).toBuffer();
};
RegisterMessage.decode = function(input) {
  var message = new RegisterMessage();
  message.events = input.readStringList();
  return message;
};

/**
 * EVENT message
 *
 * @constructor
 */
function EventMessage() {
  Response.call(this, 0x0C);
}
EventMessage.prototype = Object.create(
  Response.prototype,
  {constructor: {value: EventMessage}}
);
EventMessage.prototype.encode = function() {
  var buf = new Buf(),
      e = this['event'];
  buf.writeString(e.type);
  switch(e.type) {
    case 'TOPOLOGY_CHANGE':
    case 'STATUS_CHANGE':
      buf.writeString(e.typeOfChange);
      buf.writeInet(e.address);
      break;
    case 'SCHEMA_CHANGE':
      buf.writeString(e.typeOfChange);
      buf.writeString(e.keyspace);
      buf.writeString(e.table);
      break;
  }
  return buf.toBuffer();
};
EventMessage.decode = function(input) {
  var message = new EventMessage(),
      e = {};
  e.type = input.readString();
  switch(e.type) {
    case 'TOPOLOGY_CHANGE':
    case 'STATUS_CHANGE':
      e.typeOfChange = input.readString();
      e.address = input.readInet();
      break;
    case 'SCHEMA_CHANGE':
      e.typeOfChange = input.readString();
      e.keyspace = input.readString();
      e.table = input.readString();
      break;
  }
  message['event'] = e;
  return message;
};

/**
 * BATCH message
 *
 * @constructor
 * @since protocol version 2
 */
function BatchMessage(type) {
  var _queries = [],
      _values = [],
      _consistencyLevel = CONSISTENCY_LEVEL.ONE;
  Request.call(this, 0x0D);

  Object.defineProperties(this, {
    'type': {
      value: type
    },
    'queries': {
      get: function() { return _queries.slice(); }
    },
    'values': {
      get: function() { return _values.slice(); }
    },
    'consistencyLevel': {
      set: function(val) { _consistencyLevel = val; },
      get: function() { return _consistencyLevel; }
    }
  });

  this.add = function(query, values) {
    // query should be string or Buffer
    if (typeof query !== 'string' && !(query instanceof Buffer)) {
      throw new Error('query must be string or prepared statement id(Buffer)');
    }
    _queries.push(query);
    _values.push(values || []);
  };
}
BatchMessage.prototype = Object.create(
  Request.prototype,
  {constructor: {value: BatchMessage}}
);
BatchMessage.prototype.encode = function() {
  var buf = new Buf(),
      queries = this.queries,
      values,
      i, j,
      query;
  buf.writeByte(this.type);
  buf.writeShort(queries.length);
  for (i = 0; i < queries.length; i++) {
    query = queries[i];
    if (typeof query === 'string') {
      buf.writeByte(0);
      buf.writeLongString(query);
    } else {
      buf.writeByte(1);
      buf.writeShortBytes(query);
    }
    values = this.values[i];
    buf.writeShort(values.length);
    for (j = 0; j < values.length; j++) {
      buf.writeBytes(values[j] || new Buffer(0));
    }
  }
  buf.writeShort(this.consistencyLevel);
  return buf.toBuffer();
};
BatchMessage.decode = function(input) {
  var type = input.readByte(),
      message = new BatchMessage(type),
      kind,
      query,
      values,
      numQueries, numValues,
      i, j;
  numQueries = input.readShort();
  for (i = 0; i < numQueries; i++) {
    kind = input.readByte();
    if (kind === 0) {
      query = input.readLongString();
    } else {
      query = input.readShortBytes();
    }
    numValues = input.readShort();
    values = [];
    for (j = 0; j < numValues; j++) {
      values.push(input.readBytes());
    }
    message.add(query, values);
  }
  message.consistencyLevel = input.readShort();
  return message;
};

/**
 * AUTH_CHALLENGE message.
 *
 * @constructor
 * @since protocol version 2
 */
function AuthChallengeMessage(token) {
  var _token = token || new Buffer(0);

  Response.call(this, 0x0E);

  Object.defineProperty(this, 'token', {
    set: function(val) { _token = val; },
    get: function() { return _token; }
  });
}
AuthChallengeMessage.prototype = Object.create(
  Response.prototype,
  {constructor: {value: AuthChallengeMessage}}
);
AuthChallengeMessage.prototype.encode = function() {
  return new Buf().writeBytes(this.token).toBuffer();
};
AuthChallengeMessage.decode = function(input) {
  var message = new AuthChallengeMessage();
  message.token = input.readBytes();
  return message;
};

/**
 * AUTH_RESPONSE message.
 *
 * @constructor
 * @since protocol version 2
 */
function AuthResponseMessage(token) {
  var _token = token || new Buffer(0);

  Request.call(this, 0x0F);

  Object.defineProperty(this, 'token', {
    set: function(val) { _token = val; },
    get: function() { return _token; }
  });
}
AuthResponseMessage.prototype = Object.create(
  Request.prototype,
  {constructor: {value: AuthResponseMessage}}
);
AuthResponseMessage.prototype.encode = function() {
  return new Buf().writeBytes(this.token).toBuffer();
};
AuthResponseMessage.decode = function(input) {
  var message = new AuthResponseMessage();
  message.token = input.readBytes();
  return message;
};

/**
 * AUTH_SUCCESS message.
 *
 * @constructor
 * @since protocol version 2
 */
function AuthSuccessMessage(token) {
  var _token = token || new Buffer(0);

  Response.call(this, 0x10);

  Object.defineProperty(this, 'token', {
    set: function(val) { _token = val; },
    get: function() { return _token; }
  });
}
AuthSuccessMessage.prototype = Object.create(
  Response.prototype,
  {constructor: {value: AuthSuccessMessage}}
);
AuthSuccessMessage.prototype.encode = function() {
  return new Buf().writeBytes(this.token).toBuffer();
};
AuthSuccessMessage.decode = function(input) {
  var message = new AuthSuccessMessage();
  message.token = input.readBytes();
  return message;
};

module.exports.CONSISTENCY_LEVEL = CONSISTENCY_LEVEL;
module.exports.CL = CONSISTENCY_LEVEL; // alias

module.exports.messages = messages;
module.exports.NativeProtocol = NativeProtocol;

