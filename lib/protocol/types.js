/**
 * @module cql-protocol/types
 */
var UUID = require('node-uuid'),
    IPAddress = require('ipaddr.js'),
    BigInteger = require('jsbn');

var EMPTY_BUFFER = new Buffer(0);

/**
 * @constructor
 */
function CustomType() {
}
CustomType.prototype = Object.create(
  {},
  {constructor: {value: CustomType}}
);
CustomType.prototype.register = function(name, serializer) {
}
/**
 * Serialization of CustomType is to just return
 * copy of given Buffer.
 *
 * @param {Buffer} value - Buffer object to serialize.
 * @return {Buffer} copy of given Buffer
 */
CustomType.prototype.serialize = function(value) {
  var buf = new Buffer(value.length);
  value.copy(buf, 0);
  return buf;
};
/**
 * Deserialize of CustomType just returns given Buffer.
 *
 * @param {Buffer} bytes - Buffer
 * @return {Buffer} given Buffer
 */
CustomType.prototype.deserialize = function(bytes) {
  return bytes;
};

/**
 * @constructor
 * @param {string} encoding
 * @property {string} encoding - encoding set to this type
 */
function TextType(encoding) {
  Object.defineProperty(this, 'encoding', {
    value: encoding
  });
}
TextType.prototype = Object.create(
  {},
  {constructor: {value: TextType}}
);
/**
 * Serialize string to Buffer.
 *
 * @param {string} value - string to serialize.
 * @return {Buffer} serialized Buffer
 */
TextType.prototype.serialize = function(value) {
  return new Buffer(value || '', this.encoding);
};
/**
 * Deserialize Buffer to string.
 *
 * @param {Buffer} bytes - Buffer to deserialize.
 * @return {string} deserialized string or null if given buffer is null or undefined.
 */
TextType.prototype.deserialize = function(bytes) {
  if (!bytes) return null;
  return bytes.toString(this.encoding);
};

/**
 * @constructor
 */
function BytesType() {
}
BytesType.prototype = Object.create(
  {},
  {constructor: {value: BytesType}}
);
/**
 * Serialization of BytesType is to just return
 * copy of given Buffer.
 *
 * @param {Buffer} value - Buffer object to serialize.
 * @return {Buffer} copy of given Buffer
 */
BytesType.prototype.serialize = function(value) {
  var buf = new Buffer(value.length);
  value.copy(buf, 0);
  return buf;
};
/**
 * Deserialize of BytesType just returns given Buffer.
 *
 * @param {Buffer} bytes - Buffer
 * @return {Buffer} given Buffer
 */
BytesType.prototype.deserialize = function(bytes) {
  return bytes;
};

function BooleanType() {
}
BooleanType.prototype = Object.create(
  {},
  {constructor: {value: BooleanType}}
);
/**
 * Serialize boolean to Buffer.
 *
 * @param {boolean} value to serialize.
 * @return {Buffer} serialized Buffer of 1 byte.
 */
BooleanType.prototype.serialize = function(value) {
  var buf = new Buffer(1);
  buf.fill(0);
  if (value === true)
    buf.writeUInt8(1, 0);
  return buf;
};
/**
 * Deserialize Buffer to boolean.
 *
 * @param {Buffer} bytes Buffer of 8 bytes.
 * @return {boolean} deserialized boolean.
 */
BooleanType.prototype.deserialize = function(bytes) {
  return bytes instanceof Buffer && bytes.length === 1 && bytes[0] === 1;
};

/**
 * @constructor
 */
function LongType() {
}
LongType.prototype = Object.create(
  {},
  {constructor: {value: LongType}}
);
/**
 * Serialize number or string to Buffer.
 *
 * @param {number|string} value number to serialize.
 * @return {Buffer} Buffer of 8 bytes.
 */
LongType.prototype.serialize = function(value) {
  var buf,
      converted;

  converted = IntegerType.prototype.serialize(value);
  if (converted.length === 0 || converted.length === 8) {
    return converted;
  } else if (converted.length > 8) {
    throw new Error('overflow');
  } else {
    buf = new Buffer(8);
    buf.fill(0);
    converted.copy(buf, buf.length - converted.length);
    return buf;
  }
};
/**
 * Deserialize Buffer to string number.
 *
 * @param {Buffer} bytes Buffer of 8 bytes.
 * @return {string} deserialized string representation of number.
 */
LongType.prototype.deserialize = function(bytes) {
  var arr;
  if (!(bytes instanceof Buffer)) {
    return null;
  }
  if (bytes.length !== 8) {
    throw new Error('Invalid Buffer');
  }
  arr = bytes.toJSON();
  if (arr.type && arr.type === 'Buffer')
    arr = arr.data;
  return new BigInteger(arr).toString();
};

/**
 * @constructor
 */
function DecimalType() {
}
DecimalType.prototype = Object.create(
  {},
  {constructor: {value: DecimalType}}
);
/**
 * Serialize string 'decimal' to Buffer.
 *
 * @param {string} string representation of decimal to serialize.
 * @return {Buffer} Buffer of 8 bytes.
 */
DecimalType.prototype.serialize = function(value) {
  var scale = 0,
      precision = 0,
      bi,
      buf;
      buf;
  if (typeof value === 'numer') {
    // if number is passed, then convert to string
    value = String(value);
  }
  if (typeof value !== 'string') {
    return new Buffer(0);
  }

  var offset = 0,
      len = value.length;

  // handle the sign
  var isneg = false;          // assume positive
  if (value[offset] === '-') {
    isneg = true;               // leading minus means negative
    offset++;
    len--;
  } else if (value[offset] === '+') { // leading + allowed
    offset++;
    len--;
  }

  // should now be at numeric part of the significand
  var haveDot = false; // true when there is a '.'
  var c;  // current character

  // integer significand array & idx is the index to it. The array
  // is ONLY used when we can't use a compact representation.
  var coeff = [];
  var idx = 0;
  for (; len > 0; offset++, len--) {
    c = value[offset];
    // have digit
    if (!isNaN(c)) {
      if (c === '0') {
        if (precision === 0) {
          coeff[idx] = c;
          precision = 1;
        } else if (idx !== 0) {
          coeff[idx++] = c;
          ++precision;
        } // else c must be a redundant leading zero
      } else {
        if (precision !== 1 || idx !== 0)
          ++precision; // unchanged if preceded by 0s
        coeff[idx++] = c;
      }
      if (haveDot)
        ++scale;
    } else if (c === '.') {
      // have dot
      if (haveDot) // two dots
        throw new Error('Illegal number format:' + value);
      haveDot = true;
    } else {
      throw new Error('Illegal number format:' + value);
    }
  }
  // here when no characters left
  if (precision == 0)              // no digits found
    throw new Error('Illegal number format:' + value);

  // Remove leading zeros from precision (digits count)
  if (isneg) {
    coeff.unshift('-');
  }
  bi = new Buffer(new BigInteger(coeff.join('')).toByteArray());

  buf = new Buffer(bi.length + 4); // scale(4byte int) + BigInteger
  buf.writeInt32BE(scale, 0);
  bi.copy(buf, 4);
  return buf;
};
/**
 * Deserialize Buffer to string 'decimal'.
 *
 * @param {Buffer} bytes Buffer
 * @return {string} deserialized number.
 */
DecimalType.prototype.deserialize = function(bytes) {
  var buf = '',
      scale, 
      bi,
      arr,
      coeff,
      pad;  // count of padding zeros

  if (!(bytes instanceof Buffer)) {
    return null;
  }

  scale = bytes.readInt32BE(0);
  arr = bytes.slice(4).toJSON();
  if (arr.type && arr.type === 'Buffer')
    arr = arr.data;
  bi = new BigInteger(arr);

  if (scale === 0) 
    return bi.toString();
  
  // Get the significand as an absolute value
  coeff  = bi.abs().toString();
  pad = scale - coeff.length; 

  if (bi.signum() < 0)
    buf += '-';
  if (pad >= 0) {
    // 0.xxx form
    buf += '0';
    buf += '.';
    for (; pad > 0; pad--) {
      buf += '0';
    }
    buf += coeff;
  } else {
    // xx.xx form
    buf += coeff.slice(0, -pad);
    buf += '.';
    buf += coeff.slice(-pad);
  }
  return buf;
};

/**
 * @constructor
 */
function IntegerType() {
}
IntegerType.prototype = Object.create(
  {},
  {constructor: {value: IntegerType}}
);
/**
 * Serialize string 'decimal' to Buffer.
 *
 * @param {string} string representation of decimal to serialize.
 * @return {Buffer} Buffer of 8 bytes.
 */
IntegerType.prototype.serialize = function(value) {
  if (!value) {
    return EMPTY_BUFFER;
  } else {
    return new Buffer(new BigInteger(String(value)).toByteArray());
  }
};
/**
 * Deserialize Buffer to string 'decimal'.
 *
 * @param {Buffer} bytes Buffer
 * @return {string} deserialized number.
 */
IntegerType.prototype.deserialize = function(bytes) {
  var arr = bytes.toJSON();
  if (arr.type && arr.type === 'Buffer')
    arr = arr.data;
  return new BigInteger(arr).toString();
};

/**
 * @constructor
 */
function DoubleType() {
}
DoubleType.prototype = Object.create(
  {},
  {constructor: {value: DoubleType}}
);
/**
 * Serialize number to Buffer.
 *
 * (Currently, this does not work completely,
 *  since javascript precision is not as same as java)
 *
 * @param {number} value number to serialize.
 * @return {Buffer} Buffer of 8 bytes.
 */
DoubleType.prototype.serialize = function(value) {
  var buf;
  if (typeof value !== 'number') {
    throw new TypeError('value is not a number');
  }
  buf = new Buffer(8);
  buf.fill(0);
  buf.writeDoubleBE(value, 0);
  return buf;
};
/**
 * Deserialize Buffer to number.
 *
 * @param {Buffer} bytes Buffer of 8 bytes.
 * @return {number} deserialized number.
 * @throws bytes' length is not 8.
 */
DoubleType.prototype.deserialize = function(bytes) {
  if (!(bytes instanceof Buffer)) {
    return null;
  }
  if (bytes.length !== 8) {
    throw new Error('Invalid Buffer');
  }
  return bytes.readDoubleBE(0);
};

/**
 * @constructor
 */
function FloatType() {
}
FloatType.prototype = Object.create(
  {},
  {constructor: {value: FloatType}}
);
/**
 * Serialize number to Buffer.
 *
 * @param {number} value number to serialize.
 * @return {Buffer} Buffer of 4 bytes.
 */
FloatType.prototype.serialize = function(value) {
  var buf;
  if (typeof value !== 'number') {
    return EMPTY_BUFFER;
  }
  buf = new Buffer(4);
  buf.fill(0);
  buf.writeFloatBE(value, 0);
  return buf;
};
/**
 * Deserialize Buffer to number.
 *
 * @param {Buffer} bytes Buffer of 4 bytes.
 * @return {number} deserialized number.
 * @throws bytes' length is not 4.
 */
FloatType.prototype.deserialize = function(bytes) {
  if (!(bytes instanceof Buffer)) {
    return null;
  }
  if (bytes.length !== 4) {
    throw new Error('Invalid Buffer');
  }
  return bytes.readFloatBE(0);
};

/**
 * @constructor
 */
function Int32Type() {
}
Int32Type.prototype = Object.create(
  {},
  {constructor: {value: Int32Type}}
);
/**
 * Serialize number to Buffer.
 *
 * @param {number} value number to serialize.
 * @return {Buffer} Buffer of 4 bytes.
 */
Int32Type.prototype.serialize = function(value) {
  var buf;
  if (typeof value !== 'number') {
    return EMPTY_BUFFER;
  }
  buf = new Buffer(4);
  buf.fill(0);
  buf.writeInt32BE(value, 0);
  return buf;
};
/**
 * Deserialize Buffer to number.
 *
 * @param {Buffer} bytes Buffer of 4 bytes.
 * @return {number} deserialized number.
 * @throws bytes' length is not 4.
 */
Int32Type.prototype.deserialize = function(bytes) {
  if (!(bytes instanceof Buffer)) {
    return null;
  }
  if (bytes.length !== 4) {
    throw new Error('Invalid Buffer');
  }
  return bytes.readInt32BE(0);
};

/**
 * @constructor
 */
function TimestampType() {
}
TimestampType.prototype = Object.create(
  {},
  {constructor: {value: TimestampType}}
);
/**
 * Serialize Date object to Buffer.
 *
 * @param {Date} value Date object to serialize.
 * @return {Buffer} Buffer of 8 bytes.
 */
TimestampType.prototype.serialize = function(value) {
  var buf,
      converted;
  if (!(value instanceof Date)) {
    throw new TypeError('value is not Date');
  }
  return LongType.prototype.serialize.call(this, value.valueOf());
};
/**
 * Deserialize Buffer to Date.
 *
 * @param {Buffer} bytes Buffer of 8 bytes.
 * @return {Date} deserialized Date object.
 * @throws Error when bytes is not Buffer type or bytes' length is not 8.
 */
TimestampType.prototype.deserialize = function(bytes) {
  var deserialized = LongType.prototype.deserialize.call(this, bytes);
  return deserialized ? new Date(Number(deserialized)) : null;
};

/**
 * @constructor
 */
function UUIDType() {
}
UUIDType.prototype = Object.create(
  {},
  {constructor: {value: UUIDType}}
);
/**
 * Serialize UUID string to Buffer
 *
 * @param {string} string representation of UUID
 * @return {Buffer} serialized Buffer
 */
UUIDType.prototype.serialize = function(value) {
  return new Buffer(UUID.parse(value));
};
/**
 * Deserialize Buffer to UUID string
 *
 * @param {Buffer} bytes Buffer.
 * @return {string} string representation of UUID.
 */
UUIDType.prototype.deserialize = function(bytes) {
  if (!bytes) return '';
  return UUID.unparse(bytes).toString();
};

/**
 * @constructor
 */
function InetAddressType() {
}
InetAddressType.prototype = Object.create(
  {},
  {constructor: {value: InetAddressType}}
);
/**
 * Serialize InetAddress string to Buffer.
 * value can be either IPv4 or IPv6 represntation.
 *
 * @param {string} value - string representation of InetAddress
 * @return {Buffer} serialized Buffer
 */
InetAddressType.prototype.serialize = function(value) {
  return new Buffer(IPAddress.parse(value).toByteArray());
};
/**
 * Deserialize Buffer to InetAddress string
 *
 * @param {Buffer} bytes Buffer.
 * @return {string} string representation of InetAddress.
 */
InetAddressType.prototype.deserialize = function(bytes) {
  var decoded, i;

  if (!bytes) {
    return null;
  }
  if (bytes.length === 4) {
    // IPv4
    decoded = Array.prototype.join.call(bytes, '.');
  } else if (bytes.length === 16) {
    decoded = [];
    // IPv6
    for (i = 0; i < bytes.length; i += 2) {
      decoded.push(bytes.readUInt16BE(i).toString(16));
    }
    decoded = IPAddress.parse(decoded.join(':')).toString();
  }
  return decoded;
};

/**
 * @constructor
 */
function ListType(subType) {
  var _subType = types.fromType(subType);

  Object.defineProperty(this, 'subType', {
    value: _subType
  });
}
ListType.prototype = Object.create(
  {},
  {constructor: {value: ListType}}
);
ListType.prototype.serialize = function(value) {
  // TODO
};
ListType.prototype.deserialize = function(bytes) {
  var decoded = null,
      pos = 2,
      itemLen,
      item,
      num,
      i;
  if (bytes) {
    decoded = [];
    num = bytes.readUInt16BE(0);
    for (i = 0; i < num; i++) {
      itemLen = bytes.readUInt16BE(pos); pos += 2;
      item = bytes.slice(pos, pos + itemLen); pos += itemLen;
      decoded.push(this.subType.deserialize(item));
    }
  }
  return decoded;
};
ListType.create = function(subType) {
  return new ListType(subType);
};

/**
 * @constructor
 */
function MapType(keyType, valueType) {
  var _keyDecoder = types.fromType(keyType);
  var _valueDecoder = types.fromType(valueType);

  Object.defineProperties(this, {
    'keyDecoder': {
      value: _keyDecoder
    },
    'valueDecoder': {
      value: _valueDecoder
    }
  });
}
MapType.prototype = Object.create(
  {},
  {constructor: {value: MapType}}
);
MapType.prototype.serialize = function(value) {
  // TODO
};
MapType.prototype.deserialize = function(bytes) {
  var decoded = null,
      pos = 2,
      klen, k,
      vlen, v,
      num,
      i;
  if (bytes) {
    decoded = {};
    num = bytes.readUInt16BE(0);
    for (i = 0; i < num; i++) {
      klen = bytes.readUInt16BE(pos); pos += 2;
      k = bytes.slice(pos, pos + klen); pos += klen;
      vlen = bytes.readUInt16BE(pos); pos += 2;
      v = bytes.slice(pos, pos + vlen); pos += vlen;
      decoded[this.keyDecoder.deserialize(k)] = this.valueDecoder.deserialize(v);
    }
  }
  return decoded;
};
MapType.create = function(keyType, valueType) {
  return new MapType(keyType, valueType);
};

/**
 * CQL type decoders.
 * @private
 */
var typeMap = {
  0x0000: 'custom',
  0x0001: 'ascii',
  0x0002: 'bigint',
  0x0003: 'blob',
  0x0004: 'boolean',
  0x0005: 'counter',
  0x0006: 'decimal',
  0x0007: 'double',
  0x0008: 'float',
  0x0009: 'int',
  0x000A: 'text',
  0x000B: 'timestamp',
  0x000C: 'uuid',
  0x000D: 'varchar',
  0x000E: 'varint', 
  0x000F: 'timeuuid',
  0x0010: 'inet',
  0x0020: 'list',
  0x0021: 'map',
  0x0022: 'set'
};

var types = {}
types.custom = bytesType = new BytesType();
types.ascii = asciiType = new TextType('ascii');
types.bigint = longType = new LongType();
types.blob = bytesType;
types['boolean'] = booleanType = new BooleanType();
types.counter = longType;
types.decimal = decimalType = new DecimalType();
types['double'] = doubleType = new DoubleType();
types['float'] = floatType = new FloatType();
types['int'] = intType = new Int32Type();
types.text = utf8Type = new TextType('utf8');
types.varchar = utf8Type;
types.varint = integerType = new IntegerType();
types.timestamp = timestampType = new TimestampType();
types.uuid = uuidType = new UUIDType();
types.timeuuid = uuidType;
types.inet = inetType = new InetAddressType();
types.list = listType = ListType.create;
types.map = MapType.create;
types.set = listType;

/**
 * Convert type name or type code to type object.
 *
 * @param {string|number} type - type name or type code
 * @returns Type object for given type name or type code.
 */
types.fromType = function fromType(type) {
  if (typeof type === 'string') {
    return types[type];
  } else if (typeof type === 'number') {
    return types[typeMap[type]];
  } else {
    return bytesType;
  }
};

module.exports = types;

