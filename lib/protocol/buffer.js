var UUID = require('node-uuid'),
    IPAddress = require('ipaddr.js');

/**
 * Helper class to read and write from Buffer object.
 *
 * @constructor
 */
function Buf(buffer) {
  this._initialSize = 64 * 1024;
  this._stepSize = this._initialSize;
  this._pos = 0;

  if (typeof buffer === 'number') {
    this._initialSize = buffer;
  }
  if (buffer instanceof Buffer) {
    this._buf = buffer;
  } else {
    this._buf = new Buffer(this._initialSize);
  }
}

/**
 * @return {Buffer} - `Buffer` which has contents held by thie Buf.
 */
Buf.prototype.toBuffer = function() {
  return this._buf.slice(0, this._pos);
};

Buf.prototype.rewind = function() {
  this._pos = 0;
  return this;
};

Buf.prototype.readByte = function() {
  var b = this._buf.readUInt8(this._pos);
  this._pos += 1;
  return b;
};

Buf.prototype.writeByte = function(b) {
  if (typeof b !== 'number') throw new Error('wrong type of argument');

  this.checkCapacity(1);
  this._buf.writeUInt8(b & 0xFF, this._pos);
  this._pos += 1;
  return this;
};

Buf.prototype.readShort = function() {
  var s = this._buf.readUInt16BE(this._pos);
  this._pos += 2;
  return s;
};

Buf.prototype.writeShort = function(num) {
  if (typeof num !== 'number') throw new Error('wrong type of argument');

  this.checkCapacity(2);
  this._buf.writeUInt16BE(num & 0xFF, this._pos);
  this._pos += 2;
  return this;
};

Buf.prototype.readInt = function() {
  var n = this._buf.readInt32BE(this._pos);
  this._pos += 4;
  return n;
};

Buf.prototype.writeInt = function(num) {
  if (typeof num !== 'number') throw new Error('wrong type of argument');

  this.checkCapacity(4);
  this._buf.writeInt32BE(num & 0xFFFF, this._pos);
  this._pos += 4;
  return this;
};

Buf.prototype.readUInt = function() {
  var n = this._buf.readUInt32BE(this._pos);
  this._pos += 4;
  return n;
};

Buf.prototype.writeUInt = function(num) {
  if (typeof num !== 'number') throw new Error('wrong type of argument');

  this.checkCapacity(4);
  this._buf.writeUInt32BE(num & 0xFFFF, this._pos);
  this._pos += 4;
  return this;
};

Buf.prototype.readString = function() {
  var s = this.readShort();
  var strBuf = new Buffer(s);
  this._buf.copy(strBuf, 0, this._pos, this._pos + s);
  this._pos += s;
  return strBuf.toString();
};

Buf.prototype.writeString = function(str) {
  if (typeof str !== 'string') throw new Error('wrong type of argument');

  var strBuf = new Buffer(str);
  this.writeShort(strBuf.length);
  this.checkCapacity(strBuf.length);
  strBuf.copy(this._buf, this._pos);
  this._pos += strBuf.length;
  return this;
};

Buf.prototype.readLongString = function() {
  var s = this.readUInt(),
  strBuf = new Buffer(s);
  this._buf.copy(strBuf, 0, this._pos, this._pos + s);
  this._pos += s;
  return strBuf.toString();
};

Buf.prototype.writeLongString = function(str) {
  if (typeof str !== 'string') throw new Error('wrong type of argument');

  var strBuf = new Buffer(str);
  this.writeUInt(strBuf.length);
  this.checkCapacity(strBuf.length);
  strBuf.copy(this._buf, this._pos);
  this._pos += strBuf.length;
  return this;
};

Buf.prototype.readStringList = function() {
  var size = this.readShort(),
  i, list = [];
  for (i = 0; i < size; i++) {
    list.push(this.readString());
  }
  return list;
};

Buf.prototype.writeStringList = function(list) {
  var size = list.length, i;
  this.writeShort(size);
  for (i = 0; i < size; i++) {
    this.writeString(list[i]);
  }
  return this;
};

Buf.prototype.readStringMap = function() {
  var size = this.readShort(),
  i, map = {};
  for (i = 0; i < size; i++) {
    map[this.readString()] = this.readString();
  }
  return map;
};

Buf.prototype.writeStringMap = function(map) {
  var keys = Object.keys(map),
  size = keys.length,
  i;
  this.writeShort(size);
  for (i = 0; i < size; i++) {
    this.writeString(keys[i]).writeString(map[keys[i]]);
  }
  return this;
};

Buf.prototype.readStringMultimap = function() {
  var size = this.readShort(),
  i, map = {};
  for (i = 0; i < size; i++) {
    map[this.readString()] = this.readStringList();
  }
  return map;
};

Buf.prototype.writeStringMultimap = function(map) {
  var keys = Object.keys(map),
  size = keys.length,
  i;
  this.writeShort(size);
  for (i = 0; i < size; i++) {
    this.writeString(keys[i]).writeStringList(map[keys[i]]);
  }
  return this;
};

/**
 * Read short bytes.
 *
 * @return {Buffer}
 */
Buf.prototype.readShortBytes = function() {
  var len = this.readShort(),
  buf;
  if (len <= 0)
    return null;
  buf = new Buffer(len);
  this._buf.copy(buf, 0, this._pos, this._pos + len);
  this._pos += len;
  return buf;
};

/**
 * Write short bytes.
 *
 * @param {Buffer}
 */
Buf.prototype.writeShortBytes = function(buf) {
  if (!(buf instanceof Buffer)) {
    throw new TypeError('should be Buffer object');
  }
  this.writeShort(buf.length);
  if (buf.length > 0) {
    this.checkCapacity(buf.length);
    buf.copy(this._buf, this._pos);
    this._pos += buf.length;
  }
  return this;
};

/**
 * Read bytes.
 *
 * @return {Buffer}
 */
Buf.prototype.readBytes = function() {
  var len = this.readInt(),
  buf;
  if (len <= 0)
    return null;
  buf = new Buffer(len);
  this._buf.copy(buf, 0, this._pos, this._pos + len);
  this._pos += len;
  return buf;
};

Buf.prototype.writeBytes = function(buf) {
  var len = buf ? buf.length : 0;
  this.writeInt(len);
  if (len > 0) {
    this.checkCapacity(len);
    buf.copy(this._buf, this._pos);
    this._pos += len;
  }
  return this;
};

/**
 * Read `inet` object from this buffer.
 *
 * @return {object} - inet object has `address` and `port`.
 */
Buf.prototype.readInet = function() {
  var len = this.readByte(),
  address = this._buf.slice(this._pos, this._pos + len),
  inet = {},
  ipaddr, port;
  this._pos += len;
  if (address.length === 4) {
    // IPv4
    ipaddr = Array.prototype.join.call(address, '.');
  } else if (address.length === 16) {
    ipaddr = [];
    // IPv6
    for (var i = 0; i < address.length; i += 2) {
      ipaddr.push(address.readUInt16BE(i).toString(16));
    }
    ipaddr = IPAddress.parse(ipaddr.join(':')).toString();
  }
  inet.address = ipaddr;
  port = this.readUInt();
  if (port > 0)
    inet.port = port;
  return inet;
};

Buf.prototype.writeInet = function(inet) {
  var ipaddr = new Buffer(IPAddress.parse(inet.address).toByteArray());
  this.writeByte(ipaddr.length);
  this.checkCapacity(ipaddr.length);
  ipaddr.copy(this._buf, this._pos);
  this._pos += ipaddr.length;
  this.writeUInt(inet.port || 0);
  return this;
};


/**
 * Read UUID from this buffer.
 *
 * @return {uuid} - UUID
 */
Buf.prototype.readUUID = function() {
  var uuid = UUID.unparse(this._buf.slice(this._pos, 16));
  this._pos += 16;
  return uuid.toString();
};

Buf.prototype.writeUUID = function(uuid) {
  var buf = new Buffer(UUID.parse(uuid));
  buf.copy(this._buf, this._pos);
  this._pos += buf.length;
  return this;
};

/**
 * check if we have enough bytes and expand buf is necessary
 * @private
 */
Buf.prototype.checkCapacity = function(bytesToBeAdded) {
  var temp;
  while (this._pos + bytesToBeAdded > this._buf.length) {
    temp = this._buf;
    this._buf = new Buffer(temp.length + this._stepSize);
    temp.copy(this._buf);
  }
};

module.exports.Buf = Buf;

