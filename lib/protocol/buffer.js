var UUID = require('node-uuid'),
    IPAddress = require('ipaddr.js');

/**
 * Helper class to read and write from Buffer object.
 *
 * @constructor
 */
function Buf(buffer) {
  var _initialSize = 64 * 1024,
      _stepSize = _initialSize,
      _pos = 0, // current position
      _buf; // internal buffer

  if (typeof buffer === 'number') {
    _initialSize = buffer;
  }
  if (buffer instanceof Buffer) {
    _buf = buffer;
  } else {
    _buf = new Buffer(_initialSize);
  }

  /**
   * @return {Buffer} - `Buffer` which has contents held by thie Buf.
   */
  this.toBuffer = function() {
    return _buf.slice(0, _pos);
  };

  this.rewind = function() {
    _pos = 0;
    return this;
  };

  this.readByte = function() {
    var b = _buf.readUInt8(_pos); _pos += 1;
    return b;
  };

  this.writeByte = function(b) {
    if (typeof b !== 'number') throw new Error('wrong type of argument');

    _checkCapacity(1);
    _buf.writeUInt8(b & 0xFF, _pos);
    _pos += 1;
    return this;
  };

  this.readShort = function() {
    var s = _buf.readUInt16BE(_pos); _pos += 2;
    return s;
  };

  this.writeShort = function(num) {
    if (typeof num !== 'number') throw new Error('wrong type of argument');

    _checkCapacity(2);
    _buf.writeUInt16BE(num & 0xFF, _pos); _pos += 2;
    return this;
  };

  this.readInt = function() {
    var n = _buf.readInt32BE(_pos);
    _pos += 4;
    return n;
  };

  this.writeInt = function(num) {
    if (typeof num !== 'number') throw new Error('wrong type of argument');

    _checkCapacity(4);
    _buf.writeInt32BE(num & 0xFFFF, _pos); _pos += 4;
    return this;
  };

  this.readUInt = function() {
    var n = _buf.readUInt32BE(_pos);
    _pos += 4;
    return n;
  };

  this.writeUInt = function(num) {
    if (typeof num !== 'number') throw new Error('wrong type of argument');

    _checkCapacity(4);
    _buf.writeUInt32BE(num & 0xFFFF, _pos); _pos += 4;
    return this;
  };

  this.readString = function() {
    var s = this.readShort();
    var strBuf = new Buffer(s);
    _buf.copy(strBuf, 0, _pos, _pos + s); _pos += s;
    return strBuf.toString();
  };

  this.writeString = function(str) {
    if (typeof str !== 'string') throw new Error('wrong type of argument');

    var strBuf = new Buffer(str);
    this.writeShort(strBuf.length);
    _checkCapacity(strBuf.length);
    strBuf.copy(_buf, _pos); _pos += strBuf.length;
    return this;
  };

  this.readLongString = function() {
    var s = this.readUInt(),
        strBuf = new Buffer(s);
    _buf.copy(strBuf, 0, _pos, _pos + s); _pos += s;
    return strBuf.toString();
  };

  this.writeLongString = function(str) {
    if (typeof str !== 'string') throw new Error('wrong type of argument');

    var strBuf = new Buffer(str);
    this.writeUInt(strBuf.length);
    _checkCapacity(strBuf.length);
    strBuf.copy(_buf, _pos); _pos += strBuf.length;
    return this;
  };

  this.readStringList = function() {
    var size = this.readShort(),
        i, list = [];
    for (i = 0; i < size; i++) {
      list.push(this.readString());
    }
    return list;
  };

  this.writeStringList = function(list) {
    var size = list.length, i;
    this.writeShort(size);
    for (i = 0; i < size; i++) {
      this.writeString(list[i]);
    }
    return this;
  };

  this.readStringMap = function() {
    var size = this.readShort(),
        i, map = {};
    for (i = 0; i < size; i++) {
      map[this.readString()] = this.readString();
    }
    return map;
  };

  this.writeStringMap = function(map) {
    var keys = Object.keys(map),
        size = keys.length,
        i;
    this.writeShort(size);
    for (i = 0; i < size; i++) {
      this.writeString(keys[i]).writeString(map[keys[i]]);
    }
    return this;
  };

  this.readStringMultimap = function() {
    var size = this.readShort(),
        i, map = {};
    for (i = 0; i < size; i++) {
      map[this.readString()] = this.readStringList();
    }
    return map;
  };

  this.writeStringMultimap = function(map) {
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
  this.readShortBytes = function() {
    var len = this.readShort(),
        buf;
    if (len <= 0)
      return null;
    buf = new Buffer(len);
    _buf.copy(buf, 0, _pos, _pos + len);
    _pos += len;
    return buf;
  };

  /**
   * Write short bytes.
   *
   * @param {Buffer}
   */
  this.writeShortBytes = function(buf) {
    if (!(buf instanceof Buffer)) {
      throw new TypeError('should be Buffer object');
    }
    this.writeShort(buf.length);
    if (buf.length > 0) {
      _checkCapacity(buf.length);
      buf.copy(_buf, _pos);
      _pos += buf.length;
    }
    return this;
  };

  /**
   * Read bytes.
   *
   * @return {Buffer}
   */
  this.readBytes = function() {
    var len = this.readInt(),
        buf;
    if (len <= 0)
      return null;
    buf = new Buffer(len);
    _buf.copy(buf, 0, _pos, _pos + len);
    _pos += len;
    return buf;
  };

  this.writeBytes = function(buf) {
    var len = buf ? buf.length : 0;
    this.writeInt(len);
    if (len > 0) {
      _checkCapacity(len);
      buf.copy(_buf, _pos);
      _pos += len;
    }
    return this;
  };

  /**
   * Read `inet` object from this buffer.
   *
   * @return {object} - inet object has `address` and `port`.
   */
  this.readInet = function() {
    var len = this.readByte(),
        address = _buf.slice(_pos, _pos + len),
        inet = {},
        ipaddr, port;
    _pos += len;
    if (address.length === 4) {
      // IPv4
      ipaddr = Array.prototype.join.call(address, '.');
    } else if (address.length === 16) {
      ipaddr = [];
      // IPv6
      for (i = 0; i < address.length; i += 2) {
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

  this.writeInet = function(inet) {
    var ipaddr = new Buffer(IPAddress.parse(inet.address).toByteArray());
    this.writeByte(ipaddr.length);
    _checkCapacity(ipaddr.length);
    ipaddr.copy(_buf, _pos);
    _pos += ipaddr.length;
    this.writeUInt(inet.port || 0);
    return this;
  };


  /**
   * Read UUID from this buffer.
   *
   * @return {uuid} - UUID
   */
  this.readUUID = function() {
    var uuid = UUID.unparse(_buf.slice(_pos, 16));
    _pos += 16;
    return uuid.toString();
  };

  this.writeUUID = function(uuid) {
    var buf = new Buffer(UUID.parse(uuid));
    buf.copy(_buf, _pos);
    _pos += buf.length;
    return this;
  };

  /**
   * check if we have enough bytes and expand buf is necessary
   * @private
   */
  function _checkCapacity(bytesToBeAdded) {
    var temp;
    while (_pos + bytesToBeAdded > _buf.length) {
      temp = _buf;
      _buf = new Buffer(temp.length + _stepSize);
      temp.copy(_buf);
    }
  }
}
Buf.prototype = Object.create(
  {},
  {constructor: {value: Buf}}
);

module.exports.Buf = Buf;

