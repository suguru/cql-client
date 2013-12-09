/* global describe, it */
var Buf = require('../lib/protocol/buffer').Buf;
var expect = require('expect.js');

describe('Buffer', function() {

  var buf;

  it('Buf should expand automatically beyond initial capacity', function() {
    buf = new Buf(1);
    buf.writeString('abc');
    buf.rewind();
    expect(buf.readString()).to.eql('abc');
  });

  it('test read/write byte', function() {
    var b = 1,
    buf = new Buf();

    buf.writeByte(b);
    buf.rewind();
    expect(buf.readByte()).to.eql(b);

    buf.rewind();
    expect(function() {
      buf.writeByte(null);
    }).to.throwError();
  });

  it('test read/write/ bytes', function() {
    buf = new Buf();
    var b = new Buffer(0);
    buf.writeBytes(b);
    buf.writeBytes(null);

  });

  it('test read/write short', function() {
    var n = 2,
    buf = new Buf();

    buf.writeShort(n);
    buf.rewind();
    expect(buf.readShort()).to.eql(n);
    expect(function() { buf.writeShort('100'); }).to.throwError();

    buf = new Buf();
    buf.writeByte(-1);
    expect(buf.readShortBytes()).to.eql(null);

    buf = new Buf();
    expect(function() { buf.writeShortBytes('invalid'); }).to.throwError();
  });

  it('test read/write int', function() {
    var n = 2,
    buf = new Buf();

    buf.writeInt(n);
    buf.rewind();
    expect(n).to.eql(buf.readInt());
    expect(function() { buf.writeInt('100'); }).to.throwError();
    expect(function() { buf.writeUInt('100'); }).to.throwError();
  });

  it('test read/write string', function() {
    var s = 'abcdefg',
    buf = new Buf();

    buf.writeString(s);
    buf.rewind();
    expect(s).to.eql(buf.readString());
    expect(function() { buf.writeString(100); }).to.throwError();
  });

  it('test read/write long string', function() {
    var s = 'abcdefg',
    buf = new Buf();

    buf.writeLongString(s);
    buf.rewind();
    expect(s).to.eql(buf.readLongString());
    expect(function() { buf.writeLongString(100); }).to.throwError();
  });

  it('test read/write string list', function() {
    var s = ['abc', 'defg'],
    buf = new Buf();

    buf.writeStringList(s);
    buf.rewind();
    expect(s).to.eql(buf.readStringList());
  });

  it('test read/write string map', function() {
    var s = {abc: 'defg'},
    buf = new Buf();

    buf.writeStringMap(s);
    buf.rewind();
    expect(s).to.eql(buf.readStringMap());
  });

  it('test read/write string multi map', function() {
    var s = {abc: ['defg','hijk']},
    buf = new Buf();

    buf.writeStringMultimap(s);
    buf.rewind();
    expect(s).to.eql(buf.readStringMultimap());
  });

  it('test read/write inet', function() {
    var v4 = {
      address: '10.10.10.10',
      port: 9140
    },
    v6 = {
      address: '2001:db8:1234::1',
      port: 9140
    },
    noport = {
      address: '10.10.10.10'
    },
    buf = new Buf();

    buf.writeInet(v4);
    buf.rewind();
    expect(v4).to.eql(buf.readInet());

    buf.rewind();

    buf.writeInet(v6);
    buf.rewind();
    expect(v6).to.eql(buf.readInet());

    buf.rewind();

    buf.writeInet(noport);
    buf.rewind();
    expect(noport).to.eql(buf.readInet());
  });

  it('test read/write uuid', function() {
    var uuid = '1255cf50-045d-11e3-8ffd-0800200c9a66',
    buf = new Buf();

    buf.writeUUID(uuid);
    buf.rewind();
    expect(uuid).to.eql(buf.readUUID());
  });

});
