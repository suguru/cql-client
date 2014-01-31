/* global describe, it */
var types = require('../lib/protocol/types');
var expect = require('expect.js');

describe('test ser/de', function() {
  it('ascii type', function(done) {
    var type = types.ascii,
        serialized,
        deserialized;

    serialized = type.serialize(null);
    expect(serialized.length).to.eql(0);

    serialized = type.serialize('abc');
    expect(serialized.length).to.eql(3);
    deserialized = type.deserialize(serialized);
    expect(deserialized).to.eql('abc');

    deserialized = type.deserialize(null);
    expect(deserialized).to.eql(null);
    done();
  });

  it('bigint', function(done) {
    var buf = types.bigint.serialize(100);
    expect(buf.length).to.eql(8);
    expect(types.bigint.deserialize(buf)).to.eql('100')

    // serialize java's Long.MAX_VALUE
    buf = types.bigint.serialize('9223372036854775807');
    expect(buf.length).to.eql(8);
    expect(types.bigint.deserialize(buf)).to.eql('9223372036854775807');

    // serialize java's Long.MIN_VALUE
    buf = types.bigint.serialize('-9223372036854775808');
    expect(types.bigint.deserialize(buf)).to.eql('-9223372036854775808');

    // null should be null
    expect(types.bigint.deserialize(null)).to.eql(null);

    done();
  });

  it('blob', function(done) {
    var blob = new Buffer('abc'),
        buf = types.blob.serialize(blob),
        des;
    expect(buf).to.eql(blob);
    des = types.blob.deserialize(buf);

    expect(buf).to.eql(des);
    expect(types.blob.deserialize(null)).to.eql(null);
    done();
  });

  it('boolean', function(done) {
    var buf = types['boolean'].serialize(true);
    expect(buf.length).to.eql(1);
    expect(types['boolean'].deserialize(buf)).to.eql(true);
    buf = types['boolean'].serialize(false);
    expect(types['boolean'].deserialize(buf)).to.eql(false);

    expect(types['boolean'].deserialize(null)).to.eql(false);

    buf = types['boolean'].serialize(1);
    expect(buf.length).to.eql(1);
    expect(types['boolean'].deserialize(buf)).to.eql(true);

    buf = types['boolean'].serialize(0);
    expect(buf.length).to.eql(1);
    expect(types['boolean'].deserialize(buf)).to.eql(false);

    buf = types['boolean'].serialize('true');
    expect(buf.length).to.eql(1);
    expect(types['boolean'].deserialize(buf)).to.eql(true);

    buf = types['boolean'].serialize('1');
    expect(buf.length).to.eql(1);
    expect(types['boolean'].deserialize(buf)).to.eql(true);

    buf = types['boolean'].serialize('false');
    expect(buf.length).to.eql(1);
    expect(types['boolean'].deserialize(buf)).to.eql(false);

    done();
  });

  it('decimal type', function(done) {
    var type = types.decimal,
        patterns,
        i,
        serialized,
        deserialized;

    serialized = type.serialize(null);
    expect(serialized.length).to.eql(0);

    patterns = ['123.456', '-123.456', '123.000', '123', '0', '0.000123', '-0.000123'];
    for (i = 0; i < patterns.length; i++) {
      serialized = type.serialize(patterns[i]);
      deserialized = type.deserialize(serialized);
      expect(deserialized).to.eql(patterns[i]);
    }

    deserialized = type.deserialize(null);
    expect(deserialized).to.eql(null);
    done();
  });

  it('float', function(done) {
    var buf = types['float'].serialize(64.0);
    expect(buf.length).to.eql(4);
    expect(types['float'].deserialize(buf)).to.eql(64.0);

    buf = types['float'].serialize(-2.5);
    expect(types['float'].deserialize(buf)).to.eql(-2.5);

    expect(types['float'].deserialize(null)).to.eql(null);

    done();
  });

  it('int', function(done) {
    var buf = types['int'].serialize(64);
    expect(buf.length).to.eql(4);
    expect(types['int'].deserialize(buf)).to.eql(64);

    buf = types['int'].serialize(-2);
    expect(types['int'].deserialize(buf)).to.eql(-2);

    buf = types['int'].serialize('64');
    expect(buf.length).to.eql(4);
    expect(types['int'].deserialize(buf)).to.eql(64);

    buf = types['int'].serialize('-2');
    expect(types['int'].deserialize(buf)).to.eql(-2);

    expect(types['int'].deserialize(null)).to.eql(null);

    done();
  });

  it('timestamp', function(done) {
    var d = new Date(),
        buf = types.timestamp.serialize(d),
        s = types.timestamp.deserialize(buf);

    expect(d).to.eql(s);

    buf = types.timestamp.serialize(d.toString());
    s = types.timestamp.deserialize(buf);
    expect(d.toString()).to.eql(s.toString());

    buf = types.timestamp.serialize(d.getTime());
    s = types.timestamp.deserialize(buf);
    expect(d).to.eql(s);

    expect(function() {
      types.timestamp.serialize('foo');
    }).to.throwError();

    done();
  });

  it('inet', function(done) {
    // IPv4
    var v4 = '10.10.10.10',
        v6 = '2001:db8:1234::1',
        buf = types.inet.serialize(v4),
        addr = types.inet.deserialize(buf);
    expect(buf.length).to.eql(4);
    expect(v4).to.eql(addr);
    // IPv6
    buf = types.inet.serialize(v6);
    expect(buf.length).to.eql(16);
    addr = types.inet.deserialize(buf);
    expect(v6).to.eql(addr);

    expect(function() {
      types.inet.serialize('foo');
    }).to.throwError();
    done();
  });
});
