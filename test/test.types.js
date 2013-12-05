/* global describe, it */
var types = require('../lib/types');
var expect = require('expect.js');

describe('Types', function() {


  describe('#fromType', function() {

    it('should get type by string', function() {
      expect(types.fromType('ascii')).to.be.ok();
      expect(types.fromType('ascii').constructor.name).to.eql('TextType');
    });

    it('should get byte type by default', function() {
      expect(types.fromType()).to.be.ok();
      expect(types.fromType().constructor.name).to.eql('BytesType');
    });


  });

  describe('#use', function() {

    it('can use BytesType', function() {
      var val = new Buffer(8);
      var ser = types.blob.serialize(val);
      var des = types.blob.deserialize(ser);
      expect(des).to.eql(val);
    });

    it('can use BooleanType', function() {
      var val = true;
      var ser = types.boolean.serialize(val);
      var des = types.boolean.deserialize(ser);
      expect(des).to.eql(val);
      val = false;
      ser = types.boolean.serialize(val);
      des = types.boolean.deserialize(ser);
      expect(des).to.eql(val);
    });

    it('can use LongType', function() {
      var val = 10000;
      var ser = types.bigint.serialize(val);
      var des = types.bigint.deserialize(ser);
      expect(des).to.eql(val);
    });

    it('can use FoatType', function() {
      var val = 1000.5;
      var ser = types.float.serialize(val);
      var des = types.float.deserialize(ser);
      expect(des).to.eql(val);
    });

    it('can use Int32Type', function() {
      var val = 1000;
      var ser = types.int.serialize(val);
      var des = types.int.deserialize(ser);
      expect(des).to.eql(val);
    });

    it('can use Timestamp type', function() {
      var val = new Date();
      var ser = types.timestamp.serialize(val);
      var des = types.timestamp.deserialize(ser);
      expect(des).to.eql(val);
    });

    it('can use UUID type', function() {
      var val = '10baa2da-24a1-47e7-a0aa-65ff301e5e92';
      var ser = types.uuid.serialize(val);
      var des = types.uuid.deserialize(ser);
      expect(des).to.eql(val);
    });

    it('can use inet type', function() {
      var val = '127.0.0.1';
      var ser = types.inet.serialize(val);
      var des = types.inet.deserialize(ser);
      expect(des).to.eql(val);
    });

    it('can use TextType', function() {
      var val = 'Text Type';
      var ser = types.text.serialize(val);
      var des = types.text.deserialize(ser);
      expect(des).to.eql(val);
    });

    it('can use DecimalType', function() {
      var val = '1000000.245';
      var ser = types.decimal.serialize(val);
      var des = types.decimal.deserialize(ser);
      expect(des).to.eql(val);
    });

  });
});
