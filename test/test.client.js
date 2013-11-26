/* global describe,it,before,beforeEach,afterEach */

var cql = require('../');
var expect = require('expect.js');
var uuid = require('node-uuid');

describe('Client', function() {

  var client;

  var createClient = function(options) {
    options = options || {};
    var client = cql.createClient({
      hosts: ['127.0.0.1:9042'],
      connsPerHost: options.connsPerHost || 1,
      keyspace: options.keyspace
    });
    // client.on('log', function(level, msg) { console.log('LOG','-',level,msg); });
    client.on('error', function(err) {
      console.log('ERROR', err.stack || err.message);
    });
    return client;
  };

  before(function(done) {
    // client = new cql.createClient({ hosts: ['192.168.35.11'] });
    client = createClient();
    client.on('ready', done);
  });

  before(function(done) {
    client.execute('DROP KEYSPACE IF EXISTS cql_client', done);
  });

  before(function(done) {
    client.execute("CREATE KEYSPACE cql_client WITH replication={'class':'SimpleStrategy','replication_factor':1}", function(err, schema) {
      if (err) {
        return done(err);
      }
      expect(schema.change).to.eql('CREATED');
      expect(schema.keyspace).to.eql('cql_client');
      expect(schema).to.be.ok();
      done();
    });
  });

  before(function(done) {
    client.close();
    client = createClient({
      connsPerHost: 2,
      keyspace: 'cql_client'
    });
    client.on('ready', done);
  });


  beforeEach(function(done) {
    client.execute("DROP TABLE IF EXISTS client_test", done);
  });

  beforeEach(function(done) {
    client.execute("CREATE TABLE client_test (id timeuuid, value1 text, value2 int, PRIMARY KEY(id))", done);
  });

  describe('#execute', function() {

    it('should insert and select data', function(done) {

      var id1 = uuid.v1();

      client.execute(
        'INSERT INTO client_test (id,value1,value2) values (?,?,?)',
        [ id1, 'test-value', 100],
        function(err) {
          if (err) {
            return done(err);
          }
          client.execute(
            'SELECT * FROM client_test WHERE id = ?',
            [ id1 ],
            function(err, rs) {
              if (err) {
                return done(err);
              }
              expect(rs).to.be.ok();
              expect(rs.rows).to.have.length(1);
              expect(rs.rows[0]).to.eql({
                id: id1,
                value1: 'test-value',
                value2: 100
              });
              done();
            }
          );
        }
      );
    });
  });
});
