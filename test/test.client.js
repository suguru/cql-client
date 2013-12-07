/* global describe,it,before,beforeEach */

var cql = require('../');
var expect = require('expect.js');
var uuid = require('node-uuid');

describe('Client', function() {

  var client;

  var createClient = function(options) {
    options = options || {};
    var client = cql.createClient({
      hosts: '127.0.0.1:9042',
      // hosts: ['127.0.0.1:9042', '127.0.0.2:9042', '127.0.0.3:9042'],
      // hosts: ['192.168.35.11', '192.168.35.12'],
      connectTimeout: 200,
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
    client.on('connect', done);
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
    client.on('connect', done);
  });


  beforeEach(function(done) {
    client.execute("DROP TABLE IF EXISTS client_test", done);
  });

  beforeEach(function(done) {
    client.execute("CREATE TABLE client_test (id timeuuid, value1 text, value2 int, PRIMARY KEY(id))", done);
  });

  var findRow = function(rs, id) {
    for (var i = 0; i < rs.rows.length; i++) {
      var row = rs.rows[i];
      if (row.id === id) {
        return row;
      }
    }
    return null;
  };

  describe('#init', function() {
    it('should throw error without option', function(done) {
      try {
        new cql.Client();
      } catch (e) {
        expect(e).to.be.ok();
        return done();
      }
      done(new Error('error should be invoked'));
    });

    it('should throw invalid consistency level', function(done) {
      try {
        new cql.Client({ hosts: '127.0.0.1', consistencyLevel: 'blahblah' });
      } catch (e) {
        expect(e).to.be.ok();
        return done();
      }
      done(new Error('error should be invoked'));
    });
  });
  
  describe('#close', function() {
    it('can close after closed', function(done) {
      var cli = new cql.Client({hosts:'127.0.0.1'});
      cli.on('connect', function() {
        cli.close();
        setImmediate(function() {
          cli.close();
          done();
        });
      });
    });
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

  describe('#batch', function() {

    it('should do nothing without queries', function(done) {
      client.batch().commit(done);
    });

    it('should commit without values', function(done) {

      var id1 = uuid.v1();
      var id2 = uuid.v1();
      var id3 = uuid.v1();

      client
      .batch()
      .add('INSERT INTO client_test (id,value1,value2) VALUES (?,?,?)', [id1, 'v1', 100])
      .add('INSERT INTO client_test (id,value1,value2) VALUES (?,?,?)', [id2, 'v2', 200])
      .add('INSERT INTO client_test (id,value1,value2) VALUES (?,?,?)', [id3, 'v3', 300])
      .commit(function(err) {
        if (err) {
          return done(err);
        }

        client
        .execute('SELECT * FROM client_test', function(err, rs) {
          if (err) {
            return done(err);
          }
          expect(rs.rows).to.have.length(3);
          expect(findRow(rs, id1)).to.eql({ id: id1, value1: 'v1', value2: 100 });
          expect(findRow(rs, id2)).to.eql({ id: id2, value1: 'v2', value2: 200 });
          expect(findRow(rs, id3)).to.eql({ id: id3, value1: 'v3', value2: 300 });
          done();
        });
      });

    });
  });
});
