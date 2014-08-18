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

    it('should connect with auto detect', function(done) {
      var cli = new cql.Client({ host: '127.0.0.1', autoDetect: true });
      cli.on('connect', function() {
        done();
      });
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

    it('should prepare again when unprepared', function(done) {

      var query = 'SELECT * FROM client_test';

      client.execute(query, [], function(err) {
        if (err) {
          return done(err);
        }
        var conn = client._conns[0];
        expect(conn._preparedCache.has(query)).to.be(true);
        expect(conn.unprepare(query));
        expect(conn._preparedCache.has(query)).to.be(false);

        client.execute(query, [], function(err, rs) {
          if (err) {
            return done(err);
          }
          expect(conn._preparedCache.has(query)).to.be(true);
          expect(rs.rows).to.have.length(0);
          done();
        });
      });

    });

    it('should prepare again when unprepared on cassandra', function(done) {

      var query = 'SELECT * FROM client_test';

      client.execute(query, [], function(err) {
        if (err) {
          return done(err);
        }
        var conn = client._conns[0];
        expect(conn._preparedCache.has(query)).to.be(true);
        var prepared = conn._preparedCache.get(query);
        // modify to simulate unprepared query
        prepared.id[0]++;
        client.execute(query, [], function(err, rs) {
          expect(conn._preparedCache.has(query)).to.be(true);
          expect(rs.rows).to.have.length(0);
          done();
        });
      });

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

    it('should override insert value', function(done) {

      var id1 = uuid.v1();

      client
      .batch()
      .add('INSERT INTO client_test (id,value1,value2) VALUES (?,?,?)', [id1, 'v1', 100])
      .add('INSERT INTO client_test (id,value1,value2) VALUES (?,?,?)', [id1, 'v11', 200])
      .add('INSERT INTO client_test (id,value1,value2) VALUES (?,?,?)', [id1, 'v111', 300])
      .commit(function(err) {
        if (err) {
          return done(err);
        }

        client
        .execute('SELECT * FROM client_test', function(err, rs) {
          if (err) {
            return done(err);
          }
          expect(rs.rows).to.have.length(1);
          expect(findRow(rs, id1)).to.eql({ id: id1, value1: 'v111', value2: 300 });
          done();
        });
      });

    });

    it('should override with large batch commits', function(done) {

      var id1 = uuid.v1();

      var batch = client.batch();

      var text = 'test';
      for (var i = 0; i < 100; i++) {
        text = text + '1234567890123456789012345678901234567890';
        batch.add('INSERT INTO client_test (id,value1,value2) VALUES (?,?,?)', [id1, text, i]);
      }
      batch.commit(function(err) {
        if (err) {
          return done(err);
        }

        client
        .execute('SELECT * FROM client_test', function(err, rs) {
          if (err) {
            return done(err);
          }
          expect(rs.rows).to.have.length(1);
          expect(findRow(rs, id1)).to.eql({ id: id1, value1: text, value2: 99 });
          done();
        });
      });

    });

    it('should commit again when unprepared', function(done) {

      // stub
      client._conns.forEach(function(conn) {
        var count = 0;
        var batch = conn.batch;
        var unprepared = 0x2500;
        conn.batch = function(list, options, callback) {

          if (typeof options === 'function') {
            callback = options;
            options = {};
          } else {
            options = options || {};
          }

          if (count++ === 0) {
            return callback({ code: unprepared });
          }

          batch.call(conn, list, options, callback);
        };
      });

      var id1 = uuid.v1();

      client
      .batch()
      .add('INSERT INTO client_test (id,value1,value2) VALUES (?,?,?)', [id1, 'v1', 100])
      .commit(function(err) {
        if (err) {
          return done(err);
        }

        client
        .execute('SELECT * FROM client_test', function(err, rs) {
          if (err) {
            return done(err);
          }
          expect(rs.rows).to.have.length(1);
          expect(findRow(rs, id1)).to.eql({ id: id1, value1: 'v1', value2: 100 });
          done();
        });
      });

    });


  });

  describe('#cursor', function() {

    beforeEach(function(done) {
      var sql = 'INSERT INTO client_test (id, value1, value2) VALUES (?, ?, ?)';
      var batch = client.batch();
      for (var i = 0; i < 100; i++) {
        batch.add(sql, [uuid.v1(), 'value-'+i, i * 100]);
      }
      batch.commit(done);
    });

    it('should invoke row event', function(done) {
      client.execute('SELECT * FROM client_test', function(err, rs) {
        if (err) {
          return done(err);
        }
        expect(rs.rows).to.have.length(100);
        var count = 0;
        var cursor = rs.cursor();
        cursor.on('row', function() {
          count++;
        });
        cursor.on('error', done);
        cursor.on('end', function() {
          expect(count).to.eql(100);
          done();
        });
      });
    });

    it('should get next result', function(done) {

      var keys = {};

      client.execute('SELECT * FROM client_test', { pageSize: 20 }, function(err, rs) {
        if (err) {
          return done(err);
        }
        expect(rs.rows).to.have.length(20);
        rs.rows.forEach(function(row) {
          expect(row).to.only.have.keys('id','value1','value2');
          expect(keys).to.not.have.key(row.id);
          keys[row.id] = true;
        });
        expect(rs.hasNext()).to.be.ok();
        rs.next(function(err, rs) {
          if (err) {
            return done(err);
          }
          expect(rs.rows).to.have.length(20);
          rs.rows.forEach(function(row) {
            expect(row).to.only.have.keys('id','value1','value2');
            expect(keys).to.not.have.key(row.id);
            keys[row.id] = true;
          });
          expect(rs.hasNext()).to.be.ok();
          done();
        });
      });

    });

    it('should fail with next', function(done) {

      client.execute('SELECT * FROM client_test', { pageSize: 20 }, function(err, rs) {
        if (err) {
          return done(err);
        }
        // simulate error with bad query
        rs.query = 'SELECT * FROM client_invalid';
        rs.next(function(err) {
          expect(err).to.be.ok();
          done();
        });
      });

    });

    it('should iterate through paging', function(done) {

      var pageSize = 20;
      var fetchSize = Math.ceil(100 / pageSize);

      client.execute('SELECT * FROM client_test', { pageSize: pageSize }, function(err, rs) {
        if (err) {
          return done(err);
        }
        expect(rs.rows).to.have.length(pageSize);
        var cursor = rs.cursor();
        var rowCount = 0;
        var fetchCount = 0;
        cursor.on('fetch', function() {
          fetchCount++;
        });
        cursor.on('row', function(row) {
          expect(row).to.have.keys('id','value1','value2');
          rowCount++;
        });
        cursor.on('error', done);
        cursor.on('end', function() {
          expect(rowCount).to.eql(100);
          // TODO - cassandra 2.0.4 has bug which return fetchSize +1
          expect(fetchCount === fetchSize || fetchCount === fetchSize - 1);
          // expect(fetchCount).to.eql(fetchSize);
          done();
        });
      });

    });

    it('should iterate through paging with prepared query', function(done) {
      client.execute('SELECT * FROM client_test LIMIT 41', function(err, rs) {
        if (err) {
          return done(err);
        }
        expect(rs.rows).to.have.length(41);
        var firstId = rs.rows[0].id;
        var firstRows = rs.rows;
        client.execute(
          'SELECT * FROM client_test WHERE token(id) > token(?)',
          [firstId],
          { pageSize: 20 },
          function(err, rs) {
            if (err) {
              return done(err);
            }
            for (var i = 0; i < rs.rows.length; i++) {
              expect(rs.rows[i].id).to.eql(firstRows[i+1].id);
            }
            rs.next(function(err, rs) {
              if (err) {
                return done(err);
              }
              expect(rs.rows).to.have.length(20);
              for (var i = 0; i < rs.rows.length; i++) {
                expect(rs.rows[i].id).to.eql(firstRows[i+21].id);
              }
              done();
            });
          });
      });
    });

    it('should fail with prepared query', function(done) {
      client.execute('SELECT * FROM client_test LIMIT 1', function(err, rs) {
        if (err) {
          return done(err);
        }
        expect(rs.rows).to.have.length(1);
        client.execute(
          'SELECT * FROM client_test WHERE token(id) > token(?)',
          [rs.rows[0].id],
          { pageSize: 20 },
          function(err, rs) {
            if (err) {
              return done(err);
            }
            // simulate error with bad paging state
            rs.metadata.pagingState[0]++;
            rs.next(function(err) {
              expect(err).to.be.ok();
              done();
            });
          }
        );
      });
    });

    it('should fail while caused error', function(done) {
      client.execute('SELECT * FROM client_test', { pageSize: 20 }, function(err, rs) {
        if (err) {
          return done(err);
        }
        expect(rs.rows).to.have.length(20);
        // simulate error with bad query
        rs.query = 'SELECT * FROM client_bad';
        var cursor = rs.cursor();
        cursor.on('error', function(err) {
          expect(err).to.be.ok();
          done();
        });
      });
    });

    it('should ends immediate when aborted', function(done) {
      client.execute('SELECT * FROM client_test', { pageSize: 20 }, function(err, rs) {
        var count = 0;
        if (err) {
          return done(err);
        }
        var cursor = rs.cursor();
        cursor.on('row', function() {
          count++;
          if (count === 25) {
            cursor.abort();
          }
        });
        cursor.on('end', function() {
          expect(count).to.eql(25);
          done();
        });
      });
    });
  });
});
