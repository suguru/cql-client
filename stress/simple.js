
var cql = require('../');
var client = cql.createClient({hosts:'127.0.0.1'});
var async = require('async');
var uuid = require('node-uuid');

async.series({
  drop: function(done) {
    client.execute('DROP KEYSPACE IF EXISTS stress_test', done);
  },
  create: function(done) {
    client.execute("CREATE KEYSPACE stress_test WITH replication={'class':'SimpleStrategy','replication_factor':1}", done);
  },
  table: function(done) {
    client.execute("CREATE TABLE stress_test.stress (id uuid primary key, name text, value int)", done);
  }
}, function(err) {
  if (err) {
    console.error(err.message);
    client.close();
  } else {
    start();
  }
});

function start() {
  console.log("Starting...");
  var count = 0;
  var concurrent = 4;
  var queue = async.queue(function(task, callback) {
    count++;
    if (count % 1000 === 0) {
      var usage = process.memoryUsage();
      console.log(count, usage.heapUsed, usage.rss);
      global.gc();
    }
    var id = uuid.v4();
    var name = 'NAME' + count;
    var value = count;
    async.series({
      step1: function(done) {
        client.execute(
          "INSERT INTO stress_test.stress (id, name, value) VALUES (?, ?, ?)",
          [id, name, value],
          done
        );
      },
      step2: function(done) {
        client.execute(
          "SELECT * FROM stress_test.stress WHERE id = ?",
          [id],
          done
        );
      }
    }, function(err, res) {
      if (err) {
        console.log('ERROR:', err.message);
      }
      queue.push({});
      var row = res.step2.rows[0];
      if (!row) {
        callback(new Error("ROW IS NULL " + id));
      } else if (row.id !== id) {
        callback(new Error("ID IS INVALID " + row.id + " != " + id));
      } else {
        callback();
      }
    });
  }, concurrent);
  queue.drain = function() {
    console.log("Done");
    client.close();
  };
  for (var i = 0; i < concurrent; i++) {
    queue.push({});
  }
}


