cql-client
==========

[![Build Status](https://travis-ci.org/suguru/cql-client.png)](https://travis-ci.org/suguru/cql-client)

Node.js driver for cassandra on Cassandra Binary Protocol v2.

Features
----------

- Support CQL binary protocol v2
- Auto detection for cluster peers
- Auto fail-over and recover connections
- Retry queries when disconnected from servers
- Listening server events
- Paging large result set using paging state

Quick start
----------

```js
var client = require('cql-client').createClient({
  hosts: ['127.0.0.1']
});

client.execute('SELECT * FROM system.peers', function(err, rs) {
  var rows = rs.rows;
  rows.forEach(function(row) {
    console.log(row.peer);
    console.log(row.host_id);
    ..
  });
});
```

Usage
----------

Install library

```
npm install cql-client --save
```

Create a client

```js
var cql = require('cql-client');
var client = cql.createClient({ hosts: ['127.0.0.1'], keyspace: 'ks' });
```

Execute queries

```js
client.execute('SELECT * FROM table', function(err, rs) {
  rs.rows.forEach(function(row) {
    console.log(row);
  });
});
```

Execute prepared queries

```js
client.execute(
  'INSERT INTO table (id, v1, v2) VALUES (?, ?, ?)',
  [1000, 'foo', 'bar'],
  function(err) {
    ...
  }
);
```

Batch

```js
var batch = client.batch();
batch.add('UPDATE table SET v1 = ? WHERE id = ?', ['101', 1001]);
batch.add('UPDATE table SET v1 = ? WHERE id = ?', ['101', 1001]);
batch.add('DELETE FROM table WHERE id = ?', ['102']);
batch.commit(function(err) {
  ..
});
```

Read data with cursor

```js
var cursor = client.execute('SELECT * FROM table', function(err, rs) {
  var cursor = rs.cursor();
  cursor.on('row', function(row) {
    console.log(row);
  });
  cursor.on('error', function(err) {
    ..
  });
  cursor.on('end', function() {
    console.log('done');
  });
});
```

TODO
----------

- Authentication
- LRU cache for prepared queries

License
----------

See [LICENSE](LICENSE)

Copyright (C) Suguru Namura

patched cql-protocol. See [LICENSE](LICENSE.cql-protocol) and [cql-protocol](https://github.com/yukim/cql-protocol/)

