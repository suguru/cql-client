cql-client
==========

[![Build Status](https://travis-ci.org/suguru/cql-client.png)](https://travis-ci.org/suguru/cql-client)

Node.js driver for cassandra. The driver uses Cassandra Binary Protocol v2.

**NOTICE** This module is *NOT* ready for production use yet.

Features
----------

- Support CQL binary protocol v2
- Auto detection for cluster peers
- Auto fail-over and recover connections
- Retry queries
- Listening events
- Paging large result set using paging state

Quick start
----------

```js
var client = require('cql-client').createClient({
  hosts: ['127.0.0.1']
});

client.execute('SELECT * FROM system.peers', function(err, rs) {
  var rows = rs.rows;
  ..
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
batch.add('UPDATE table SET v1 = \'100\' WHERE id = 1000');
batch.add('UPDATE table SET v1 = ? WHERE id = ?', ['101', 1001]);
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

Copyright (C) CyberAgent, Inc.

patched cql-protocol. See [LICENSE](LICENSE.cql-protocol) and [cql-protocol](https://github.com/yukim/cql-protocol/)

