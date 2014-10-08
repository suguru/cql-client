cql-client
==========

[![Build Status](https://img.shields.io/travis/suguru/cql-client.svg?style=flat)](https://travis-ci.org/suguru/cql-client)
[![Coverage Status](https://img.shields.io/coveralls/suguru/cql-client.svg?style=flat)](https://coveralls.io/r/suguru/cql-client)
[![Coverage Status](http://img.shields.io/codecov/c/github/suguru/cql-client.svg?style=flat)](https://codecov.io/github/suguru/cql-client)
[![npm](http://img.shields.io/npm/v/cql-client.svg?style=flat)](https://www.npmjs.org/package/cql-client)
[![license](http://img.shields.io/npm/l/express.svg?style=flat)](https://www.npmjs.org/package/cql-client)


Node.js driver for cassandra on Cassandra Binary Protocol v2.

Features
----------

- CQL binary protocol v2
- Automatic discovery of cluster peers
- Fail-over cluster peers
- Retry queries when disconnected from servers
- Paging large result set with paging state
- Events

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
API
----------

### CQL

#### cql.createClient([options])

Create a client with options. The created client will connect the cluster automatically.

* `options`
 * `hosts` List of cassandra hosts you are connecting to. Use array to connect multiple hosts.
 * `keyspace` Name of the default keyspace.
 * `autoDetect` Set true to detect peers automatically. (Optional)
 * `connsPerHost` The size of connection pool per host. (Default: 2)
 * `connectTimeout` Milliseconds to determine connections are timed out. (Default: 5000)
 * `reconnectInterval` Milliseconds of interval for reconnecting. (Default: 5000)
 * `preparedCacheSize` Size of cache for prepared statement. (Default: 10000)

### Client

#### client.connect(cb)

Connect to the cluster. Note that you do not need to call the method since client will connect automatically.

* `cb` callback when connected

#### client.close()

Close the connection to the cluster.

#### client.getAvailableConnection()

Get available connection from connections.

#### client.execute(query, [values], [options], callback)

Execute CQL. It will use prepared query when values are specified.

* `query` Query statement to be executed.
* `values` Array of bound values.
* `options`
 * `consistencyLevel` Consistency level of the query. cql.CL.ONE,THREE,QUORUM,ALL,LOCAL_QUORUM,EACH_QUORUM
 * `pageSize` Paging size of the result.
 * `pagingState` Paging state of the query.
 * `serialConsistency` Serial consistency of the query.
* `callback` Callback with error and result set.

#### client.batch()

Create batch instance. See Batch.

### ResultSet

Result data of the query.

#### resultSet.metadata

Metadata of result rows and columns.

#### resultSet.rows

Array of rows.

#### resultSet.cursor()

Create cursor instance of the result.

### Cursor

#### cursor.on(event, listener)

Register event listener.

* Events
 * `row` when cursor have an available row
 * `end` when cursor ends
 * `error` when error occurs

#### cursor.abort()

Abort cursor. It will fire `end` event.

### Batch

#### batch.add(query, [values])

Add query to the batch.

* `query` Query statement.
* `values` Array of values to be bound.

#### batch.option(options)

Set option parameters to the batch.

* `options` Same as options of client.execute()

#### batch.commit(callback)

Commit queries. It will clean up queries in the batch after committing.

TODO
----------

- Authentication

License
----------

See [LICENSE](LICENSE)

Copyright (C) Suguru Namura

patched cql-protocol. See [LICENSE](LICENSE.cql-protocol) and [cql-protocol](https://github.com/yukim/cql-protocol/)

