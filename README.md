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
- Paging large result set with v2 paging state

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

TODO
----------

- Result paging
- Retry queries
- Authentication
- LRU cache for prepared queries

License
----------

See [LICENSE](LICENSE)

Copyright (C) CyberAgent, Inc.

Patched cql-protocol. See [LICENSE](LICENSE.cql-protocol) and [cql-protocol](https://github.com/yukim/cql-protocol/)

