cql-client
==========

Node.js driver for cassandra. The driver uses Cassandra Binary Protocol v2.

*NOTICE* This module is NOT ready for production use.

Features
----------

- CQL binary protocol
- Auto reconnect failed connection

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

- Batch
- Result paging
- Authentication

License
----------
See [LICENSE](LICENSE)
Copyright (C) Suguru Namura

[cql-protocol]: <https://github.com/yukim/cql-protocol/>

