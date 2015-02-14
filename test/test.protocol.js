/* global describe, it */
var protocol = require('../lib/protocol/protocol');
var expect = require('expect.js');

describe('test ser/de messages', function() {
  var serde = new protocol.NativeProtocol();

  // default protocol version should be 2
  expect(serde.protocolVersion).to.eql(2);

  serde.pipe(serde);

  if (!serde.listeners('readable').length) {
    serde.on('readable', function() {
      var state = serde._readableState;
      state.ranOut = false;
      var chunk;
      do {
        chunk = serde.read();
      } while (null !== chunk && state.flowing);
    });
  }

  it('messages.Error', function(done) {
    var message = new protocol.messages.Error(0x0000, 'Server error');
    expect(message.opcode).to.eql(0x00)
    expect(message.errorCode).to.eql(0x0000)
    expect(message.errorMessage).to.eql('Server error')
    expect(message.details).to.eql({})

    serde.once('message', function(received) {
      // verify message
      expect(typeof received).to.eql(typeof message)
      expect(received.opcode).to.eql(message.opcode)
      expect(received.errorCode).to.eql(message.errorCode)
      expect(received.errorMessage).to.eql(message.errorMessage)
      expect(received.details).to.eql(message.details)
      done();
    });

    serde.sendMessage(message);
  });

  it('messages.Error with details(Unavailable)', function(done) {
    var details = {
      consistencyLevel: protocol.CONSISTENCY_LEVEL.ONE,
      required: 3,
      alive: 1
    },
    message = new protocol.messages.Error(0x1000, 'Unavailable exception', details);
    expect(message.opcode).to.eql(0x00)
    expect(message.errorCode).to.eql(0x1000)
    expect(message.errorMessage).to.eql('Unavailable exception')
    expect(message.details).to.eql(details)

    serde.once('message', function(received) {
      expect(typeof received).to.eql(typeof message)
      expect(received.opcode).to.eql(message.opcode)
      expect(received.errorCode).to.eql(message.errorCode)
      expect(received.errorMessage).to.eql(message.errorMessage)
      expect(received.details).to.eql(message.details)
      done();
    });

    serde.sendMessage(message);
  });

  it('messages.Startup', function(done) {
    var message = new protocol.messages.Startup();
    expect(message.opcode).to.eql(0x01)
    expect(message.options).to.eql({CQL_VERSION: '3.1.0'})

    serde.once('message', function(received) {
      expect(typeof received).to.eql(typeof message)
      expect(received.opcode).to.eql(message.opcode)
      expect(received.options).to.eql(message.options)
      done();
    });

    serde.sendMessage(message);
  });

  it('messages.Ready', function(done) {
    var message = new protocol.messages.Ready();
    expect(message.opcode).to.eql(0x02)

    serde.once('message', function(received) {
      expect(typeof received).to.eql(typeof message)
      expect(received.opcode).to.eql(message.opcode)
      done();
    });

    serde.sendMessage(message);
  });

  it('messages.Authenticate', function(done) {
    var message = new protocol.messages.Authenticate();
    message.authenticator = 'dummy authenticator';

    expect(message.opcode).to.eql(0x03)

    serde.once('message', function(received) {
      expect(typeof received).to.eql(typeof message)
      expect(received.opcode).to.eql(message.opcode)
      expect(received.authenticator).to.eql(message.authenticator)
      done();
    });

    serde.sendMessage(message);
  });

  it('messages.Credentials', function(done) {
    var message = new protocol.messages.Credentials();
    message.credentials = {user: 'dummy'};
    expect(message.opcode).to.eql(0x04)

    serde.once('message', function(received) {
      expect(typeof received).to.eql(typeof message)
      expect(received.opcode).to.eql(message.opcode)
      expect(received.credentials).to.eql(message.credentials)
      done();
    });
    serde.sendMessage(message);
  });

  it('messages.Options', function(done) {
    var message = new protocol.messages.Options();
    expect(message.opcode).to.eql(0x05)
    expect(message.isRequest).to.eql(true)

    serde.once('message', function(received) {
      expect(typeof received).to.eql(typeof message)
      expect(received.opcode).to.eql(message.opcode)
      done();
    });

    serde.sendMessage(message);
  });

  it('messages.Supported', function(done) {
    var message = new protocol.messages.Supported();
    message.options = {CQL_VERSION: ['3.1.0']};
    expect(message.opcode).to.eql(0x06)
    expect(message.isRequest).to.eql(false)

    serde.once('message', function(received) {
      expect(typeof received).to.eql(typeof message)
      expect(received.opcode).to.eql(message.opcode)
      done();
    });

    serde.sendMessage(message);
  });

  it('messages.Query', function(done) {
    var message = new protocol.messages.Query();
    message.streamId = 99;
    message.enableTracing();
    message.query = 'SELECT * FROM table';
    expect(message.opcode).to.eql(0x07)
    expect(message.isRequest).to.eql(true)

    serde.once('message', function(received) {
      expect(typeof received).to.eql(typeof message)
      expect(received.opcode).to.eql(message.opcode)
      expect(received.flags).to.eql(message.flags)
      expect(received.streamId).to.eql(message.streamId)
      expect(received.query).to.eql(message.query)
      done();
    });

    serde.sendMessage(message);
  });

  it('messages.Query with options', function(done) {
    var message = new protocol.messages.Query();
    message.query = 'SELECT * FROM table';
    message.options.pageSize = 5;
    message.options.pagingState = new Buffer('dummy paging state');

    serde.once('message', function(received) {
      expect(typeof received).to.eql(typeof message)
      expect(received.opcode).to.eql(message.opcode)
      expect(received.query).to.eql(message.query)
      expect(received.options.consistencyLevel).to.eql(1)
      expect(received.options.pageSize).to.eql(5)
      expect(received.options.pagingState).to.eql(new Buffer('dummy paging state'))
      done();
    });

    serde.sendMessage(message);
  });

  it('messages.Prepare', function(done) {
    var message = new protocol.messages.Prepare();
    message.query = 'SELECT * FROM table';
    expect(message.opcode).to.eql(0x09)
    expect(message.isRequest).to.eql(true)

    serde.once('message', function(received) {
      expect(typeof received).to.eql(typeof message)
      expect(received.opcode).to.eql(message.opcode)
      expect(received.query).to.eql(message.query)
      done();
    });

    serde.sendMessage(message);
  });

  it('messages.Execute', function(done) {
    var id = new Buffer('some prepare statement id'),
        v = new Buffer('some binding value'),
        message = new protocol.messages.Execute(id);
    message.options.setValues(v);
    expect(message.opcode).to.eql(0x0A)
    expect(message.isRequest).to.eql(true)

    serde.once('message', function(received) {
      expect(typeof received).to.eql(typeof message)
      expect(received.opcode).to.eql(message.opcode)
      expect(received.id).to.eql(message.id)
      expect(received.options.values[0]).to.eql(message.options.values[0])
      done();
    });

    serde.sendMessage(message);
  });

  it('messages.Register', function(done) {
    var message = new protocol.messages.Register();
    message.events = ['TOPOLOGY_CHANGE', 'STATUS_CHANGE', 'SCHEMA_CHANGE'];
    expect(message.opcode).to.eql(0x0B)
    expect(message.isRequest).to.eql(true)

    serde.once('message', function(received) {
      expect(typeof received).to.eql(typeof message)
      expect(received.opcode).to.eql(message.opcode)
      expect(received.events).to.eql(message.events)
      done();
    });

    serde.sendMessage(message);
  });

  it('messages.Event', function(done) {
    var message = new protocol.messages.Event();
    message.event = {
      type: 'SCHEMA_CHANGE',
      typeOfChange: 'DROP',
      keyspace: 'test',
      table: 'test_cf'
    };
    message.streamId = -1;
    expect(message.opcode).to.eql(0x0C)
    expect(message.isRequest).to.eql(false)

    serde.once('message', function(received) {
      expect(typeof received).to.eql(typeof message)
      expect(received.opcode).to.eql(message.opcode)
      expect(received.event).to.eql(message.event)
      done();
    });

    serde.sendMessage(message);
  });

  it('messages.Batch', function(done) {
    var message = new protocol.messages.Batch(0),
        i, j;
    expect(message.opcode).to.eql(0x0D)
    expect(message.type).to.eql(0)
    expect(message.queries.length).to.eql(0)
    expect(message.values.length).to.eql(0)
    expect(message.isRequest).to.eql(true)

    message.add('SELECT * FROM table');
    message.add(new Buffer('dummy prepare statement id'), [new Buffer('dummy value')]);

    expect(message.queries.length).to.eql(2)
    expect(message.values.length).to.eql(2)

    serde.once('message', function(received) {
      expect(typeof received).to.eql(typeof message)
      expect(received.opcode).to.eql(message.opcode)
      // since node-tap does not handle Buffers in an array...
      expect(received.queries.length).to.eql(received.values.length)
      expect(received.queries.length).to.eql(message.queries.length)
      expect(received.values.length).to.eql(message.values.length)
      for (i = 0; i < received.queries.length; i++) {
        expect(received.queries[i]).to.eql(message.queries[i])
        for (j = 0; j < received.values[i].length; j++) {
          expect(received.values[i][j]).to.eql(message.values[i][j])
        }
      }
      done();
    });

    serde.sendMessage(message);
  });

  it('messages.AuthChallenge', function(done) {
    var message = new protocol.messages.AuthChallenge();
    expect(message.token.length).to.eql(0)
    expect(message.opcode).to.eql(0x0E)
    expect(message.isRequest).to.eql(false)
    message.token = new Buffer('dummy challenge');

    serde.once('message', function(received) {
      expect(typeof received).to.eql(typeof message)
      expect(received.opcode).to.eql(message.opcode)
      expect(received.token).to.eql(message.token)
      done();
    });

    serde.sendMessage(message);
  });

  it('messages.AuthResponse', function(done) {
    var message = new protocol.messages.AuthResponse();
    expect(message.token.length).to.eql(0)
    expect(message.opcode).to.eql(0x0F)
    expect(message.isRequest).to.eql(true)
    message.token = new Buffer('dummy response');

    serde.once('message', function(received) {
      expect(typeof received).to.eql(typeof message)
      expect(received.opcode).to.eql(message.opcode)
      expect(received.token).to.eql(message.token)
      done();
    });

    serde.sendMessage(message);
  });

  it('messages.AuthSuccess', function(done) {
    var message = new protocol.messages.AuthSuccess();
    expect(message.token.length).to.eql(0)
    expect(message.opcode).to.eql(0x10)
    expect(message.isRequest).to.eql(false)
    message.token = new Buffer('dummy success');

    serde.once('message', function(received) {
      expect(typeof received).to.eql(typeof message)
      expect(received.opcode).to.eql(message.opcode)
      expect(received.token).to.eql(message.token)
      done();
    });

    serde.sendMessage(message);
  });

  it("teardown", function (done) {
    serde.shutdown();
    done();
  });
});
