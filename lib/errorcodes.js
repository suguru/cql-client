// refer https://github.com/apache/cassandra/blob/trunk/doc/native_protocol_v2.spec
module.exports = {

  unexpected: 0x0000,
  protocol  : 0x000A,
  badCredentials: 0x0100,
  unavailable: 0x1000,
  overload: 0x1001,
  boostrapping: 0x1002,
  truncate: 0x1003,
  writeTimeout: 0x1100,
  readTimeout: 0x1200,
  syntax: 0x2000,
  unauthorized: 0x2100,
  invalid: 0x2200,
  config: 0x2300,
  alreadyExists: 0x2400,
  unprepared: 0x2500

};
