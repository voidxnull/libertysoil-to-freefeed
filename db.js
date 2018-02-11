const { Client } = require('pg');

exports.createLsConnection = async function createLsConnection(program) {
  let client;
  if (program.lsConn) {
    client = new Client({
      connectionString: program.lsConn,
    });
  } else {
    client = new Client({
      user: 'libertysoil',
      password: 'libertysoil',
      database: 'libertysoil',
    });
  }

  await client.connect();

  return client;
}

exports.createFfConnection = async function createFfConnection(program) {
  let client;
  if (program.ffConn) {
    client = new Client({
      connectionString: program.ffConn,
    });
  } else {
    client = new Client({
      user: 'freefeed',
      password: 'freefeed',
      database: 'freefeed',
    });
  }

  await client.connect();

  return client;
}
