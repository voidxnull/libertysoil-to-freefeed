#!/usr/bin/env node

const fs = require('fs');
const path = require('path');

const program = require('commander');
const ora = require('ora');
const JSONStream = require('JSONStream');

const { createLsConnection, createFfConnection } = require('./db');

program
  .version('1.0.0')
  .option('-d, --dump-dir [path]', 'Directory containing dump files (users.js, comments.js, etc.)', process.cwd())
  .option('--ls-conn [str]', 'Libertysoil connection string')
  .option('--ff-conn [str]', 'Freefeed connection string');

program
  .command('dump-ls-db')
  .description('Dumps libertysoil tables to json files')
  .action(asyncAction(dumpLsTables, 'Dumping LibertySoil database'));

program
  .command('import [tables...]')
  .description('Import specified freefeed tables from libertysoil dumps.')
  .action(asyncAction(importCommand))

program.parse(process.argv);

if (!process.argv.slice(2).length) {
  program.help();
}

function asyncAction(command, title = null) {
  return async (...args) => {
    const spinner = ora(title).start();
    try {
      await command(...args);
      spinner.succeed(`${title} - Done`);
    } catch (e) {
      spinner.fail(`${title} - Failed`);
      console.error(e.stack || e);
      process.exit(1);
    }
  }
}

async function forEachRowChunked(client, table, callback, options = { batch: 1000 }) {
  const batchSize = options.batch || 1000;
  const numRows = (await client.query(`SELECT count(*) FROM ${table}`)).rows[0].count;

  for (let offset = 0; offset < numRows; offset += batchSize) {
    const rows = (await client.query(`SELECT * FROM ${table} LIMIT ${batchSize} OFFSET ${offset}`)).rows;

    for (const row of rows) {
      callback(row);
    }
  }
}

async function dumpTable(client, table, outDir) {
  const filePath = path.join(outDir, `${table}.json`);
  const transformStream = JSONStream.stringify();
  const outputStream = fs.createWriteStream(filePath);
  transformStream.pipe(outputStream);

  await forEachRowChunked(client, table, function (row) {
    transformStream.write(row);
  });

  transformStream.end();

  return new Promise(function (resolve, reject) {
    outputStream.on(
      "finish",
      function handleFinish() {
        resolve();
      }
    );

    outputStream.on(
      "error",
      function handleError(e) {
        reject(e);
      }
    );
  });
}

async function dumpLsTables() {
  const client = await createLsConnection(program);
  const dumpPath = path.resolve(program.dumpDir);

  await dumpTable(client, 'users', dumpPath);
  await dumpTable(client, 'posts', dumpPath);
  await dumpTable(client, 'comments', dumpPath);
  await dumpTable(client, 'followers', dumpPath);
  await dumpTable(client, 'likes', dumpPath);

  client.end();
}

const ALL_FREEFEED_TABLES = ['users', 'posts', 'comments', 'subscriptions', 'likes'];

async function importCommand(tables) {
  if (tables.length === 0) {
    tables = ALL_FREEFEED_TABLES;
  }

  for (const table of tables) {
    switch (table) {
      case 'users': await importUsers(); break;
      case 'posts': break;
      case 'comments': break;
      case 'subscriptions': break;
      case 'likes': break;
      default: throw Error(`Unknown table: '${table}'`);
    }
  }
}

async function importUsers() {
  const client = await createFfConnection(program);
  const usersPath = path.resolve(path.join(program.dumpDir, 'users.json'));
  const fileStream = fs.createReadStream(usersPath);
  const stream = fileStream.pipe(JSONStream.parse());

}
