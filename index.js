#!/usr/bin/env node

const fs = require('fs');
const path = require('path');

const program = require('commander');
const ora = require('ora');
const JSONStream = require('JSONStream');

const { createLsConnection, createFfConnection } = require('./db');

program
  .version('1.0.0')
  .option('-d, --dump-dir [path]', 'directory containing dump files (users.js, comments.js, etc.)')
  .option('--ls-conn [str]', 'libertysoil connection string')
  .option('--ff-conn [str]', 'freefeed connection string');

program
  .command('dump-ls-db')
  .description('dumps libertysoil tables to json files')
  .action(asyncAction(dumpLsTables, 'Dumping LibertySoil database'));

program.parse(process.argv);

if (!process.argv.slice(2).length) {
  program.help();
}

function asyncAction(command, title) {
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

async function dumpLsTables() {
  const client = await createLsConnection(program);
  const dumpPath = path.resolve(program.dumpDir || process.cwd());

  await dumpTable(client, 'users', dumpPath);
  await dumpTable(client, 'posts', dumpPath);
  await dumpTable(client, 'comments', dumpPath);
  await dumpTable(client, 'followers', dumpPath);
  await dumpTable(client, 'likes', dumpPath);

  client.end();
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
