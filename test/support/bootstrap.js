/**
 * Support functions for helping with Postgres tests
 */

var pg = require('pg');

var Support = module.exports = {};

Support.Config = {
  host: 'localhost',
  user: process.env.DB_USER || 'root',
  password: process.env.DB_PASS || '',
  database: 'sailspg',
  port: 5432
};

Support.Setup = function(tableName, cb) {
  var client = new pg.Client(Support.Config);
  client.connect(function(err) {
    createTable(tableName, client, function() {
      cb();
    });
  });
};

Support.Teardown = function(tableName, cb) {
  var client = new pg.Client(Support.Config);
  client.connect(function(err) {
    dropTable(tableName, client, function() {
      cb();
    });
  });
};

Support.Client = function(cb) {
  var client = new pg.Client(Support.Config);
  client.connect(function(err) {
    cb(err, client);
  });
};

Support.Seed = function(tableName, cb) {
  var client = new pg.Client(Support.Config);
  client.connect(function(err) {
    createRecord(tableName, client, function() {
      cb();
    });
  });
};

function createTable(table, client, cb) {
  table = '"' + table + '"';

  var query = [
    "CREATE TABLE " + table + ' (',
    "id SERIAL PRIMARY KEY,",
    "field_1 TEXT NOT NULL,",
    "field_2 TEXT NOT NULL",
    ");"
  ].join('');

  client.query(query, cb);
}

function dropTable(table, client, cb) {
  table = '"' + table + '"';

  var query = "DROP TABLE " + table + ';';
  client.query(query, cb);
}

function createRecord(table, client, cb) {
  table = '"' + table + '"';

  var query = [
  "INSERT INTO " + table + ' (field_1, field_2)',
  "values ('foo', 'bar');"
  ].join('');

  client.query(query, cb);
}