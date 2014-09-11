/**
 * Support functions for helping with Postgres tests
 */

var pg = require('pg.js'),
    _ = require('lodash'),
    adapter = require('../../../lib/adapter');

var Support = module.exports = {};

Support.Config = {
  host: 'localhost',
  user: process.env.DB_USER || 'root',
  password: process.env.DB_PASS || '',
  database: 'sailspg',
  port: 5432
};

// Fixture Collection Def
Support.Collection = function(name) {
  return {
    identity: name,
    connection: 'test',
    definition: Support.Definition
  };
};

// Fixture Table Definition
Support.Definition = {
  field_1: { type: 'string' },
  field_2: { type: 'string' },
  id: {
    type: 'integer',
    autoIncrement: true,
    defaultsTo: 'AUTO_INCREMENT',
    primaryKey: true
  }
};

// Register and Define a Collection
Support.Setup = function(tableName, cb) {

  var collection = Support.Collection(tableName);

  var collections = {};
  collections[tableName] = collection;

  var connection = _.cloneDeep(Support.Config);
  connection.identity = 'test';

  adapter.registerConnection(connection, collections, function(err) {
    if(err) return cb(err);
    adapter.define('test', tableName, Support.Definition, function(err) {
      if(err) return cb(err);
      cb();
    });
  });
};

// Just register a connection
Support.registerConnection = function(tableNames, cb) {
  var collections = {};

  tableNames.forEach(function(name) {
    var collection = Support.Collection(name);
    collections[name] = collection;
  });

  var connection = _.cloneDeep(Support.Config);
  connection.identity = 'test';

  adapter.registerConnection(connection, collections, cb);
};

// Remove a table
Support.Teardown = function(tableName, cb) {
  pg.connect(Support.Config, function(err, client, done) {
    dropTable(tableName, client, function(err) {
      if(err) {
        done();
        return cb(err);
      }

      adapter.teardown('test', function(err) {
        done();
        cb();
      });

    });
  });
};

// Return a client used for testing
Support.Client = function(cb) {
  pg.connect(Support.Config, cb);
};

// Seed a record to use for testing
Support.Seed = function(tableName, cb) {
  pg.connect(Support.Config, function(err, client, done) {
    createRecord(tableName, client, function(err) {
      if(err) {
        done();
        return cb(err);
      }

      done();
      cb();
    });
  });
};

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
