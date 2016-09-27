/**
 * Support functions for helping with Postgres tests
 */

var pg = require('pg'),
    _ = require('lodash'),
    adapter = require('../../../lib/adapter');

var Support = module.exports = {};

Support.SqlOptions = {
  parameterized: true,
  caseSensitive: true,
  escapeCharacter: '"',
  casting: true,
  canReturnValues: true,
  escapeInserts: true,
  declareDeleteAlias: false
};

Support.Config = {
  host: process.env.POSTGRES_1_PORT_5432_TCP_ADDR || process.env.WATERLINE_ADAPTER_TESTS_HOST || 'localhost',
  user: process.env.POSTGRES_ENV_POSTGRES_USER || process.env.WATERLINE_ADAPTER_TESTS_USER || 'sails',
  password: process.env.POSTGRES_ENV_POSTGRES_PASSWORD || process.env.WATERLINE_ADAPTER_TESTS_PASSWORD || 'sails',
  database: process.env.POSTGRES_ENV_POSTGRES_DB || process.env.WATERLINE_ADAPTER_TESTS_DATABASE || 'sailspg',
  port: process.env.POSTGRES_PORT_5432_TCP_PORT || process.env.WATERLINE_ADAPTER_TESTS_PORT || 5432
};

// Fixture Collection Def
Support.Collection = function(name, def) {
  var schemaDef = {};
  schemaDef[name] = Support.Schema(name, def);
  return {
    identity: name,
    tableName: name,
    connection: 'test',
    definition: def || Support.Definition,
    waterline: { schema: schemaDef }
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

Support.Schema = function(name, def) {
  return {
    connection: 'test',
    identity: name,
    tableName: name,
    attributes: def || Support.Definition
  };
}

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
