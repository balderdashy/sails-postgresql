/**
 * Support functions for helping with Postgres tests
 */

var _ = require('lodash');
var PG = require('machinepack-postgresql');
var adapter = require('../../../lib/adapter');

var Support = module.exports = {};

// Support.SqlOptions = {
//   parameterized: true,
//   caseSensitive: true,
//   escapeCharacter: '"',
//   casting: true,
//   canReturnValues: true,
//   escapeInserts: true,
//   declareDeleteAlias: false
// };

Support.Config = {
  host: process.env.POSTGRES_1_PORT_5432_TCP_ADDR || process.env.WATERLINE_ADAPTER_TESTS_HOST || 'localhost',
  user: process.env.POSTGRES_ENV_POSTGRES_USER || process.env.WATERLINE_ADAPTER_TESTS_USER || 'sails',
  password: process.env.POSTGRES_ENV_POSTGRES_PASSWORD || process.env.WATERLINE_ADAPTER_TESTS_PASSWORD || 'sails',
  database: process.env.POSTGRES_ENV_POSTGRES_DB || process.env.WATERLINE_ADAPTER_TESTS_DATABASE || 'sailspg',
  port: process.env.POSTGRES_PORT_5432_TCP_PORT || process.env.WATERLINE_ADAPTER_TESTS_PORT || 5432
};

// Fixture Collection Def
Support.Collection = function collection(name, def) {
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
  fieldA: { type: 'string' },
  fieldB: { type: 'string' },
  id: {
    type: 'integer',
    autoIncrement: true,
    defaultsTo: 'AUTO_INCREMENT',
    primaryKey: true
  }
};

Support.Schema = function schema(name, def) {
  return {
    connection: 'test',
    identity: name,
    tableName: name,
    attributes: def || Support.Definition
  };
};

// Register and Define a Collection
Support.Setup = function setup(tableName, cb) {
  var collection = Support.Collection(tableName);
  var collections = {};
  collections[tableName] = collection;

  var connection = _.cloneDeep(Support.Config);
  connection.identity = 'test';

  adapter.registerConnection(connection, collections, function registerCb(err) {
    if (err) {
      return cb(err);
    }

    adapter.define('test', tableName, Support.Definition, cb);
  });
};

// Just register a connection
Support.registerConnection = function registerConnection(tableNames, cb) {
  var collections = {};

  _.each(tableNames, function processTable(name) {
    var collection = Support.Collection(name);
    collections[name] = collection;
  });

  var connection = _.cloneDeep(Support.Config);
  connection.identity = 'test';

  adapter.registerConnection(connection, collections, cb);
};

// Remove a table and destroy the manager
Support.Teardown = function teardown(tableName, cb) {
  var manager = adapter._datastores[_.first(_.keys(adapter._datastores))].manager;
  PG.getConnection({
    manager: manager,
    meta: Support.Config
  }).exec(function getConnectionCb(err, report) {
    if (err) {
      return cb(err);
    }

    var query = 'DROP TABLE IF EXISTS \"' + tableName + '";';
    PG.sendNativeQuery({
      connection: report.connection,
      nativeQuery: query
    }).exec(function dropTableCb(err) {
      if (err) {
        return cb(err);
      }

      PG.releaseConnection({
        connection: report.connection
      }).exec(function releaseConnectionCb(err) {
        if (err) {
          return cb(err);
        }

        delete adapter._datastores[_.first(_.keys(adapter._datastores))];
        return cb();
      });
    });
  });
};

// Seed a record to use for testing
Support.Seed = function seed(tableName, cb) {
  var manager = adapter._datastores[_.first(_.keys(adapter._datastores))].manager;
  PG.getConnection({
    manager: manager,
    meta: Support.Config
  }).exec(function getConnectionCb(err, report) {
    if (err) {
      return cb(err);
    }

    var query = [
      'INSERT INTO "' + tableName + '" ("fieldA", "fieldB") ',
      'values (\'foo\', \'bar\'), (\'foo_2\', \'bAr_2\');'
    ].join('');

    PG.sendNativeQuery({
      connection: report.connection,
      nativeQuery: query
    }).exec(function seedCb(err) {
      if (err) {
        return cb(err);
      }

      PG.releaseConnection({
        connection: report.connection
      }).exec(cb);
    });
  });
};
