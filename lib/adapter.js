//  ███████╗ █████╗ ██╗██╗     ███████╗
//  ██╔════╝██╔══██╗██║██║     ██╔════╝
//  ███████╗███████║██║██║     ███████╗
//  ╚════██║██╔══██║██║██║     ╚════██║
//  ███████║██║  ██║██║███████╗███████║
//  ╚══════╝╚═╝  ╚═╝╚═╝╚══════╝╚══════╝
//
//  ██████╗  ██████╗ ███████╗████████╗ ██████╗ ██████╗ ███████╗███████╗ ██████╗ ██╗
//  ██╔══██╗██╔═══██╗██╔════╝╚══██╔══╝██╔════╝ ██╔══██╗██╔════╝██╔════╝██╔═══██╗██║
//  ██████╔╝██║   ██║███████╗   ██║   ██║  ███╗██████╔╝█████╗  ███████╗██║   ██║██║
//  ██╔═══╝ ██║   ██║╚════██║   ██║   ██║   ██║██╔══██╗██╔══╝  ╚════██║██║▄▄ ██║██║
//  ██║     ╚██████╔╝███████║   ██║   ╚██████╔╝██║  ██║███████╗███████║╚██████╔╝███████╗
//  ╚═╝      ╚═════╝ ╚══════╝   ╚═╝    ╚═════╝ ╚═╝  ╚═╝╚══════╝╚══════╝ ╚══▀▀═╝ ╚══════╝
//
// An adapter for PostgreSQL and Waterline

var _ = require('lodash');
var Helpers = require('../helpers');

module.exports = (function sailsPostgresql() {
  // Keep track of all the datastores used by the app
  var datastores = {};

  // Keep track of all the connection model definitions
  var modelDefinitions = {};

  var adapter = {
    identity: 'sails-postgresql',

    // Waterline Adapter API Version
    adapterApiVersion: 1,

    // Which type of primary key is used by default
    pkFormat: 'integer',

    syncable: true,

    defaults: {
      host: 'localhost',
      port: 5432,
      schema: true,
      ssl: false
    },

    //  ╔═╗═╗ ╦╔═╗╔═╗╔═╗╔═╗  ┌─┐┬─┐┬┬  ┬┌─┐┌┬┐┌─┐
    //  ║╣ ╔╩╦╝╠═╝║ ║╚═╗║╣   ├─┘├┬┘│└┐┌┘├─┤ │ ├┤
    //  ╚═╝╩ ╚═╩  ╚═╝╚═╝╚═╝  ┴  ┴└─┴ └┘ ┴ ┴ ┴ └─┘
    //  ┌┬┐┌─┐┌┬┐┌─┐┌─┐┌┬┐┌─┐┬─┐┌─┐┌─┐
    //   ││├─┤ │ ├─┤└─┐ │ │ │├┬┘├┤ └─┐
    //  ─┴┘┴ ┴ ┴ ┴ ┴└─┘ ┴ └─┘┴└─└─┘└─┘
    // This allows outside access to the connection manager.
    datastores: datastores,


    //  ╦═╗╔═╗╔═╗╦╔═╗╔╦╗╔═╗╦═╗  ┌─┐┌─┐┌┐┌┌┐┌┌─┐┌─┐┌┬┐┬┌─┐┌┐┌
    //  ╠╦╝║╣ ║ ╦║╚═╗ ║ ║╣ ╠╦╝  │  │ │││││││├┤ │   │ ││ ││││
    //  ╩╚═╚═╝╚═╝╩╚═╝ ╩ ╚═╝╩╚═  └─┘└─┘┘└┘┘└┘└─┘└─┘ ┴ ┴└─┘┘└┘
    // Register a connection config and generate a connection manager for it.
    registerConnection: function registerConnection(connectionConfig, models, cb) {
      var identity = connectionConfig.identity;
      if (!identity) {
        return cb(new Error('Invalid connection config. A connection should contain a unique identity property.'));
      }

      try {
        Helpers.registerDataStore({
          identity: identity,
          config: connectionConfig,
          models: models,
          datastores: datastores,
          modelDefinitions: modelDefinitions
        }).execSync();
      } catch (e) {
        setImmediate(function done() {
          return cb(e);
        });
        return;
      }

      setImmediate(function done() {
        return cb();
      });
    },


    //  ╔╦╗╔═╗╔═╗╦═╗╔╦╗╔═╗╦ ╦╔╗╔  ┌─┐┌─┐┌┐┌┌┐┌┌─┐┌─┐┌┬┐┬┌─┐┌┐┌
    //   ║ ║╣ ╠═╣╠╦╝ ║║║ ║║║║║║║  │  │ │││││││├┤ │   │ ││ ││││
    //   ╩ ╚═╝╩ ╩╩╚══╩╝╚═╝╚╩╝╝╚╝  └─┘└─┘┘└┘┘└┘└─┘└─┘ ┴ ┴└─┘┘└┘
    // Destroy a manager and close any connections in it's pool.
    teardown: function teardown(identity, cb) {
      Helpers.teardown({
        identity: identity,
        datastores: datastores,
        modelDefinitions: modelDefinitions
      }).exec({
        error: function error(err) {
          return cb(err);
        },
        success: function success() {
          return cb();
        }
      });
    },


    //  ╦═╗╦ ╦╔╗╔  ┌┐┌┌─┐┌┬┐┬┬  ┬┌─┐  ┌─┐ ┬ ┬┌─┐┬─┐┬ ┬
    //  ╠╦╝║ ║║║║  │││├─┤ │ │└┐┌┘├┤   │─┼┐│ │├┤ ├┬┘└┬┘
    //  ╩╚═╚═╝╝╚╝  ┘└┘┴ ┴ ┴ ┴ └┘ └─┘  └─┘└└─┘└─┘┴└─ ┴
    // Run a native SQL query using the give datastore.
    query: function query(datastoreName, tableName, query, data, cb) {
      var datastore = datastores[datastoreName];
      Helpers.query({
        datastore: datastore,
        query: query,
        data: data
      }).exec({
        error: function error(err) {
          return cb(err);
        },
        badConnection: function badConnection(err) {
          return cb(err);
        },
        success: function success(results) {
          return cb(null, results);
        }
      });
    },


    //  ██████╗ ██████╗ ██╗
    //  ██╔══██╗██╔══██╗██║
    //  ██║  ██║██║  ██║██║
    //  ██║  ██║██║  ██║██║
    //  ██████╔╝██████╔╝███████╗
    //  ╚═════╝ ╚═════╝ ╚══════╝
    //
    // Methods related to modifying the underlying data structure of the
    // database.


    //  ╔╦╗╔═╗╔═╗╔═╗╦═╗╦╔╗ ╔═╗  ┌┬┐┌─┐┌┐ ┬  ┌─┐
    //   ║║║╣ ╚═╗║  ╠╦╝║╠╩╗║╣    │ ├─┤├┴┐│  ├┤
    //  ═╩╝╚═╝╚═╝╚═╝╩╚═╩╚═╝╚═╝   ┴ ┴ ┴└─┘┴─┘└─┘
    // Describe a table and get back a normalized model schema format.
    // (This is used to allow Sails to do auto-migrations)
    describe: function describe(datastoreName, tableName, cb) {
      var datastore = datastores[datastoreName];
      Helpers.describe({
        datastore: datastore,
        tableName: tableName
      }).exec({
        error: function error(err) {
          return cb(err);
        },
        success: function success(report) {
          // Waterline expects the result to be undefined if the table doesn't
          // exist.
          if (_.keys(report.schema).length) {
            return cb(null, report.schema);
          }

          return cb();
        }
      });
    },


    //  ╔╦╗╔═╗╔═╗╦╔╗╔╔═╗  ┌┬┐┌─┐┌┐ ┬  ┌─┐
    //   ║║║╣ ╠╣ ║║║║║╣    │ ├─┤├┴┐│  ├┤
    //  ═╩╝╚═╝╚  ╩╝╚╝╚═╝   ┴ ┴ ┴└─┘┴─┘└─┘
    // Build a new table in the database.
    // (This is used to allow Sails to do auto-migrations)
    define: function define(datastoreName, tableName, definition, cb) {
      var datastore = datastores[datastoreName];
      Helpers.define({
        datastore: datastore,
        tableName: tableName,
        definition: definition
      }).exec({
        error: function error(err) {
          return cb(err);
        },
        success: function success() {
          return cb();
        }
      });
    },


    //  ╔═╗╦═╗╔═╗╔═╗╔╦╗╔═╗  ┌─┐┌─┐┬ ┬┌─┐┌┬┐┌─┐
    //  ║  ╠╦╝║╣ ╠═╣ ║ ║╣   └─┐│  ├─┤├┤ │││├─┤
    //  ╚═╝╩╚═╚═╝╩ ╩ ╩ ╚═╝  └─┘└─┘┴ ┴└─┘┴ ┴┴ ┴
    // Create a new Postgres Schema (namespace) in the database.
    createSchema: function createSchema(datastoreName, schemaName, cb) {
      var datastore = datastores[datastoreName];
      Helpers.createSchema({
        datastore: datastore,
        schemaName: schemaName
      }).exec({
        error: function error(err) {
          return cb(err);
        },
        success: function success() {
          return cb();
        }
      });
    },


    //  ╔╦╗╦═╗╔═╗╔═╗  ┌┬┐┌─┐┌┐ ┬  ┌─┐
    //   ║║╠╦╝║ ║╠═╝   │ ├─┤├┴┐│  ├┤
    //  ═╩╝╩╚═╚═╝╩     ┴ ┴ ┴└─┘┴─┘└─┘
    // Remove a table from the database.
    drop: function drop(datastoreName, tableName, relations, cb) {
      var datastore = datastores[datastoreName];
      Helpers.drop({
        datastore: datastore,
        tableName: tableName
      }).exec({
        error: function error(err) {
          return cb(err);
        },
        badConnection: function badConnection(err) {
          return cb(err);
        },
        success: function success() {
          return cb();
        }
      });
    },


    //  ╔═╗╔╦╗╔╦╗  ┌─┐┌┬┐┌┬┐┬─┐┬┌┐ ┬ ┬┌┬┐┌─┐
    //  ╠═╣ ║║ ║║  ├─┤ │  │ ├┬┘│├┴┐│ │ │ ├┤
    //  ╩ ╩═╩╝═╩╝  ┴ ┴ ┴  ┴ ┴└─┴└─┘└─┘ ┴ └─┘
    // Add a column to a table using Waterline model attribute syntax.
    addAttribute: function addAttribute(datastoreName, tableName, attrName, attrDef, cb) {
      var datastore = datastores[datastoreName];

      // Setup a Attribute Definition
      var def = {};
      def[attrName] = attrDef;

      Helpers.addAttribute({
        datastore: datastore,
        tableName: tableName,
        definition: def
      }).exec({
        error: function error(err) {
          return cb(err);
        },
        badConfiguration: function badConfiguration(err) {
          return cb(err);
        },
        success: function success() {
          return cb();
        }
      });
    },


    //  ╦═╗╔═╗╔╦╗╔═╗╦  ╦╔═╗  ┌─┐┌┬┐┌┬┐┬─┐┬┌┐ ┬ ┬┌┬┐┌─┐
    //  ╠╦╝║╣ ║║║║ ║╚╗╔╝║╣   ├─┤ │  │ ├┬┘│├┴┐│ │ │ ├┤
    //  ╩╚═╚═╝╩ ╩╚═╝ ╚╝ ╚═╝  ┴ ┴ ┴  ┴ ┴└─┴└─┘└─┘ ┴ └─┘
    // Remove a column from a table.
    removeAttribute: function removeAttribute(datastoreName, tableName, attrName, cb) {
      var datastore = datastores[datastoreName];
      Helpers.removeAttribute({
        datastore: datastore,
        tableName: tableName,
        attributeName: attrName
      }).exec({
        error: function error(err) {
          return cb(err);
        },
        badConfiguration: function badConfiguration(err) {
          return cb(err);
        },
        success: function success() {
          return cb();
        }
      });
    },


    //  ██████╗  ██████╗ ██╗
    //  ██╔══██╗██╔═══██╗██║
    //  ██║  ██║██║   ██║██║
    //  ██║  ██║██║▄▄ ██║██║
    //  ██████╔╝╚██████╔╝███████╗
    //  ╚═════╝  ╚══▀▀═╝ ╚══════╝
    //
    // Methods related to manipulating data stored in the database.


    //  ╔═╗╦═╗╔═╗╔═╗╔╦╗╔═╗  ┬─┐┌─┐┌─┐┌─┐┬─┐┌┬┐
    //  ║  ╠╦╝║╣ ╠═╣ ║ ║╣   ├┬┘├┤ │  │ │├┬┘ ││
    //  ╚═╝╩╚═╚═╝╩ ╩ ╩ ╚═╝  ┴└─└─┘└─┘└─┘┴└──┴┘
    // Add a new row to the table
    create: function create(datastoreName, tableName, data, cb, meta) {
      var datastore = datastores[datastoreName];
      var models = modelDefinitions[datastoreName];
      Helpers.create({
        datastore: datastore,
        models: models,
        tableName: tableName,
        record: data,
        meta: meta
      }).exec({
        error: function error(err) {
          return cb(err);
        },
        success: function success(report) {
          return cb(null, report.record);
        }
      });
    },


    //  ╔═╗╔═╗╦  ╔═╗╔═╗╔╦╗  ┌─┐ ┬ ┬┌─┐┬─┐┬ ┬
    //  ╚═╗║╣ ║  ║╣ ║   ║   │─┼┐│ │├┤ ├┬┘└┬┘
    //  ╚═╝╚═╝╩═╝╚═╝╚═╝ ╩   └─┘└└─┘└─┘┴└─ ┴
    // Select Query Logic
    find: function find(datastoreName, tableName, criteria, cb, meta) {
      var datastore = datastores[datastoreName];
      var models = modelDefinitions[datastoreName];
      Helpers.select({
        datastore: datastore,
        models: models,
        tableName: tableName,
        criteria: criteria,
        meta: meta
      }).exec({
        error: function error(err) {
          return cb(err);
        },
        success: function success(report) {
          return cb(null, report.records);
        }
      });
    },


    //  ╦ ╦╔═╗╔╦╗╔═╗╔╦╗╔═╗  ┌─┐ ┬ ┬┌─┐┬─┐┬ ┬
    //  ║ ║╠═╝ ║║╠═╣ ║ ║╣   │─┼┐│ │├┤ ├┬┘└┬┘
    //  ╚═╝╩  ═╩╝╩ ╩ ╩ ╚═╝  └─┘└└─┘└─┘┴└─ ┴
    // Update one or more models in the table
    update: function update(datastoreName, tableName, criteria, values, cb, meta) {
      var datastore = datastores[datastoreName];
      var models = modelDefinitions[datastoreName];
      Helpers.update({
        datastore: datastore,
        models: models,
        tableName: tableName,
        criteria: criteria,
        values: values,
        meta: meta
      }).exec({
        error: function error(err) {
          return cb(err);
        },
        success: function success(report) {
          return cb(null, report.records);
        }
      });
    },


    //  ╔╦╗╔═╗╔═╗╔╦╗╦═╗╔═╗╦ ╦  ┌─┐ ┬ ┬┌─┐┬─┐┬ ┬
    //   ║║║╣ ╚═╗ ║ ╠╦╝║ ║╚╦╝  │─┼┐│ │├┤ ├┬┘└┬┘
    //  ═╩╝╚═╝╚═╝ ╩ ╩╚═╚═╝ ╩   └─┘└└─┘└─┘┴└─ ┴
    // Delete one or more records in a table
    destroy: function destroy(datastoreName, tableName, criteria, cb, meta) {
      var datastore = datastores[datastoreName];
      var models = modelDefinitions[datastoreName];
      Helpers.destroy({
        datastore: datastore,
        models: models,
        tableName: tableName,
        criteria: criteria,
        meta: meta
      }).exec({
        error: function error(err) {
          return cb(err);
        },
        success: function success(report) {
          return cb(null, report);
        }
      });
    },


    //  ╔╗╔╔═╗╔╦╗╦╦  ╦╔═╗   ┬┌─┐┬┌┐┌  ┌─┐┬ ┬┌─┐┌─┐┌─┐┬─┐┌┬┐
    //  ║║║╠═╣ ║ ║╚╗╔╝║╣    ││ │││││  └─┐│ │├─┘├─┘│ │├┬┘ │
    //  ╝╚╝╩ ╩ ╩ ╩ ╚╝ ╚═╝  └┘└─┘┴┘└┘  └─┘└─┘┴  ┴  └─┘┴└─ ┴
    // Build up native joins to run on the adapter.
    join: function join(datastoreName, tableName, criteria, cb, meta) {
      var datastore = datastores[datastoreName];
      var models = modelDefinitions[datastoreName];
      Helpers.join({
        datastore: datastore,
        models: models,
        tableName: tableName,
        criteria: criteria,
        meta: meta
      }).exec({
        error: function error(err) {
          return cb(err);
        },
        success: function success(report) {
          return cb(null, report);
        }
      });
    }
  };

  return adapter;
})();
