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

var _ = require('@sailshq/lodash');
var async = require('async');
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
      var datastoreIdentities = [];

      // If no specific identity was sent, teardown all the datastores
      if (!identity || identity === null) {
        datastoreIdentities = datastoreIdentities.concat(_.keys(datastores));
      } else {
        datastoreIdentities.push(identity);
      }

      // Teardown each datastore identity manager
      async.eachSeries(datastoreIdentities, function teardownDatastore(datastoreIdentity, next) {
        Helpers.teardown({
          identity: datastoreIdentity,
          datastores: datastores,
          modelDefinitions: modelDefinitions
        }).exec({
          error: function error(err) {
            return next(err);
          },
          success: function success() {
            return next();
          }
        });
      }, function asyncCb(err) {
        cb(err);
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
    create: function create(datastoreName, query, cb, meta) {
      var datastore = datastores[datastoreName];
      var models = modelDefinitions[datastoreName];
      Helpers.create({
        datastore: datastore,
        models: models,
        tableName: query.using,
        record: query.newRecord,
        meta: meta
      }).exec({
        error: function error(err) {
          return cb(err);
        },
        success: function success(report) {
          return cb(undefined, report.record);
        }
      });
    },


    //  ╔═╗╦═╗╔═╗╔═╗╔╦╗╔═╗  ╔═╗╔═╗╔═╗╦ ╦  ┬─┐┌─┐┌─┐┌─┐┬─┐┌┬┐
    //  ║  ╠╦╝║╣ ╠═╣ ║ ║╣   ║╣ ╠═╣║  ╠═╣  ├┬┘├┤ │  │ │├┬┘ ││
    //  ╚═╝╩╚═╚═╝╩ ╩ ╩ ╚═╝  ╚═╝╩ ╩╚═╝╩ ╩  ┴└─└─┘└─┘└─┘┴└──┴┘
    // Add multiple new rows to the table
    createEach: function createEach(datastoreName, query, cb, meta) {
      var datastore = datastores[datastoreName];
      var models = modelDefinitions[datastoreName];
      Helpers.createEach({
        datastore: datastore,
        models: models,
        tableName: query.using,
        records: query.newRecords,
        meta: meta
      }).exec({
        error: function error(err) {
          return cb(err);
        },
        success: function success(report) {
          return cb(undefined, report.records);
        }
      });
    },


    //  ╔═╗╔═╗╦  ╔═╗╔═╗╔╦╗  ┌─┐ ┬ ┬┌─┐┬─┐┬ ┬
    //  ╚═╗║╣ ║  ║╣ ║   ║   │─┼┐│ │├┤ ├┬┘└┬┘
    //  ╚═╝╚═╝╩═╝╚═╝╚═╝ ╩   └─┘└└─┘└─┘┴└─ ┴
    // Select Query Logic
    find: function find(datastoreName, query, cb, meta) {
      var datastore = datastores[datastoreName];
      var models = modelDefinitions[datastoreName];
      Helpers.select({
        datastore: datastore,
        models: models,
        tableName: query.using,
        criteria: query.criteria,
        meta: meta
      }).exec({
        error: function error(err) {
          return cb(err);
        },
        success: function success(report) {
          return cb(undefined, report.records);
        }
      });
    },


    //  ╦ ╦╔═╗╔╦╗╔═╗╔╦╗╔═╗  ┌─┐ ┬ ┬┌─┐┬─┐┬ ┬
    //  ║ ║╠═╝ ║║╠═╣ ║ ║╣   │─┼┐│ │├┤ ├┬┘└┬┘
    //  ╚═╝╩  ═╩╝╩ ╩ ╩ ╚═╝  └─┘└└─┘└─┘┴└─ ┴
    // Update one or more models in the table
    update: function update(datastoreName, query, cb, meta) {
      var datastore = datastores[datastoreName];
      var models = modelDefinitions[datastoreName];
      Helpers.update({
        datastore: datastore,
        models: models,
        tableName: query.using,
        criteria: query.criteria,
        values: query.valuesToSet,
        meta: meta
      }).exec({
        error: function error(err) {
          return cb(err);
        },
        success: function success(report) {
          return cb(undefined, report.records);
        }
      });
    },


    //  ╔╦╗╔═╗╔═╗╔╦╗╦═╗╔═╗╦ ╦  ┌─┐ ┬ ┬┌─┐┬─┐┬ ┬
    //   ║║║╣ ╚═╗ ║ ╠╦╝║ ║╚╦╝  │─┼┐│ │├┤ ├┬┘└┬┘
    //  ═╩╝╚═╝╚═╝ ╩ ╩╚═╚═╝ ╩   └─┘└└─┘└─┘┴└─ ┴
    // Delete one or more records in a table
    destroy: function destroy(datastoreName, query, cb, meta) {
      var datastore = datastores[datastoreName];
      var models = modelDefinitions[datastoreName];
      Helpers.destroy({
        datastore: datastore,
        models: models,
        tableName: query.using,
        criteria: query.criteria,
        meta: meta
      }).exec({
        error: function error(err) {
          return cb(err);
        },
        success: function success(report) {
          return cb(undefined, report);
        }
      });
    },


    //  ╔╗╔╔═╗╔╦╗╦╦  ╦╔═╗   ┬┌─┐┬┌┐┌  ┌─┐┬ ┬┌─┐┌─┐┌─┐┬─┐┌┬┐
    //  ║║║╠═╣ ║ ║╚╗╔╝║╣    ││ │││││  └─┐│ │├─┘├─┘│ │├┬┘ │
    //  ╝╚╝╩ ╩ ╩ ╩ ╚╝ ╚═╝  └┘└─┘┴┘└┘  └─┘└─┘┴  ┴  └─┘┴└─ ┴
    // Build up native joins to run on the adapter.
    join: function join(datastoreName, query, cb, meta) {
      var datastore = datastores[datastoreName];
      var models = modelDefinitions[datastoreName];
      Helpers.join({
        datastore: datastore,
        models: models,
        tableName: query.using,
        criteria: query.criteria,
        meta: meta
      }).exec({
        error: function error(err) {
          return cb(err);
        },
        success: function success(report) {
          return cb(undefined, report);
        }
      });
    },


    //  ╔═╗╦  ╦╔═╗  ┌─┐ ┬ ┬┌─┐┬─┐┬ ┬
    //  ╠═╣╚╗╔╝║ ╦  │─┼┐│ │├┤ ├┬┘└┬┘
    //  ╩ ╩ ╚╝ ╚═╝  └─┘└└─┘└─┘┴└─ ┴
    // Find out the average of the query.
    avg: function avg(datastoreName, query, cb, meta) {
      var datastore = datastores[datastoreName];
      var models = modelDefinitions[datastoreName];
      Helpers.avg({
        datastore: datastore,
        models: models,
        tableName: query.using,
        criteria: query.criteria,
        columnName: query.numericAttrName,
        meta: meta
      }).exec({
        error: function error(err) {
          return cb(err);
        },
        success: function success(report) {
          return cb(undefined, report);
        }
      });
    },


    //  ╔═╗╦ ╦╔╦╗  ┌─┐ ┬ ┬┌─┐┬─┐┬ ┬
    //  ╚═╗║ ║║║║  │─┼┐│ │├┤ ├┬┘└┬┘
    //  ╚═╝╚═╝╩ ╩  └─┘└└─┘└─┘┴└─ ┴
    // Find out the sum of the query.
    sum: function sum(datastoreName, query, cb, meta) {
      var datastore = datastores[datastoreName];
      var models = modelDefinitions[datastoreName];
      Helpers.sum({
        datastore: datastore,
        models: models,
        tableName: query.using,
        criteria: query.criteria,
        columnName: query.numericAttrName,
        meta: meta
      }).exec({
        error: function error(err) {
          return cb(err);
        },
        success: function success(report) {
          return cb(undefined, report);
        }
      });
    },


    //  ╔═╗╔═╗╦ ╦╔╗╔╔╦╗  ┌─┐ ┬ ┬┌─┐┬─┐┬ ┬
    //  ║  ║ ║║ ║║║║ ║   │─┼┐│ │├┤ ├┬┘└┬┘
    //  ╚═╝╚═╝╚═╝╝╚╝ ╩   └─┘└└─┘└─┘┴└─ ┴
    // Return the number of matching records.
    count: function count(datastoreName, query, cb, meta) {
      var datastore = datastores[datastoreName];
      var models = modelDefinitions[datastoreName];
      Helpers.count({
        datastore: datastore,
        models: models,
        tableName: query.using,
        criteria: query.criteria,
        meta: meta
      }).exec({
        error: function error(err) {
          return cb(err);
        },
        success: function success(report) {
          return cb(undefined, report);
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
    describe: function describe(datastoreName, tableName, cb, meta) {
      var datastore = datastores[datastoreName];
      Helpers.describe({
        datastore: datastore,
        tableName: tableName,
        meta: meta
      }).exec({
        error: function error(err) {
          return cb(err);
        },
        success: function success(report) {
          // Waterline expects the result to be undefined if the table doesn't
          // exist.
          if (_.keys(report.schema).length) {
            return cb(undefined, report.schema);
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
    define: function define(datastoreName, tableName, definition, cb, meta) {
      var datastore = datastores[datastoreName];
      Helpers.define({
        datastore: datastore,
        tableName: tableName,
        definition: definition,
        meta: meta
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
    createSchema: function createSchema(datastoreName, schemaName, cb, meta) {
      var datastore = datastores[datastoreName];
      Helpers.createSchema({
        datastore: datastore,
        schemaName: schemaName,
        meta: meta
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
    drop: function drop(datastoreName, tableName, relations, cb, meta) {
      var datastore = datastores[datastoreName];
      Helpers.drop({
        datastore: datastore,
        tableName: tableName,
        meta: meta
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
    addAttribute: function addAttribute(datastoreName, tableName, attrName, attrDef, cb, meta) {
      var datastore = datastores[datastoreName];

      // Setup a Attribute Definition
      var def = {};
      def[attrName] = attrDef;

      Helpers.addAttribute({
        datastore: datastore,
        tableName: tableName,
        definition: def,
        meta: meta
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
    removeAttribute: function removeAttribute(datastoreName, tableName, attrName, cb, meta) {
      var datastore = datastores[datastoreName];
      Helpers.removeAttribute({
        datastore: datastore,
        tableName: tableName,
        attributeName: attrName,
        meta: meta
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
    }

  };

  return adapter;
})();
