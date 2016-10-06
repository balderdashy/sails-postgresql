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

      Helpers.registerDataStore({
        identity: identity,
        config: connectionConfig,
        models: models,
        datastores: datastores,
        modelDefinitions: modelDefinitions
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
        badConnection: function badConnection(err) {
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
        badConnection: function badConnection(err) {
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
    }


    //   █████╗ ██████╗ ██████╗ ██╗████████╗██╗ ██████╗ ███╗   ██╗ █████╗ ██╗
    //  ██╔══██╗██╔══██╗██╔══██╗██║╚══██╔══╝██║██╔═══██╗████╗  ██║██╔══██╗██║
    //  ███████║██║  ██║██║  ██║██║   ██║   ██║██║   ██║██╔██╗ ██║███████║██║
    //  ██╔══██║██║  ██║██║  ██║██║   ██║   ██║██║   ██║██║╚██╗██║██╔══██║██║
    //  ██║  ██║██████╔╝██████╔╝██║   ██║   ██║╚██████╔╝██║ ╚████║██║  ██║███████╗
    //  ╚═╝  ╚═╝╚═════╝ ╚═════╝ ╚═╝   ╚═╝   ╚═╝ ╚═════╝ ╚═╝  ╚═══╝╚═╝  ╚═╝╚══════╝
    //
    // Additional methods exposed by the adapter for use in the ORM.

    // Native Join Support
  //   join: function(connectionName, table, options, cb) {
  //
  //     spawnConnection(connectionName, function __FIND__(client, done) {
  //
  //       // Populate associated records for each parent result
  //       // (or do them all at once as an optimization, if possible)
  //       Cursor({
  //
  //         instructions: options,
  //         nativeJoins: true,
  //
  //         /**
  //          * Find some records directly (using only this adapter)
  //          * from the specified collection.
  //          *
  //          * @param  {String}   collectionIdentity
  //          * @param  {Object}   criteria
  //          * @param  {Function} _cb
  //          */
  //         $find: function (collectionName, criteria, _cb) {
  //           return adapter.find(conn, collectionIdentity, criteria, _cb, client);
  //         },
  //
  //         /**
  //          * Look up the name of the primary key field
  //          * for the collection with the specified identity.
  //          *
  //          * @param  {String}   collectionIdentity
  //          * @return {String}
  //          */
  //         $getPK: function (collectionName) {
  //           if (!collectionName) return;
  //           return _getPK(connectionName, collectionName);
  //         },
  //
  //         /**
  //          * Given a strategy type, build up and execute a SQL query for it.
  //          *
  //          * @param {}
  //          */
  //
  //         $populateBuffers: function populateBuffers(options, next) {
  //
  //           var buffers = options.buffers;
  //           var instructions = options.instructions;
  //
  //           // Grab the collection by looking into the connection
  //           var connectionObject = connections[connectionName];
  //           var collection = connectionObject.collections[table];
  //           var tableName = table;
  //
  //           var parentRecords = [];
  //           var cachedChildren = {};
  //
  //           // Grab Connection Schema
  //           var schema = {};
  //
  //           Object.keys(connectionObject.collections).forEach(function(coll) {
  //             schema[coll] = connectionObject.collections[coll].schema;
  //           });
  //
  //           // Build Query
  //           var _schema = connectionObject.schema;
  //
  //           // Mixin WL Next connection overrides to sqlOptions
  //           var overrides = connectionOverrides[connectionName] || {};
  //           var _options = _.cloneDeep(sqlOptions);
  //           if(hop(overrides, 'wlNext')) {
  //             _options.wlNext = overrides.wlNext;
  //           }
  //
  //           var sequel = new Sequel(_schema, _options);
  //           var _query;
  //
  //           // Build a query for the specific query strategy
  //           try {
  //             _query = sequel.find(tableName, instructions);
  //           } catch(e) {
  //             return next(e);
  //           }
  //
  //           async.auto({
  //
  //             processParent: function(next) {
  //
  //               client.query(_query.query[0], _query.values[0], function __FIND__(err, result) {
  //                 if(err) return next(handleQueryError(err));
  //
  //                 parentRecords = result.rows;
  //
  //                 var splitChildren = function(parent, next) {
  //                   var cache = {};
  //
  //                   _.keys(parent).forEach(function(key) {
  //
  //                     // Check if we can split this on our special alias identifier '___' and if
  //                     // so put the result in the cache
  //                     var split = key.split('___');
  //                     if(split.length < 2) return;
  //
  //                     if(!hop(cache, split[0])) cache[split[0]] = {};
  //                     cache[split[0]][split[1]] = parent[key];
  //                     delete parent[key];
  //                   });
  //
  //                   // Combine the local cache into the cachedChildren
  //                   if(_.keys(cache).length > 0) {
  //                     _.keys(cache).forEach(function(pop) {
  //                       if(!hop(cachedChildren, pop)) cachedChildren[pop] = [];
  //                       cachedChildren[pop] = cachedChildren[pop].concat(cache[pop]);
  //                     });
  //                   }
  //
  //                   next();
  //                 };
  //
  //
  //                 // Pull out any aliased child records that have come from a hasFK association
  //                 async.eachSeries(parentRecords, splitChildren, function(err) {
  //                   if(err) return next(err);
  //                   buffers.parents = parentRecords;
  //                   next();
  //                 });
  //               });
  //             },
  //
  //             // Build child buffers.
  //             // For each instruction, loop through the parent records and build up a
  //             // buffer for the record.
  //             buildChildBuffers: ['processParent', function(next, results) {
  //               async.each(_.keys(instructions.instructions), function(population, nextPop) {
  //
  //                 var populationObject = instructions.instructions[population];
  //                 var popInstructions = populationObject.instructions;
  //                 var pk = _getPK(connectionName, popInstructions[0].parent);
  //
  //                 var alias = populationObject.strategy.strategy === 1 ? popInstructions[0].parentKey : popInstructions[0].alias;
  //
  //                 // Use eachSeries here to keep ordering
  //                 async.eachSeries(parentRecords, function(parent, nextParent) {
  //                   var buffer = {
  //                     attrName: population,
  //                     parentPK: parent[pk],
  //                     pkAttr: pk,
  //                     keyName: alias
  //                   };
  //
  //                   var records = [];
  //                   var recordsMap = {};
  //
  //                   // Check for any cached parent records
  //                   if(hop(cachedChildren, alias)) {
  //                     cachedChildren[alias].forEach(function(cachedChild) {
  //                       var childVal = popInstructions[0].childKey;
  //                       var parentVal = popInstructions[0].parentKey;
  //
  //                       if(cachedChild[childVal] !== parent[parentVal]) {
  //                         return;
  //                       }
  //
  //                       // If null value for the parentVal, ignore it
  //                       if(parent[parentVal] === null) return;
  //                       // If the same record is alreay there, ignore it
  //                       if (!recordsMap[cachedChild[childVal]]) {
  //                         records.push(cachedChild);
  //                         recordsMap[cachedChild[childVal]] = true;
  //                       }
  //                     });
  //                   }
  //
  //                   if(records.length > 0) {
  //                     buffer.records = records;
  //                   }
  //
  //                   buffers.add(buffer);
  //                   nextParent();
  //                 }, nextPop);
  //               }, next);
  //             }],
  //
  //
  //             processChildren: ['buildChildBuffers', function(next, results) {
  //
  //               // Remove the parent query
  //               _query.query.shift();
  //
  //               async.each(_query.query, function(q, next) {
  //
  //                 var qs = '';
  //                 var pk;
  //
  //                 if(!Array.isArray(q.instructions)) {
  //                   pk = _getPK(connectionName, q.instructions.parent);
  //                 }
  //                 else if(q.instructions.length > 1) {
  //                   pk = _getPK(connectionName, q.instructions[0].parent);
  //                 }
  //
  //                 parentRecords.forEach(function(parent) {
  //                   if(_.isNumber(parent[pk])) {
  //                     qs += q.qs.replace('^?^', parent[pk]) + ' UNION ALL ';
  //                   } else {
  //                     qs += q.qs.replace('^?^', "'" + parent[pk] + "'") + ' UNION ALL ';
  //                   }
  //                 });
  //
  //                 // Remove the last UNION ALL
  //                 qs = qs.slice(0, -11);
  //
  //                 // Add a final sort to the Union clause for integration
  //                 if(parentRecords.length > 1) {
  //                   qs += ' ORDER BY ';
  //
  //                   if(!Array.isArray(q.instructions)) {
  //                     _.keys(q.instructions.criteria.sort).forEach(function(sortKey) {
  //                       var direction = q.instructions.criteria.sort[sortKey] === 1 ? 'ASC' : 'DESC';
  //                       qs += '"' + sortKey + '"' + ' ' + direction + ', ';
  //                     });
  //                   }
  //                   else if(q.instructions.length === 2) {
  //                     _.keys(q.instructions[1].criteria.sort).forEach(function(sortKey) {
  //                       var direction = q.instructions[1].criteria.sort[sortKey] === 1 ? 'ASC' : 'DESC';
  //                       qs += '"' + sortKey + '"' + ' ' + direction + ', ';
  //                     });
  //                   }
  //
  //                   // Remove the last comma
  //                   qs = qs.slice(0, -2);
  //                 }
  //                 client.query(qs, q.values, function __FIND__(err, result) {
  //                   if(err) return next(handleQueryError(err));
  //
  //                   var groupedRecords = {};
  //
  //                   result.rows.forEach(function(row) {
  //
  //                     if(!Array.isArray(q.instructions)) {
  //
  //                       if(!hop(groupedRecords, row[q.instructions.childKey])) {
  //                         groupedRecords[row[q.instructions.childKey]] = [];
  //                       }
  //
  //                       groupedRecords[row[q.instructions.childKey]].push(row);
  //                     }
  //                     else {
  //
  //                       // Grab the special "foreign key" we attach and make sure to remove it
  //                       var fk = '___' + q.instructions[0].childKey;
  //
  //                       if(!hop(groupedRecords, row[fk])) {
  //                         groupedRecords[row[fk]] = [];
  //                       }
  //
  //                       var data = _.cloneDeep(row);
  //                       delete data[fk];
  //                       groupedRecords[row[fk]].push(data);
  //
  //                       // Ensure we don't have duplicates in here
  //                       groupedRecords[row[fk]] = _.uniq(groupedRecords[row[fk]], q.instructions[1].childKey);
  //                     }
  //                   });
  //
  //                   buffers.store.forEach(function(buffer) {
  //                     if(buffer.attrName !== q.attrName) return;
  //                     var records = groupedRecords[buffer.belongsToPKValue];
  //                     if(!records) return;
  //                     if(!buffer.records) buffer.records = [];
  //                     buffer.records = buffer.records.concat(records);
  //                   });
  //
  //                   next();
  //                 });
  //               }, function(err) {
  //                 next();
  //               });
  //
  //             }]
  //
  //           },
  //           function(err) {
  //             if(err) return next(err);
  //             next();
  //           });
  //
  //         }
  //
  //       }, done);
  //
  //     }, cb);
  //
  //   },
  //
  };


  // Normalize a Query Error
  // var handleQueryError = function handleQueryError(err) {
  //   var formattedErr;
  //
  //   // Check for uniqueness constraint violations:
  //   if (err.code === '23505') {
  //     // Manually parse the error response and extract the relevant bits,
  //     // then build the formatted properties that will be passed directly to
  //     // WLValidationError in Waterline core.
  //     var matches = err.detail.match(/Key \((.*)\)=\((.*)\) already exists\.$/);
  //     if (matches && matches.length) {
  //       formattedErr = {};
  //       formattedErr.code = 'E_UNIQUE';
  //       // This is the same property that is set on WLError instances.
  //       // Accessible as `.originalError` on a WLValidationError instance.
  //       formattedErr.originalError = err;
  //       formattedErr.invalidAttributes = {};
  //       formattedErr.invalidAttributes[matches[1]] = [{
  //         value: matches[2],
  //         rule: 'unique'
  //       }];
  //     }
  //   } else if (err.code === '3F000') {
  //     formattedErr = {};
  //     formattedErr.message = 'Attempted to create a table `' + err.table + '` in a schema `' + err.schemaName + '` that does not exist.  Either create the schema manually or make sure that the `createSchemas` flag in your connection configuration is not set to `false`.';
  //     delete err.table;
  //     delete err.schemaName;
  //     formattedErr.originalError = err;
  //   } else if (err.type === 'CREATING_SCHEMA') {
  //     formattedErr = {};
  //     formattedErr.code = 'CREATING_SCHEMA';
  //     formattedErr.message = 'An error occurred creating the schema "' + err.schemaName + '"; perhaps it contains invalid characters?';
  //     delete err.type;
  //     delete err.schemaName;
  //     formattedErr.originalError = err;
  //   }
  //
  //   return formattedErr || err;
  // };


  return adapter;
})();
