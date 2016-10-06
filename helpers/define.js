//  ██████╗ ███████╗███████╗██╗███╗   ██╗███████╗
//  ██╔══██╗██╔════╝██╔════╝██║████╗  ██║██╔════╝
//  ██║  ██║█████╗  █████╗  ██║██╔██╗ ██║█████╗
//  ██║  ██║██╔══╝  ██╔══╝  ██║██║╚██╗██║██╔══╝
//  ██████╔╝███████╗██║     ██║██║ ╚████║███████╗
//  ╚═════╝ ╚══════╝╚═╝     ╚═╝╚═╝  ╚═══╝╚══════╝
//

module.exports = require('machine').build({


  friendlyName: 'Define',


  description: 'Create a new table in the database based on a given schema.',


  inputs: {

    datastore: {
      description: 'The datastore to use for connections.',
      extendedDescription: 'Datastores represent the config and manager required to obtain an active database connection.',
      required: true,
      example: '==='
    },

    tableName: {
      description: 'The name of the table to describe.',
      required: true,
      example: 'users'
    },

    definition: {
      description: 'The definition of the schema to build.',
      required: true,
      example: {}
    },

    meta: {
      friendlyName: 'Meta (custom)',
      description: 'Additional stuff to pass to the driver.',
      extendedDescription: 'This is reserved for custom driver-specific extensions.',
      example: '==='
    }

  },


  exits: {

    success: {
      description: 'The table was created successfully.'
    },

    badConnection: {
      friendlyName: 'Bad connection',
      description: 'A connection either could not be obtained or there was an error using the connection.'
    }

  },


  fn: function define(inputs, exits) {
    var _ = require('lodash');
    var async = require('async');
    var PG = require('machinepack-postgresql');
    var Helpers = require('./private');


    //  ╔═╗╦ ╦╔═╗╔═╗╦╔═  ┌─┐┌─┐┬─┐  ┌─┐  ┌─┐┌─┐  ┌─┐┌─┐┬ ┬┌─┐┌┬┐┌─┐
    //  ║  ╠═╣║╣ ║  ╠╩╗  ├┤ │ │├┬┘  ├─┤  ├─┘│ ┬  └─┐│  ├─┤├┤ │││├─┤
    //  ╚═╝╩ ╩╚═╝╚═╝╩ ╩  └  └─┘┴└─  ┴ ┴  ┴  └─┘  └─┘└─┘┴ ┴└─┘┴ ┴┴ ┴
    // This is a unique feature of Postgres. It may be passed in on a query
    // by query basis using the meta input or configured on the datastore. Default
    // to use the public schema.
    var schemaName = 'public';
    if (inputs.meta && inputs.meta.schema) {
      schemaName = inputs.meta.schema;
    } else if (inputs.datastore.config && inputs.datastore.config.schema) {
      schemaName = inputs.datastore.config.schema;
    }


    //  ███╗   ██╗ █████╗ ███╗   ███╗███████╗██████╗
    //  ████╗  ██║██╔══██╗████╗ ████║██╔════╝██╔══██╗
    //  ██╔██╗ ██║███████║██╔████╔██║█████╗  ██║  ██║
    //  ██║╚██╗██║██╔══██║██║╚██╔╝██║██╔══╝  ██║  ██║
    //  ██║ ╚████║██║  ██║██║ ╚═╝ ██║███████╗██████╔╝
    //  ╚═╝  ╚═══╝╚═╝  ╚═╝╚═╝     ╚═╝╚══════╝╚═════╝
    //
    //  ███████╗██╗   ██╗███╗   ██╗ ██████╗████████╗██╗ ██████╗ ███╗   ██╗███████╗
    //  ██╔════╝██║   ██║████╗  ██║██╔════╝╚══██╔══╝██║██╔═══██╗████╗  ██║██╔════╝
    //  █████╗  ██║   ██║██╔██╗ ██║██║        ██║   ██║██║   ██║██╔██╗ ██║███████╗
    //  ██╔══╝  ██║   ██║██║╚██╗██║██║        ██║   ██║██║   ██║██║╚██╗██║╚════██║
    //  ██║     ╚██████╔╝██║ ╚████║╚██████╗   ██║   ██║╚██████╔╝██║ ╚████║███████║
    //  ╚═╝      ╚═════╝ ╚═╝  ╚═══╝ ╚═════╝   ╚═╝   ╚═╝ ╚═════╝ ╚═╝  ╚═══╝╚══════╝
    //
    // Prevent Callback Hell and such.

    //  ╔═╗╔═╗╔═╗╦ ╦╔╗╔  ┌─┐┌─┐┌┐┌┌┐┌┌─┐┌─┐┌┬┐┬┌─┐┌┐┌
    //  ╚═╗╠═╝╠═╣║║║║║║  │  │ │││││││├┤ │   │ ││ ││││
    //  ╚═╝╩  ╩ ╩╚╩╝╝╚╝  └─┘└─┘┘└┘┘└┘└─┘└─┘ ┴ ┴└─┘┘└┘
    var spawnConnection = function spawnConnection(done) {
      Helpers.spawnConnection({
        datastore: inputs.datastore
      })
      .exec({
        error: function error(err) {
          return done(new Error('There was an error spawing a connection from the pool.' + err.stack));
        },
        success: function success(connection) {
          return done(null, connection);
        }
      });
    };


    //  ╦═╗╦ ╦╔╗╔  ┌┐┌┌─┐┌┬┐┬┬  ┬┌─┐  ┌─┐ ┬ ┬┌─┐┬─┐┬ ┬
    //  ╠╦╝║ ║║║║  │││├─┤ │ │└┐┌┘├┤   │─┼┐│ │├┤ ├┬┘└┬┘
    //  ╩╚═╚═╝╝╚╝  ┘└┘┴ ┴ ┴ ┴ └┘ └─┘  └─┘└└─┘└─┘┴└─ ┴
    var runNativeQuery = function runNativeQuery(connection, query, done) {
      PG.sendNativeQuery({
        connection: connection,
        nativeQuery: query
      })
      .exec(function execCb(err, report) {
        if (err) {
          return done(new Error('There was an error with the query used to define the table.' + err.stack));
        }

        return done(null, report.result.rows);
      });
    };


    //  ╔═╗╦═╗╔═╗╔═╗╔╦╗╔═╗  ┌─┐┌─┐┬ ┬┌─┐┌┬┐┌─┐
    //  ║  ╠╦╝║╣ ╠═╣ ║ ║╣   └─┐│  ├─┤├┤ │││├─┤
    //  ╚═╝╩╚═╚═╝╩ ╩ ╩ ╚═╝  └─┘└─┘┴ ┴└─┘┴ ┴┴ ┴
    // AKA namespace if one is given.
    var createSchema = function createSchema(connection, done) {
      // If we're being told NOT to create schemas, then skip right to
      // creating the table.
      if (inputs.datastore.config && inputs.datastore.config.createSchemas === false) {
        return done();
      }

      // If the schema name is "public" there is nothing to create
      if (schemaName === 'public') {
        return done();
      }

      Helpers.createNamespace({
        datastore: inputs.datastore,
        schemaName: schemaName
      }).exec({
        error: function error(err) {
          return done(new Error('There was an error creating the schema name.' + err.stack));
        },
        badConnection: function badConnection(err) {
          return done(new Error('There was an error connecting to the database.' + err.stack));
        },
        success: function success() {
          return done();
        }
      });
    };


    //  ╔═╗╔═╗╔═╗╔═╗╔═╗╔═╗  ┌┬┐┌─┐┌┐ ┬  ┌─┐  ┌┐┌┌─┐┌┬┐┌─┐
    //  ║╣ ╚═╗║  ╠═╣╠═╝║╣    │ ├─┤├┴┐│  ├┤   │││├─┤│││├┤
    //  ╚═╝╚═╝╚═╝╩ ╩╩  ╚═╝   ┴ ┴ ┴└─┘┴─┘└─┘  ┘└┘┴ ┴┴ ┴└─┘
    // Ensure the name is escaped in quotes.
    var escapeName = function escapeName(name, schema) {
      name = '"' + name + '"';
      if (schema) {
        name = '"' + schema + '".' + name;
      }

      return name;
    };


    //  ╔╗ ╦ ╦╦╦  ╔╦╗  ┬┌┐┌┌┬┐┌─┐─┐ ┬┌─┐┌─┐
    //  ╠╩╗║ ║║║   ║║  ││││ ││├┤ ┌┴┬┘├┤ └─┐
    //  ╚═╝╚═╝╩╩═╝═╩╝  ┴┘└┘─┴┘└─┘┴ └─└─┘└─┘
    var buildIndexes = function buildIndexes(connection, done) {
      var indexes = _.reduce(inputs.definition, function reduce(meta, val, key) {
        if (_.has(val, 'index')) {
          meta.push(key);
        }

        return meta;
      }, []);

      var build = function build(name, nextIndex) {
        // Strip slashes from table name, used to namespace index
        var cleanTable = inputs.tableName.replace(/['"]/g, '');

        // Build a query to create a namespaced index tableName_key
        var query = 'CREATE INDEX ' + escapeName(cleanTable + '_' + name) + ' on ' + inputs.tableName + ' (' + escapeName(name) + ');';

        // Run the native query
        runNativeQuery(connection, query, nextIndex);
      };

      // Build indexes in series
      async.eachSeries(indexes, build, done);
    };


    //  ╦═╗╔═╗╦  ╔═╗╔═╗╔═╗╔═╗  ┌─┐┌─┐┌┐┌┌┐┌┌─┐┌─┐┌┬┐┬┌─┐┌┐┌
    //  ╠╦╝║╣ ║  ║╣ ╠═╣╚═╗║╣   │  │ │││││││├┤ │   │ ││ ││││
    //  ╩╚═╚═╝╩═╝╚═╝╩ ╩╚═╝╚═╝  └─┘└─┘┘└┘┘└┘└─┘└─┘ ┴ ┴└─┘┘└┘
    var releaseConnection = function releaseConnection(connection, done) {
      PG.releaseConnection({
        connection: connection
      }).exec({
        error: function error(err) {
          return done(new Error('There was an error releasing the connection back into the pool.' + err.stack));
        },
        badConnection: function badConnection() {
          return done(new Error('Bad connection when trying to release an active connection.'));
        },
        success: function success() {
          return done();
        }
      });
    };


    //   █████╗  ██████╗████████╗██╗ ██████╗ ███╗   ██╗
    //  ██╔══██╗██╔════╝╚══██╔══╝██║██╔═══██╗████╗  ██║
    //  ███████║██║        ██║   ██║██║   ██║██╔██╗ ██║
    //  ██╔══██║██║        ██║   ██║██║   ██║██║╚██╗██║
    //  ██║  ██║╚██████╗   ██║   ██║╚██████╔╝██║ ╚████║
    //  ╚═╝  ╚═╝ ╚═════╝   ╚═╝   ╚═╝ ╚═════╝ ╚═╝  ╚═══╝
    //
    //  ██╗      ██████╗  ██████╗ ██╗ ██████╗
    //  ██║     ██╔═══██╗██╔════╝ ██║██╔════╝
    //  ██║     ██║   ██║██║  ███╗██║██║
    //  ██║     ██║   ██║██║   ██║██║██║
    //  ███████╗╚██████╔╝╚██████╔╝██║╚██████╗
    //  ╚══════╝ ╚═════╝  ╚═════╝ ╚═╝ ╚═════╝
    //

    // Open a new connection to use
    spawnConnection(function cb(err, connection) {
      if (err) {
        return exits.badConnection(err);
      }

      // Create the schema if needed.
      // This will succeed if the schema already exists.
      createSchema(connection, function cb(err) {
        if (err) {
          releaseConnection(connection, function cb() {
            return exits.error(err);
          });
          return;
        }

        // Escape Table Name
        var tableName = escapeName(inputs.tableName, schemaName);

        // Iterate through each attribute, building a query string
        var schema;
        try {
          schema = Helpers.buildSchema({
            definition: inputs.definition
          }).execSync();
        } catch (e) {
          releaseConnection(connection, function cb() {
            return exits.error(e);
          });
          return;
        }

        // Build Query
        var query = 'CREATE TABLE IF NOT EXISTS ' + tableName + ' (' + schema + ')';

        // Run the CREATE TABLE query
        runNativeQuery(connection, query, function cb(err) {
          if (err) {
            releaseConnection(connection, function cb() {
              return exits.error(err);
            });
            return;
          }

          // Build any indexes
          buildIndexes(connection, function cb(err) {
            if (err) {
              releaseConnection(connection, function cb() {
                return exits.error(err);
              });
              return;
            }

            // Ensure the connection is always released
            releaseConnection(connection, function cb() {
              return exits.success();
            });
          });
        });
      });
    });
  }

});
