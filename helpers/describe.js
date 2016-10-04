//  ██████╗ ███████╗███████╗ ██████╗██████╗ ██╗██████╗ ███████╗
//  ██╔══██╗██╔════╝██╔════╝██╔════╝██╔══██╗██║██╔══██╗██╔════╝
//  ██║  ██║█████╗  ███████╗██║     ██████╔╝██║██████╔╝█████╗
//  ██║  ██║██╔══╝  ╚════██║██║     ██╔══██╗██║██╔══██╗██╔══╝
//  ██████╔╝███████╗███████║╚██████╗██║  ██║██║██████╔╝███████╗
//  ╚═════╝ ╚══════╝╚══════╝ ╚═════╝╚═╝  ╚═╝╚═╝╚═════╝ ╚══════╝
//

module.exports = require('machine').build({


  friendlyName: 'Describe',


  description: 'Describe a table and update the model schema in the related data store.',


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
    }

  },


  exits: {

    success: {
      description: 'The results of the describe query.',
      outputVariableName: 'records',
      example: {
        schema: {}
      }
    },

    badConnection: {
      friendlyName: 'Bad connection',
      description: 'A connection either could not be obtained or there was an error using the connection.'
    }

  },


  fn: function describe(inputs, exits) {
    var _ = require('lodash');
    var PG = require('machinepack-postgresql');
    var Helpers = require('./private');

    // Find the model in the datastore
    var model = inputs.datastore.models[inputs.tableName];

    // Determine a schema name
    var schemaName = model.meta && model.meta.schemaName || 'public';

    // Build an object for holding information about the schema
    var dbSchema = {};


    //   ██████╗ ██╗   ██╗███████╗██████╗ ██╗███████╗███████╗
    //  ██╔═══██╗██║   ██║██╔════╝██╔══██╗██║██╔════╝██╔════╝
    //  ██║   ██║██║   ██║█████╗  ██████╔╝██║█████╗  ███████╗
    //  ██║▄▄ ██║██║   ██║██╔══╝  ██╔══██╗██║██╔══╝  ╚════██║
    //  ╚██████╔╝╚██████╔╝███████╗██║  ██║██║███████╗███████║
    //   ╚══▀▀═╝  ╚═════╝ ╚══════╝╚═╝  ╚═╝╚═╝╚══════╝╚══════╝
    //
    // These native queries are responsible for describing a single table and the
    // various attributes that make them.

    // Build query to get a bunch of info from the information_schema
    // It's not super important to understand it only that it returns the following fields:
    // [Table, #, Column, Type, Null, Constraint, C, consrc, F Key, Default]
    var describeQuery = "SELECT x.nspname || '.' || x.relname as \"Table\", x.attnum as \"#\", x.attname as \"Column\", x.\"Type\"," +
      " case x.attnotnull when true then 'NOT NULL' else '' end as \"NULL\", r.conname as \"Constraint\", r.contype as \"C\", " +
      "r.consrc, fn.nspname || '.' || f.relname as \"F Key\", d.adsrc as \"Default\" FROM (" +
      "SELECT c.oid, a.attrelid, a.attnum, n.nspname, c.relname, a.attname, pg_catalog.format_type(a.atttypid, a.atttypmod) as \"Type\", " +
      "a.attnotnull FROM pg_catalog.pg_attribute a, pg_namespace n, pg_class c WHERE a.attnum > 0 AND NOT a.attisdropped AND a.attrelid = c.oid " +
      "and c.relkind not in ('S','v') and c.relnamespace = n.oid and n.nspname not in ('pg_catalog','pg_toast','information_schema')) x " +
      "left join pg_attrdef d on d.adrelid = x.attrelid and d.adnum = x.attnum " +
      "left join pg_constraint r on r.conrelid = x.oid and r.conkey[1] = x.attnum " +
      "left join pg_class f on r.confrelid = f.oid " +
      "left join pg_namespace fn on f.relnamespace = fn.oid " +
      "where x.relname = '" + inputs.tableName + "' and x.nspname = '" + schemaName + "' order by 1,2;";

    // Get Sequences to test if column auto-increments
    var autoIncrementQuery = "SELECT t.relname as related_table, a.attname as related_column, s.relname as sequence_name " +
      "FROM pg_class s JOIN pg_depend d ON d.objid = s.oid JOIN pg_class t ON d.objid = s.oid AND d.refobjid = t.oid " +
      "JOIN pg_attribute a ON (d.refobjid, d.refobjsubid) = (a.attrelid, a.attnum) JOIN pg_namespace n ON n.oid = s.relnamespace " +
      "WHERE s.relkind = 'S' AND n.nspname = '" + schemaName + "';";

    // Get Indexes
    var indiciesQuery = "SELECT n.nspname as \"Schema\", c.relname as \"Name\", CASE c.relkind WHEN 'r' THEN 'table' " +
      "WHEN 'v' THEN 'view' WHEN 'i' THEN 'index' WHEN 'S' THEN 'sequence' WHEN 's' THEN 'special' WHEN 'f' THEN " +
      "'foreign table' END as \"Type\", pg_catalog.pg_get_userbyid(c.relowner) as \"Owner\", c2.relname as \"Table\" " +
      "FROM pg_catalog.pg_class c LEFT JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace " +
      "LEFT JOIN pg_catalog.pg_index i ON i.indexrelid = c.oid " +
      "LEFT JOIN pg_catalog.pg_class c2 ON i.indrelid = c2.oid " +
      "WHERE c.relkind IN ('i','') AND n.nspname <> 'pg_catalog' AND n.nspname <> 'information_schema' " +
      "AND n.nspname !~ '^pg_toast' AND pg_catalog.pg_table_is_visible(c.oid) ORDER BY 1,2;";


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
          return done(new Error('There was an error spawning a connection from the pool.' + err.stack));
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
          return done(new Error('There was an error with a query used to describe the database.' + err.stack));
        }

        return done(null, report.result.rows);
      });
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


    //  ╔═╗╦═╗╔═╗╔═╗╔═╗╔═╗╔═╗  ┌─┐ ┬ ┬┌─┐┬─┐┬ ┬  ┬─┐┌─┐┌─┐┬ ┬┬ ┌┬┐┌─┐
    //  ╠═╝╠╦╝║ ║║  ║╣ ╚═╗╚═╗  │─┼┐│ │├┤ ├┬┘└┬┘  ├┬┘├┤ └─┐│ ││  │ └─┐
    //  ╩  ╩╚═╚═╝╚═╝╚═╝╚═╝╚═╝  └─┘└└─┘└─┘┴└─ ┴   ┴└─└─┘└─┘└─┘┴─┘┴ └─┘
    // Augments the schema for the model.
    var processQueryResults = function processQueryResults(describeResults, incrementResults, indiciesResults) {
      // Add autoIncrement flag to schema
      _.each(incrementResults, function processSequence(row) {
        if (row.related_table !== inputs.tableName) {
          return;
        }

        // Look through query results and see if related_column exists
        _.each(describeResults, function extendColumn(column) {
          if (column.Column !== row.related_column) {
            return;
          }

          column.autoIncrement = true;
        });
      });

      // Add index flag to schema
      _.each(indiciesResults, function processIndex(column) {
        var key = column.Name.split('_index_')[1];

        // Look through query results and see if key exists
        _.each(describeResults, function extendColumn(column) {
          if (column.Column !== key) {
            return;
          }

          column.indexed = true;
        });
      });

      // Normalize Schema
      var schema = {};
      _.each(describeResults, function normalize(column) {
        // Set Type
        schema[column.Column] = {
          type: column.Type
        };

        // Check for Primary Key
        if (column.Constraint && column.C === 'p') {
          schema[column.Column].primaryKey = true;
        }

        // Check for Unique Constraint
        if (column.Constraint && column.C === 'u') {
          schema[column.Column].unique = true;
        }

        // Check for autoIncrement
        if (column.autoIncrement) {
          schema[column.Column].autoIncrement = column.autoIncrement;
        }

        // Check for index
        if (column.indexed) {
          schema[column.Column].indexed = column.indexed;
        }
      });

      // Set Internal Schema Mapping
      dbSchema = schema;
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

    spawnConnection(function cb(err, connection) {
      if (err) {
        return exits.badConnection(err);
      }

      // Run DESCRIBE query
      runNativeQuery(connection, describeQuery, function cb(err, describeResults) {
        if (err) {
          releaseConnection(connection, function cb() {
            return exits.error(err);
          });
          return;
        }

        // Run AUTO INCREMENT query
        runNativeQuery(connection, autoIncrementQuery, function cb(err, incrementResults) {
          if (err) {
            releaseConnection(connection, function cb() {
              return exits.error(err);
            });
            return;
          }

          // Run INDICIES query
          runNativeQuery(connection, indiciesQuery, function cb(err, indiciesResults) {
            if (err) {
              releaseConnection(connection, function cb() {
                return exits.error(err);
              });
              return;
            }

            // Process the results
            processQueryResults(describeResults, incrementResults, indiciesResults);

            // Ensure the connection is always released
            releaseConnection(connection, function cb() {
              // Return the model schema
              return exits.success({ schema: dbSchema });
            });
          });
        });
      });
    });
  }

});
