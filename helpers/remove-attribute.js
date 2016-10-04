//  ██████╗ ███████╗███╗   ███╗ ██████╗ ██╗   ██╗███████╗     █████╗ ████████╗████████╗██████╗ ██╗██████╗ ██╗   ██╗████████╗███████╗
//  ██╔══██╗██╔════╝████╗ ████║██╔═══██╗██║   ██║██╔════╝    ██╔══██╗╚══██╔══╝╚══██╔══╝██╔══██╗██║██╔══██╗██║   ██║╚══██╔══╝██╔════╝
//  ██████╔╝█████╗  ██╔████╔██║██║   ██║██║   ██║█████╗      ███████║   ██║      ██║   ██████╔╝██║██████╔╝██║   ██║   ██║   █████╗
//  ██╔══██╗██╔══╝  ██║╚██╔╝██║██║   ██║╚██╗ ██╔╝██╔══╝      ██╔══██║   ██║      ██║   ██╔══██╗██║██╔══██╗██║   ██║   ██║   ██╔══╝
//  ██║  ██║███████╗██║ ╚═╝ ██║╚██████╔╝ ╚████╔╝ ███████╗    ██║  ██║   ██║      ██║   ██║  ██║██║██████╔╝╚██████╔╝   ██║   ███████╗
//  ╚═╝  ╚═╝╚══════╝╚═╝     ╚═╝ ╚═════╝   ╚═══╝  ╚══════╝    ╚═╝  ╚═╝   ╚═╝      ╚═╝   ╚═╝  ╚═╝╚═╝╚═════╝  ╚═════╝    ╚═╝   ╚══════╝
//

module.exports = require('machine').build({


  friendlyName: 'Remove Attribute',


  description: 'Remove an attribute from an existing table.',


  inputs: {

    datastore: {
      description: 'The datastore to use for connections.',
      extendedDescription: 'Datastores represent the config and manager required to obtain an active database connection.',
      required: true,
      readOnly: true,
      example: '==='
    },

    tableName: {
      description: 'The name of the table to create.',
      required: true,
      example: 'users'
    },

    attributeName: {
      description: 'The name of the attribute to remove.',
      required: true,
      example: 'name'
    }

  },


  exits: {

    success: {
      description: 'The attribute was removed successfully.'
    },

    badConfiguration: {
      description: 'The configuration was invalid.'
    }

  },


  fn: function removeAttribute(inputs, exits) {
    var PG = require('machinepack-postgresql');
    var Helpers = require('./private');


    // Find the model in the datastore
    var model = inputs.datastore.models[inputs.tableName];

    // Determine a schema name
    var schemaName = model.meta && model.meta.schemaName || 'public';


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
          return done(new Error('There was an erro spawning a connection from the pool.' + err.stack));
        },
        success: function success(connection) {
          return done(null, connection);
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
          return done(new Error('There was an error running the native query for remove attribute.' + err.stack));
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
          return done(new Error('There was an error releasing the connection from the pool.' + err.stack));
        },
        badConnection: function badConnection() {
          return done(new Error('Bad connection when trying to release an active connection.'));
        },
        success: function success() {
          return done();
        }
      });
    };


    //  ╦═╗╦ ╦╔╗╔  ┌─┐ ┬ ┬┌─┐┬─┐┬ ┬  ┌─┐┌┐┌┌┬┐  ┬─┐┌─┐┬  ┌─┐┌─┐┌─┐┌─┐
    //  ╠╦╝║ ║║║║  │─┼┐│ │├┤ ├┬┘└┬┘  ├─┤│││ ││  ├┬┘├┤ │  ├┤ ├─┤└─┐├┤
    //  ╩╚═╚═╝╝╚╝  └─┘└└─┘└─┘┴└─ ┴   ┴ ┴┘└┘─┴┘  ┴└─└─┘┴─┘└─┘┴ ┴└─┘└─┘
    //  ┌─┐┌─┐┌┐┌┌┐┌┌─┐┌─┐┌┬┐┬┌─┐┌┐┌
    //  │  │ │││││││├┤ │   │ ││ ││││
    //  └─┘└─┘┘└┘┘└┘└─┘└─┘ ┴ ┴└─┘┘└┘
    var runQuery = function runQuery(connection, query, done) {
      runNativeQuery(connection, query, function cb(err) {
        // Always release the connection no matter what the error state.
        releaseConnection(connection, function cb() {
          // If the native query had an error, return that error
          if (err) {
            return done(err);
          }

          return done();
        });
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

      // Escape Table Name
      var tableName = escapeName(inputs.tableName, schemaName);

      // Build Query
      var query = 'ALTER TABLE ' + tableName + ' DROP COLUMN "' + inputs.attributeName + '" RESTRICT';

      // Run the ALTER TABLE query and release the connection
      runQuery(connection, query, function cb(err) {
        if (err) {
          return exits.error(err);
        }

        return exits.success();
      });
    });
  }

});
