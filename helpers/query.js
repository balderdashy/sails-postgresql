//   ██████╗ ██╗   ██╗███████╗██████╗ ██╗   ██╗
//  ██╔═══██╗██║   ██║██╔════╝██╔══██╗╚██╗ ██╔╝
//  ██║   ██║██║   ██║█████╗  ██████╔╝ ╚████╔╝
//  ██║▄▄ ██║██║   ██║██╔══╝  ██╔══██╗  ╚██╔╝
//  ╚██████╔╝╚██████╔╝███████╗██║  ██║   ██║
//   ╚══▀▀═╝  ╚═════╝ ╚══════╝╚═╝  ╚═╝   ╚═╝
//

module.exports = require('machine').build({


  friendlyName: 'Query',


  description: 'Open a connection and run a native query.',


  inputs: {

    datastore: {
      description: 'The datastore to use for connections.',
      extendedDescription: 'Datastores represent the config and manager required to obtain an active database connection.',
      required: true,
      readOnly: true,
      example: '==='
    },

    query: {
      description: 'The name of the table to destroy.',
      required: true,
      example: 'SELECT * FROM "users";'
    },

    data: {
      description: 'Data to use in parameterized queries.',
      example: [],
      defaultsTo: []
    }

  },


  exits: {

    success: {
      description: 'The query was run successfully.',
      example: '==='
    },

    badConnection: {
      friendlyName: 'Bad connection',
      description: 'A connection either could not be obtained or there was an error using the connection.'
    }

  },


  fn: function drop(inputs, exits) {
    var PG = require('machinepack-postgresql');
    var Helpers = require('./private');

    //  ╔═╗╔═╗╔═╗╦ ╦╔╗╔  ┌─┐┌─┐┌┐┌┌┐┌┌─┐┌─┐┌┬┐┬┌─┐┌┐┌
    //  ╚═╗╠═╝╠═╣║║║║║║  │  │ │││││││├┤ │   │ ││ ││││
    //  ╚═╝╩  ╩ ╩╚╩╝╝╚╝  └─┘└─┘┘└┘┘└┘└─┘└─┘ ┴ ┴└─┘┘└┘
    var spawnConnection = function spawnConnection(done) {
      Helpers.spawnConnection(inputs.datastore, function cb(err, connection) {
        if (err) {
          return done(new Error('Failed to spawn a connection from the pool.' + err.stack));
        }

        return done(null, connection);
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
          return done(new Error('There was an error running the native query. It could be invalid or malformed. \n\n' + query + '\n\n' + err.stack));
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


    spawnConnection(function cb(err, connection) {
      if (err) {
        return exits.badConnection(err);
      }

      // Normalize query if data was passed in
      var query = {
        sql: inputs.query,
        bindings: inputs.data
      };

      // Run the native query
      runNativeQuery(connection, query, function cb(err, results) {
        if (err) {
          releaseConnection(connection, function cb() {
            return exits.error(err);
          });
          return;
        }

        // Release the connection back to the pool
        releaseConnection(connection, function cb(err) {
          if (err) {
            return exits.error(err);
          }

          return exits.success(results);
        });
      });
    });
  }

});
