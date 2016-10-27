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
    // Dependencies
    var Helpers = require('./private');


    //  ╔═╗╔═╗╔═╗╦ ╦╔╗╔  ┌─┐┌─┐┌┐┌┌┐┌┌─┐┌─┐┌┬┐┬┌─┐┌┐┌
    //  ╚═╗╠═╝╠═╣║║║║║║  │  │ │││││││├┤ │   │ ││ ││││
    //  ╚═╝╩  ╩ ╩╚╩╝╝╚╝  └─┘└─┘┘└┘┘└┘└─┘└─┘ ┴ ┴└─┘┘└┘
    Helpers.connection.spawnConnection(inputs.datastore, function spawnConnectionCb(err, connection) {
      if (err) {
        return exits.badConnection(err);
      }

      // Normalize query if data was passed in
      var query = {
        sql: inputs.query,
        bindings: inputs.data
      };


      //  ╦═╗╦ ╦╔╗╔  ┌┐┌┌─┐┌┬┐┬┬  ┬┌─┐  ┌─┐ ┬ ┬┌─┐┬─┐┬ ┬
      //  ╠╦╝║ ║║║║  │││├─┤ │ │└┐┌┘├┤   │─┼┐│ │├┤ ├┬┘└┬┘
      //  ╩╚═╚═╝╝╚╝  ┘└┘┴ ┴ ┴ ┴ └┘ └─┘  └─┘└└─┘└─┘┴└─ ┴
      Helpers.query.runNativeQuery(connection, query, function runNativeQueryCb(err, results) {
        // Always release the connection back into the pool
        Helpers.connection.releaseConnection(connection, function releaseConnectionCb() {
          if (err) {
            return exits.error(err);
          }

          return exits.success(results);
        }); // </ releaseConnection >
      }); // </ runNativeQuery >
    }); // </ spawnConnection >
  }
});
