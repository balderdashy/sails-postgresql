//  ██████╗ ███████╗███████╗████████╗██████╗  ██████╗ ██╗   ██╗     █████╗  ██████╗████████╗██╗ ██████╗ ███╗   ██╗
//  ██╔══██╗██╔════╝██╔════╝╚══██╔══╝██╔══██╗██╔═══██╗╚██╗ ██╔╝    ██╔══██╗██╔════╝╚══██╔══╝██║██╔═══██╗████╗  ██║
//  ██║  ██║█████╗  ███████╗   ██║   ██████╔╝██║   ██║ ╚████╔╝     ███████║██║        ██║   ██║██║   ██║██╔██╗ ██║
//  ██║  ██║██╔══╝  ╚════██║   ██║   ██╔══██╗██║   ██║  ╚██╔╝      ██╔══██║██║        ██║   ██║██║   ██║██║╚██╗██║
//  ██████╔╝███████╗███████║   ██║   ██║  ██║╚██████╔╝   ██║       ██║  ██║╚██████╗   ██║   ██║╚██████╔╝██║ ╚████║
//  ╚═════╝ ╚══════╝╚══════╝   ╚═╝   ╚═╝  ╚═╝ ╚═════╝    ╚═╝       ╚═╝  ╚═╝ ╚═════╝   ╚═╝   ╚═╝ ╚═════╝ ╚═╝  ╚═══╝
//

module.exports = require('machine').build({


  friendlyName: 'Destroy',


  description: 'Destroy record(s) in the database matching a query criteria.',


  inputs: {

    datastore: {
      description: 'The datastore to use for connections.',
      extendedDescription: 'Datastores represent the config and manager required to obtain an active database connection.',
      required: true,
      readOnly: true,
      example: '==='
    },

    models: {
      description: 'An object containing all of the model definitions that have been registered.',
      required: true,
      example: '==='
    },

    tableName: {
      description: 'The name of the table to destroy the record from.',
      required: true,
      example: 'users'
    },

    criteria: {
      description: 'The Waterline criteria object to use for the query.',
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
      description: 'The results of the destroy query.',
      outputVariableName: 'records',
      example: {
        numRecordsDeleted: 1
      }
    },

    invalidDatastore: {
      description: 'The datastore used is invalid. It is missing key pieces.'
    },

    badConnection: {
      friendlyName: 'Bad connection',
      description: 'A connection either could not be obtained or there was an error using the connection.'
    }

  },


  fn: function destroy(inputs, exits) {
    // Dependencies
    var _ = require('@sailshq/lodash');
    var Converter = require('waterline-utils').query.converter;
    var Helpers = require('./private');


    // Set a flag if a leased connection from outside the adapter was used or not.
    var leased = _.has(inputs.meta, 'leasedConnection');


    //  ╔═╗╦ ╦╔═╗╔═╗╦╔═  ┌─┐┌─┐┬─┐  ┌─┐  ┌─┐┌─┐  ┌─┐┌─┐┬ ┬┌─┐┌┬┐┌─┐
    //  ║  ╠═╣║╣ ║  ╠╩╗  ├┤ │ │├┬┘  ├─┤  ├─┘│ ┬  └─┐│  ├─┤├┤ │││├─┤
    //  ╚═╝╩ ╩╚═╝╚═╝╩ ╩  └  └─┘┴└─  ┴ ┴  ┴  └─┘  └─┘└─┘┴ ┴└─┘┴ ┴┴ ┴
    // This is a unique feature of Postgres. It may be passed in on a query
    // by query basis using the meta input or configured on the datastore. Default
    // to use the public schema.
    var schemaName = 'public';
    if (inputs.meta && inputs.meta.schemaName) {
      schemaName = inputs.meta.schemaName;
    } else if (inputs.datastore.config && inputs.datastore.config.schemaName) {
      schemaName = inputs.datastore.config.schemaName;
    }


    //  ╔═╗╔═╗╔╗╔╦  ╦╔═╗╦═╗╔╦╗  ┌┬┐┌─┐  ┌─┐┌┬┐┌─┐┌┬┐┌─┐┌┬┐┌─┐┌┐┌┌┬┐
    //  ║  ║ ║║║║╚╗╔╝║╣ ╠╦╝ ║    │ │ │  └─┐ │ ├─┤ │ ├┤ │││├┤ │││ │
    //  ╚═╝╚═╝╝╚╝ ╚╝ ╚═╝╩╚═ ╩    ┴ └─┘  └─┘ ┴ ┴ ┴ ┴ └─┘┴ ┴└─┘┘└┘ ┴
    // Convert the Waterline criteria into a Waterline Query Statement. This
    // turns it into something that is declarative and can be easily used to
    // build a SQL query.
    // See: https://github.com/treelinehq/waterline-query-docs for more info
    // on Waterline Query Statements.
    var statement;
    try {
      statement = Converter({
        model: inputs.tableName,
        method: 'destroy',
        criteria: inputs.criteria,
        opts: {
          schema: schemaName
        }
      });
    } catch (e) {
      return exits.error(e);
    }


    // Compile the original Waterline Query
    var query;
    try {
      query = Helpers.query.compileStatement(statement);
    } catch (e) {
      return exits.error(e);
    }


    //  ╔═╗╔═╗╔═╗╦ ╦╔╗╔  ┌─┐┌─┐┌┐┌┌┐┌┌─┐┌─┐┌┬┐┬┌─┐┌┐┌
    //  ╚═╗╠═╝╠═╣║║║║║║  │  │ │││││││├┤ │   │ ││ ││││
    //  ╚═╝╩  ╩ ╩╚╩╝╝╚╝  └─┘└─┘┘└┘┘└┘└─┘└─┘ ┴ ┴└─┘┘└┘
    //  ┌─┐┬─┐  ┬ ┬┌─┐┌─┐  ┬  ┌─┐┌─┐┌─┐┌─┐┌┬┐  ┌─┐┌─┐┌┐┌┌┐┌┌─┐┌─┐┌┬┐┬┌─┐┌┐┌
    //  │ │├┬┘  │ │└─┐├┤   │  ├┤ ├─┤└─┐├┤  ││  │  │ │││││││├┤ │   │ ││ ││││
    //  └─┘┴└─  └─┘└─┘└─┘  ┴─┘└─┘┴ ┴└─┘└─┘─┴┘  └─┘└─┘┘└┘┘└┘└─┘└─┘ ┴ ┴└─┘┘└┘
    // Spawn a new connection for running queries on.
    Helpers.connection.spawnOrLeaseConnection(inputs.datastore, inputs.meta, function spawnConnectionCb(err, connection) {
      if (err) {
        return exits.badConnection(err);
      }


      //  ╦═╗╦ ╦╔╗╔  ┌┬┐┌─┐┌─┐┌┬┐┬─┐┌─┐┬ ┬  ┌─┐ ┬ ┬┌─┐┬─┐┬ ┬
      //  ╠╦╝║ ║║║║   ││├┤ └─┐ │ ├┬┘│ │└┬┘  │─┼┐│ │├┤ ├┬┘└┬┘
      //  ╩╚═╚═╝╝╚╝  ─┴┘└─┘└─┘ ┴ ┴└─└─┘ ┴   └─┘└└─┘└─┘┴└─ ┴
      Helpers.query.runQuery({
        connection: connection,
        nativeQuery: query,
        queryType: 'delete',
        disconnectOnError: leased ? false : true
      },

      function runQueryCb(err, report) {
        // The connection will have been disconnected on error already if needed.
        if (err) {
          return exits.error(err);
        }

        // Always release the connection unless a leased connection from outside
        // the adapter was used.
        Helpers.connection.releaseConnection(connection, leased, function cb() {
          return exits.success({
            numRecordsDeleted: report.result.numRecordsDeleted
          });
        }); // </ releaseConnection >
      }); // </ runQuery >
    }); // </ spawnConnection >
  }
});
