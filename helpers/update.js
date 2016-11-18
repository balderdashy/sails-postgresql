//  ██╗   ██╗██████╗ ██████╗  █████╗ ████████╗███████╗     █████╗  ██████╗████████╗██╗ ██████╗ ███╗   ██╗
//  ██║   ██║██╔══██╗██╔══██╗██╔══██╗╚══██╔══╝██╔════╝    ██╔══██╗██╔════╝╚══██╔══╝██║██╔═══██╗████╗  ██║
//  ██║   ██║██████╔╝██║  ██║███████║   ██║   █████╗      ███████║██║        ██║   ██║██║   ██║██╔██╗ ██║
//  ██║   ██║██╔═══╝ ██║  ██║██╔══██║   ██║   ██╔══╝      ██╔══██║██║        ██║   ██║██║   ██║██║╚██╗██║
//  ╚██████╔╝██║     ██████╔╝██║  ██║   ██║   ███████╗    ██║  ██║╚██████╗   ██║   ██║╚██████╔╝██║ ╚████║
//   ╚═════╝ ╚═╝     ╚═════╝ ╚═╝  ╚═╝   ╚═╝   ╚══════╝    ╚═╝  ╚═╝ ╚═════╝   ╚═╝   ╚═╝ ╚═════╝ ╚═╝  ╚═══╝
//

module.exports = require('machine').build({


  friendlyName: 'Update',


  description: 'Update record(s) in the database based on a query criteria.',


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
      description: 'The name of the table to search for records to update in.',
      required: true,
      example: 'users'
    },

    criteria: {
      description: 'The Waterline criteria object to use for the query.',
      required: true,
      example: {}
    },

    values: {
      description: 'The values to set on the matching records.',
      required: true,
      example: '==='
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
      description: 'The records were successfully updated.',
      outputVariableName: 'records',
      example: {
        records: [{}]
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


  fn: function update(inputs, exits) {
    // Dependencies
    var util = require('util');
    var _ = require('lodash');
    var Converter = require('waterline-utils').query.converter;
    var Helpers = require('./private');


    // Find the model definition
    var model = inputs.models[inputs.tableName];
    if (!model) {
      return exits.invalidDatastore();
    }


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
        method: 'update',
        criteria: inputs.criteria,
        values: inputs.values,
        opts: {
          schema: schemaName
        }
      });

      // Add the postgres RETURNING * piece to the statement to prevent the
      // overhead of running two additional queries.
      statement.returning = '*';
    } catch (e) {
      return exits.error(new Error('The Waterline Query failed to convert into a Waterline Statement. ' + e.stack));
    }


    //  ╔═╗╔═╗╔╦╗╔═╗╦╦  ╔═╗  ┌─┐ ┬ ┬┌─┐┬─┐┬ ┬
    //  ║  ║ ║║║║╠═╝║║  ║╣   │─┼┐│ │├┤ ├┬┘└┬┘
    //  ╚═╝╚═╝╩ ╩╩  ╩╩═╝╚═╝  └─┘└└─┘└─┘┴└─ ┴
    // Compile statement into a native query.
    var query;
    try {
      query = Helpers.query.compileStatement(statement);
    } catch (e) {
      return exits.error(new Error('There was an issue compiling the statement: ' + util.inspect(statement, false, null) + '\n\n' + e.stack));
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


      //  ╦═╗╦ ╦╔╗╔  ┬ ┬┌─┐┌┬┐┌─┐┌┬┐┌─┐  ┌─┐ ┬ ┬┌─┐┬─┐┬ ┬
      //  ╠╦╝║ ║║║║  │ │├─┘ ││├─┤ │ ├┤   │─┼┐│ │├┤ ├┬┘└┬┘
      //  ╩╚═╚═╝╝╚╝  └─┘┴  ─┴┘┴ ┴ ┴ └─┘  └─┘└└─┘└─┘┴└─ ┴
      Helpers.query.updateRecord(connection, query, function updateRecordCb(err, updatedRecords) {
        // Always release the connection unless a leased connection from outside
        // the adapter was used.
        Helpers.connection.releaseConnection(connection, leased, function releaseConnectionCb() {
          if (err) {
            return exits.error(err);
          }

          return exits.success({ records: updatedRecords });
        }); // </ releaseConnection >
      }); // </ updateRecord >
    }); // </ spawnConnection >
  }
});
