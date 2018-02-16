//   ██████╗██████╗ ███████╗ █████╗ ████████╗███████╗     █████╗  ██████╗████████╗██╗ ██████╗ ███╗   ██╗
//  ██╔════╝██╔══██╗██╔════╝██╔══██╗╚══██╔══╝██╔════╝    ██╔══██╗██╔════╝╚══██╔══╝██║██╔═══██╗████╗  ██║
//  ██║     ██████╔╝█████╗  ███████║   ██║   █████╗      ███████║██║        ██║   ██║██║   ██║██╔██╗ ██║
//  ██║     ██╔══██╗██╔══╝  ██╔══██║   ██║   ██╔══╝      ██╔══██║██║        ██║   ██║██║   ██║██║╚██╗██║
//  ╚██████╗██║  ██║███████╗██║  ██║   ██║   ███████╗    ██║  ██║╚██████╗   ██║   ██║╚██████╔╝██║ ╚████║
//   ╚═════╝╚═╝  ╚═╝╚══════╝╚═╝  ╚═╝   ╚═╝   ╚══════╝    ╚═╝  ╚═╝ ╚═════╝   ╚═╝   ╚═╝ ╚═════╝ ╚═╝  ╚═══╝
//

module.exports = require('machine').build({


  friendlyName: 'Create',


  description: 'Insert a record into a table in the database.',


  inputs: {

    datastore: {
      description: 'The datastore to use for connections.',
      extendedDescription: 'Datastores represent the config and manager required to obtain an active database connection.',
      required: true,
      readOnly: true,
      type: 'ref'
    },

    models: {
      description: 'An object containing all of the model definitions that have been registered.',
      required: true,
      type: 'ref'
    },

    query: {
      description: 'A valid stage three Waterline query.',
      required: true,
      type: 'ref'
    }

  },


  exits: {

    success: {
      description: 'The record was successfully inserted.',
      outputVariableName: 'record',
      outputType: 'ref'
    },

    invalidDatastore: {
      description: 'The datastore used is invalid. It is missing key pieces.'
    },

    badConnection: {
      friendlyName: 'Bad connection',
      description: 'A connection either could not be obtained or there was an error using the connection.'
    },

    notUnique: {
      friendlyName: 'Not Unique',
      outputType: 'ref'
    }

  },


  fn: function create(inputs, exits) {
    // Dependencies
    var _ = require('@sailshq/lodash');
    var utils = require('waterline-utils');
    var Helpers = require('./private');


    // Store the Query input for easier access
    var query = inputs.query;
    query.meta = query.meta || {};

    // Find the model definition
    var model = inputs.models[query.using];
    if (!model) {
      return exits.invalidDatastore();
    }

    // Build up a faux ORM instance for processing records
    var orm = {
      collections: inputs.models
    };

    // Set a flag if a leased connection from outside the adapter was used or not.
    var leased = _.has(query.meta, 'leasedConnection');

    // Set a flag to determine if records are being returned
    var fetchRecords = false;


    //  ╔═╗╦ ╦╔═╗╔═╗╦╔═  ┌─┐┌─┐┬─┐  ┌─┐  ┌─┐┌─┐  ┌─┐┌─┐┬ ┬┌─┐┌┬┐┌─┐
    //  ║  ╠═╣║╣ ║  ╠╩╗  ├┤ │ │├┬┘  ├─┤  ├─┘│ ┬  └─┐│  ├─┤├┤ │││├─┤
    //  ╚═╝╩ ╩╚═╝╚═╝╩ ╩  └  └─┘┴└─  ┴ ┴  ┴  └─┘  └─┘└─┘┴ ┴└─┘┴ ┴┴ ┴
    // This is a unique feature of Postgres. It may be passed in on a query
    // by query basis using the meta input or configured on the datastore. Default
    // to use the public schema.
    var schemaName = 'public';
    if (_.has(query.meta, 'schemaName')) {
      schemaName = query.meta.schemaName;
    } else if (inputs.datastore.config && inputs.datastore.config.schemaName) {
      schemaName = inputs.datastore.config.schemaName;
    }


    //  ┌─┐┬─┐┌─┐  ┌─┐┬─┐┌─┐┌─┐┌─┐┌─┐┌─┐  ┌─┐┌─┐┌─┐┬ ┬  ┬─┐┌─┐┌─┐┌─┐┬─┐┌┬┐
    //  ├─┘├┬┘├┤───├─┘├┬┘│ ││  ├┤ └─┐└─┐  ├┤ ├─┤│  ├─┤  ├┬┘├┤ │  │ │├┬┘ ││
    //  ┴  ┴└─└─┘  ┴  ┴└─└─┘└─┘└─┘└─┘└─┘  └─┘┴ ┴└─┘┴ ┴  ┴└─└─┘└─┘└─┘┴└──┴┘
    try {
      Helpers.query.preProcessEachRecord({
        records: [query.newRecord],
        identity: model.identity,
        orm: orm
      });
    } catch (e) {
      return exits.error(e);
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
      statement = utils.query.converter({
        model: query.using,
        method: 'create',
        values: query.newRecord,
        opts: {
          schema: schemaName
        }
      });
    } catch (e) {
      return exits.error(e);
    }


    //  ╔╦╗╔═╗╔╦╗╔═╗╦═╗╔╦╗╦╔╗╔╔═╗  ┬ ┬┬ ┬┬┌─┐┬ ┬  ┬  ┬┌─┐┬  ┬ ┬┌─┐┌─┐
    //   ║║║╣  ║ ║╣ ╠╦╝║║║║║║║║╣   │││├─┤││  ├─┤  └┐┌┘├─┤│  │ │├┤ └─┐
    //  ═╩╝╚═╝ ╩ ╚═╝╩╚═╩ ╩╩╝╚╝╚═╝  └┴┘┴ ┴┴└─┘┴ ┴   └┘ ┴ ┴┴─┘└─┘└─┘└─┘
    //  ┌┬┐┌─┐  ┬─┐┌─┐┌┬┐┬ ┬┬─┐┌┐┌
    //   │ │ │  ├┬┘├┤  │ │ │├┬┘│││
    //   ┴ └─┘  ┴└─└─┘ ┴ └─┘┴└─┘└┘
    if (_.has(query.meta, 'fetch') && query.meta.fetch) {
      fetchRecords = true;

      // Add the postgres RETURNING * piece to the statement to prevent the
      // overhead of running two additional queries.
      statement.returning = '*';
    }


    // Find the Primary Key
    var primaryKeyField = model.primaryKey;
    var primaryKeyColumnName = model.definition[primaryKeyField].columnName;

    // Remove primary key if the value is NULL. This allows the auto-increment
    // to work properly if set.
    if (_.isNull(statement.insert[primaryKeyColumnName])) {
      delete statement.insert[primaryKeyColumnName];
    }


    //  ╔═╗╔═╗╔═╗╦ ╦╔╗╔  ┌─┐┌─┐┌┐┌┌┐┌┌─┐┌─┐┌┬┐┬┌─┐┌┐┌
    //  ╚═╗╠═╝╠═╣║║║║║║  │  │ │││││││├┤ │   │ ││ ││││
    //  ╚═╝╩  ╩ ╩╚╩╝╝╚╝  └─┘└─┘┘└┘┘└┘└─┘└─┘ ┴ ┴└─┘┘└┘
    //  ┌─┐┬─┐  ┬ ┬┌─┐┌─┐  ┬  ┌─┐┌─┐┌─┐┌─┐┌┬┐  ┌─┐┌─┐┌┐┌┌┐┌┌─┐┌─┐┌┬┐┬┌─┐┌┐┌
    //  │ │├┬┘  │ │└─┐├┤   │  ├┤ ├─┤└─┐├┤  ││  │  │ │││││││├┤ │   │ ││ ││││
    //  └─┘┴└─  └─┘└─┘└─┘  ┴─┘└─┘┴ ┴└─┘└─┘─┴┘  └─┘└─┘┘└┘┘└┘└─┘└─┘ ┴ ┴└─┘┘└┘
    // Spawn a new connection for running queries on.
    Helpers.connection.spawnOrLeaseConnection(inputs.datastore, query.meta, function spawnOrLeaseConnectionCb(err, connection) {
      if (err) {
        return exits.badConnection(err);
      }


      //  ╔═╗╔═╗╔╦╗╔═╗╦╦  ╔═╗  ┌─┐ ┬ ┬┌─┐┬─┐┬ ┬
      //  ║  ║ ║║║║╠═╝║║  ║╣   │─┼┐│ │├┤ ├┬┘└┬┘
      //  ╚═╝╚═╝╩ ╩╩  ╩╩═╝╚═╝  └─┘└└─┘└─┘┴└─ ┴
      // Compile the original Waterline Query
      var compiledQuery;
      try {
        compiledQuery = Helpers.query.compileStatement(statement);
      } catch (e) {
        // If the statement could not be compiled, release the connection and end
        // the transaction.
        Helpers.connection.releaseConnection(connection, leased, function releaseConnectionCb() {
          return exits.error(e);
        });

        return;
      }

      //  ╦╔╗╔╔═╗╔═╗╦═╗╔╦╗  ┬─┐┌─┐┌─┐┌─┐┬─┐┌┬┐
      //  ║║║║╚═╗║╣ ╠╦╝ ║   ├┬┘├┤ │  │ │├┬┘ ││
      //  ╩╝╚╝╚═╝╚═╝╩╚═ ╩   ┴└─└─┘└─┘└─┘┴└──┴┘
      // Insert the record and return the new values
      Helpers.query.modifyRecord({
        connection: connection,
        query: compiledQuery.nativeQuery,
        valuesToEscape: compiledQuery.valuesToEscape,
        leased: leased,
        fetchRecords: fetchRecords
      },

      function modifyRecordCb(err, insertedRecords) {
        // If there was an error the helper takes care of closing the connection
        // if a connection was spawned internally.
        if (err) {
          if (err.footprint && err.footprint.identity === 'notUnique') {
            return exits.notUnique(err);
          }

          return exits.error(err);
        }

        // Release the connection if needed.
        Helpers.connection.releaseConnection(connection, leased, function releaseCb() {
          if (fetchRecords) {
            // Process each record to normalize output
            try {
              Helpers.query.processEachRecord({
                records: insertedRecords,
                identity: model.identity,
                orm: orm
              });
            } catch (e) {
              return exits.error(e);
            }

            // Only return the first record (there should only ever be one)
            var insertedRecord = _.first(insertedRecords);
            return exits.success({ record: insertedRecord });
          }

          return exits.success();
        }); // </ .releaseConnection(); >
      }); // </ .insertRecord(); >
    }); // </ .spawnOrLeaseConnection(); >
  }
});
