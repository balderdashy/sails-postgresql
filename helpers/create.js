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
      example: '==='
    },

    models: {
      description: 'An object containing all of the model definitions that have been registered.',
      required: true,
      example: '==='
    },

    tableName: {
      description: 'The name of the table to insert the record into.',
      required: true,
      example: 'users'
    },

    record: {
      description: 'The record to insert into the table. It should match the schema used to build the table.',
      required: true,
      readOnly: true,
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
      description: 'The record was successfully inserted.',
      outputVariableName: 'record',
      example: {
        record: {}
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


  fn: function create(inputs, exits) {
    // Dependencies
    var _ = require('lodash');
    var utils = require('waterline-utils');
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


    //  ╔═╗╦ ╦╔═╗╔═╗╦╔═  ┌─┐┌─┐┬─┐  ┌─┐┌─┐┌─┐ ┬ ┬┌─┐┌┐┌┌─┐┌─┐┌─┐
    //  ║  ╠═╣║╣ ║  ╠╩╗  ├┤ │ │├┬┘  └─┐├┤ │─┼┐│ │├┤ ││││  ├┤ └─┐
    //  ╚═╝╩ ╩╚═╝╚═╝╩ ╩  └  └─┘┴└─  └─┘└─┘└─┘└└─┘└─┘┘└┘└─┘└─┘└─┘
    // If any auto-incrementing sequences are used on the table and the values
    // are manually being set in the record then there will be an issue. To fix
    // this the sequence needs to be updated to use the newer value.
    //
    // An example of this would be if the schema had an id property set as an
    // auto-incrementing field and a record was being created that had the id
    // set to 5.
    //
    // If the sequence isn't up to 5, say it's only on 2, then the next record
    // that gets inserted without an id field defined will just get the next
    // value in the sequence, (3). This could end up generating a non-unique id
    // when it eventually gets to 5.
    //
    // To prevent this the sequence is updated manually whenever a record is
    // created that has any of the sequence values defined.
    var incrementSequences = [];
    _.each(model.definition, function checkSequences(val, key) {
      if (!_.has(val, 'autoIncrement')) {
        return;
      }

      if (_.indexOf(_.keys(inputs.record), key) < 0) {
        return;
      }

      incrementSequences.push(key);
    });


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
        model: inputs.tableName,
        method: 'create',
        values: inputs.record
      });

      // Add the postgres schema object to the statement
      statement.opts = {
        schema: schemaName
      };
    } catch (e) {
      return exits.error(new Error('The Waterline query could not be converted.' + e.message));
    }


    //  ╔═╗╔═╗╔═╗╦ ╦╔╗╔  ┌─┐┌─┐┌┐┌┌┐┌┌─┐┌─┐┌┬┐┬┌─┐┌┐┌
    //  ╚═╗╠═╝╠═╣║║║║║║  │  │ │││││││├┤ │   │ ││ ││││
    //  ╚═╝╩  ╩ ╩╚╩╝╝╚╝  └─┘└─┘┘└┘┘└┘└─┘└─┘ ┴ ┴└─┘┘└┘
    //  ┌─┐┬─┐  ┬ ┬┌─┐┌─┐  ┬  ┌─┐┌─┐┌─┐┌─┐┌┬┐  ┌─┐┌─┐┌┐┌┌┐┌┌─┐┌─┐┌┬┐┬┌─┐┌┐┌
    //  │ │├┬┘  │ │└─┐├┤   │  ├┤ ├─┤└─┐├┤  ││  │  │ │││││││├┤ │   │ ││ ││││
    //  └─┘┴└─  └─┘└─┘└─┘  ┴─┘└─┘┴ ┴└─┘└─┘─┴┘  └─┘└─┘┘└┘┘└┘└─┘└─┘ ┴ ┴└─┘┘└┘
    // Spawn a new connection and open a transaction for running queries on.
    Helpers.connection.spawnTransaction(inputs.datastore, inputs.meta, function spawnTransactionCb(err, connection) {
      if (err) {
        return exits.badConnection(err);
      }


      // Find the Primary Key and add a "returning" clause to the statement.
      var primaryKeyField = Helpers.schema.findPrimaryKey(model.definition);

      // Return the values of the primary key field
      statement.returning = primaryKeyField;


      //  ╔═╗╔═╗╔╦╗╔═╗╦╦  ╔═╗  ┌─┐ ┬ ┬┌─┐┬─┐┬ ┬
      //  ║  ║ ║║║║╠═╝║║  ║╣   │─┼┐│ │├┤ ├┬┘└┬┘
      //  ╚═╝╚═╝╩ ╩╩  ╩╩═╝╚═╝  └─┘└└─┘└─┘┴└─ ┴
      // Compile the original Waterline Query
      var query;
      try {
        query = Helpers.query.compileStatement(statement);
      } catch (e) {
        // If the statement could not be compiled, release the connection and end
        // the transaction.
        Helpers.connection.rollbackAndRelease(connection, leased, function rollbackAndReleaseCb() {
          return exits.error(e);
        });

        return;
      }

      //  ╦╔╗╔╔═╗╔═╗╦═╗╔╦╗  ┬─┐┌─┐┌─┐┌─┐┬─┐┌┬┐
      //  ║║║║╚═╗║╣ ╠╦╝ ║   ├┬┘├┤ │  │ │├┬┘ ││
      //  ╩╝╚╝╚═╝╚═╝╩╚═ ╩   ┴└─└─┘└─┘└─┘┴└──┴┘
      // Insert the record and return the new values
      Helpers.query.insertRecord({
        connection: connection,
        query: query,
        model: model,
        schemaName: schemaName,
        tableName: inputs.tableName,
        leased: leased
      },

      function insertRecordCb(err, insertedRecords) {
        if (err) {
          return exits.error(err);
        }

        //  ╔═╗╔╗╔╔═╗╦ ╦╦═╗╔═╗  ┌─┐┌─┐┌─┐ ┬ ┬┌─┐┌┐┌┌─┐┌─┐┌─┐
        //  ║╣ ║║║╚═╗║ ║╠╦╝║╣   └─┐├┤ │─┼┐│ │├┤ ││││  ├┤ └─┐
        //  ╚═╝╝╚╝╚═╝╚═╝╩╚═╚═╝  └─┘└─┘└─┘└└─┘└─┘┘└┘└─┘└─┘└─┘
        //  ┌─┐┬─┐┌─┐  ┬ ┬┌─┐  ┌┬┐┌─┐  ┌┬┐┌─┐┌┬┐┌─┐
        //  ├─┤├┬┘├┤   │ │├─┘   │ │ │   ││├─┤ │ ├┤
        //  ┴ ┴┴└─└─┘  └─┘┴     ┴ └─┘  ─┴┘┴ ┴ ┴ └─┘
        // Update any sequences that may have been used
        Helpers.schema.setSequenceValues({
          connection: connection,
          sequences: incrementSequences,
          record: inputs.record,
          schemaName: schemaName,
          tableName: inputs.tableName,
          leased: leased
        },

        function setSequencesCb(err) {
          if (err) {
            // If there was an error, release the connection and end the transaction.
            Helpers.connection.rollbackAndRelease(connection, leased, function rollbackAndReleaseCb() {
              return exits.error(err);
            });

            return;
          }

          // Commit the transaction
          Helpers.connection.commitAndRelease(connection, leased, function commitCb(err) {
            if (err) {
              return exits.error(new Error('There was an error commiting the transaction.' + err.stack));
            }

            // Only return the first record (there should only ever be one)
            var insertedRecord = _.first(insertedRecords);
            return exits.success({ record: insertedRecord });
          }); // </ .commitAndRelease(); >
        }); // </ .setSequenceValues(); >
      }); // </ .insertRecord(); >
    }); // </ .spawnTransaction(); >
  }
});
