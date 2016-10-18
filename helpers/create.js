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
    var _ = require('lodash');
    var async = require('async');
    var PG = require('machinepack-postgresql');
    var Converter = require('waterline-query-parser').converter;
    var Helpers = require('./private');

    // Find the model definition
    var model = inputs.models[inputs.tableName];
    if (!model) {
      return exits.invalidDatastore();
    }


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


    //  ╔═╗╔═╗╦═╗╦╔═╗╦  ╦╔═╗╔═╗  ┬─┐┌─┐┌─┐┌─┐┬─┐┌┬┐
    //  ╚═╗║╣ ╠╦╝║╠═╣║  ║╔═╝║╣   ├┬┘├┤ │  │ │├┬┘ ││
    //  ╚═╝╚═╝╩╚═╩╩ ╩╩═╝╩╚═╝╚═╝  ┴└─└─┘└─┘└─┘┴└──┴┘
    // Ensure that all the values being stored are valid for the database.
    var serializedValues;
    try {
      serializedValues = Helpers.serializeValues(inputs.record);
    } catch (e) {
      return exits.error(new Error('There was an error serializing the insert values.' + e.stack));
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
        method: 'create',
        values: serializedValues
      });

      // Add the postgres schema object to the statement
      statement.opts = {
        schema: schemaName
      };
    } catch (e) {
      return exits.error(new Error('The Waterline query could not be converted.' + e.stack));
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


    //  ╔═╗╔═╗╔╦╗╔═╗╦╦  ╔═╗  ┌─┐┌┬┐┌─┐┌┬┐┌─┐┌┬┐┌─┐┌┐┌┌┬┐
    //  ║  ║ ║║║║╠═╝║║  ║╣   └─┐ │ ├─┤ │ ├┤ │││├┤ │││ │
    //  ╚═╝╚═╝╩ ╩╩  ╩╩═╝╚═╝  └─┘ ┴ ┴ ┴ ┴ └─┘┴ ┴└─┘┘└┘ ┴
    // Transform the Waterline Query Statement into a SQL query.
    var compileStatement = function compileStatement() {
      // Find the Primary Key and add a "returning" clause to the statement.
      var primaryKeyField = findPrimaryKey();

      // Return the values of the primary key field
      statement.returning = primaryKeyField;

      var report;
      try {
        report = PG.compileStatement({
          statement: statement
        }).execSync();
      } catch (e) {
        throw new Error('Could not compile the statement.\n\n' + e.stack);
      }

      return report.nativeQuery;
    };


    //  ╔═╗╦╔╗╔╔╦╗  ┌─┐┬─┐┬┌┬┐┌─┐┬─┐┬ ┬  ┬┌─┌─┐┬ ┬
    //  ╠╣ ║║║║ ║║  ├─┘├┬┘││││├─┤├┬┘└┬┘  ├┴┐├┤ └┬┘
    //  ╚  ╩╝╚╝═╩╝  ┴  ┴└─┴┴ ┴┴ ┴┴└─ ┴   ┴ ┴└─┘ ┴
    var findPrimaryKey = function findPrimaryKey() {
      var pk;
      try {
        pk = Helpers.findPrimaryKey(model.definition);
      } catch (e) {
        throw new Error('Could not determine a Primary Key for the model: ' + model.tableName + '.\n\n' + e.stack);
      }

      return pk;
    };


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


    //  ╔╗ ╔═╗╔═╗╦╔╗╔  ┌┬┐┬─┐┌─┐┌┐┌┌─┐┌─┐┌─┐┌┬┐┬┌─┐┌┐┌
    //  ╠╩╗║╣ ║ ╦║║║║   │ ├┬┘├─┤│││└─┐├─┤│   │ ││ ││││
    //  ╚═╝╚═╝╚═╝╩╝╚╝   ┴ ┴└─┴ ┴┘└┘└─┘┴ ┴└─┘ ┴ ┴└─┘┘└┘
    var beginTransaction = function beginTransaction(connection, done) {
      PG.beginTransaction({
        connection: connection
      })
      .exec({
        // If there was an error opening a transaction, release the connection.
        // After releasing the connection always return the original error.
        error: function error(err) {
          PG.releaseConnection({
            connection: connection
          }).exec({
            error: function error(err) {
              return done(new Error('There was an error releasing the connection back into the pool.' + err.stack));
            },
            success: function success() {
              return done(new Error('There was an error starting a transaction.' + err.stack));
            }
          });
        },
        success: function success() {
          return done();
        }
      });
    };


    //  ╔═╗╔═╗╔═╗╦ ╦╔╗╔  ┌─┐┌─┐┌┐┌┌┐┌┌─┐┌─┐┌┬┐┬┌─┐┌┐┌
    //  ╚═╗╠═╝╠═╣║║║║║║  │  │ │││││││├┤ │   │ ││ ││││
    //  ╚═╝╩  ╩ ╩╚╩╝╝╚╝  └─┘└─┘┘└┘┘└┘└─┘└─┘ ┴ ┴└─┘┘└┘
    //   ┬   ╔═╗╔╦╗╔═╗╦═╗╔╦╗  ┌┬┐┬─┐┌─┐┌┐┌┌─┐┌─┐┌─┐┌┬┐┬┌─┐┌┐┌
    //  ┌┼─  ╚═╗ ║ ╠═╣╠╦╝ ║    │ ├┬┘├─┤│││└─┐├─┤│   │ ││ ││││
    //  └┘   ╚═╝ ╩ ╩ ╩╩╚═ ╩    ┴ ┴└─┴ ┴┘└┘└─┘┴ ┴└─┘ ┴ ┴└─┘┘└┘
    var spawnTransaction = function spawnTransaction(done) {
      spawnConnection(function cb(err, connection) {
        if (err) {
          return done(err);
        }

        beginTransaction(connection, function cb(err) {
          if (err) {
            return done(err);
          }

          return done(null, connection);
        });
      });
    };


    //  ╦═╗╦ ╦╔╗╔  ┬┌┐┌┌─┐┌─┐┬─┐┌┬┐  ┌─┐ ┬ ┬┌─┐┬─┐┬ ┬
    //  ╠╦╝║ ║║║║  ││││└─┐├┤ ├┬┘ │   │─┼┐│ │├┤ ├┬┘└┬┘
    //  ╩╚═╚═╝╝╚╝  ┴┘└┘└─┘└─┘┴└─ ┴   └─┘└└─┘└─┘┴└─ ┴
    var runInsertQuery = function runInsertQuery(connection, query, done) {
      Helpers.runQuery({
        connection: connection,
        nativeQuery: query,
        queryType: 'insert',
        disconnectOnError: false
      }, function cb(err, report) {
        // If the query failed to run, rollback the transaction and release the connection.
        if (err) {
          Helpers.rollbackAndRelease(connection, function _rollbackCB(err) {
            if (err) {
              return done(new Error('There was an error rolling back the transaction.\n\n' + err.stack));
            }

            return done(new Error('There was an error attempting to run the query: ' + '\n\n' + query.sql + '\nusing values: (' + query.bindings + ')\n\n' + 'The transaction has been rolled back.\n\n' + err.stack));
          });

          return;
        }

        return done(null, report.result);
      });
    };


    //  ╔═╗╦╔╗╔╔╦╗  ┬┌┐┌┌─┐┌─┐┬─┐┌┬┐┌─┐┌┬┐  ┬─┐┌─┐┌─┐┌─┐┬─┐┌┬┐┌─┐
    //  ╠╣ ║║║║ ║║  ││││└─┐├┤ ├┬┘ │ ├┤  ││  ├┬┘├┤ │  │ │├┬┘ ││└─┐
    //  ╚  ╩╝╚╝═╩╝  ┴┘└┘└─┘└─┘┴└─ ┴ └─┘─┴┘  ┴└─└─┘└─┘└─┘┴└──┴┘└─┘
    var runFindQuery = function runFindQuery(connection, insertResults, done) {
      // Find the Primary Key field for the model
      var pk = findPrimaryKey();

      // Build up a criteria statement to run
      var criteriaStatement = {
        select: ['*'],
        from: inputs.tableName,
        where: {},
        opts: {
          schema: schemaName
        }
      };

      // Insert dynamic primary key value into query
      criteriaStatement.where[pk] = {
        in: insertResults.inserted
      };

      // Build an IN query from the results of the insert query
      var report;
      try {
        report = PG.compileStatement({
          statement: criteriaStatement
        }).execSync();
      } catch (e) {
        return done(new Error('There was an error compiling the statement into a query.\n\n' + e.stack));
      }

      // Run the FIND query
      Helpers.runQuery({
        connection: connection,
        nativeQuery: report.nativeQuery,
        queryType: 'select',
        disconnectOnError: false
      }, function cb(err, report) {
        // If the query failed to run, rollback the transaction and release the connection.
        if (err) {
          Helpers.rollbackAndRelease(connection, function _rollbackCB(err) {
            if (err) {
              return done(new Error('There was an error rolling back the transaction.\n\n' + err.stack));
            }

            return done(new Error('There was an error attempting to run the query: ' + '\n\n' + report.nativeQuery.sql + '\nusing values: (' + report.nativeQuery.bindings + ')\n\n' + 'The transaction has been rolled back.\n\n' + err.stack));
          });

          return;
        }

        return done(null, report.result);
      });
    };


    //  ╦╔╗╔╔═╗╔═╗╦═╗╔╦╗  ┌─┐┌┐┌┌┬┐  ┌─┐┬┌┐┌┌┬┐
    //  ║║║║╚═╗║╣ ╠╦╝ ║   ├─┤│││ ││  ├┤ ││││ ││
    //  ╩╝╚╝╚═╝╚═╝╩╚═ ╩   ┴ ┴┘└┘─┴┘  └  ┴┘└┘─┴┘
    var insertAndFind = function insertAndFind(connection, query, done) {
      runInsertQuery(connection, query, function cb(err, insertResults) {
        if (err) {
          return done(err);
        }

        runFindQuery(connection, insertResults, function cb(err, findResults) {
          if (err) {
            return done(err);
          }

          return done(null, findResults);
        });
      });
    };


    //  ╔═╗╔═╗╔╦╗  ┌─┐┌─┐┌─┐ ┬ ┬┌─┐┌┐┌┌─┐┌─┐┌─┐  ┬  ┬┌─┐┬  ┬ ┬┌─┐┌─┐
    //  ╚═╗║╣  ║   └─┐├┤ │─┼┐│ │├┤ │││├┤ │  ├┤   └┐┌┘├─┤│  │ │├┤ └─┐
    //  ╚═╝╚═╝ ╩   └─┘└─┘└─┘└└─┘└─┘┘└┘└─┘└─┘└─┘   └┘ ┴ ┴┴─┘└─┘└─┘└─┘
    var setSequenceValues = function setSequenceValues(connection, done) {
      // Build a function for handling sequence queries on a single sequence.
      var setSequence = function setSequence(item, next) {
        var sequenceName = "'\"" + schemaName + '\".\"' + inputs.tableName + '_' + item + '_seq' + "\"'";
        var sequenceValue = inputs.record[item];
        var sequenceQuery = 'SELECT setval(' + sequenceName + ', ' + sequenceValue + ', true)';

        // Run Sequence Query
        Helpers.runQuery({
          connection: connection,
          nativeQuery: sequenceQuery,
          disconnectOnError: false
        }, next);
      };

      async.each(incrementSequences, setSequence, function doneWithSequences(err) {
        if (err) {
          Helpers.rollbackAndRelease(connection, function _rollbackCb(err) {
            if (err) {
              return done(new Error('There was an error rolling back and releasing the connection.' + err.stack));
            }

            return done(new Error('There was an error incrementing a sequence on the create.' + err.stack));
          });
          return;
        }

        return done();
      });
    };


    //  ╔═╗╔═╗╔╦╗╔╦╗╦╔╦╗  ┌┬┐┬─┐┌─┐┌┐┌┌─┐┌─┐┌─┐┌┬┐┬┌─┐┌┐┌
    //  ║  ║ ║║║║║║║║ ║    │ ├┬┘├─┤│││└─┐├─┤│   │ ││ ││││
    //  ╚═╝╚═╝╩ ╩╩ ╩╩ ╩    ┴ ┴└─┴ ┴┘└┘└─┘┴ ┴└─┘ ┴ ┴└─┘┘└┘
    // Commit the transaction and release the connection.
    var commitTransaction = function commitTransaction(connection, done) {
      Helpers.commitAndRelease(connection, function _commitCb(err) {
        if (err) {
          return done(new Error('There was an error commiting the transaction.' + err.stack));
        }

        return done();
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

    // Spawn a new connection and open a transaction for running queries on.
    spawnTransaction(function cb(err, connection) {
      if (err) {
        return exits.badConnection(err);
      }

      // Compile the original Waterline Query
      var query;
      try {
        query = compileStatement();
      } catch (e) {
        return exits.error(e);
      }

      // Insert the new record and if successfully look it up again to get any
      // inferred values. This should be updated in the future to use the PG
      // "returning *" query clause but that isn't currently supported by the
      // query builder.
      insertAndFind(connection, query, function cb(err, insertedRecords) {
        if (err) {
          return exits.error(err);
        }

        // Update any sequences that may have been used
        setSequenceValues(connection, function cb(err) {
          if (err) {
            return exits.error(err);
          }

          // Commit the transaction
          commitTransaction(connection, function cb(err) {
            if (err) {
              return exits.error(err);
            }

            //  ╔═╗╔═╗╔═╗╔╦╗  ┬  ┬┌─┐┬  ┬ ┬┌─┐┌─┐
            //  ║  ╠═╣╚═╗ ║   └┐┌┘├─┤│  │ │├┤ └─┐
            //  ╚═╝╩ ╩╚═╝ ╩    └┘ ┴ ┴┴─┘└─┘└─┘└─┘
            var castResults;
            try {
              castResults = Helpers.unserializeValues({
                definition: model.definition,
                records: insertedRecords
              });
            } catch (e) {
              return exits.error(new Error('There was an error normalizing the insert values.' + e.stack));
            }

            // Only return the first record (there should only ever be one)
            return exits.success({ record: _.first(castResults) });
          }); // </ .commitTransaction(); >
        }); // </ .setSequenceValues(); >
      }); // </ .insertAndFind(); >
    }); // </ .spawnTransaction(); >
  }

});
