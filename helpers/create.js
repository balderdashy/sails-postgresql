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
      example: {}
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
    var Converter = require('machinepack-waterline-query-converter');
    var Helpers = require('./private');


    // Ensure that a collection can be found on the datastore.
    var collection = inputs.datastore.collections && inputs.datastore.collections[inputs.tableName];
    if (!collection) {
      return exits.invalidDatastore();
    }

    // Default the postgres schemaName to "public"
    var schemaName = 'public';

    // Check if a schemaName was manually defined
    if (collection.meta && collection.meta.schemaName) {
      schemaName = collection.meta.schemaName;
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
    _.each(_.keys(collection.dbSchema), function checkSequences(schemaKey) {
      if (!_.has(collection.dbSchema[schemaKey], 'autoIncrement')) {
        return;
      }

      if (_.indexOf(_.keys(inputs.record), schemaKey) < 0) {
        return;
      }

      incrementSequences.push(schemaKey);
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
      statement = Converter.convert({
        model: inputs.tableName,
        method: 'create',
        values: inputs.record
      }).execSync();
    } catch (e) {
      return exits.error(e);
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
    var compileStatement = function compileStatement(done) {
      PG.compileStatement({
        statement: statement
      })
      .exec({
        error: function error(err) {
          return done(err);
        },
        success: function success(report) {
          return done(null, report.nativeQuery);
        }
      });
    };


    //  ╔═╗╔═╗╔═╗╦ ╦╔╗╔  ┌─┐┌─┐┌┐┌┌┐┌┌─┐┌─┐┌┬┐┬┌─┐┌┐┌
    //  ╚═╗╠═╝╠═╣║║║║║║  │  │ │││││││├┤ │   │ ││ ││││
    //  ╚═╝╩  ╩ ╩╚╩╝╝╚╝  └─┘└─┘┘└┘┘└┘└─┘└─┘ ┴ ┴└─┘┘└┘
    var spawnConnection = function spawnConnection(done) {
      Helpers.spawnConnection({
        datastore: inputs.datastore
      })
      .exec({
        error: function error(err) {
          return done(err);
        },
        success: function success(connection) {
          return done(null, connection);
        }
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
            error: function error() {
              return done(err);
            },
            success: function success() {
              return done(err);
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
      })
      .exec({
        // Rollback the transaction and release the connection on error.
        error: function error(err) {
          Helpers.rollbackAndRelease({
            connection: connection
          }).exec({
            error: function error() {
              return done(err);
            },
            success: function success() {
              return done(err);
            }
          });
        },
        success: function success(report) {
          return done(null, report.result);
        }
      });
    };


    //  ╔═╗╦╔╗╔╔╦╗  ┬┌┐┌┌─┐┌─┐┬─┐┌┬┐┌─┐┌┬┐  ┬─┐┌─┐┌─┐┌─┐┬─┐┌┬┐┌─┐
    //  ╠╣ ║║║║ ║║  ││││└─┐├┤ ├┬┘ │ ├┤  ││  ├┬┘├┤ │  │ │├┬┘ ││└─┐
    //  ╚  ╩╝╚╝═╩╝  ┴┘└┘└─┘└─┘┴└─ ┴ └─┘─┴┘  ┴└─└─┘└─┘└─┘┴└──┴┘└─┘
    var runFindQuery = function runFindQuery(connection, insertResults, done) {
      // Find the Primary Key field in the collection
      var primaryKey;
      try {
        primaryKey = Helpers.findPrimaryKey({
          collection: collection
        }).execSync();
      } catch (e) {
        return done(new Error('Error determining Primary Key to use.'));
      }
      // Build up a criteria statement to run
      var criteriaStatement = {
        select: ['*'],
        from: inputs.tableName,
        where: {}
      };

      // Insert dynamic primary key value into query
      criteriaStatement.where[primaryKey] = {
        in: insertResults.inserted
      };

      // Build an IN query from the results of the insert query
      PG.compileStatement({
        statement: criteriaStatement
      }).exec({
        error: function error(err) {
          return done(err);
        },
        success: function success(report) {
          // Run the FIND query
          Helpers.runQuery({
            connection: connection,
            nativeQuery: report.nativeQuery,
            queryType: 'select',
            disconnectOnError: false
          }).exec({
            // Rollback the transaction and release the connection on error.
            error: function error(err) {
              Helpers.rollbackAndRelease({
                connection: connection
              }).exec({
                error: function error() {
                  return done(err);
                },
                success: function success() {
                  return done(err);
                }
              });
            },
            success: function success(report) {
              return done(null, report.result);
            }
          });
        }
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
        })
        .exec(next);
      };

      async.each(incrementSequences, setSequence, function doneWithSequences(err) {
        if (err) {
          Helpers.rollbackAndRelease({
            connection: connection
          }).exec({
            error: function error() {
              return done(err);
            },
            success: function success() {
              return done(err);
            }
          });
        }

        return done();
      });
    };


    //  ╔═╗╔═╗╔╦╗╔╦╗╦╔╦╗  ┌┬┐┬─┐┌─┐┌┐┌┌─┐┌─┐┌─┐┌┬┐┬┌─┐┌┐┌
    //  ║  ║ ║║║║║║║║ ║    │ ├┬┘├─┤│││└─┐├─┤│   │ ││ ││││
    //  ╚═╝╚═╝╩ ╩╩ ╩╩ ╩    ┴ ┴└─┴ ┴┘└┘└─┘┴ ┴└─┘ ┴ ┴└─┘┘└┘
    // Commit the transaction and release the connection.
    var commitTransaction = function commitTransaction(connection, done) {
      Helpers.commitAndRelease({
        connection: connection
      })
      .exec({
        error: function error(err) {
          return done(err);
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

    // Compile the original Waterline Query
    compileStatement(function cb(err, query) {
      if (err) {
        return exits.error(err);
      }

      // Spawn a new connection and open a transaction for running queries on.
      spawnTransaction(function cb(err, connection) {
        if (err) {
          return exits.badConnection(err);
        }

        // Insert the new record and if successfully look it up again to get any
        // inferred values. This should be updated in the future to use the PG
        // "returning *" query clause but that isn't currently supported by the
        // query builder.
        insertAndFind(connection, query, function cb(err, insertedRecords) {
          if (err) {
            return exits.badConnection(err);
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
              var castResults = Helpers.normalizeValues({
                schema: collection.dbSchema,
                records: insertedRecords
              }).execSync();

              // Only return the first record (there should only ever be one)
              return exits.success({ record: _.first(castResults) });
            }); // </ .commitTransaction(); >
          }); // </ .setSequenceValues(); >
        }); // </ .insertAndFind(); >
      }); // </ .spawnTransaction(); >
    }); // </ .compileStatement(); >
  }

});
