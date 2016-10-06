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
    var _ = require('lodash');
    var PG = require('machinepack-postgresql');
    var Converter = require('machinepack-waterline-query-converter');
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
    if (inputs.meta && inputs.meta.schema) {
      schemaName = inputs.meta.schema;
    } else if (inputs.datastore.config && inputs.datastore.config.schema) {
      schemaName = inputs.datastore.config.schema;
    }


    //  ╔═╗╔═╗╦═╗╦╔═╗╦  ╦╔═╗╔═╗  ┬  ┬┌─┐┬  ┬ ┬┌─┐┌─┐
    //  ╚═╗║╣ ╠╦╝║╠═╣║  ║╔═╝║╣   └┐┌┘├─┤│  │ │├┤ └─┐
    //  ╚═╝╚═╝╩╚═╩╩ ╩╩═╝╩╚═╝╚═╝   └┘ ┴ ┴┴─┘└─┘└─┘└─┘
    // Ensure that all the values being stored are valid for the database.
    var serializedValues;
    try {
      serializedValues = Helpers.serializeValues({
        records: inputs.values
      }).execSync();
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
    var updateStatement;
    try {
      updateStatement = Converter.convert({
        model: inputs.tableName,
        method: 'update',
        criteria: inputs.criteria,
        values: serializedValues
      }).execSync();

      // Add the postgres schema object to the statement
      updateStatement.opts = {
        schema: schemaName
      };
    } catch (e) {
      return exits.error(new Error('The Waterline Query failed to convert into a Waterline Statement.' + e.stack));
    }

    // Generate a FIND statement as well that will get all the records being updated.
    var findStatement;
    try {
      findStatement = Converter.convert({
        model: inputs.tableName,
        method: 'find',
        criteria: inputs.criteria
      }).execSync();

      // Add the postgres schema object to the statement
      findStatement.opts = {
        schema: schemaName
      };
    } catch (e) {
      return exits.error(new Error('The Waterline Query failed to convert into a Waterline Statement.' + e.stack));
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
    var compileStatement = function compileStatement(statement, done) {
      PG.compileStatement({
        statement: statement
      })
      .exec({
        error: function error(err) {
          return done(new Error('The Statement failed to be compiled into a query.' + err.stack));
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
          return done(new Error('There was an error spawning a new connection from the pool.' + err.stack));
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
              return done(new Error('There was an issue releasing the collection back into the pool.' + err.stack));
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


    //  ╔═╗╦╔╗╔╔╦╗  ┬─┐┌─┐┌─┐┌─┐┬─┐┌┬┐┌─┐  ┌┬┐┌─┐  ┬ ┬┌─┐┌┬┐┌─┐┌┬┐┌─┐
    //  ╠╣ ║║║║ ║║  ├┬┘├┤ │  │ │├┬┘ ││└─┐   │ │ │  │ │├─┘ ││├─┤ │ ├┤
    //  ╚  ╩╝╚╝═╩╝  ┴└─└─┘└─┘└─┘┴└──┴┘└─┘   ┴ └─┘  └─┘┴  ─┴┘┴ ┴ ┴ └─┘
    var runFindQuery = function runFindQuery(connection, query, done) {
      Helpers.runQuery({
        connection: connection,
        nativeQuery: query,
        queryType: 'select',
        disconnectOnError: false
      })
      .exec({
        // Rollback the transaction and release the connection on error.
        error: function error(err) {
          Helpers.rollbackAndRelease({
            connection: connection
          }).exec({
            error: function error() {
              return done(new Error('There was an error rolling back and releasing the transaction.' + err.stack));
            },
            success: function success() {
              return done(new Error('There was an error running the select query.' + err.stack));
            }
          });
        },
        success: function success(report) {
          return done(null, report.result);
        }
      });
    };


    //  ╦═╗╦ ╦╔╗╔  ┬ ┬┌─┐┌┬┐┌─┐┌┬┐┌─┐  ┌─┐ ┬ ┬┌─┐┬─┐┬ ┬
    //  ╠╦╝║ ║║║║  │ │├─┘ ││├─┤ │ ├┤   │─┼┐│ │├┤ ├┬┘└┬┘
    //  ╩╚═╚═╝╝╚╝  └─┘┴  ─┴┘┴ ┴ ┴ └─┘  └─┘└└─┘└─┘┴└─ ┴
    var runUpdateQuery = function runUpdateQuery(connection, query, done) {
      Helpers.runQuery({
        connection: connection,
        nativeQuery: query,
        queryType: 'update',
        disconnectOnError: false
      })
      .exec({
        // Rollback the transaction and release the connection on error.
        error: function error(err) {
          Helpers.rollbackAndRelease({
            connection: connection
          }).exec({
            error: function error() {
              return done(new Error('There was an error rolling back and releasing the transaction.' + err.stack));
            },
            success: function success() {
              return done(new Error('There was an error running the update query.' + err.stack));
            }
          });
        },
        success: function success(report) {
          return done(null, report.result);
        }
      });
    };


    //  ╔═╗╦╔╗╔╔╦╗  ┬ ┬┌─┐┌┬┐┌─┐┌┬┐┌─┐  ┬─┐┌─┐┌─┐┬ ┬┬ ┌┬┐┌─┐
    //  ╠╣ ║║║║ ║║  │ │├─┘ ││├─┤ │ ├┤   ├┬┘├┤ └─┐│ ││  │ └─┐
    //  ╚  ╩╝╚╝═╩╝  └─┘┴  ─┴┘┴ ┴ ┴ └─┘  ┴└─└─┘└─┘└─┘┴─┘┴ └─┘
    var findUpdateResults = function findUpdateResults(connection, findResults, done) {
      // Find the Primary Key field in the model
      var primaryKey;
      try {
        primaryKey = Helpers.findPrimaryKey({
          definition: model.definition
        }).execSync();
      } catch (e) {
        return done(new Error('Error determining Primary Key to use.' + e.stack));
      }

      // Grab the values of the Primary key from each record
      var values = _.pluck(findResults, primaryKey);

      // Build up a criteria statement to run
      var criteriaStatement = {
        select: ['*'],
        from: inputs.tableName,
        where: {}
      };

      // Add the postgres schema object to the statement
      criteriaStatement.opts = {
        schema: schemaName
      };

      // Insert dynamic primary key value into query
      criteriaStatement.where[primaryKey] = {
        in: values
      };

      // Build an IN query from the results of the find query
      PG.compileStatement({
        statement: criteriaStatement
      }).exec({
        error: function error(err) {
          return done(new Error('There was an error compiling a statement into a query.' + err.stack));
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
                  return done(new Error('There was an error rolling back and releasing the transaction.' + err.stack));
                },
                success: function success() {
                  return done(new Error('There was an error running the select query.' + err.stack));
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


    //  ╦ ╦╔═╗╔╦╗╔═╗╔╦╗╔═╗  ┌─┐┌┐┌┌┬┐  ┌─┐┬┌┐┌┌┬┐
    //  ║ ║╠═╝ ║║╠═╣ ║ ║╣   ├─┤│││ ││  ├┤ ││││ ││
    //  ╚═╝╩  ═╩╝╩ ╩ ╩ ╚═╝  ┴ ┴┘└┘─┴┘  └  ┴┘└┘─┴┘
    // Currently the query builder doesn't have support for using Postgres
    // "returning *" clauses. To get around this, first a query using the given
    // criteria is ran and the results from that are used to build up an "IN"
    // query using the primary key of the table. Then the update query is ran and
    // then the built query is ran to get the results of the update.
    var updateAndFind = function updateAndFind(connection, done) {
      // Compile the FIND statement
      compileStatement(findStatement, function cb(err, findQuery) {
        if (err) {
          return done(err);
        }

        // Run the initial FIND query
        runFindQuery(connection, findQuery, function cb(err, findResults) {
          if (err) {
            return done(err);
          }

          // Compile the UPDATE statement
          compileStatement(updateStatement, function cb(err, updateQuery) {
            if (err) {
              return done(err);
            }

            // Run the UPDATE query
            runUpdateQuery(connection, updateQuery, function cb(err) {
              if (err) {
                return done(err);
              }

              // Use the results from the FIND query to get the updates records
              findUpdateResults(connection, findResults, function cb(err, queryResults) {
                if (err) {
                  return done(err);
                }

                return done(null, queryResults);
              });
            });
          });
        });
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
          return done(new Error('There was an error commiting the transaction.' + err.stack));
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


    // Spawn a new connection and open a transaction for running queries on.
    spawnTransaction(function cb(err, connection) {
      if (err) {
        return exits.badConnection(err);
      }

      // Process the Update
      updateAndFind(connection, function cb(err, updatedRecords) {
        if (err) {
          return exits.badConnection(err);
        }

        // Commit the transaction
        commitTransaction(connection, function cb(err) {
          if (err) {
            return exits.error(err);
          }

          //  ╔═╗╔═╗╔═╗╔╦╗  ┬  ┬┌─┐┬  ┬ ┬┌─┐┌─┐
          //  ║  ╠═╣╚═╗ ║   └┐┌┘├─┤│  │ │├┤ └─┐
          //  ╚═╝╩ ╩╚═╝ ╩    └┘ ┴ ┴┴─┘└─┘└─┘└─┘
          var castResults = Helpers.unserializeValues({
            definition: model.definition,
            records: updatedRecords
          }).execSync();

          return exits.success({ records: castResults });
        }); // </ .commitTransaction(); >
      }); // </ .updateAndFind(); >
    }); // </ .spawnTransaction(); >
  }

});
