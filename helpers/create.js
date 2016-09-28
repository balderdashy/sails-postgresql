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
    var spawnConnection = require('./private/spawn-connection');
    var runQuery = require('./private/run-query');
    var rollbackAndRelease = require('./private/rollback-and-release');
    var commitAndRelease = require('./private/commit-and-release');
    var normalizeValues = require('./private/normalize-values');


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


    //  ╔═╗╔═╗╔╦╗╔═╗╦╦  ╔═╗  ┌─┐┌┬┐┌─┐┌┬┐┌─┐┌┬┐┌─┐┌┐┌┌┬┐
    //  ║  ║ ║║║║╠═╝║║  ║╣   └─┐ │ ├─┤ │ ├┤ │││├┤ │││ │
    //  ╚═╝╚═╝╩ ╩╩  ╩╩═╝╚═╝  └─┘ ┴ ┴ ┴ ┴ └─┘┴ ┴└─┘┘└┘ ┴
    // Transform the Waterline Query Statement into a SQL query.
    PG.compileStatement({
      statement: statement
    })
    .exec({
      error: function error(err) {
        return exits.error(err);
      },

      //  ╔═╗╔═╗╔═╗╦ ╦╔╗╔  ┌─┐┌─┐┌┐┌┌┐┌┌─┐┌─┐┌┬┐┬┌─┐┌┐┌
      //  ╚═╗╠═╝╠═╣║║║║║║  │  │ │││││││├┤ │   │ ││ ││││
      //  ╚═╝╩  ╩ ╩╚╩╝╝╚╝  └─┘└─┘┘└┘┘└┘└─┘└─┘ ┴ ┴└─┘┘└┘
      success: function success(report) {
        var query = report.nativeQuery;

        spawnConnection({
          datastore: inputs.datastore
        })
        .exec({
          error: function error(err) {
            return exits.badConnection(err);
          },


          //  ╔╗ ╔═╗╔═╗╦╔╗╔  ┌┬┐┬─┐┌─┐┌┐┌┌─┐┌─┐┌─┐┌┬┐┬┌─┐┌┐┌
          //  ╠╩╗║╣ ║ ╦║║║║   │ ├┬┘├─┤│││└─┐├─┤│   │ ││ ││││
          //  ╚═╝╚═╝╚═╝╩╝╚╝   ┴ ┴└─┴ ┴┘└┘└─┘┴ ┴└─┘ ┴ ┴└─┘┘└┘
          success: function success(connection) {
            PG.beginTransaction({
              connection: connection
            })
            .exec(function beginTransactionCb(err) {
              // If there was an error, release the transaction
              if (err) {
                PG.releaseConnection({
                  connection: connection
                }).exec(function releaseCb() {
                  return exits.error(err);
                });
              }

              //  ╦═╗╦ ╦╔╗╔  ┬┌┐┌┌─┐┌─┐┬─┐┌┬┐  ┌─┐ ┬ ┬┌─┐┬─┐┬ ┬
              //  ╠╦╝║ ║║║║  ││││└─┐├┤ ├┬┘ │   │─┼┐│ │├┤ ├┬┘└┬┘
              //  ╩╚═╚═╝╝╚╝  ┴┘└┘└─┘└─┘┴└─ ┴   └─┘└└─┘└─┘┴└─ ┴
              runQuery({
                connection: connection,
                nativeQuery: query,
                queryType: 'insert',
                disconnectOnError: false
              })
              .exec({
                error: function error(err) {
                  // Rollback the transaction and release the connection
                  rollbackAndRelease({
                    connection: connection
                  }).exec(function releaseCb() {
                    return exits.error(err);
                  });
                },

                //  ╔═╗╦╔╗╔╔╦╗  ┬┌┐┌┌─┐┌─┐┬─┐┌┬┐┌─┐┌┬┐  ┬─┐┌─┐┌─┐┌─┐┬─┐┌┬┐┌─┐
                //  ╠╣ ║║║║ ║║  ││││└─┐├┤ ├┬┘ │ ├┤  ││  ├┬┘├┤ │  │ │├┬┘ ││└─┐
                //  ╚  ╩╝╚╝═╩╝  ┴┘└┘└─┘└─┘┴└─ ┴ └─┘─┴┘  ┴└─└─┘└─┘└─┘┴└──┴┘└─┘
                success: function success(insertReport) {
                  // Build an IN query from the results of the insert
                  PG.compileStatement({
                    statement: {
                      select: ['*'],
                      from: inputs.tableName,
                      where: {
                        id: {
                          in: insertReport.result.inserted
                        }
                      }
                    }
                  })
                  .exec({
                    error: function error(err) {
                      return exits.error(err);
                    },
                    success: function success(report) {
                      var query = report.nativeQuery;

                      // Run the FIND query
                      runQuery({
                        connection: connection,
                        nativeQuery: query,
                        queryType: 'select',
                        disconnectOnError: false
                      })
                      .exec({
                        error: function error(err) {
                          // Rollback the transaction and release the connection
                          rollbackAndRelease({
                            connection: connection
                          }).exec(function releaseCb() {
                            return exits.error(err);
                          });
                        },


                        //  ╔═╗╔═╗╔╦╗  ┌─┐┌─┐┌─┐ ┬ ┬┌─┐┌┐┌┌─┐┌─┐┌─┐  ┬  ┬┌─┐┬  ┬ ┬┌─┐┌─┐
                        //  ╚═╗║╣  ║   └─┐├┤ │─┼┐│ │├┤ │││├┤ │  ├┤   └┐┌┘├─┤│  │ │├┤ └─┐
                        //  ╚═╝╚═╝ ╩   └─┘└─┘└─┘└└─┘└─┘┘└┘└─┘└─┘└─┘   └┘ ┴ ┴┴─┘└─┘└─┘└─┘
                        success: function success(selectReport) {
                          var setSequence = function setSequence(item, next) {
                            var sequenceName = "'\"" + schemaName + '\".\"' + inputs.tableName + '_' + item + '_seq' + "\"'";
                            var sequenceValue = inputs.record[item];
                            var sequenceQuery = 'SELECT setval(' + sequenceName + ', ' + sequenceValue + ', true)';

                            // Run Sequence Query
                            runQuery({
                              connection: connection,
                              nativeQuery: sequenceQuery,
                              disconnectOnError: false
                            })
                            .exec(next);
                          };

                          async.each(incrementSequences, setSequence, function doneWithSequences(err) {
                            if (err) {
                              // Rollback the transaction and release the connection
                              rollbackAndRelease({
                                connection: connection
                              }).exec(function releaseCb() {
                                return exits.error(err);
                              });
                            }

                            //  ╔═╗╔═╗╔╦╗╔╦╗╦╔╦╗  ┌┬┐┬─┐┌─┐┌┐┌┌─┐┌─┐┌─┐┌┬┐┬┌─┐┌┐┌
                            //  ║  ║ ║║║║║║║║ ║    │ ├┬┘├─┤│││└─┐├─┤│   │ ││ ││││
                            //  ╚═╝╚═╝╩ ╩╩ ╩╩ ╩    ┴ ┴└─┴ ┴┘└┘└─┘┴ ┴└─┘ ┴ ┴└─┘┘└┘
                            // Commit the transaction and release the connection
                            commitAndRelease({
                              connection: connection
                            })
                            .exec(function commitCb(err) {
                              if (err) {
                                return exits.error(err);
                              }


                              //  ╔═╗╔═╗╔═╗╔╦╗  ┬  ┬┌─┐┬  ┬ ┬┌─┐┌─┐
                              //  ║  ╠═╣╚═╗ ║   └┐┌┘├─┤│  │ │├┤ └─┐
                              //  ╚═╝╩ ╩╚═╝ ╩    └┘ ┴ ┴┴─┘└─┘└─┘└─┘
                              var queryResults = normalizeValues({
                                schema: collection.dbSchema,
                                records: selectReport.result
                              }).execSync();

                              return exits.success({
                                record: _.first(queryResults)
                              });
                            }); // </ commitAndRelease >
                          }); // </ async.each >
                        } // </ setSequenceValues >
                      }); // </ Find Query >
                    } // </ Compile Inserted Statement - success >
                  }); // </ Compile Inserted Statement >
                } // </ Find Inserted Records >
              }); // </ Run Insert Query >
            }); // </ PG Begin Transaction >
          } // </ begin transaction >
        }); // </ spawnConnection >
      } // </ Compile Statement success >
    }); // </ Compile Statement >
  }


});
