//       ██╗ ██████╗ ██╗███╗   ██╗
//       ██║██╔═══██╗██║████╗  ██║
//       ██║██║   ██║██║██╔██╗ ██║
//  ██   ██║██║   ██║██║██║╚██╗██║
//  ╚█████╔╝╚██████╔╝██║██║ ╚████║
//   ╚════╝  ╚═════╝ ╚═╝╚═╝  ╚═══╝
//
module.exports = require('machine').build({


  friendlyName: 'Join',


  description: 'Support native joins on the database.',


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
      description: 'The name of the table to search in.',
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
      description: 'The query was run successfully.',
      example: '==='
    },

    badConnection: {
      friendlyName: 'Bad connection',
      description: 'A connection either could not be obtained or there was an error using the connection.'
    }

  },


  fn: function drop(inputs, exits) {
    var _ = require('lodash');
    var async = require('async');
    var utils = require('waterline-utils');
    var Helpers = require('./private');


    // Set a flag if a leased connection from outside the adapter was used or not.
    var leased = _.has(inputs.meta, 'leasedConnection');


    //  ╔═╗╦ ╦╔═╗╔═╗╦╔═  ┌─┐┌─┐┬─┐  ┌─┐  ┌─┐┌─┐  ┌─┐┌─┐┬ ┬┌─┐┌┬┐┌─┐
    //  ║  ╠═╣║╣ ║  ╠╩╗  ├┤ │ │├┬┘  ├─┤  ├─┘│ ┬  └─┐│  ├─┤├┤ │││├─┤
    //  ╚═╝╩ ╩╚═╝╚═╝╩ ╩  └  └─┘┴└─  ┴ ┴  ┴  └─┘  └─┘└─┘┴ ┴└─┘┴ ┴┴ ┴
    // This is a unique feature of Postgres. It may be passed in on a query
    // by query basis using the meta input or configured on the datastore. Default
    // to use the "public" schema.
    var schemaName = 'public';
    if (inputs.meta && inputs.meta.schemaName) {
      schemaName = inputs.meta.schemaName;
    } else if (inputs.datastore.config && inputs.datastore.config.schemaName) {
      schemaName = inputs.datastore.config.schemaName;
    }


    //  ╔═╗╦╔╗╔╔╦╗  ┌┬┐┌─┐┌┐ ┬  ┌─┐  ┌─┐┬─┐┬┌┬┐┌─┐┬─┐┬ ┬  ┬┌─┌─┐┬ ┬
    //  ╠╣ ║║║║ ║║   │ ├─┤├┴┐│  ├┤   ├─┘├┬┘││││├─┤├┬┘└┬┘  ├┴┐├┤ └┬┘
    //  ╚  ╩╝╚╝═╩╝   ┴ ┴ ┴└─┘┴─┘└─┘  ┴  ┴└─┴┴ ┴┴ ┴┴└─ ┴   ┴ ┴└─┘ ┴
    // Find the model definition
    var model = inputs.models[inputs.tableName];
    if (!model) {
      return exits.invalidDatastore();
    }

    // Grab the primary key attribute for the main table name
    var primaryKeyAttr = Helpers.schema.findPrimaryKey(model.definition);


    //  ╔╗ ╦ ╦╦╦  ╔╦╗  ┌─┐┌┬┐┌─┐┌┬┐┌─┐┌┬┐┌─┐┌┐┌┌┬┐┌─┐
    //  ╠╩╗║ ║║║   ║║  └─┐ │ ├─┤ │ ├┤ │││├┤ │││ │ └─┐
    //  ╚═╝╚═╝╩╩═╝═╩╝  └─┘ ┴ ┴ ┴ ┴ └─┘┴ ┴└─┘┘└┘ ┴ └─┘
    // Attempt to build up the statements necessary for the query.
    var statements;
    try {
      statements = utils.joins.convertJoinCriteria({
        tableName: inputs.tableName,
        schemaName: schemaName,
        getPk: function getPk(tableName) {
          var model = inputs.models[tableName];
          if (!model) {
            throw new Error('Invalid parent table name used when caching query results. Perhaps the join criteria is invalid?');
          }

          var pk = Helpers.schema.findPrimaryKey(model.definition);
          return pk;
        },
        criteria: inputs.criteria
      });
    } catch (e) {
      return exits.error(e);
    }


    //  ╔═╗╔═╗╔╗╔╦  ╦╔═╗╦═╗╔╦╗  ┌─┐┌─┐┬─┐┌─┐┌┐┌┌┬┐
    //  ║  ║ ║║║║╚╗╔╝║╣ ╠╦╝ ║   ├─┘├─┤├┬┘├┤ │││ │
    //  ╚═╝╚═╝╝╚╝ ╚╝ ╚═╝╩╚═ ╩   ┴  ┴ ┴┴└─└─┘┘└┘ ┴
    //  ┌─┐┌┬┐┌─┐┌┬┐┌─┐┌┬┐┌─┐┌┐┌┌┬┐
    //  └─┐ │ ├─┤ │ ├┤ │││├┤ │││ │
    //  └─┘ ┴ ┴ ┴ ┴ └─┘┴ ┴└─┘┘└┘ ┴
    // Convert the parent statement into a native query. If the query can be run
    // in a single query then this will be the only query that runs.
    var nativeQuery;
    try {
      nativeQuery = Helpers.query.compileStatement(statements.parentStatement);
    } catch (e) {
      return exits.error(new Error('There was an issue compiling the statement to a SQL query. ' + e.stack));
    }


    //  ╔═╗╔═╗╔═╗╦ ╦╔╗╔  ┌─┐┌─┐┌┐┌┌┐┌┌─┐┌─┐┌┬┐┬┌─┐┌┐┌
    //  ╚═╗╠═╝╠═╣║║║║║║  │  │ │││││││├┤ │   │ ││ ││││
    //  ╚═╝╩  ╩ ╩╚╩╝╝╚╝  └─┘└─┘┘└┘┘└┘└─┘└─┘ ┴ ┴└─┘┘└┘
    //  ┌─┐┬─┐  ┬ ┬┌─┐┌─┐  ┬  ┌─┐┌─┐┌─┐┌─┐┌┬┐  ┌─┐┌─┐┌┐┌┌┐┌┌─┐┌─┐┌┬┐┬┌─┐┌┐┌
    //  │ │├┬┘  │ │└─┐├┤   │  ├┤ ├─┤└─┐├┤  ││  │  │ │││││││├┤ │   │ ││ ││││
    //  └─┘┴└─  └─┘└─┘└─┘  ┴─┘└─┘┴ ┴└─┘└─┘─┴┘  └─┘└─┘┘└┘┘└┘└─┘└─┘ ┴ ┴└─┘┘└┘
    // Spawn a new connection for running queries on.
    Helpers.connection.spawnOrLeaseConnection(inputs.datastore, inputs.meta, function spawnCb(err, connection) {
      if (err) {
        return exits.error(new Error('There was an issue spawning a new connection when attempting to populate that buffers. ' + err.stack));
      }


      //  ╦═╗╦ ╦╔╗╔  ┌┬┐┬ ┬┌─┐  ┌┐┌┌─┐┌┬┐┬┬  ┬┌─┐  ┌─┐ ┬ ┬┌─┐┬─┐┬ ┬
      //  ╠╦╝║ ║║║║   │ ├─┤├┤   │││├─┤ │ │└┐┌┘├┤   │─┼┐│ │├┤ ├┬┘└┬┘
      //  ╩╚═╚═╝╝╚╝   ┴ ┴ ┴└─┘  ┘└┘┴ ┴ ┴ ┴ └┘ └─┘  └─┘└└─┘└─┘┴└─ ┴
      Helpers.query.runNativeQuery(connection, nativeQuery, function parentQueryCb(err, parentResults) {
        if (err) {
          // Release the connection on error
          Helpers.connection.releaseConnection(connection, leased, function releaseConnectionCb() {
            return exits.error(new Error('There was an issue running a query. The query was: \n\n' + nativeQuery + '\n\n' + err.stack));
          });
          return;
        }

        // If there weren't any joins being performed, release the connection and
        // return the results.
        if (!_.has(inputs.criteria, 'instructions')) {
          Helpers.connection.releaseConnection(connection, leased, function releaseConnectionCb(err) {
            if (err) {
              return exits.error(err);
            }

            return exits.success(parentResults);
          });
          return;
        }


        //  ╔═╗╦╔╗╔╔╦╗  ┌─┐┬ ┬┬┬  ┌┬┐┬─┐┌─┐┌┐┌  ┬─┐┌─┐┌─┐┌─┐┬─┐┌┬┐┌─┐
        //  ╠╣ ║║║║ ║║  │  ├─┤││   ││├┬┘├┤ │││  ├┬┘├┤ │  │ │├┬┘ ││└─┐
        //  ╚  ╩╝╚╝═╩╝  └─┘┴ ┴┴┴─┘─┴┘┴└─└─┘┘└┘  ┴└─└─┘└─┘└─┘┴└──┴┘└─┘
        // If there was a join that was either performed or still needs to be
        // performed, look into the results for any children records that may
        // have been joined and splt them out from the parent.
        var sortedResults;
        try {
          sortedResults = utils.joins.detectChildrenRecords(primaryKeyAttr, parentResults);
        } catch (e) {
          // Release the connection if there was an error.
          Helpers.connection.releaseConnection(connection, leased, function releaseConnectionCb() {
            return exits.error(e);
          });
          return;
        }


        //  ╦╔╗╔╦╔╦╗╦╔═╗╦  ╦╔═╗╔═╗  ┌─┐ ┬ ┬┌─┐┬─┐┬ ┬  ┌─┐┌─┐┌─┐┬ ┬┌─┐
        //  ║║║║║ ║ ║╠═╣║  ║╔═╝║╣   │─┼┐│ │├┤ ├┬┘└┬┘  │  ├─┤│  ├─┤├┤
        //  ╩╝╚╝╩ ╩ ╩╩ ╩╩═╝╩╚═╝╚═╝  └─┘└└─┘└─┘┴└─ ┴   └─┘┴ ┴└─┘┴ ┴└─┘
        var queryCache;
        try {
          queryCache = Helpers.query.initializeQueryCache({
            instructions: inputs.criteria.instructions,
            models: inputs.models,
            sortedResults: sortedResults
          });
        } catch (e) {
          // Release the connection if there was an error.
          Helpers.connection.releaseConnection(connection, leased, function releaseConnectionCb() {
            return exits.error(e);
          });
          return;
        }


        //  ╔═╗╔╦╗╔═╗╦═╗╔═╗  ┌─┐┌─┐┬─┐┌─┐┌┐┌┌┬┐┌─┐
        //  ╚═╗ ║ ║ ║╠╦╝║╣   ├─┘├─┤├┬┘├┤ │││ │ └─┐
        //  ╚═╝ ╩ ╚═╝╩╚═╚═╝  ┴  ┴ ┴┴└─└─┘┘└┘ ┴ └─┘
        try {
          queryCache.setParents(sortedResults.parents);
        } catch (e) {
          // Release the connection if there was an error.
          Helpers.connection.releaseConnection(connection, leased, function releaseConnectionCb() {
            return exits.error(e);
          });
          return;
        }


        //  ╔═╗╦ ╦╔═╗╔═╗╦╔═  ┌─┐┌─┐┬─┐  ┌─┐┬ ┬┬┬  ┌┬┐┬─┐┌─┐┌┐┌
        //  ║  ╠═╣║╣ ║  ╠╩╗  ├┤ │ │├┬┘  │  ├─┤││   ││├┬┘├┤ │││
        //  ╚═╝╩ ╩╚═╝╚═╝╩ ╩  └  └─┘┴└─  └─┘┴ ┴┴┴─┘─┴┘┴└─└─┘┘└┘
        //  ┌─┐ ┬ ┬┌─┐┬─┐┬┌─┐┌─┐
        //  │─┼┐│ │├┤ ├┬┘│├┤ └─┐
        //  └─┘└└─┘└─┘┴└─┴└─┘└─┘
        // Now that all the parents are found, check if there are any child
        // statements that need to be processed. If not, close the connection and
        // return the combined results.
        if (!statements.childStatements || !statements.childStatements.length) {
          Helpers.connection.releaseConnection(connection, leased, function releaseConnectionCb(err) {
            if (err) {
              return exits.error(err);
            }

            // Combine records in the cache to form nested results
            var combinedResults;
            try {
              combinedResults = queryCache.combineRecords();
            } catch (e) {
              return exits.error(e);
            }

            // Return the combined results
            exits.success(combinedResults);
          });
          return;
        }


        //  ╔═╗╔═╗╦  ╦  ╔═╗╔═╗╔╦╗  ┌─┐┌─┐┬─┐┌─┐┌┐┌┌┬┐
        //  ║  ║ ║║  ║  ║╣ ║   ║   ├─┘├─┤├┬┘├┤ │││ │
        //  ╚═╝╚═╝╩═╝╩═╝╚═╝╚═╝ ╩   ┴  ┴ ┴┴└─└─┘┘└┘ ┴
        //  ┬─┐┌─┐┌─┐┌─┐┬─┐┌┬┐┌─┐
        //  ├┬┘├┤ │  │ │├┬┘ ││└─┐
        //  ┴└─└─┘└─┘└─┘┴└──┴┘└─┘
        // There is more work to be done now. Go through the parent records and
        // build up an array of the primary keys.
        var parentKeys = _.map(queryCache.getParents(), function pluckPk(record) {
          return record[primaryKeyAttr];
        });


        //  ╔═╗╦═╗╔═╗╔═╗╔═╗╔═╗╔═╗  ┌─┐┬ ┬┬┬  ┌┬┐  ┌─┐┌┬┐┌─┐┌┬┐┌─┐┌┬┐┌─┐┌┐┌┌┬┐┌─┐
        //  ╠═╝╠╦╝║ ║║  ║╣ ╚═╗╚═╗  │  ├─┤││   ││  └─┐ │ ├─┤ │ ├┤ │││├┤ │││ │ └─┐
        //  ╩  ╩╚═╚═╝╚═╝╚═╝╚═╝╚═╝  └─┘┴ ┴┴┴─┘─┴┘  └─┘ ┴ ┴ ┴ ┴ └─┘┴ ┴└─┘┘└┘ ┴ └─┘
        // For each child statement, figure out how to turn the statement into
        // a native query and then run it. Add the results to the query cache.
        async.each(statements.childStatements, function processChildStatements(template, next) {
          //  ╦═╗╔═╗╔╗╔╔╦╗╔═╗╦═╗  ┬┌┐┌  ┌─┐ ┬ ┬┌─┐┬─┐┬ ┬
          //  ╠╦╝║╣ ║║║ ║║║╣ ╠╦╝  ││││  │─┼┐│ │├┤ ├┬┘└┬┘
          //  ╩╚═╚═╝╝╚╝═╩╝╚═╝╩╚═  ┴┘└┘  └─┘└└─┘└─┘┴└─ ┴
          //  ┌┬┐┌─┐┌┬┐┌─┐┬  ┌─┐┌┬┐┌─┐
          //   │ ├┤ │││├─┘│  ├─┤ │ ├┤
          //   ┴ └─┘┴ ┴┴  ┴─┘┴ ┴ ┴ └─┘
          // If the statement is an IN query, replace the values with the parent
          // keys.
          if (template.queryType === 'in') {
            // Pull the last AND clause out - it's the one we added
            var inClause = _.pullAt(template.statement.where.and, template.statement.where.and.length - 1);

            // Grab the object inside the array that comes back
            inClause = _.first(inClause);

            // Modify the inClause using the actual parent key values
            _.each(inClause, function modifyInClause(val) {
              val.in = parentKeys;
            });

            // Reset the statement
            template.statement.where.and.push(inClause);
          }


          //  ╦═╗╔═╗╔╗╔╔╦╗╔═╗╦═╗  ┬ ┬┌┐┌┬┌─┐┌┐┌  ┌─┐ ┬ ┬┌─┐┬─┐┬ ┬
          //  ╠╦╝║╣ ║║║ ║║║╣ ╠╦╝  │ ││││││ ││││  │─┼┐│ │├┤ ├┬┘└┬┘
          //  ╩╚═╚═╝╝╚╝═╩╝╚═╝╩╚═  └─┘┘└┘┴└─┘┘└┘  └─┘└└─┘└─┘┴└─ ┴
          //  ┌┬┐┌─┐┌┬┐┌─┐┬  ┌─┐┌┬┐┌─┐
          //   │ ├┤ │││├─┘│  ├─┤ │ ├┤
          //   ┴ └─┘┴ ┴┴  ┴─┘┴ ┴ ┴ └─┘
          // If the statement is a UNION type, loop through each parent key and
          // build up a proper query.
          if (template.queryType === 'union') {
            var unionStatements = [];

            // Build up an array of generated statements
            _.each(parentKeys, function buildUnion(parentPk) {
              var unionStatement = _.merge({}, template.statement);

              // Replace the placeholder `?` values with the primary key of the
              // parent record.
              var andClause = _.pullAt(unionStatement.where.and, unionStatement.where.and.length - 1);
              _.each(_.first(andClause), function replaceValue(val, key) {
                _.first(andClause)[key] = parentPk;
              });

              // Add the UNION statement to the array of other statements
              unionStatement.where.and.push(_.first(andClause));
              unionStatements.push(unionStatement);
            });

            // Replace the final statement with the UNION ALL clause
            template.statement = { unionAll: unionStatements };
          }


          //  ╔═╗╔═╗╔╦╗╔═╗╦╦  ╔═╗  ┌─┐┌┬┐┌─┐┌┬┐┌─┐┌┬┐┌─┐┌┐┌┌┬┐
          //  ║  ║ ║║║║╠═╝║║  ║╣   └─┐ │ ├─┤ │ ├┤ │││├┤ │││ │
          //  ╚═╝╚═╝╩ ╩╩  ╩╩═╝╚═╝  └─┘ ┴ ┴ ┴ ┴ └─┘┴ ┴└─┘┘└┘ ┴
          // Attempt to convert the statement into a native query
          var nativeQuery;
          try {
            nativeQuery = Helpers.query.compileStatement(template.statement);
          } catch (e) {
            return next(new Error('There was an error compiling a child statement in the join logic. Perhaps the criteria is incorrect? \n\n', e.stack));
          }


          //  ╦═╗╦ ╦╔╗╔  ┌─┐┬ ┬┬┬  ┌┬┐  ┌─┐ ┬ ┬┌─┐┬─┐┬ ┬
          //  ╠╦╝║ ║║║║  │  ├─┤││   ││  │─┼┐│ │├┤ ├┬┘└┬┘
          //  ╩╚═╚═╝╝╚╝  └─┘┴ ┴┴┴─┘─┴┘  └─┘└└─┘└─┘┴└─ ┴
          // Run the native query
          Helpers.query.runNativeQuery(connection, nativeQuery, function parentQueryCb(err, queryResults) {
            if (err) {
              return next(new Error('There was an issue running a query. The query was: \n\n' + nativeQuery + '\n\n' + err.stack));
            }

            // Extend the values in the cache to include the values from the
            // child query.
            queryCache.extend(queryResults, template.instructions);

            return next();
          });
        },

        function asyncEachCb(err) {
          // Always release the connection unless a leased connection from outside
          // the adapter was used.
          Helpers.connection.releaseConnection(connection, leased, function releaseConnectionCb() {
            if (err) {
              return exits.error(err);
            }

            // Combine records in the cache to form nested results
            var combinedResults = queryCache.combineRecords();

            // Return the combined results
            return exits.success(combinedResults);
          }); // </ releaseConnection >
        }); // </ asyncEachCb >
      }); // </ runNativeQuery >
    }); // </ spawnConnection >
  }
});
