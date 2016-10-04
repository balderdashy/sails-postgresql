//  ██████╗ ██╗   ██╗███╗   ██╗     ██████╗ ██╗   ██╗███████╗██████╗ ██╗   ██╗
//  ██╔══██╗██║   ██║████╗  ██║    ██╔═══██╗██║   ██║██╔════╝██╔══██╗╚██╗ ██╔╝
//  ██████╔╝██║   ██║██╔██╗ ██║    ██║   ██║██║   ██║█████╗  ██████╔╝ ╚████╔╝
//  ██╔══██╗██║   ██║██║╚██╗██║    ██║▄▄ ██║██║   ██║██╔══╝  ██╔══██╗  ╚██╔╝
//  ██║  ██║╚██████╔╝██║ ╚████║    ╚██████╔╝╚██████╔╝███████╗██║  ██║   ██║
//  ╚═╝  ╚═╝ ╚═════╝ ╚═╝  ╚═══╝     ╚══▀▀═╝  ╚═════╝ ╚══════╝╚═╝  ╚═╝   ╚═╝
//
// Send a Native Query to the datastore and gracefully handle errors.

module.exports = require('machine').build({


  friendlyName: 'Run Query',


  description: 'Run a native query on the datastore and gracefully handle errors.',


  inputs: {

    connection: {
      friendlyName: 'Connection',
      description: 'An active database connection.',
      extendedDescription: 'The provided database connection instance must still be active. Only database ' +
        'connection instances created by the `getConnection()` machine in the driver are supported.',
      required: true,
      readOnly: true,
      example: '==='
    },

    nativeQuery: {
      description: 'A SQL statement as a string (or to use parameterized queries, this should be provided as a dictionary).',
      extendedDescription: 'If provided as a dictionary, this should contain `sql` (the SQL statement string; ' +
        'e.g. \'SELECT * FROM dogs WHERE name = $1\') as well as an array of `bindings` (e.g. [\'Rover\']).',
      moreInfoUrl: 'https://github.com/brianc/node-postgres/wiki/Prepared-Statements#parameterized-queries',
      whereToGet: {
        description: 'This is oftentimes compiled from Waterline query syntax using "Compile statement", however it ' +
          'could also originate from userland code.'
      },
      example: '*',
      required: true
    },

    queryType: {
      description: 'The type of query operation to perform.',
      extendedDescription: 'Either "select", "insert", "delete", or "update". This ' +
        'determines how the provided raw result will be parsed/coerced.',
      moreInfoUrl: 'https://github.com/particlebanana/waterline-query-builder/blob/master/docs/syntax.md',
      example: 'select'
    },


    disconnectOnError: {
      description: 'A flag detailing if the connection should be released on error.',
      extendedDescription: 'For most instances it\'s much easier to automatically release it. There are times ' +
        'where you want to act on the error, such as transactions, where you don\'t want the connection ' +
        'to be disconnected automatically.',
      example: true,
      defaultsTo: true
    }

  },


  exits: {

    success: {
      description: 'A connection was successfully spawned.',
      outputVariableName: 'connection',
      example: '==='
    }

  },


  fn: function runQuery(inputs, exits) {
    var PG = require('machinepack-postgresql');


    //  ╦═╗╔═╗╦  ╔═╗╔═╗╔═╗╔═╗  ┌─┐┌─┐┌┐┌┌┐┌┌─┐┌─┐┌┬┐┬┌─┐┌┐┌
    //  ╠╦╝║╣ ║  ║╣ ╠═╣╚═╗║╣   │  │ │││││││├┤ │   │ ││ ││││
    //  ╩╚═╚═╝╩═╝╚═╝╩ ╩╚═╝╚═╝  └─┘└─┘┘└┘┘└┘└─┘└─┘ ┴ ┴└─┘┘└┘
    var releaseConnection = function releaseConnection(connection, done) {
      PG.releaseConnection({
        connection: connection
      }).exec({
        error: function error(err) {
          return done(err);
        },
        badConnection: function badConnection() {
          return done(err);
        },
        success: function success() {
          return done();
        }
      });
    };


    PG.sendNativeQuery({
      connection: inputs.connection,
      nativeQuery: inputs.nativeQuery
    })
    .exec({
      // If there was an error, check if the connection should be
      // released back into the pool automatically.
      error: function error(err) {
        if (!inputs.disconnectOnError) {
          return exits.error(err);
        }

        releaseConnection(inputs.connection, function cb(err) {
          return exits.error(err);
        });
      },
      // If the query failed, try and parse it into a normalized format and
      // release the connection if needed.
      queryFailed: function queryFailed(report) {
        // Parse the native query error into a normalized format
        var parsedError;
        try {
          parsedError = PG.parseNativeQueryError({
            nativeQueryError: report.error
          }).execSync();
        } catch (e) {
          parsedError = report.error;
        }

        // If the catch all error was used, return an error instance instead of
        // the footprint.
        var catchAllError = false;
        if (!parsedError.footprint) {
          catchAllError = true;
        }

        if (parsedError.footprint && parsedError.footprint.identity && parsedError.footprint.identity === 'catchall') {
          catchAllError = true;
        }

        if (!inputs.disconnectOnError) {
          if (catchAllError) {
            return exits.error(report.error);
          }

          return exits.error(parsedError);
        }

        releaseConnection(inputs.connection, function cb() {
          if (catchAllError) {
            return exits.error(report.error);
          }

          return exits.error(parsedError);
        });
      },
      success: function success(report) {
        //  ╔═╗╔═╗╦═╗╔═╗╔═╗  ┌─┐ ┬ ┬┌─┐┬─┐┬ ┬  ┬─┐┌─┐┌─┐┬ ┬┬ ┌┬┐┌─┐
        //  ╠═╝╠═╣╠╦╝╚═╗║╣   │─┼┐│ │├┤ ├┬┘└┬┘  ├┬┘├┤ └─┐│ ││  │ └─┐
        //  ╩  ╩ ╩╩╚═╚═╝╚═╝  └─┘└└─┘└─┘┴└─ ┴   ┴└─└─┘└─┘└─┘┴─┘┴ └─┘
        // If there was a query type given, parse the results.
        var queryResults = report.result;
        if (inputs.queryType) {
          try {
            queryResults = PG.parseNativeQueryResult({
              queryType: inputs.queryType,
              nativeQueryResult: report.result
            }).execSync();
          } catch (e) {
            return exits.error(e);
          }
        }

        return exits.success(queryResults);
      }
    });
  }


});
