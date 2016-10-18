//  ██████╗ ██╗   ██╗███╗   ██╗     ██████╗ ██╗   ██╗███████╗██████╗ ██╗   ██╗
//  ██╔══██╗██║   ██║████╗  ██║    ██╔═══██╗██║   ██║██╔════╝██╔══██╗╚██╗ ██╔╝
//  ██████╔╝██║   ██║██╔██╗ ██║    ██║   ██║██║   ██║█████╗  ██████╔╝ ╚████╔╝
//  ██╔══██╗██║   ██║██║╚██╗██║    ██║▄▄ ██║██║   ██║██╔══╝  ██╔══██╗  ╚██╔╝
//  ██║  ██║╚██████╔╝██║ ╚████║    ╚██████╔╝╚██████╔╝███████╗██║  ██║   ██║
//  ╚═╝  ╚═╝ ╚═════╝ ╚═╝  ╚═══╝     ╚══▀▀═╝  ╚═════╝ ╚══════╝╚═╝  ╚═╝   ╚═╝
//
// Send a Native Query to the datastore and gracefully handle errors.

var PG = require('machinepack-postgresql');

module.exports = function runQuery(options, cb) {
  // Validate input options
  if (!options.connection) {
    return cb(new Error('Invalid options. Run Query requires a valid connection option.'));
  }

  if (!options.nativeQuery) {
    return cb(new Error('Invalid options. Run Query requires a native query option.'));
  }

  // Default disconnectOnError to false
  var disconnectOnError = options.disconnectOnError || false;


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
      badConnection: function badConnection(err) {
        return done(err);
      },
      success: function success() {
        return done();
      }
    });
  };


  PG.sendNativeQuery({
    connection: options.connection,
    nativeQuery: options.nativeQuery
  })
  .exec({
    // If there was an error, check if the connection should be
    // released back into the pool automatically.
    error: function error(err) {
      if (!disconnectOnError) {
        return cb(err);
      }

      releaseConnection(options.connection, function cb(err) {
        return cb(err);
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
        releaseConnection(options.connection, function cb() {
          return cb(e);
        });
        return;
      }

      // If the catch all error was used, return an error instance instead of
      // the footprint.
      var catchAllError = false;

      if (parsedError.footprint.identity === 'catchall') {
        catchAllError = true;
      }

      if (!disconnectOnError) {
        if (catchAllError) {
          return cb(report.error);
        }

        return cb(parsedError);
      }

      releaseConnection(options.connection, function cb() {
        if (catchAllError) {
          return cb(report.error);
        }

        return cb(parsedError);
      });
    },
    success: function success(report) {
      //  ╔═╗╔═╗╦═╗╔═╗╔═╗  ┌─┐ ┬ ┬┌─┐┬─┐┬ ┬  ┬─┐┌─┐┌─┐┬ ┬┬ ┌┬┐┌─┐
      //  ╠═╝╠═╣╠╦╝╚═╗║╣   │─┼┐│ │├┤ ├┬┘└┬┘  ├┬┘├┤ └─┐│ ││  │ └─┐
      //  ╩  ╩ ╩╩╚═╚═╝╚═╝  └─┘└└─┘└─┘┴└─ ┴   ┴└─└─┘└─┘└─┘┴─┘┴ └─┘
      // If there was a query type given, parse the results.
      var queryResults = report.result;
      if (options.queryType) {
        try {
          queryResults = PG.parseNativeQueryResult({
            queryType: options.queryType,
            nativeQueryResult: report.result
          }).execSync();
        } catch (e) {
          return cb(e);
        }
      }

      return cb(null, queryResults);
    }
  });
};
