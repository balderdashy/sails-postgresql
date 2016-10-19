//   ██████╗██████╗ ███████╗ █████╗ ████████╗███████╗
//  ██╔════╝██╔══██╗██╔════╝██╔══██╗╚══██╔══╝██╔════╝
//  ██║     ██████╔╝█████╗  ███████║   ██║   █████╗
//  ██║     ██╔══██╗██╔══╝  ██╔══██║   ██║   ██╔══╝
//  ╚██████╗██║  ██║███████╗██║  ██║   ██║   ███████╗
//   ╚═════╝╚═╝  ╚═╝╚══════╝╚═╝  ╚═╝   ╚═╝   ╚══════╝
//
//  ███╗   ██╗ █████╗ ███╗   ███╗███████╗███████╗██████╗  █████╗  ██████╗███████╗
//  ████╗  ██║██╔══██╗████╗ ████║██╔════╝██╔════╝██╔══██╗██╔══██╗██╔════╝██╔════╝
//  ██╔██╗ ██║███████║██╔████╔██║█████╗  ███████╗██████╔╝███████║██║     █████╗
//  ██║╚██╗██║██╔══██║██║╚██╔╝██║██╔══╝  ╚════██║██╔═══╝ ██╔══██║██║     ██╔══╝
//  ██║ ╚████║██║  ██║██║ ╚═╝ ██║███████╗███████║██║     ██║  ██║╚██████╗███████╗
//  ╚═╝  ╚═══╝╚═╝  ╚═╝╚═╝     ╚═╝╚══════╝╚══════╝╚═╝     ╚═╝  ╚═╝ ╚═════╝╚══════╝
//
// This is a Postgres Schema but because the name is so overloaded use namespace
// internally.

var PG = require('machinepack-postgresql');
var spawnConnection = require('./spawn-connection');

module.exports = function createNamespace(options, cb) {
  if (!options.datastore) {
    return cb(new Error('Invalid options to Create Namespace - datastore is a required option.'));
  }

  if (!options.schemaName) {
    return cb(new Error('Invalid options to Create Namespace - schemaName is a required option.'));
  }

  //  ╦═╗╦ ╦╔╗╔  ┌┐┌┌─┐┌┬┐┬┬  ┬┌─┐  ┌─┐ ┬ ┬┌─┐┬─┐┬ ┬
  //  ╠╦╝║ ║║║║  │││├─┤ │ │└┐┌┘├┤   │─┼┐│ │├┤ ├┬┘└┬┘
  //  ╩╚═╚═╝╝╚╝  ┘└┘┴ ┴ ┴ ┴ └┘ └─┘  └─┘└└─┘└─┘┴└─ ┴
  var runNativeQuery = function runNativeQuery(connection, query, done) {
    PG.sendNativeQuery({
      connection: connection,
      nativeQuery: query
    })
    .exec(function execCb(err, report) {
      if (err) {
        return done(err);
      }

      return done(null, report.result.rows);
    });
  };


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
        return done(new Error('Bad connection when trying to release an active connection.'));
      },
      success: function success() {
        return done();
      }
    });
  };


  //  ╦═╗╦ ╦╔╗╔  ┌─┐ ┬ ┬┌─┐┬─┐┬ ┬  ┌─┐┌┐┌┌┬┐  ┬─┐┌─┐┬  ┌─┐┌─┐┌─┐┌─┐
  //  ╠╦╝║ ║║║║  │─┼┐│ │├┤ ├┬┘└┬┘  ├─┤│││ ││  ├┬┘├┤ │  ├┤ ├─┤└─┐├┤
  //  ╩╚═╚═╝╝╚╝  └─┘└└─┘└─┘┴└─ ┴   ┴ ┴┘└┘─┴┘  ┴└─└─┘┴─┘└─┘┴ ┴└─┘└─┘
  //  ┌─┐┌─┐┌┐┌┌┐┌┌─┐┌─┐┌┬┐┬┌─┐┌┐┌
  //  │  │ │││││││├┤ │   │ ││ ││││
  //  └─┘└─┘┘└┘┘└┘└─┘└─┘ ┴ ┴└─┘┘└┘
  var runQuery = function runQuery(connection, query, done) {
    runNativeQuery(connection, query, function nativeQueryCb(err) {
      // Always release the connection no matter what the error state.
      releaseConnection(connection, function releaseConnectionCb() {
        // If the native query had an error, return that error
        if (err) {
          return done(err);
        }

        return done();
      });
    });
  };


  // Spawn a connection and create the schema
  spawnConnection(options.datastore, function spawnConnectionCb(err, connection) {
    if (err) {
      err.code = 'badConnection';
      return cb(err);
    }

    // Build Query
    var query = 'CREATE SCHEMA "' + options.schemaName + '"';

    // Run the CREATE SCHEMA query and release the connection
    runQuery(connection, query, function runQueryCb(err) {
      if (err) {
        return cb(err);
      }

      return cb();
    });
  });
};
