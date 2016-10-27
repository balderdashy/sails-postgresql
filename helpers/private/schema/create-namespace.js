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

var spawnConnection = require('../connection/spawn-connection');
var runQuery = require('../query/run-query');

module.exports = function createNamespace(options, cb) {
  if (!options.datastore) {
    return cb(new Error('Invalid options to Create Namespace - datastore is a required option.'));
  }

  if (!options.schemaName) {
    return cb(new Error('Invalid options to Create Namespace - schemaName is a required option.'));
  }


  //  ╔═╗╔═╗╔═╗╦ ╦╔╗╔  ┌─┐┌─┐┌┐┌┌┐┌┌─┐┌─┐┌┬┐┬┌─┐┌┐┌
  //  ╚═╗╠═╝╠═╣║║║║║║  │  │ │││││││├┤ │   │ ││ ││││
  //  ╚═╝╩  ╩ ╩╚╩╝╝╚╝  └─┘└─┘┘└┘┘└┘└─┘└─┘ ┴ ┴└─┘┘└┘
  // Spawn a new connection to run the queries on.
  spawnConnection(options.datastore, function spawnConnectionCb(err, connection) {
    if (err) {
      err.code = 'badConnection';
      return cb(err);
    }

    // Build Query
    var query = 'CREATE SCHEMA "' + options.schemaName + '"';


    //  ╦═╗╦ ╦╔╗╔  ┌─┐ ┬ ┬┌─┐┬─┐┬ ┬
    //  ╠╦╝║ ║║║║  │─┼┐│ │├┤ ├┬┘└┬┘
    //  ╩╚═╚═╝╝╚╝  └─┘└└─┘└─┘┴└─ ┴
    // Run the CREATE SCHEMA query and release the connection
    runQuery({
      connection: connection,
      nativeQuery: query
    }, function runQueryCb(err) {
      if (err) {
        return cb(err);
      }

      return cb();
    });
  });
};
