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

var _ = require('@sailshq/lodash');
var spawnOrLeaseConnection = require('../connection/spawn-or-lease-connection');
var runQuery = require('../query/run-query');

module.exports = function createNamespace(options, cb) {
  //  ╦  ╦╔═╗╦  ╦╔╦╗╔═╗╔╦╗╔═╗  ┌─┐┌─┐┌┬┐┬┌─┐┌┐┌┌─┐
  //  ╚╗╔╝╠═╣║  ║ ║║╠═╣ ║ ║╣   │ │├─┘ │ ││ ││││└─┐
  //   ╚╝ ╩ ╩╩═╝╩═╩╝╩ ╩ ╩ ╚═╝  └─┘┴   ┴ ┴└─┘┘└┘└─┘
  if (_.isUndefined(options) || !_.isPlainObject(options)) {
    throw new Error('Invalid options argument. Options must contain: datastore, schemaName, and meta.');
  }

  if (!_.has(options, 'datastore') || !_.isPlainObject(options.datastore)) {
    throw new Error('Invalid option used in options argument. Missing or invalid datastore.');
  }

  if (!_.has(options, 'schemaName') || !_.isString(options.schemaName)) {
    throw new Error('Invalid option used in options argument. Missing or invalid schemaName.');
  }

  if (!_.has(options, 'meta') || !_.isPlainObject(options.meta)) {
    throw new Error('Invalid option used in options argument. Missing or invalid meta.');
  }

  // Set a flag if a leased connection from outside the adapter was used or not.
  var leased = _.has(options.meta, 'leasedConnection');


  //  ╔═╗╔═╗╔═╗╦ ╦╔╗╔  ┌─┐┌─┐┌┐┌┌┐┌┌─┐┌─┐┌┬┐┬┌─┐┌┐┌
  //  ╚═╗╠═╝╠═╣║║║║║║  │  │ │││││││├┤ │   │ ││ ││││
  //  ╚═╝╩  ╩ ╩╚╩╝╝╚╝  └─┘└─┘┘└┘┘└┘└─┘└─┘ ┴ ┴└─┘┘└┘
  // Spawn a new connection to run the queries on.
  spawnOrLeaseConnection(options.datastore, options.meta, function spawnConnectionCb(err, connection) {
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
      nativeQuery: query,
      disconnectOnError: leased ? false : true
    }, function runQueryCb(err) {
      if (err) {
        return cb(err);
      }

      return cb();
    });
  });
};
