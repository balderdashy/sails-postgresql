//  ██████╗  ██████╗ ██╗     ██╗     ██████╗  █████╗  ██████╗██╗  ██╗     █████╗ ███╗   ██╗██████╗
//  ██╔══██╗██╔═══██╗██║     ██║     ██╔══██╗██╔══██╗██╔════╝██║ ██╔╝    ██╔══██╗████╗  ██║██╔══██╗
//  ██████╔╝██║   ██║██║     ██║     ██████╔╝███████║██║     █████╔╝     ███████║██╔██╗ ██║██║  ██║
//  ██╔══██╗██║   ██║██║     ██║     ██╔══██╗██╔══██║██║     ██╔═██╗     ██╔══██║██║╚██╗██║██║  ██║
//  ██║  ██║╚██████╔╝███████╗███████╗██████╔╝██║  ██║╚██████╗██║  ██╗    ██║  ██║██║ ╚████║██████╔╝
//  ╚═╝  ╚═╝ ╚═════╝ ╚══════╝╚══════╝╚═════╝ ╚═╝  ╚═╝ ╚═════╝╚═╝  ╚═╝    ╚═╝  ╚═╝╚═╝  ╚═══╝╚═════╝
//
//  ██████╗ ███████╗██╗     ███████╗ █████╗ ███████╗███████╗
//  ██╔══██╗██╔════╝██║     ██╔════╝██╔══██╗██╔════╝██╔════╝
//  ██████╔╝█████╗  ██║     █████╗  ███████║███████╗█████╗
//  ██╔══██╗██╔══╝  ██║     ██╔══╝  ██╔══██║╚════██║██╔══╝
//  ██║  ██║███████╗███████╗███████╗██║  ██║███████║███████╗
//  ╚═╝  ╚═╝╚══════╝╚══════╝╚══════╝╚═╝  ╚═╝╚══════╝╚══════╝
//
// If an error occurs in a transaction, rollback the transaction and release the connection
// back into the pool, if it's not a leased connection from outside the adapter.

var _ = require('lodash');
var PG = require('machinepack-postgresql');

module.exports = function rollbackAndRelease(connection, leased, cb) {
  if (!connection || _.isFunction(connection)) {
    return cb(new Error('Commit and Release requires a valid connection.'));
  }

  // Rollback the transaction
  PG.rollbackTransaction({
    connection: connection
  })
  .exec(function rollbackCb() {
    // Only release the connection if the leased flag is false
    if (leased) {
      return cb();
    }

    // Release the connection back into the pool
    PG.releaseConnection({
      connection: connection
    })
    .exec(function releaseCb() {
      return cb();
    });
  });
};
