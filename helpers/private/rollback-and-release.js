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
// back into the pool.

var _ = require('lodash');
var PG = require('machinepack-postgresql');

module.exports = function rollbackAndRelease(connection, cb) {
  if (!connection || _.isFunction(connection)) {
    return cb(new Error('Commit and Release requires a valid connection.'));
  }

  // Rollback the transaction
  PG.rollbackTransaction({
    connection: connection
  })
  .exec(function rollbackCb() {
    // Release the connection back into the pool
    PG.releaseConnection({
      connection: connection
    })
    .exec(function releaseCb() {
      return cb();
    });
  });
};
