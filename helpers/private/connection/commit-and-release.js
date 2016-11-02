//   ██████╗ ██████╗ ███╗   ███╗███╗   ███╗██╗████████╗     █████╗ ███╗   ██╗██████╗
//  ██╔════╝██╔═══██╗████╗ ████║████╗ ████║██║╚══██╔══╝    ██╔══██╗████╗  ██║██╔══██╗
//  ██║     ██║   ██║██╔████╔██║██╔████╔██║██║   ██║       ███████║██╔██╗ ██║██║  ██║
//  ██║     ██║   ██║██║╚██╔╝██║██║╚██╔╝██║██║   ██║       ██╔══██║██║╚██╗██║██║  ██║
//  ╚██████╗╚██████╔╝██║ ╚═╝ ██║██║ ╚═╝ ██║██║   ██║       ██║  ██║██║ ╚████║██████╔╝
//   ╚═════╝ ╚═════╝ ╚═╝     ╚═╝╚═╝     ╚═╝╚═╝   ╚═╝       ╚═╝  ╚═╝╚═╝  ╚═══╝╚═════╝
//
//  ██████╗ ███████╗██╗     ███████╗ █████╗ ███████╗███████╗
//  ██╔══██╗██╔════╝██║     ██╔════╝██╔══██╗██╔════╝██╔════╝
//  ██████╔╝█████╗  ██║     █████╗  ███████║███████╗█████╗
//  ██╔══██╗██╔══╝  ██║     ██╔══╝  ██╔══██║╚════██║██╔══╝
//  ██║  ██║███████╗███████╗███████╗██║  ██║███████║███████╗
//  ╚═╝  ╚═╝╚══════╝╚══════╝╚══════╝╚═╝  ╚═╝╚══════╝╚══════╝
//
// Commit a transaction and release the connection back into the pool.

var _ = require('lodash');
var PG = require('machinepack-postgresql');

module.exports = function commitAndRelease(connection, leased, cb) {
  if (!connection || _.isFunction(connection)) {
    return cb(new Error('Commit and Release requires a valid connection.'));
  }

  // Commit the transaction
  PG.commitTransaction({
    connection: connection
  })
  .exec(function commitCb(err) {
    if (err) {
      // Rollback the transaction
      PG.rollbackTransaction({
        connection: connection
      })
      .exec(function rollbackCb() {
        // Only release the connection if it wasn't leased from outside the
        // adapter.
        if (leased) {
          return cb(err);
        }

        // Release the connection back into the pool
        PG.releaseConnection({
          connection: connection
        })
        .exec(function releaseCb() {
          return cb(err);
        });
      });

      return;
    }

    // Only release the connection if it wasn't leased from outside the
    // adapter.
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
