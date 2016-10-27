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

module.exports = function commitAndRelease(connection, cb) {
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

    // Release the connection back into the pool
    PG.releaseConnection({
      connection: connection
    })
    .exec(function releaseCb() {
      return cb();
    });
  });
};
