//  ██████╗ ███████╗ ██████╗ ██╗███╗   ██╗
//  ██╔══██╗██╔════╝██╔════╝ ██║████╗  ██║
//  ██████╔╝█████╗  ██║  ███╗██║██╔██╗ ██║
//  ██╔══██╗██╔══╝  ██║   ██║██║██║╚██╗██║
//  ██████╔╝███████╗╚██████╔╝██║██║ ╚████║
//  ╚═════╝ ╚══════╝ ╚═════╝ ╚═╝╚═╝  ╚═══╝
//
//  ████████╗██████╗  █████╗ ███╗   ██╗███████╗ █████╗  ██████╗████████╗██╗ ██████╗ ███╗   ██╗
//  ╚══██╔══╝██╔══██╗██╔══██╗████╗  ██║██╔════╝██╔══██╗██╔════╝╚══██╔══╝██║██╔═══██╗████╗  ██║
//     ██║   ██████╔╝███████║██╔██╗ ██║███████╗███████║██║        ██║   ██║██║   ██║██╔██╗ ██║
//     ██║   ██╔══██╗██╔══██║██║╚██╗██║╚════██║██╔══██║██║        ██║   ██║██║   ██║██║╚██╗██║
//     ██║   ██║  ██║██║  ██║██║ ╚████║███████║██║  ██║╚██████╗   ██║   ██║╚██████╔╝██║ ╚████║
//     ╚═╝   ╚═╝  ╚═╝╚═╝  ╚═╝╚═╝  ╚═══╝╚══════╝╚═╝  ╚═╝ ╚═════╝   ╚═╝   ╚═╝ ╚═════╝ ╚═╝  ╚═══╝
//
// Opens a new transaction on a given connection.

var PG = require('machinepack-postgresql');

module.exports = function beginTransaction(connection, cb) {
  PG.beginTransaction({
    connection: connection
  })
  .exec({
    // If there was an error opening a transaction, release the connection.
    // After releasing the connection always return the original error.
    error: function error(err) {
      PG.releaseConnection({
        connection: connection
      }).exec({
        error: function error(err) {
          return cb(new Error('There was an error releasing the connection back into the pool. ' + err.stack));
        },
        success: function success() {
          return cb(new Error('There was an error starting a transaction. ' + err.stack));
        }
      });
    },
    success: function success() {
      return cb();
    }
  });
};
