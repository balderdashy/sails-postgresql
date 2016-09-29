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

module.exports = require('machine').build({


  friendlyName: 'Rollback and Release',


  description: 'Rollback a transaction and release the connection back into the pool.',


  inputs: {

    connection: {
      friendlyName: 'Connection',
      description: 'An active database connection.',
      extendedDescription: 'The provided database connection instance must still be active. Only database ' +
        'connection instances created by the `getConnection()` machine in the driver are supported.',
      required: true,
      readOnly: true,
      example: '==='
    }

  },


  exits: {

    success: {
      description: 'The transaction was rolled back successfully.'
    }

  },


  fn: function spawnConnection(inputs, exits) {
    var PG = require('machinepack-postgresql');

    // Rollback the transaction
    PG.rollbackTransaction({
      connection: inputs.connection
    })
    .exec(function rollbackCb() {
      // Release the connection back into the pool
      PG.releaseConnection({
        connection: inputs.connection
      })
      .exec(function releaseCb() {
        return exits.success();
      });
    });
  }


});
