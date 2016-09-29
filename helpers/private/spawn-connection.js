//  ███████╗██████╗  █████╗ ██╗    ██╗███╗   ██╗
//  ██╔════╝██╔══██╗██╔══██╗██║    ██║████╗  ██║
//  ███████╗██████╔╝███████║██║ █╗ ██║██╔██╗ ██║
//  ╚════██║██╔═══╝ ██╔══██║██║███╗██║██║╚██╗██║
//  ███████║██║     ██║  ██║╚███╔███╔╝██║ ╚████║
//  ╚══════╝╚═╝     ╚═╝  ╚═╝ ╚══╝╚══╝ ╚═╝  ╚═══╝
//
//   ██████╗ ██████╗ ███╗   ██╗███╗   ██╗███████╗ ██████╗████████╗██╗ ██████╗ ███╗   ██╗
//  ██╔════╝██╔═══██╗████╗  ██║████╗  ██║██╔════╝██╔════╝╚══██╔══╝██║██╔═══██╗████╗  ██║
//  ██║     ██║   ██║██╔██╗ ██║██╔██╗ ██║█████╗  ██║        ██║   ██║██║   ██║██╔██╗ ██║
//  ██║     ██║   ██║██║╚██╗██║██║╚██╗██║██╔══╝  ██║        ██║   ██║██║   ██║██║╚██╗██║
//  ╚██████╗╚██████╔╝██║ ╚████║██║ ╚████║███████╗╚██████╗   ██║   ██║╚██████╔╝██║ ╚████║
//   ╚═════╝ ╚═════╝ ╚═╝  ╚═══╝╚═╝  ╚═══╝╚══════╝ ╚═════╝   ╚═╝   ╚═╝ ╚═════╝ ╚═╝  ╚═══╝
//
// Instantiate a new connection from the connection manager.

module.exports = require('machine').build({


  friendlyName: 'Spawn Connection',


  description: 'Grab a connection from the connection manager.',


  inputs: {

    datastore: {
      description: 'The datastore to use for connections.',
      extendedDescription: 'Datastores represent the config and manager required to obtain an active database connection.',
      required: true,
      readOnly: true,
      example: '==='
    }

  },


  exits: {

    success: {
      description: 'A connection was successfully spawned.',
      outputVariableName: 'connection',
      example: '==='
    }

  },


  fn: function spawnConnection(inputs, exits) {
    var PG = require('machinepack-postgresql');

    PG.getConnection({
      manager: inputs.datastore.manager,
      meta: inputs.datastore.config
    })
    .exec({
      error: function error(err) {
        return exits.error(err);
      },
      malformed: function malformed(err) {
        // // Remove password before reporting error so that it doesn't show up in logs
        // var connectionValues = _.clone(datastoreConfig.config);
        // connectionValues.password = '[redacted]';
        // var Error = new Error('Error creating a connection to Postgresql using the following settings:\n', connectionValues);
        // Error.originalError = err;
        return exits.error(err);
      },
      failedToConnect: function failedToConnect(err) {
        return exits.error(err);
      },
      success: function success(connection) {
        return exits.success(connection.connection);
      }
    });
  }


});
