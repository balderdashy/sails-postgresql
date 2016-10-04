//   ██████╗██████╗ ███████╗ █████╗ ████████╗███████╗    ███████╗ ██████╗██╗  ██╗███████╗███╗   ███╗ █████╗
//  ██╔════╝██╔══██╗██╔════╝██╔══██╗╚══██╔══╝██╔════╝    ██╔════╝██╔════╝██║  ██║██╔════╝████╗ ████║██╔══██╗
//  ██║     ██████╔╝█████╗  ███████║   ██║   █████╗      ███████╗██║     ███████║█████╗  ██╔████╔██║███████║
//  ██║     ██╔══██╗██╔══╝  ██╔══██║   ██║   ██╔══╝      ╚════██║██║     ██╔══██║██╔══╝  ██║╚██╔╝██║██╔══██║
//  ╚██████╗██║  ██║███████╗██║  ██║   ██║   ███████╗    ███████║╚██████╗██║  ██║███████╗██║ ╚═╝ ██║██║  ██║
//   ╚═════╝╚═╝  ╚═╝╚══════╝╚═╝  ╚═╝   ╚═╝   ╚══════╝    ╚══════╝ ╚═════╝╚═╝  ╚═╝╚══════╝╚═╝     ╚═╝╚═╝  ╚═╝
//
// Create a Postgres "schema". Used as namespace internally to avoid confusion.

module.exports = require('machine').build({


  friendlyName: 'Create Schema',


  description: 'Create a Postgres schema namespace.',


  inputs: {

    datastore: {
      description: 'The datastore to use for connections.',
      extendedDescription: 'Datastores represent the config and manager required to obtain an active database connection.',
      required: true,
      readOnly: true,
      example: '==='
    },

    schemaName: {
      description: 'The name of the schema to create.',
      required: true,
      example: 'users'
    }

  },


  exits: {

    success: {
      description: 'The schema was created successfully.'
    },

    badConfiguration: {
      description: 'The configuration was invalid.'
    }

  },


  fn: function createSchema(inputs, exits) {
    var Helpers = require('./private');

    //  ╔═╗╦═╗╔═╗╔═╗╔╦╗╔═╗  ┌┐┌┌─┐┌┬┐┌─┐┌─┐┌─┐┌─┐┌─┐┌─┐
    //  ║  ╠╦╝║╣ ╠═╣ ║ ║╣   │││├─┤│││├┤ └─┐├─┘├─┤│  ├┤
    //  ╚═╝╩╚═╚═╝╩ ╩ ╩ ╚═╝  ┘└┘┴ ┴┴ ┴└─┘└─┘┴  ┴ ┴└─┘└─┘
    Helpers.createNamespace({
      datastore: inputs.datastore,
      schemaName: inputs.schemaName
    }).exec({
      error: function error(err) {
        return exits.error(new Error('There was an error creating the postgres schema.' + err.stack));
      },
      success: function success() {
        return exits.success();
      }
    });
  }

});
