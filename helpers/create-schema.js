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
      type: 'ref'
    },

    schemaName: {
      description: 'The name of the schema to create.',
      required: true,
      example: 'users'
    },

    meta: {
      friendlyName: 'Meta (custom)',
      description: 'Additional stuff to pass to the driver.',
      extendedDescription: 'This is reserved for custom driver-specific extensions.',
      type: 'ref'
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
    // Dependencies
    var _ = require('@sailshq/lodash');
    var Helpers = require('./private');


    // Set a flag if a leased connection from outside the adapter was used or not.
    var leased = _.has(inputs.meta, 'leasedConnection');


    //  ╔═╗╦═╗╔═╗╔═╗╔╦╗╔═╗  ┌┐┌┌─┐┌┬┐┌─┐┌─┐┌─┐┌─┐┌─┐┌─┐
    //  ║  ╠╦╝║╣ ╠═╣ ║ ║╣   │││├─┤│││├┤ └─┐├─┘├─┤│  ├┤
    //  ╚═╝╩╚═╚═╝╩ ╩ ╩ ╚═╝  ┘└┘┴ ┴┴ ┴└─┘└─┘┴  ┴ ┴└─┘└─┘
    Helpers.schema.createNamespace({
      datastore: inputs.datastore,
      schemaName: inputs.schemaName,
      meta: inputs.meta,
      leased: leased
    }, function cb(err) {
      if (err) {
        return exits.error(new Error('There was an error creating the postgres schema.' + err.stack));
      }

      return exits.success();
    });
  }

});
