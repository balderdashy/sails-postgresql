//  ███████╗██╗███╗   ██╗██████╗     ██████╗ ██████╗ ██╗███╗   ███╗ █████╗ ██████╗ ██╗   ██╗
//  ██╔════╝██║████╗  ██║██╔══██╗    ██╔══██╗██╔══██╗██║████╗ ████║██╔══██╗██╔══██╗╚██╗ ██╔╝
//  █████╗  ██║██╔██╗ ██║██║  ██║    ██████╔╝██████╔╝██║██╔████╔██║███████║██████╔╝ ╚████╔╝
//  ██╔══╝  ██║██║╚██╗██║██║  ██║    ██╔═══╝ ██╔══██╗██║██║╚██╔╝██║██╔══██║██╔══██╗  ╚██╔╝
//  ██║     ██║██║ ╚████║██████╔╝    ██║     ██║  ██║██║██║ ╚═╝ ██║██║  ██║██║  ██║   ██║
//  ╚═╝     ╚═╝╚═╝  ╚═══╝╚═════╝     ╚═╝     ╚═╝  ╚═╝╚═╝╚═╝     ╚═╝╚═╝  ╚═╝╚═╝  ╚═╝   ╚═╝
//
//  ██╗  ██╗███████╗██╗   ██╗
//  ██║ ██╔╝██╔════╝╚██╗ ██╔╝
//  █████╔╝ █████╗   ╚████╔╝
//  ██╔═██╗ ██╔══╝    ╚██╔╝
//  ██║  ██╗███████╗   ██║
//  ╚═╝  ╚═╝╚══════╝   ╚═╝
//

module.exports = require('machine').build({


  friendlyName: 'Find Primary Key',


  description: 'Given a model schema, return the primary key field.',


  cacheable: true,


  sync: true,


  inputs: {

    definition: {
      description: 'The Waterline model definition for a table.',
      required: true,
      readOnly: true,
      example: '==='
    }

  },


  exits: {

    success: {
      description: 'The schema has a primary key.',
      example: 'id'
    }

  },


  fn: function findPrimaryKey(inputs, exits) {
    var _ = require('lodash');

    // Look for an attribute that has a primaryKey flag on it
    var pk = _.findKey(inputs.definition, function find(val) {
      if (_.has(val, 'primaryKey')) {
        return true;
      }
    });

    // Default the primary key to `id`
    if (!pk) {
      pk = 'id';
    }

    return exits.success(pk);
  }
});
