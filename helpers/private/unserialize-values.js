//  ██╗   ██╗███╗   ██╗███████╗███████╗██████╗ ██╗ █████╗ ██╗     ██╗███████╗███████╗
//  ██║   ██║████╗  ██║██╔════╝██╔════╝██╔══██╗██║██╔══██╗██║     ██║╚══███╔╝██╔════╝
//  ██║   ██║██╔██╗ ██║███████╗█████╗  ██████╔╝██║███████║██║     ██║  ███╔╝ █████╗
//  ██║   ██║██║╚██╗██║╚════██║██╔══╝  ██╔══██╗██║██╔══██║██║     ██║ ███╔╝  ██╔══╝
//  ╚██████╔╝██║ ╚████║███████║███████╗██║  ██║██║██║  ██║███████╗██║███████╗███████╗
//   ╚═════╝ ╚═╝  ╚═══╝╚══════╝╚══════╝╚═╝  ╚═╝╚═╝╚═╝  ╚═╝╚══════╝╚═╝╚══════╝╚══════╝
//
//  ██╗   ██╗ █████╗ ██╗     ██╗   ██╗███████╗███████╗
//  ██║   ██║██╔══██╗██║     ██║   ██║██╔════╝██╔════╝
//  ██║   ██║███████║██║     ██║   ██║█████╗  ███████╗
//  ╚██╗ ██╔╝██╔══██║██║     ██║   ██║██╔══╝  ╚════██║
//   ╚████╔╝ ██║  ██║███████╗╚██████╔╝███████╗███████║
//    ╚═══╝  ╚═╝  ╚═╝╚══════╝ ╚═════╝ ╚══════╝╚══════╝
//
// Given a model schema unserialize any values returned from the database into
// values suitable for Waterline.

module.exports = require('machine').build({


  friendlyName: 'Unserialize Values',


  description: 'Based on the model schema, cast the values into proper types.',


  cacheable: true,


  sync: true,


  inputs: {

    definition: {
      description: 'The model definition from Waterline',
      required: true,
      readOnly: true,
      example: '==='
    },

    records: {
      description: 'The database records that are being cast.',
      required: true,
      readOnly: true,
      example: '==='
    }

  },


  exits: {

    success: {
      description: 'The records were successfully casted and normalized.',
      example: '==='
    }

  },


  fn: function unserializeValues(inputs, exits) {
    var _ = require('lodash');

    // Ensure the records are an array
    if (!_.isArray(inputs.records)) {
      inputs.records = [inputs.records];
    }

    //  ╔═╗╦ ╦╔═╗╔═╗╦╔═  ┌─┐┌─┐┬─┐  ┌─┐┌─┐┌─┐┌┬┐  ┌─┐┌┐ ┬  ┌─┐
    //  ║  ╠═╣║╣ ║  ╠╩╗  ├┤ │ │├┬┘  │  ├─┤└─┐ │───├─┤├┴┐│  ├┤
    //  ╚═╝╩ ╩╚═╝╚═╝╩ ╩  └  └─┘┴└─  └─┘┴ ┴└─┘ ┴   ┴ ┴└─┘┴─┘└─┘
    //  ┌─┐┌┬┐┌┬┐┬─┐┬┌┐ ┬ ┬┌┬┐┌─┐┌─┐
    //  ├─┤ │  │ ├┬┘│├┴┐│ │ │ ├┤ └─┐
    //  ┴ ┴ ┴  ┴ ┴└─┴└─┘└─┘ ┴ └─┘└─┘
    // Currently only ARRAY types are castable. This is because they are stored
    // as JSON encoded strings in the database.
    var castable = [];
    _.each(inputs.definition, function schemaCheck(val, key) {
      var type;
      if (!_.isPlainObject(val)) {
        type = val;
      } else {
        type = val.type;
      }

      if (type.toLowerCase() === 'array') {
        // Check if a column name is being used
        if (_.has(val, 'columnName')) {
          key = val.columnName;
        }

        castable.push(key);
      }
    });

    // If there was nothing to cast, return the records
    if (!castable.length) {
      return exits.success(inputs.records);
    }

    //  ╔═╗╦═╗╔═╗╔═╗╔═╗╔═╗╔═╗  ┬─┐┌─┐┌─┐┌─┐┬─┐┌┬┐┌─┐
    //  ╠═╝╠╦╝║ ║║  ║╣ ╚═╗╚═╗  ├┬┘├┤ │  │ │├┬┘ ││└─┐
    //  ╩  ╩╚═╚═╝╚═╝╚═╝╚═╝╚═╝  ┴└─└─┘└─┘└─┘┴└──┴┘└─┘
    _.each(inputs.records, function processRecord(record) {
      _.each(castable, function castRecordValue(key) {
        if (_.has(record, key)) {
          try {
            record[key] = JSON.parse(record[key]);
          } catch (e) {
            return;
          }
        }
      });
    });


    return exits.success(inputs.records);
  }


});
