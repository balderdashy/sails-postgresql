//  ███████╗███████╗██████╗ ██╗ █████╗ ██╗     ██╗███████╗███████╗
//  ██╔════╝██╔════╝██╔══██╗██║██╔══██╗██║     ██║╚══███╔╝██╔════╝
//  ███████╗█████╗  ██████╔╝██║███████║██║     ██║  ███╔╝ █████╗
//  ╚════██║██╔══╝  ██╔══██╗██║██╔══██║██║     ██║ ███╔╝  ██╔══╝
//  ███████║███████╗██║  ██║██║██║  ██║███████╗██║███████╗███████╗
//  ╚══════╝╚══════╝╚═╝  ╚═╝╚═╝╚═╝  ╚═╝╚══════╝╚═╝╚══════╝╚══════╝
//
//  ██╗   ██╗ █████╗ ██╗     ██╗   ██╗███████╗███████╗
//  ██║   ██║██╔══██╗██║     ██║   ██║██╔════╝██╔════╝
//  ██║   ██║███████║██║     ██║   ██║█████╗  ███████╗
//  ╚██╗ ██╔╝██╔══██║██║     ██║   ██║██╔══╝  ╚════██║
//   ╚████╔╝ ██║  ██║███████╗╚██████╔╝███████╗███████║
//    ╚═══╝  ╚═╝  ╚═╝╚══════╝ ╚═════╝ ╚══════╝╚══════╝
//
// Prepare values to be stored in a Postgres table.

module.exports = require('machine').build({


  friendlyName: 'Serialize Values',


  description: 'Prepare values to be stored in the database.',


  cacheable: true,


  sync: true,


  inputs: {

    records: {
      description: 'The database records that are being cast.',
      required: true,
      example: '==='
    }

  },


  exits: {

    success: {
      description: 'The records were successfully casted and normalized.',
      example: '==='
    }

  },


  fn: function serializeValues(inputs, exits) {
    var _ = require('lodash');

    // Hold a reference to the values being serialized
    var recordsToSerialize = inputs.records;

    // Ensure the records are an array
    if (!_.isArray(recordsToSerialize)) {
      recordsToSerialize = [recordsToSerialize];
    }


    _.each(recordsToSerialize, function processRecord(record) {
      _.each(record, function processAttribute(val, key) {
        // Cast dates to SQL
        // if (_.isDate(val)) {
        //   record = utils.toSqlDate(val);
        // }

        // Store Arrays as strings
        if (_.isArray(val)) {
          try {
            val = JSON.stringify(val);
          } catch (e) {
            throw new Error('There was an error stringifying the JSON encoded array.\n' + e.stack);
          }
        }

        // Set the value back on the key
        record[key] = val;
      });
    });

    // Be sure to return the records in the same format as they came in.
    // i.e. array vs object
    if (_.isArray(inputs.records)) {
      return exits.success(recordsToSerialize);
    }

    // Otherwise return the first item in the array
    return exits.success(_.first(recordsToSerialize));
  }


});
