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

var _ = require('lodash');

module.exports = function serialzeValues(records) {
  if (!records) {
    throw new Error('Serialize Values requires a result set to serialize.');
  }

  // Hold a reference to the records
  var recordsToSerialize = records;

  // Ensure the records are an array
  if (!_.isArray(records)) {
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
  if (_.isArray(records)) {
    return recordsToSerialize;
  }

  // Otherwise return the first item in the array
  return _.first(recordsToSerialize);
};
