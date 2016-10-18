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

var _ = require('lodash');

module.exports = function unserializeValues(options) {
  if (!options.definition) {
    throw new Error('Invalid options to unserializeValues - definition is a required option.');
  }

  if (!options.records) {
    throw new Error('Invalid options to unserializeValues - records is a required option.');
  }

  // Ensure the records are an array
  if (!_.isArray(options.records)) {
    options.records = [options.records];
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
  _.each(options.definition, function schemaCheck(val, key) {
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
    return options.records;
  }

  //  ╔═╗╦═╗╔═╗╔═╗╔═╗╔═╗╔═╗  ┬─┐┌─┐┌─┐┌─┐┬─┐┌┬┐┌─┐
  //  ╠═╝╠╦╝║ ║║  ║╣ ╚═╗╚═╗  ├┬┘├┤ │  │ │├┬┘ ││└─┐
  //  ╩  ╩╚═╚═╝╚═╝╚═╝╚═╝╚═╝  ┴└─└─┘└─┘└─┘┴└──┴┘└─┘
  _.each(options.records, function processRecord(record) {
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


  return options.records;
};
