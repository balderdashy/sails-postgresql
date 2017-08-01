//  ██████╗ ██████╗  ██████╗  ██████╗███████╗███████╗███████╗    ███████╗ █████╗  ██████╗██╗  ██╗
//  ██╔══██╗██╔══██╗██╔═══██╗██╔════╝██╔════╝██╔════╝██╔════╝    ██╔════╝██╔══██╗██╔════╝██║  ██║
//  ██████╔╝██████╔╝██║   ██║██║     █████╗  ███████╗███████╗    █████╗  ███████║██║     ███████║
//  ██╔═══╝ ██╔══██╗██║   ██║██║     ██╔══╝  ╚════██║╚════██║    ██╔══╝  ██╔══██║██║     ██╔══██║
//  ██║     ██║  ██║╚██████╔╝╚██████╗███████╗███████║███████║    ███████╗██║  ██║╚██████╗██║  ██║
//  ╚═╝     ╚═╝  ╚═╝ ╚═════╝  ╚═════╝╚══════╝╚══════╝╚══════╝    ╚══════╝╚═╝  ╚═╝ ╚═════╝╚═╝  ╚═╝
//
//  ██████╗ ███████╗ ██████╗ ██████╗ ██████╗ ██████╗
//  ██╔══██╗██╔════╝██╔════╝██╔═══██╗██╔══██╗██╔══██╗
//  ██████╔╝█████╗  ██║     ██║   ██║██████╔╝██║  ██║
//  ██╔══██╗██╔══╝  ██║     ██║   ██║██╔══██╗██║  ██║
//  ██║  ██║███████╗╚██████╗╚██████╔╝██║  ██║██████╔╝
//  ╚═╝  ╚═╝╚══════╝ ╚═════╝ ╚═════╝ ╚═╝  ╚═╝╚═════╝
//

var _ = require('@sailshq/lodash');
var utils = require('waterline-utils');
var eachRecordDeep = utils.eachRecordDeep;

module.exports = function processEachRecord(options) {
  //  ╦  ╦╔═╗╦  ╦╔╦╗╔═╗╔╦╗╔═╗  ┌─┐┌─┐┌┬┐┬┌─┐┌┐┌┌─┐
  //  ╚╗╔╝╠═╣║  ║ ║║╠═╣ ║ ║╣   │ │├─┘ │ ││ ││││└─┐
  //   ╚╝ ╩ ╩╩═╝╩═╩╝╩ ╩ ╩ ╚═╝  └─┘┴   ┴ ┴└─┘┘└┘└─┘
  if (_.isUndefined(options) || !_.isPlainObject(options)) {
    throw new Error('Invalid options argument. Options must contain: records, identity, and orm.');
  }

  if (!_.has(options, 'records') || !_.isArray(options.records)) {
    throw new Error('Invalid option used in options argument. Missing or invalid records.');
  }

  if (!_.has(options, 'identity') || !_.isString(options.identity)) {
    throw new Error('Invalid option used in options argument. Missing or invalid identity.');
  }

  if (!_.has(options, 'orm') || !_.isPlainObject(options.orm)) {
    throw new Error('Invalid option used in options argument. Missing or invalid orm.');
  }

  // Key the collections by identity instead of column name
  var collections = _.reduce(options.orm.collections, function(memo, val) {
    memo[val.identity] = val;
    return memo;
  }, {});

  options.orm.collections = collections;

  // Run all the records through the iterator so that they can be normalized.
  eachRecordDeep(options.records, function iterator(record, WLModel) {
    // Check if the record and the model contain auto timestamps and make
    // sure that if they are type number that they are actually numbers and
    // not strings.
    _.each(WLModel.definition, function checkAttributes(attrVal) {
      var columnName = attrVal.columnName;

      if (_.has(attrVal, 'autoUpdatedAt') && attrVal.autoUpdatedAt === true && attrVal.type === 'number') {
        if (_.has(record, columnName) && !_.isUndefined(record[columnName])) {
          record[columnName] = Number(record[columnName]);
        }
      }

      if (_.has(attrVal, 'autoCreatedAt') && attrVal.autoCreatedAt === true && attrVal.type === 'number') {
        if (_.has(record, columnName) && !_.isUndefined(record[columnName])) {
          record[columnName] = Number(record[columnName]);
        }
      }
    });
  }, true, options.identity, options.orm);
};
