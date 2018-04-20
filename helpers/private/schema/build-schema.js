//  ██████╗ ██╗   ██╗██╗██╗     ██████╗     ███████╗ ██████╗██╗  ██╗███████╗███╗   ███╗ █████╗
//  ██╔══██╗██║   ██║██║██║     ██╔══██╗    ██╔════╝██╔════╝██║  ██║██╔════╝████╗ ████║██╔══██╗
//  ██████╔╝██║   ██║██║██║     ██║  ██║    ███████╗██║     ███████║█████╗  ██╔████╔██║███████║
//  ██╔══██╗██║   ██║██║██║     ██║  ██║    ╚════██║██║     ██╔══██║██╔══╝  ██║╚██╔╝██║██╔══██║
//  ██████╔╝╚██████╔╝██║███████╗██████╔╝    ███████║╚██████╗██║  ██║███████╗██║ ╚═╝ ██║██║  ██║
//  ╚═════╝  ╚═════╝ ╚═╝╚══════╝╚═════╝     ╚══════╝ ╚═════╝╚═╝  ╚═╝╚══════╝╚═╝     ╚═╝╚═╝  ╚═╝
//
// Build a schema object that is suitable for using in a Create Table query.

var _ = require('@sailshq/lodash');

module.exports = function buildSchema(definition) {
  if (!definition) {
    throw new Error('`buildSchema()` requires a valid definition be passed in, but no argument was provided.');
  }

  //  ╔╗╔╔═╗╦═╗╔╦╗╔═╗╦  ╦╔═╗╔═╗  ┌─┐┌─┐┬  ┬ ┬┌┬┐┌┐┌  ┌┬┐┬ ┬┌─┐┌─┐
  //  ║║║║ ║╠╦╝║║║╠═╣║  ║╔═╝║╣   │  │ ││  │ │││││││   │ └┬┘├─┘├┤
  //  ╝╚╝╚═╝╩╚═╩ ╩╩ ╩╩═╝╩╚═╝╚═╝  └─┘└─┘┴─┘└─┘┴ ┴┘└┘   ┴  ┴ ┴  └─┘
  var normalizeType = function normalizeType(type) {
    switch (type.toLowerCase()) {

      // Default types from sails-hook-orm.
      case '_number':
        return 'REAL';
      case '_numberkey':
        return 'INTEGER';
      case '_numbertimestamp':
        return 'BIGINT';
      case '_string':
        return 'TEXT';
      case '_stringkey':
        return 'VARCHAR';
      case '_stringtimestamp':
        return 'VARCHAR';
      case '_boolean':
        return 'BOOLEAN';
      case '_json':
        return 'JSON';
      case '_ref':
        return 'TEXT';

      default:
        return type;
    }
  };

  // Build up a string of column attributes
  var columns = _.map(definition, function map(attribute, name) {
    if (_.isString(attribute)) {
      var val = attribute;
      attribute = {};
      attribute.type = val;
    }

    // Note:  For auto-increment columns, in the general case (i.e. when logical
    // type is number + no specific physical column type set), always use SERIAL
    // as the columnType.  Otherwise, usethe specific column type that was set.
    // For example, this allows for UUID autoincrement:
	  //     columnType: 'UUID DEFAULT uuid_generate_v4()'
    var computedColumnType = attribute.autoIncrement && attribute.columnType === '_number' ? 'SERIAL' : attribute.columnType;
    var columnType = normalizeType(computedColumnType || '');
    var nullable = attribute.notNull && 'NOT NULL';
    var unique = attribute.unique && 'UNIQUE';

    return _.compact(['"' + name + '"', columnType, nullable, unique]).join(' ');
  }).join(',');

  // Grab the Primary Key
  var primaryKeys = _.keys(_.pick(definition, function findPK(attribute) {
    return attribute.primaryKey;
  }));

  // Add the Primary Key to the definition
  var constraints = _.compact([
    primaryKeys.length && 'PRIMARY KEY ("' + primaryKeys.join('","') + '")'
  ]).join(', ');

  var schema = _.compact([columns, constraints]).join(', ');

  return schema;
};
