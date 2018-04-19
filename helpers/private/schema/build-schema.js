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
    throw new Error('Build Schema requires a valid definition.');
  }

  //  ╔╗╔╔═╗╦═╗╔╦╗╔═╗╦  ╦╔═╗╔═╗  ┌┬┐┬ ┬┌─┐┌─┐
  //  ║║║║ ║╠╦╝║║║╠═╣║  ║╔═╝║╣    │ └┬┘├─┘├┤
  //  ╝╚╝╚═╝╩╚═╩ ╩╩ ╩╩═╝╩╚═╝╚═╝   ┴  ┴ ┴  └─┘
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

    // Use SERIAL type on auto-increment, but only if type is number. Allows for UUID autoincrement
	  // columnType: "UUID DEFAULT uuid_generate_v4()"  
    var computedType = attribute.autoIncrement && attribute.type === "number" ? 'SERIAL' : attribute.columnType;
    var type = normalizeType(computedType || '');
    var nullable = attribute.notNull && 'NOT NULL';
    var unique = attribute.unique && 'UNIQUE';

    return _.compact(['"' + name + '"', type, nullable, unique]).join(' ');
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
