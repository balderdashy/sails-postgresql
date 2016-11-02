//  ██████╗ ██╗   ██╗██╗██╗     ██████╗     ███████╗ ██████╗██╗  ██╗███████╗███╗   ███╗ █████╗
//  ██╔══██╗██║   ██║██║██║     ██╔══██╗    ██╔════╝██╔════╝██║  ██║██╔════╝████╗ ████║██╔══██╗
//  ██████╔╝██║   ██║██║██║     ██║  ██║    ███████╗██║     ███████║█████╗  ██╔████╔██║███████║
//  ██╔══██╗██║   ██║██║██║     ██║  ██║    ╚════██║██║     ██╔══██║██╔══╝  ██║╚██╔╝██║██╔══██║
//  ██████╔╝╚██████╔╝██║███████╗██████╔╝    ███████║╚██████╗██║  ██║███████╗██║ ╚═╝ ██║██║  ██║
//  ╚═════╝  ╚═════╝ ╚═╝╚══════╝╚═════╝     ╚══════╝ ╚═════╝╚═╝  ╚═╝╚══════╝╚═╝     ╚═╝╚═╝  ╚═╝
//
// Build a schema object that is suitable for using in a Create Table query.

var _ = require('lodash');

module.exports = function buildSchema(definition) {
  if (!definition) {
    throw new Error('Build Schema requires a valid definition.');
  }

  //  ╔╗╔╔═╗╦═╗╔╦╗╔═╗╦  ╦╔═╗╔═╗  ┌┬┐┬ ┬┌─┐┌─┐
  //  ║║║║ ║╠╦╝║║║╠═╣║  ║╔═╝║╣    │ └┬┘├─┘├┤
  //  ╝╚╝╚═╝╩╚═╩ ╩╩ ╩╩═╝╩╚═╝╚═╝   ┴  ┴ ┴  └─┘
  var normalizeType = function normalizeType(type) {
    switch (type.toLowerCase()) {
      case 'serial':
        return 'SERIAL';

      case 'smallserial':
        return 'SMALLSERIAL';

      case 'bigserial':
        return 'BIGSERIAL';

      case 'string':
      case 'text':
      case 'mediumtext':
      case 'longtext':
        return 'TEXT';

      case 'boolean':
        return 'BOOLEAN';

      case 'int':
      case 'integer':
        return 'INT';

      case 'smallint':
        return 'SMALLINT';

      case 'bigint':
        return 'BIGINT';

      case 'real':
      case 'float':
        return 'REAL';

      case 'double':
        return 'DOUBLE PRECISION';

      case 'decimal':
        return 'DECIMAL';

      // Store all time with the time zone
      case 'time':
        return 'TIME WITH TIME ZONE';

      // Store all dates as timestamps with the time zone
      case 'date':
        return 'DATE';
      case 'datestamp':
      case 'datetime':
        return 'TIMESTAMP WITH TIME ZONE';

      case 'array':
        return 'TEXT';

      case 'json':
        return 'JSON';

      case 'binary':
      case 'bytea':
        return 'BYTEA';

      default:
        return 'TEXT';
    }
  };

  // Build up a string of column attributes
  var columns = _.map(definition, function map(attribute, name) {
    if (_.isString(attribute)) {
      var val = attribute;
      attribute = {};
      attribute.type = val;
    }

    var type = normalizeType(attribute.autoIncrement ? 'SERIAL' : attribute.type);
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
