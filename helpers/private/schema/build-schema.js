//  ██████╗ ██╗   ██╗██╗██╗     ██████╗     ███████╗ ██████╗██╗  ██╗███████╗███╗   ███╗ █████╗
//  ██╔══██╗██║   ██║██║██║     ██╔══██╗    ██╔════╝██╔════╝██║  ██║██╔════╝████╗ ████║██╔══██╗
//  ██████╔╝██║   ██║██║██║     ██║  ██║    ███████╗██║     ███████║█████╗  ██╔████╔██║███████║
//  ██╔══██╗██║   ██║██║██║     ██║  ██║    ╚════██║██║     ██╔══██║██╔══╝  ██║╚██╔╝██║██╔══██║
//  ██████╔╝╚██████╔╝██║███████╗██████╔╝    ███████║╚██████╗██║  ██║███████╗██║ ╚═╝ ██║██║  ██║
//  ╚═════╝  ╚═════╝ ╚═╝╚══════╝╚═════╝     ╚══════╝ ╚═════╝╚═╝  ╚═╝╚══════╝╚═╝     ╚═╝╚═╝  ╚═╝
//
// Build data that is suitable for use in a Create Table query.

var _ = require('@sailshq/lodash');

module.exports = function buildSchema(definition) {
  if (!definition) {
    throw new Error('`buildSchema()` requires a valid definition be passed in, but no argument was provided.');
  }

  //  ╔╗╔╔═╗╦═╗╔╦╗╔═╗╦  ╦╔═╗╔═╗  ┌─┐┌─┐┬  ┬ ┬┌┬┐┌┐┌  ┌┬┐┬ ┬┌─┐┌─┐   ┌─┐┌┬┐┌─┐
  //  ║║║║ ║╠╦╝║║║╠═╣║  ║╔═╝║╣   │  │ ││  │ │││││││   │ └┬┘├─┘├┤    ├┤  │ │
  //  ╝╚╝╚═╝╩╚═╩ ╩╩ ╩╩═╝╩╚═╝╚═╝  └─┘└─┘┴─┘└─┘┴ ┴┘└┘   ┴  ┴ ┴  └─┘┘  └─┘ ┴ └─┘

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
    var computedColumnType = ((attribute.autoIncrement && attribute.columnType === '_number') ? 'SERIAL' : attribute.columnType) || '';

    // Handle default column types from sails-hook-orm.
    switch (computedColumnType.toLowerCase()) {
      case '_number':
        computedColumnType = 'REAL';
      case '_numberkey':
        computedColumnType = 'INTEGER';
      case '_numbertimestamp':
        computedColumnType = 'BIGINT';
      case '_string':
        computedColumnType = 'TEXT';
      case '_stringkey':
        computedColumnType = 'VARCHAR';
      case '_stringtimestamp':
        computedColumnType = 'VARCHAR';
      case '_boolean':
        computedColumnType = 'BOOLEAN';
      case '_json':
        computedColumnType = 'JSON';
      case '_ref':
        computedColumnType = 'TEXT';
    }

    // Mix in other auto-migration directives to get the PostgreSQL-ready SQL
    // we'll use to declare this column's physical data type.
    var nullable = attribute.notNull && 'NOT NULL';
    var unique = attribute.unique && 'UNIQUE';
    return _.compact(['"' + name + '"', computedColumnType, nullable, unique]).join(' ');
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
