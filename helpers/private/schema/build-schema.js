//  ██████╗ ██╗   ██╗██╗██╗     ██████╗     ███████╗ ██████╗██╗  ██╗███████╗███╗   ███╗ █████╗
//  ██╔══██╗██║   ██║██║██║     ██╔══██╗    ██╔════╝██╔════╝██║  ██║██╔════╝████╗ ████║██╔══██╗
//  ██████╔╝██║   ██║██║██║     ██║  ██║    ███████╗██║     ███████║█████╗  ██╔████╔██║███████║
//  ██╔══██╗██║   ██║██║██║     ██║  ██║    ╚════██║██║     ██╔══██║██╔══╝  ██║╚██╔╝██║██╔══██║
//  ██████╔╝╚██████╔╝██║███████╗██████╔╝    ███████║╚██████╗██║  ██║███████╗██║ ╚═╝ ██║██║  ██║
//  ╚═════╝  ╚═════╝ ╚═╝╚══════╝╚═════╝     ╚══════╝ ╚═════╝╚═╝  ╚═╝╚══════╝╚═╝     ╚═╝╚═╝  ╚═╝
//
// Build data that is suitable for use in a Create Table query.

var util = require('util');
var _ = require('@sailshq/lodash');

module.exports = function buildSchema(definition) {
  if (!definition) {
    throw new Error('`buildSchema()` requires a valid definition be passed in, but no argument was provided.');
  }

  //  ╔╗╔╔═╗╦═╗╔╦╗╔═╗╦  ╦╔═╗╔═╗  ┌─┐┌─┐┬  ┬ ┬┌┬┐┌┐┌  ┌┬┐┬ ┬┌─┐┌─┐   ┌─┐┌┬┐┌─┐
  //  ║║║║ ║╠╦╝║║║╠═╣║  ║╔═╝║╣   │  │ ││  │ │││││││   │ └┬┘├─┘├┤    ├┤  │ │
  //  ╝╚╝╚═╝╩╚═╩ ╩╩ ╩╩═╝╩╚═╝╚═╝  └─┘└─┘┴─┘└─┘┴ ┴┘└┘   ┴  ┴ ┴  └─┘┘  └─┘ ┴ └─┘

  // Build up a string of column attributes
  var columns = _.map(definition, function map(attribute, columnName) {
    // - - - - - - - - - - - - - - - - - - - - -
    // No longer relevant:
    // ```
    // if (_.isString(attribute)) {
    //   var val = attribute;
    //   attribute = {};
    //   attribute.type = val;
    // }
    // ```
    // ^^TODO: Remove this completely
    // - - - - - - - - - - - - - - - - - - - - -
    if (!_.isObject(attribute) || _.isArray(attribute) || _.isFunction(attribute)) {
      throw new Error('Invalid attribute ("'+columnName+'") in DDL definition in `build-schema` utility: '+util.inspect(attribute, {depth:5}));
    }//•

    if (!_.isString(attribute.columnType) || attribute.columnType === '') {
      throw new Error('Invalid column type `'+util.inspect(attribute.columnType,{depth:5})+'` for attribute ("'+columnName+'") in DDL definition in `build-schema` utility: '+util.inspect(attribute, {depth:5}));
    }//•

    // Handle default column types from sails-hook-orm.
    //
    // Note:  For auto-increment columns, in the general case where we're not
    // using a specific columnType (e.g. when logical type is 'number' + no
    // specific `columnType` set), always use SERIAL as the columnType.
    // Otherwise, use the specific column type that was set. This allows for
    // all kinds of wacky stuff.  For example, this could be implemented using
    // UUIDs as a pseudo-"autoincrement" using the following hack:
	  //     columnType: 'UUID DEFAULT uuid_generate_v4()'
    //
    // > Side note: These are all of PostgreSQL's numeric column types:
    // > https://www.postgresql.org/docs/9.5/static/datatype-numeric.html
    var computedColumnType;
    switch (attribute.columnType.toLowerCase()) {
      // Default `columnType` (automigrate):
      case '_number':          computedColumnType = (attribute.autoIncrement ? 'SERIAL' : 'REAL'); break;
      case '_numberkey':       computedColumnType = (attribute.autoIncrement ? 'SERIAL' : 'INTEGER'); break;
      case '_numbertimestamp': computedColumnType = (attribute.autoIncrement ? 'BIGSERIAL' : 'BIGINT'); break;
      case '_string':          computedColumnType = 'TEXT'; break;
      case '_stringkey':       computedColumnType = 'VARCHAR'; break;
      case '_stringtimestamp': computedColumnType = 'VARCHAR'; break;
      case '_boolean':         computedColumnType = 'BOOLEAN'; break;
      case '_json':            computedColumnType = 'JSON'; break;
      case '_ref':             computedColumnType = 'TEXT'; break;

      // Custom `columnType`:
      default:                 computedColumnType = attribute.columnType; break;
    }

    // If auto-incrementing, and our normalized column type doesn't contain what
    // looks to be a valid auto-incrementing PostgreSQL type (e.g. "SERIAL"),
    // then freak out with a reasonable error message.
    if (attribute.autoIncrement && computedColumnType.match(/^(SMALLINT|INTEGER|BIGINT|DECIMAL|NUMERIC|REAL|DOUBLE\sPRECISION)$/i)) {
      throw new Error('Incompatible `columnType` for auto-incrementing column ("'+columnName+'").  Expecting `columnType` to be left undefined, or to be set explicitly to SERIAL, BIGSERIAL, or SMALLSERIAL.  But instead got a different numeric PostgreSQL column type, "'+attribute.columnType+'", which unfortunately does not support auto-increment.  To resolve this, please remove this explicit `columnType`, or set it to an auto-increment-compatible PostgreSQL column type.');
    }//•

    // Mix in other auto-migration directives to get the PostgreSQL-ready SQL
    // we'll use to declare this column's physical data type.
    return [
      '"'+columnName+'"',
      computedColumnType || '',
      attribute.notNull ? 'NOT NULL' : '',
      attribute.unique ? 'UNIQUE' : ''
    ].join(' ');

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
