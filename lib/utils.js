/**
 * Utility Functions
 */

// Dependencies
var _ = require('underscore');

// Module Exports

var utils = module.exports = {};

/**
 * Marshall a Config Object into a PG connection object
 */

utils.marshalConfig = function(config) {
  return _.extend(config, {
    host: config.host,
    user: config.user,
    password: config.password,
    database: config.database,
    port: config.port
  });
};

/**
 * Escape Table Name
 *
 * Wraps a table name in quotes to allow reserved
 * words as table names such as user.
 */

utils.escapeTable = function(table) {
  return '"' + table + '"';
};

/**
 * Build a schema from an attributes object
 */

utils.buildSchema = function(obj) {
  var schema = "";

  // Iterate through the Object Keys and build a string
  Object.keys(obj).forEach(function(key) {
    var attr = {};

    // Normalize Simple Key/Value attribute
    // ex: name: 'string'
    if(typeof obj[key] === 'string') {
      attr.type = obj[key];
    }

    // Set the attribute values to the object key
    else {
      attr = obj[key];
    }

    // Override Type for autoIncrement
    if(attr.autoIncrement) attr.type = 'serial';

    var str = [
      '"' + key + '"',                        // attribute name
      utils.sqlTypeCast(attr.type),           // attribute type
      attr.primaryKey ? 'PRIMARY KEY' : '',   // primary key
      attr.unique ? 'UNIQUE' : ''             // unique constraint
    ].join(' ').trim();

    schema += str + ', ';
  });

  // Remove trailing seperator/trim
  return schema.slice(0, -2);
};

/**
 * Build an Index array from any attributes that
 * have an index key set.
 */

utils.buildIndexes = function(obj) {
  var indexes = [];

  // Iterate through the Object keys and pull out any index attributes
  Object.keys(obj).forEach(function(key) {
    if(obj[key].hasOwnProperty('index')) {
      indexes.push(key);
    }
  });

  return indexes;
};


/**
 * Map Attributes
 *
 * Takes a js object and creates arrays used for parameterized
 * queries in postgres.
 */

utils.mapAttributes = function(data) {
  var keys = [],   // Column Names
      values = [], // Column Values
      params = [], // Param Index, ex: $1, $2
      i = 1;

  Object.keys(data).forEach(function(key) {
    keys.push('"' + key + '"');
    values.push(utils.prepareValue(data[key]));
    params.push('$' + i);
    i++;
  });

  return({ keys: keys, values: values, params: params });
};

/**
 * Prepare values
 *
 * Transform a JS date to SQL date and functions
 * to strings.
 */

utils.prepareValue = function(value) {

  // Cast dates to SQL
  if (_.isDate(value)) {
    value = utils.toSqlDate(value);
  }

  // Cast functions to strings
  if (_.isFunction(value)) {
    value = value.toString();
  }

  return value;
};

/**
 * Normalize a schema for use with Waterline
 */

utils.normalizeSchema = function(schema) {
  var normalized = {};

  schema.forEach(function(column) {

    // Set Type
    normalized[column.Column] = {
      type: column.Type
    };

    // Check for Primary Key
    if(column.Constraint && column.C === 'p') {
      normalized[column.Column].primaryKey = true;
    }

    // Check for Unique Constraint
    if(column.Constraint && column.C === 'u') {
      normalized[column.Column].unique = true;
    }

    // Check for autoIncrement
    if(column.autoIncrement) {
      normalized[column.Column].autoIncrement = column.autoIncrement;
    }

    // Check for index
    if(column.indexed) {
      normalized[column.Column].indexed = column.indexed;
    }

  });

  return normalized;
};

/**
 * JS Date to UTC Timestamp
 *
 * Dates should be stored in Postgres with UTC timestamps
 * and then converted to local time on the client.
 */

utils.toSqlDate = function(date) {
  return date.toUTCString();
};

/**
 * Cast waterline types to Postgresql data types
 */

utils.sqlTypeCast = function(type) {
  switch(type.toLowerCase()) {
    case 'serial':
      return 'SERIAL';

    case 'string':
    case 'text':
      return 'TEXT';

    case 'boolean':
    case 'int':
    case 'integer':
      return 'INT';

    case 'float':
    case 'double':
      return 'FLOAT';

    // Store all dates as timestamps with the time zone
    case 'date':
    case 'datestamp':
      return 'TIMESTAMP WITH TIME ZONE';

    default:
      console.error("Unregistered type given: " + type);
      return "TEXT";
  }
};
