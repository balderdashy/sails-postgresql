/**
 * Utility Functions
 */

// Dependencies
var _ = require('lodash');

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
 * Escape Name
 *
 * Wraps a name in quotes to allow reserved
 * words as column names such as user.
 */

function escapeName(name, schema) {
  name = '"' + name + '"';
  if (schema) {
    name = '"' + schema + '".' + name;
  }
  return name;
}
utils.escapeName = escapeName;

/**
 * Build a schema from an attributes object
 */
utils.buildSchema = function(obj) {
  var columns = _.map(obj, function (attribute, name) {
    if (_.isString(attribute)) {
      var val = attribute;
      attribute = {};
      attribute.type = val;
    }

    var type = utils.sqlTypeCast(attribute.autoIncrement ? 'SERIAL' : attribute.type);
    var nullable = attribute.notNull && 'NOT NULL';
    var unique = attribute.unique && 'UNIQUE';

    return _.compact([ '"' + name + '"', type, nullable, unique ]).join(' ');
  }).join(',');

  var primaryKeys = _.keys(_.pick(obj, function (attribute) {
    return attribute.primaryKey;
  }));

  var constraints = _.compact([
    primaryKeys.length && 'PRIMARY KEY ("' + primaryKeys.join('","') + '")'
  ]).join(', ');

  return _.compact([ columns, constraints ]).join(', ');
};

/**
 * Build an Index array from any attributes that
 * have an index key set.
 */

utils.buildIndexes = function(obj) {
  var indexes = [];

  // Iterate through the Object keys and pull out any index attributes
  _.each(_.keys(obj), function(key) {
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

  _.each(_.keys(data), function(key) {
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

  // Store Arrays as strings
  if (_.isArray(value)) {
    value = JSON.stringify(value);
  }

  // Store Buffers as hex strings (for BYTEA)
  if (Buffer.isBuffer(value)) {
    value = '\\x' + value.toString('hex');
  }

  return value;
};

/**
 * Normalize a schema for use with Waterline
 */

utils.normalizeSchema = function(schema) {
  var normalized = {};
  var clone = _.clone(schema);

  _.each(clone, function(column) {

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
      console.error('Unregistered type given: ' + type);
      return 'TEXT';
  }
};
