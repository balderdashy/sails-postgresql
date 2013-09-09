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

  // Store Arrays as strings
  if (Array.isArray(value)) {
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

  clone.forEach(function(column) {

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
      return 'BOOLEAN';

    case 'int':
    case 'integer':
      return 'INT';

    case 'float':
    case 'double':
      return 'FLOAT';

    // Store all dates as timestamps with the time zone
    case 'date':
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
      console.error("Unregistered type given: " + type);
      return "TEXT";
  }
};


/**
 * Builds a Select statement determining if Aggeragate options are needed.
 */

utils.buildSelectStatement = function(criteria, table, attributes, schema) {

  var query = '';

  if(criteria.groupBy || criteria.sum || criteria.average || criteria.min || criteria.max) {
    query = 'SELECT ';

    // Append groupBy columns to select statement
    if(criteria.groupBy) {
      if(criteria.groupBy instanceof Array) {
        criteria.groupBy.forEach(function(opt){
          query += opt + ', ';
        });

      } else {
        query += criteria.groupBy + ', ';
      }
    }

    // Handle SUM
    if (criteria.sum) {
      if(criteria.sum instanceof Array) {
        criteria.sum.forEach(function(opt){
          query += 'CAST(SUM(' + opt + ') AS float) AS ' + opt + ', ';
        });

      } else {
        query += 'CAST(SUM(' + criteria.sum + ') AS float) AS ' + criteria.sum + ', ';
      }
    }

    // Handle AVG (casting to float to fix percision with trailing zeros)
    if (criteria.average) {
      if(criteria.average instanceof Array) {
        criteria.average.forEach(function(opt){
          query += 'CAST(AVG(' + opt + ') AS float) AS ' + opt + ', ';
        });

      } else {
        query += 'CAST(AVG(' + criteria.average + ') AS float) AS ' + criteria.average + ', ';
      }
    }

    // Handle MAX
    if (criteria.max) {
      if(criteria.max instanceof Array) {
        criteria.max.forEach(function(opt){
          query += 'MAX(' + opt + ') AS ' + opt + ', ';
        });

      } else {
        query += 'MAX(' + criteria.max + ') AS ' + criteria.max + ', ';
      }
    }

    // Handle MIN
    if (criteria.min) {
      if(criteria.min instanceof Array) {
        criteria.min.forEach(function(opt){
          query += 'MIN(' + opt + ') AS ' + opt + ', ';
        });

      } else {
        query += 'MIN(' + criteria.min + ') AS ' + criteria.min + ', ';
      }
    }

    // trim trailing comma
    query = query.slice(0, -2) + ' ';

    // Add FROM clause
    return query += 'FROM ' + utils.escapeTable(table) + ' ';
  }

  /**
   * If no aggregate options lets just build a normal query
   */


  // Add all keys to the select statement for this table
  query += 'SELECT ';

  var selectKeys = [],
      joinSelectKeys = [];

  Object.keys(schema[table].schema).forEach(function(key) {
    selectKeys.push({ table: table, key: key });
  });

  // Check for joins
  if(criteria.joins || criteria.join) {

    var joins = criteria.joins || criteria.join;

    joins.forEach(function(join) {
      if(!join.select) return;

      Object.keys(schema[join.child.toLowerCase()].schema).forEach(function(key) {
        joinSelectKeys.push({ table: join.child.toLowerCase(), key: key });
      });

      // Remove the foreign key for this join from the selectKeys array
      selectKeys = selectKeys.filter(function(select) {
        var keep = true;
        if(select.key === join.parentKey && join.removeParentKey) keep = false;
        return keep;
      });
    });
  }

  selectKeys.forEach(function(select) {
    query += utils.escapeTable(select.table) + '.' + utils.escapeTable(select.key) + ', ';
  });

  joinSelectKeys.forEach(function(select) {
    query += utils.escapeTable(select.table) + '.' + utils.escapeTable(select.key) + ' AS ' +
          utils.escapeTable(select.table + '__' + select.key) + ', ';
  });

  // Remove the last comma
  query = query.slice(0, -2) + ' FROM ' + utils.escapeTable(table) + ' ';

  return query;
};
};
