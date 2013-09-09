/**
 * Dependencies
 */

var _ = require('underscore'),
    utils = require('./utils');

/**
 * Query Builder for creating parameterized queries for use
 * with the Postgres adapter
 *
 * This is nasty. Hacked together while figuring out the options
 * that get passed to the various finders in waterline.
 *
 * It should get cleaned up soon and some better test coverage.
 */

var Query = function(schema, tableDefs) {
  this._values = [];
  this._paramCount = 1;
  this._query = '';
  this._tableDefs = tableDefs || {};

  this._schema = _.clone(schema);

  return this;
};

/**
 * SELECT Statement
 */

Query.prototype.find = function(table, criteria) {

  this._query = utils.buildSelectStatement(criteria, table, this._schema, this._tableDefs);
  if(criteria) this._build(table, criteria);

  return {
    query: this._query,
    values: this._values
  };
};

/**
 * UPDATE Statement
 */

Query.prototype.update = function(table, criteria, data) {
  this._query = 'UPDATE ' + utils.escapeTable(table) + ' ';

  // Transform the Data object into arrays used in a parameterized query
  var attributes = utils.mapAttributes(data);

  // Update the paramCount
  this._paramCount = attributes.params.length + 1;

  // Build SET string
  var str = '';
  for(var i=0; i < attributes.keys.length; i++) {
    str += attributes.keys[i] + ' = ' + attributes.params[i] + ', ';
  }

  // Remove trailing comma
  str = str.slice(0, -2);

  this._query += 'SET ' + str + ' ';

  // Add data values to this._values
  this._values = this._values.concat(attributes.values);

  // Build Criteria clause
  if(criteria) this._build(table, criteria);

  // Add RETURNING clause
  this._query += ' RETURNING *';

  return {
    query: this._query,
    values: this._values
  };
};

/**
 * DELETE Statement
 */

Query.prototype.destroy = function(table, criteria) {
  this._query = 'DELETE FROM ' + utils.escapeTable(table) + ' ';
  if(criteria) this._build(table, criteria);

  // Add RETURNING clause
  this._query += ' RETURNING *';

  return {
    query: this._query,
    values: this._values
  };
};

/**
 * String Builder
 */

Query.prototype._build = function(table, criteria) {
  var self = this;

  // Normalize joins key, allows use of both join and joins
  if (criteria.join) criteria.joins = criteria.join;

  // Ensure criteria keys are in correct order
  var orderedCriteria = {};
  if (criteria.joins) orderedCriteria.joins = criteria.joins;
  if (criteria.where) orderedCriteria.where = criteria.where;
  if (criteria.limit && !criteria.joins) orderedCriteria.limit = criteria.limit;
  if (criteria.skip) orderedCriteria.skip = criteria.skip;
  if (criteria.sort) orderedCriteria.sort = criteria.sort;
  if (criteria.groupBy) orderedCriteria.groupBy = criteria.groupBy;

  // Loop through criteria parent keys
  Object.keys(orderedCriteria).forEach(function(key) {

    switch(key.toLowerCase()) {
      case 'joins':
        self.joins(criteria[key]);
        return;

      case 'where':
        self.where(table, criteria[key]);
        return;

      case 'limit':
        self.limit(criteria[key]);
        return;

      case 'skip':
        self.skip(criteria[key]);
        return;

      case 'sort':
        self.sort(criteria[key]);
        return;

      case 'groupby':
        self.group(criteria[key]);
        return;

    }
  });

  return {
    query: this._query,
    values: this._values
  };
};

/**
 * Specify association joins
 *
 */

Query.prototype.joins = function(joins) {
  var queryPart = '';

  if (joins.length === 0) return;

  joins.forEach(function(join) {
    var tableName = join.child.toLowerCase(),
        foreignKey,
        parentKey;

    // Escape Table Name
    tableName = utils.escapeTable(tableName);

    // Build Full Key Names
    foreignKey = tableName + '.' + utils.escapeTable(join.childKey);
    parentKey = utils.escapeTable(join.parent) + '.' + utils.escapeTable(join.parentKey);

    // Build Join Clause
    queryPart += 'LEFT JOIN ' + tableName + ' AS ' + tableName + ' ON ' +
      parentKey + ' = ' + foreignKey + ' ';
  });

  this._query += queryPart;
};


/**
 * Specifiy a `where` condition
 *
 * `Where` conditions may use key/value model attributes for simple query
 * look ups as well as more complex conditions.
 *
 * The following conditions are supported along with simple criteria:
 *
 *   Conditions:
 *     [And, Or, Like, Not]
 *
 *   Criteria Operators:
 *     [<, <=, >, >=, !]
 *
 *   Criteria Helpers:
 *     [lessThan, lessThanOrEqual, greaterThan, greaterThanOrEqual, not, like, contains, startsWith, endsWith]
 *
 * ####Example
 *
 *   where: {
 *     name: 'foo',
 *     age: {
 *       '>': 25
 *     },
 *     like: {
 *       name: '%foo%'
 *     },
 *     or: [
 *       { like: { foo: '%foo%' } },
 *       { like: { bar: '%bar%' } }
 *     ],
 *     name: [ 'foo', 'bar;, 'baz' ],
 *     age: {
 *       not: 40
 *     }
 *   }
 */

Query.prototype.where = function(table, options) {
  var self = this,
      operators = this.operators();

  if(!options) return;

  // Begin WHERE query
  this._query += 'WHERE ';

  // Process `where` criteria
  Object.keys(options).forEach(function(key) {
    switch(key.toLowerCase()) {

      case 'or':

        options[key].forEach(function(statement) {
          Object.keys(statement).forEach(function(key) {

            switch(key) {
              case 'and':
                Object.keys(statement[key]).forEach(function(attribute) {
                  operators.and(table, attribute, statement[key][attribute], ' OR ');
                });
                return;

              case 'like':
                Object.keys(statement[key]).forEach(function(attribute) {
                  operators.like(table, attribute, key, statement, ' OR ');
                });
                return;

              default:
                if(typeof statement[key] === 'object') {
                  Object.keys(statement[key]).forEach(function(attribute) {
                    operators.and(table, attribute, statement[key][attribute], ' OR ');
                  });
                  return;
                }

                operators.and(table, key, statement[key], ' OR ');
                return;
            }

          });
        });

        return;

      case 'like':
        Object.keys(options[key]).forEach(function(parent) {
          operators.like(table, parent, key, options);
        });

        return;

      // Key/Value
      default:

        // `IN`
        if(options[key] instanceof Array) {
          operators.in(table, key, options[key]);
          return;
        }

        // `AND`
        operators.and(table, key, options[key]);
        return;
    }

  });

  // Remove trailing AND if it exists
  if(this._query.slice(-4) === 'AND ') {
    this._query = this._query.slice(0, -5);
  }

  // Remove trailing OR if it exists
  if(this._query.slice(-3) === 'OR ') {
    this._query = this._query.slice(0, -4);
  }
};

/**
 * Operator Functions
 */

Query.prototype.operators = function() {
  var self = this;

  var sql = {
    and: function(table, key, options, comparator) {
      var caseSensitive = true;

      // Check if key is a string
      if(self._schema[key].type === 'text') caseSensitive = false;

      processCriteria.call(self, table, key, options, '=', caseSensitive);
      self._query += (comparator || ' AND ');
    },

    like: function(table, parent, key, options, comparator) {
      var caseSensitive = true;

      // Check if parent is a string
      if(self._schema[parent].type === 'text') caseSensitive = false;

      processCriteria.call(self, table, parent, options[key][parent], 'ILIKE', caseSensitive);
      self._query += (comparator || ' AND ');
    },

    in: function(table, key, options) {

      // Set case sensitive by default
      var caseSensitivity = true;

      // Check if key is a string
      if(self._schema[key].type === 'text') caseSensitivity = false;

      // Check case sensitivity to decide if LOWER logic is used
      if(!caseSensitivity) {
        key = 'LOWER(' + utils.escapeTable(table) + '.' + utils.escapeTable(key) + ')';
        self._query += key + ' IN (';
      } else {
        self._query += utils.escapeTable(table) + '.' + utils.escapeTable(key) + ' IN (';
      }

      // Append each value to query
      options.forEach(function(value) {
        self._query += '$' + self._paramCount + ', ';
        self._paramCount++;

        // If case sensitivity if off lowercase the value
        if(!caseSensitivity) value = value.toLowerCase();

        self._values.push(value);
      });

      // Strip last comma and close criteria
      self._query = self._query.slice(0, -2) + ')';
      self._query += ' AND ';
    }
  };

  return sql;
};

/**
 * Process Criteria
 *
 * Processes a query criteria object
 */

function processCriteria(table, parent, value, combinator, caseSensitive) {
  var self = this;

  // Complex Object Attributes
  if(typeof value === 'object' && value !== null) {
    var keys = Object.keys(value);

    for(var i=0; i < keys.length; i++) {

      // Check if value is a string and if so add LOWER logic
      // to work with case in-sensitive queries
      if(!caseSensitive && typeof value[[keys][i]] === 'string') {
        parent = 'LOWER(' + utils.escapeTable(table) + '.' + utils.escapeTable(parent) + ')';
        value[keys][i] = value[keys][i].toLowerCase();
      }

      self._query += parent + ' ';
      prepareCriterion.call(self, keys[i], value[keys[i]]);

      if(i+1 < keys.length) self._query += ' AND ';
    }

    return;
  }

  // Check if value is a string and if so add LOWER logic
  // to work with case in-sensitive queries
  if(!caseSensitive && typeof value === 'string') {

    // ADD LOWER to parent
    parent = 'LOWER(' + utils.escapeTable(table) + '.' + utils.escapeTable(parent) + ')';
    value = value.toLowerCase();

  } else {
    // Escape parent
    parent = utils.escapeTable(table) + '.' + utils.escapeTable(parent);
  }

  if(value !== null) {

    // Simple Key/Value attributes
    this._query += parent + ' ' + combinator + ' $' + this._paramCount;

    this._values.push(value);
    this._paramCount++;
  }

  else {
    this._query += parent + ' IS NULL';
  }
}

/**
 * Prepare Criterion
 *
 * Processes comparators in a query.
 */

function prepareCriterion(key, value) {
  var str;

  switch(key) {

    case '<':
    case 'lessThan':
      this._values.push(value);
      str = '< ' + '$' + this._paramCount;
      break;

    case '<=':
    case 'lessThanOrEqual':
      this._values.push(value);
      str = '<= ' + '$' + this._paramCount;
      break;

    case '>':
    case 'greaterThan':
      this._values.push(value);
      str = '> ' + '$' + this._paramCount;
      break;

    case '>=':
    case 'greaterThanOrEqual':
      this._values.push(value);
      str = '>= ' + '$' + this._paramCount;
      break;

    case '!':
    case 'not':
      if(value === null) {
        str = 'IS NOT NULL';
      }
      else {
        this._values.push(value);
        str = '<> ' + '$' + this._paramCount;
      }
      break;

    case 'like':
      this._values.push(value);
      str = 'ILIKE ' + '$' + this._paramCount;
      break;

    case 'contains':
      this._values.push('%' + value + '%');
      str = 'ILIKE ' + '$' + this._paramCount;
      break;

    case 'startsWith':
      this._values.push(value + '%');
      str = 'ILIKE ' + '$' + this._paramCount;
      break;

    case 'endsWith':
      this._values.push('%' + value);
      str = 'ILIKE ' + '$' + this._paramCount;
      break;

    default:
      throw new Error('Unknown comparator: ' + key);
  }

  // Bump paramCount
  this._paramCount++;

  // Add str to query
  this._query += str;
}

/**
 * Specify a `limit` condition
 */

Query.prototype.limit = function(options) {
  this._query += ' LIMIT ' + options;
};

/**
 * Specify a `skip` condition
 */

Query.prototype.skip = function(options) {
  this._query += ' OFFSET ' + options;
};

/**
 * Specify a `sort` condition
 */

Query.prototype.sort = function(options) {
  var self = this;

  this._query += ' ORDER BY ';

  Object.keys(options).forEach(function(key) {
    var direction = options[key] === 1 ? 'ASC' : 'DESC';
    self._query += '"' + key + '" ' + direction + ', ';
  });

  // Remove trailing comma
  this._query = this._query.slice(0, -2);
};

/**
 * Specify a `group by` condition
 */

Query.prototype.group = function(options) {
  var self = this;

  this._query += ' GROUP BY ';

  // Normalize to array
  if(!Array.isArray(options)) options = [options];

  options.forEach(function(key) {
    self._query += key + ', ';
  });

  // Remove trailing comma
  this._query = this._query.slice(0, -2);
};

/**
 * Cast special values to proper types.
 *
 * Ex: Array is stored as "[0,1,2,3]" and should be cast to proper
 * array for return values.
 */

Query.prototype.cast = function(values) {
  var self = this;
  var _values = _.clone(values);

  Object.keys(values).forEach(function(key) {
    self.castValue(key, _values[key], _values, self._schema);
  });

  return _values;
};

/**
 * Cast a value
 *
 * @param {String} key
 * @param {Object|String|Integer|Array} value
 * @param {Object} schema
 * @api private
 */

Query.prototype.castValue = function(key, value, attributes, schema, joinKey) {

  // Check if key is a special "join" key, identified with a '__' split
  var attr = key.split('__');
  if(attr.length === 2) {

    // Find schema
    if(this._tableDefs) {
      var joinSchema = this._tableDefs[attr[0]];
      if(joinSchema) return this.castValue(attr[1], value, attributes, joinSchema.definition, key);
    }
  }

  // Lookup Schema "Type"
  var type = schema[key].type;
  if(!type) return;

  // Attempt to parse Array
  if(type === 'array') {
    try {
      if(joinKey) attributes[joinKey] = JSON.parse(value);
      else attributes[key] = JSON.parse(value);
    } catch(e) {
      return;
    }
  }
};


module.exports = Query;
