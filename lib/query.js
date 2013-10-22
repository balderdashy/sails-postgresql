/**
 * Dependencies
 */

var _ = require('lodash'),
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

  // Normalize joins key, allows use of both join and joins
  if (criteria.join) {
    criteria.joins = _.cloneDeep(criteria.join);
    delete criteria.join;
  }

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

  // Ensure criteria keys are in correct order
  var orderedCriteria = {};
  if (criteria.joins) orderedCriteria.joins = criteria.joins;
  if (criteria.where) orderedCriteria.where = criteria.where;
  if (criteria.sort) orderedCriteria.sort = criteria.sort;
  if (criteria.groupBy) orderedCriteria.groupBy = criteria.groupBy;
  if (criteria.limit) orderedCriteria.limit = criteria.limit;
  if (criteria.skip) orderedCriteria.skip = criteria.skip;

  // If joins are used, a subquery is needed to ensure that limit and skip work correctly
  if (criteria.joins) {
    this.buildSubQuery(table, orderedCriteria);
    return {
      query: this._query,
      values: this._values
    };
  }

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
        self.sort(table, criteria[key]);
        return;

      case 'groupby':
        self.group(table, criteria[key]);
        return;

    }
  });

  return {
    query: this._query,
    values: this._values
  };
};

/**
 * Build Subquery
 *
 * Builds up a subquery to find all the matching parent ID's then do an IN query with the joins
 * to ensure limit and offset work correctly on the parent records. When doing a `populate` in
 * Waterline, the expected behaviour is that the modifiers will be used on the matching parent
 * records and that the populate will return all associated records for the matched parents.
 *
 * @param {String} tableName
 * @param {Object} criteria
 * @api private
 */

Query.prototype.buildSubQuery = function buildSubQuery(table, criteria) {
  var self = this,
      tableName,
      pk,
      namedPk;

  // Escape the tableName so reserved words such as "user" can be used
  tableName = utils.escapeTable(table);

  // Find the primary key for the table
  Object.keys(this._schema).forEach(function(key) {
    if(self._schema[key].hasOwnProperty('primaryKey')) pk = key;
  });

  // Combine the tableName with the primary key
  namedPk = tableName + '.' + utils.escapeTable(pk);

  // Build up the Joins needed for the query
  if(criteria.hasOwnProperty('joins')) this.joins(criteria.joins);

  // Build up the subquery
  this._query += 'WHERE ' + namedPk + ' IN (SELECT ' + namedPk + ' FROM ' + tableName + ' ';

  if(criteria.hasOwnProperty('where')) this.where(table, criteria.where);
  if(criteria.hasOwnProperty('limit')) this.limit(criteria.limit);
  if(criteria.hasOwnProperty('skip')) this.skip(criteria.skip);
  if(criteria.hasOwnProperty('sort')) this.where(table, criteria.sort);
  if(criteria.hasOwnProperty('groupby')) this.group(table, criteria.groupby);

  // Close out query
  this._query += ')';
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

Query.prototype.where = function(table, criteria) {
  var self = this,
      operators = this.operators();

  if(!criteria) return;

  // Begin WHERE query
  this._query += 'WHERE ';

  // Process `where` criteria
  function processWhere(options) {
    Object.keys(options).forEach(function(key) {
      switch(key.toLowerCase()) {

        case 'or':

          // Wrap the entire OR clause
          self._query += '(';

          options[key].forEach(function(statement) {
            self._query += '(';
            processWhere(statement);

            if(self._query.slice(-4) === 'AND ') {
              self._query = self._query.slice(0, -5);
            }
            self._query += ') OR ';
          });

          if(self._query.slice(-3) === 'OR ') self._query = self._query.slice(0, -4);
          self._query += ') AND ';
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
  }

  processWhere(criteria);

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
      if(self._schema[key] && self._schema[key].type === 'text') caseSensitive = false;

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

  function expandCriteria(obj) {
    var _param;

    Object.keys(obj).forEach(function(key) {

      // If value is an object, recursivly expand it
      if(_.isPlainObject(obj[key])) return expandCriteria(obj[key]);

      // Check if value is a string and if so add LOWER logic
      // to work with case in-sensitive queries
      if(!caseSensitive && _.isString(obj[key])) {
        _param = 'LOWER(' + utils.escapeTable(table) + '.' + utils.escapeTable(parent) + ')';
        obj[key] = obj[key].toLowerCase();
      } else {
        _param = utils.escapeTable(table) + '.' + utils.escapeTable(parent);
      }

      self._query += _param + ' ';
      prepareCriterion.call(self, key, obj[key]);
      self._query += ' AND ';
    });
  }

  // Complex Object Attributes
  if(_.isPlainObject(value)) {

    // Expand the Object Criteria
    expandCriteria(value);

    // Remove trailing `AND`
    this._query = this._query.slice(0, -4);

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
        // For array values, do a "NOT IN"
        if (Array.isArray(value)) {
          var self = this;
          this._values = this._values.concat(value);
          str = 'NOT IN (';
          var params = [];
          value.forEach(function() {
            params.push('$' + self._paramCount++);
          });
          str += params.join(',') + ')';
          // Roll back one since we bump the count at the end
          this._paramCount--;
        }
        // Otherwise do a regular <>
        else {
          this._values.push(value);
          str = '<> ' + '$' + this._paramCount;
        }
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

Query.prototype.sort = function(table, options) {
  var self = this;

  this._query += ' ORDER BY ';

  Object.keys(options).forEach(function(key) {
    var direction = options[key] === 1 ? 'ASC' : 'DESC';
    self._query += utils.escapeTable(table) + '.' + utils.escapeTable(key) + ' ' + direction + ', ';
  });

  // Remove trailing comma
  this._query = this._query.slice(0, -2);
};

/**
 * Specify a `group by` condition
 */

Query.prototype.group = function(table, options) {
  var self = this;

  this._query += ' GROUP BY ';

  // Normalize to array
  if(!Array.isArray(options)) options = [options];

  options.forEach(function(key) {
    self._query += utils.escapeTable(table) + '.' + utils.escapeTable(key) + ', ';
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
