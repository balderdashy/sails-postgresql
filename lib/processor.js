/**
 * Module Dependencies
 */

var _ = require('lodash');

/**
 * Processes data returned from a SQL query.
 */

var Processor = module.exports = function Processor(schema) {
  this.schema = _.cloneDeep(schema);
  return this;
};


/**
 * Cast special values to proper types.
 *
 * Ex: Array is stored as "[0,1,2,3]" and should be cast to proper
 * array for return values.
 */

Processor.prototype.cast = function(table, values) {

  var self = this;
  var _values = _.cloneDeep(values);

  Object.keys(values).forEach(function(key) {
    self.castValue(table, key, _values[key], _values);
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

 Processor.prototype.castValue = function(table, key, value, attributes) {
  var _schema = this.schema[identity];
  if (!_schema) return;

  // Cache mappings of column names to attribute names to boost further castings
  if (!_schema._columns) {
    _schema._columns = {};
    _.each(_schema.attributes, function(attr, attrName) {
      _schema._columns[attr.columnName || attrName] = attrName;
    });
  }

  var attr = _schema._columns[key];
  if (!_schema.attributes[attr]) return;

  var type = _.isPlainObject(_schema.attributes[attr])
    ? _schema.attributes[attr].type
    : _schema.attributes[attr];
  if(!type) return;

  // Attempt to parse Array
  switch(type) {
    case 'array':
      try {
        attributes[key] = JSON.parse(value);
      } catch(e) {
        return;
      }
      break;
  }
};
