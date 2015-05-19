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

  var self = this;
  var identity = table;
  var attr;

  // Check for a columnName, serialize so we can do any casting
  Object.keys(this.schema[identity].attributes).forEach(function(attribute) {
    if(self.schema[identity].attributes[attribute].columnName === key) {
      attr = attribute;
      return;
    }
  });

  if(!attr) attr = key;

  // Lookup Schema "Type"
  if(!this.schema[identity] || !this.schema[identity].attributes[attr]) return;
  var type;

  if(!_.isPlainObject(this.schema[identity].attributes[attr])) {
    type = this.schema[identity].attributes[attr];
  } else {
    type = this.schema[identity].attributes[attr].type;
  }


  if(!type) return;

  // Attempt to parse Array
  if(type === 'array') {
    try {
      attributes[key] = JSON.parse(value);
    } catch(e) {
      return;
    }
  }

};
