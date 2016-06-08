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

  _.each(_.keys(values), function(key) {
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

  var attr = this.schema[table] && this.schema[table].definition && this.schema[table].definition[key];
  if(!attr) {
    return;
  }

  var type = attr.type;

  // Attempt to parse Array
  if(type === 'array') {
    try {
      attributes[key] = JSON.parse(value);
    } catch(e) {
      return;
    }
  }

};
