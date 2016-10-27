//  ███████╗██╗███╗   ██╗██████╗     ██████╗ ██████╗ ██╗███╗   ███╗ █████╗ ██████╗ ██╗   ██╗
//  ██╔════╝██║████╗  ██║██╔══██╗    ██╔══██╗██╔══██╗██║████╗ ████║██╔══██╗██╔══██╗╚██╗ ██╔╝
//  █████╗  ██║██╔██╗ ██║██║  ██║    ██████╔╝██████╔╝██║██╔████╔██║███████║██████╔╝ ╚████╔╝
//  ██╔══╝  ██║██║╚██╗██║██║  ██║    ██╔═══╝ ██╔══██╗██║██║╚██╔╝██║██╔══██║██╔══██╗  ╚██╔╝
//  ██║     ██║██║ ╚████║██████╔╝    ██║     ██║  ██║██║██║ ╚═╝ ██║██║  ██║██║  ██║   ██║
//  ╚═╝     ╚═╝╚═╝  ╚═══╝╚═════╝     ╚═╝     ╚═╝  ╚═╝╚═╝╚═╝     ╚═╝╚═╝  ╚═╝╚═╝  ╚═╝   ╚═╝
//
//  ██╗  ██╗███████╗██╗   ██╗
//  ██║ ██╔╝██╔════╝╚██╗ ██╔╝
//  █████╔╝ █████╗   ╚████╔╝
//  ██╔═██╗ ██╔══╝    ╚██╔╝
//  ██║  ██╗███████╗   ██║
//  ╚═╝  ╚═╝╚══════╝   ╚═╝
//
// Given a model schema, return the primary key field.

var _ = require('lodash');

module.exports = function findPrimaryKey(definition) {
  if (!definition) {
    throw new Error('Find Primary Key requires a valid definition.');
  }

  // Look for an attribute that has a primaryKey flag on it
  var pk = _.findKey(definition, function find(val) {
    if (_.has(val, 'primaryKey')) {
      return true;
    }
  });

  // Default the primary key to `id`
  if (!pk) {
    pk = 'id';
  }

  return pk;
};
