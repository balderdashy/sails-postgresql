var adapter = require('../lib/adapter'),
    _ = require('underscore'),
    should = require('should'),
    support = require('./support/bootstrap');

describe('adapter', function() {

  /**
   * Teardown
   */

  after(function(done) {
    support.Teardown('test_index', done);
  });

  // Attributes for the test table
  var definition = {
    id: {
      type: 'serial',
      autoIncrement: true
    },
    name: {
      type: 'string',
      index: true
    }
  };

  /**
   * Indexes
   *
   * Ensure Indexes get created correctly
   */

  describe('Index Attributes', function() {

    before(function(done) {
      var collection = _.extend({ config: support.Config }, {
        identity: 'test_index'
      });

      adapter.registerCollection(collection, done);
    });

    // Build Indicies from definition
    it('should add indicies', function(done) {

      adapter.define('test_index', definition, function(err) {
        adapter.describe('test_index', function(err, result) {
          result.name.indexed.should.eql(true);
          done();
        });
      });

    });

  });
});
