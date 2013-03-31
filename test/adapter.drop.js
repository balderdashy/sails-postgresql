var adapter = require('../lib/adapter'),
    _ = require('underscore'),
    should = require('should'),
    support = require('./support/bootstrap');

describe('adapter', function() {

  /**
   * Setup and Teardown
   */

  before(function(done) {
    support.Setup('test_drop', done);
  });

  /**
   * DROP
   *
   * Drop a table and all it's records.
   */

  describe('.drop()', function() {

    // Register the collection
    before(function(done) {
      var collection = _.extend(support.Config, {
        identity: 'test_drop'
      });

      adapter.registerCollection(collection, done);
    });

    // Drop the Test table
    it('should drop the table', function(done) {

      adapter.drop('test_drop', function(err, result) {
        adapter.describe('test_drop', function(err, result) {
          should.not.exist(result);
          done();
        });
      });

    });
  });
});