var adapter = require('../lib/adapter'),
    _ = require('underscore'),
    should = require('should'),
    support = require('./support/bootstrap');

describe('adapter', function() {

  /**
   * Setup and Teardown
   */

  before(function(done) {
    support.Setup('test_removeAttribute', done);
  });

  after(function(done) {
    support.Teardown('test_removeAttribute', done);
  });

  /**
   * REMOVE ATTRIBUTE
   *
   * Drops a Column from a Table
   */

  describe('.removeAttribute()', function() {

    // Register the collection
    before(function(done) {
      var collection = _.extend(support.Config, {
        identity: 'test_removeAttribute'
      });

      adapter.registerCollection(collection, done);
    });

    // Remove a column to a table
    it('should remove column field_2 from the table', function(done) {

      adapter.removeAttribute('test_removeAttribute', 'field_2', function(err) {
        adapter.describe('test_removeAttribute', function(err, result) {

          // Test Row length
          Object.keys(result).length.should.eql(2);

          // Test the name of the last column
          should.not.exist(result.field_2);

          done();
        });
      });

    });
  });
});