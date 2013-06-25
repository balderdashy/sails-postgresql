var adapter = require('../../lib/adapter'),
    _ = require('underscore'),
    should = require('should'),
    support = require('./support/bootstrap');

describe('adapter', function() {

  /**
   * Setup and Teardown
   */

  before(function(done) {
    support.Setup('test_addAttribute', done);
  });

  after(function(done) {
    support.Teardown('test_addAttribute', done);
  });

  /**
   * ADD ATTRIBUTE
   *
   * Adds a column to a Table
   */

  describe('.addAttribute()', function() {

    // Register the collection
    before(function(done) {
      var collection = _.extend({ config: support.Config }, {
        identity: 'test_addAttribute'
      });

      adapter.registerCollection(collection, done);
    });

    // Add a column to a table
    it('should add column color to the table', function(done) {

      adapter.addAttribute('test_addAttribute', 'color', 'string', function(err, result) {
        adapter.describe('test_addAttribute', function(err, result) {

          // Test Row length
          Object.keys(result).length.should.eql(4);

          // Test the name of the last column
          should.exist(result.color);

          done();
        });
      });

    });
  });
});
