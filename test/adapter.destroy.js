var adapter = require('../lib/adapter'),
    _ = require('underscore'),
    should = require('should'),
    support = require('./support/bootstrap');

describe('adapter', function() {

  /**
   * Setup and Teardown
   */

  before(function(done) {
    support.Setup('test_destroy', done);
  });

  after(function(done) {
    support.Teardown('test_destroy', done);
  });

  /**
   * DESTROY
   *
   * Remove a row from a table
   */

  describe('.destroy()', function() {

    // Register the collection
    before(function(done) {
      var collection = _.extend({ config: support.Config }, {
        identity: 'test_destroy'
      });

      adapter.registerCollection(collection, done);
    });


    describe('with options', function() {

      before(function(done) {
        support.Seed('test_destroy', done);
      });

      it('should destroy the record', function(done) {
        adapter.destroy('test_destroy', { where: { id: 1 }}, function(err, result) {

          // Check record was actually removed
          support.Client(function(err, client) {
            client.query('SELECT * FROM "test_destroy"', function(err, result) {

              // Test no rows are returned
              result.rows.length.should.eql(0);

              done();
            });
          });

        });
      });

    });
  });
});
