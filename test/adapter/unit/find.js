var assert = require('assert');
var _ = require('lodash');
var Adapter = require('../../../lib/adapter');
var Support = require('../../support/bootstrap');

describe('Unit Tests ::', function() {
  describe('Find', function() {
    // Test Setup
    before(function(done) {
      Support.Setup('test_find', function(err) {
        if (err) {
          return done(err);
        }

        // Seed the database with two simple records.
        Support.Seed('test_find', done);
      });
    });

    after(function(done) {
      Support.Teardown('test_find', done);
    });


    it('should select the correct record', function(done) {
      var wlQuery = {
        where: {
          fieldA: 'foo'
        }
      };

      Adapter.find('test', 'test_find', wlQuery, function(err, results) {
        assert(!err);
        assert(_.isArray(results));
        assert.equal(results.length, 1);
        assert.equal(_.first(results).fieldA, 'foo');
        assert.equal(_.first(results).fieldB, 'bar');

        done();
      });
    });

    it('should return all the records', function(done) {
      Adapter.find('test', 'test_find', {}, function(err, results) {
        assert(!err);
        assert(_.isArray(results));
        assert.equal(results.length, 2);
        done();
      });
    });

    it('should be case sensitive', function(done) {
      var wlQuery = {
        where: {
          fieldB: 'bAr_2'
        }
      };

      Adapter.find('test', 'test_find', wlQuery, function(err, results) {
        assert(!err);
        assert(_.isArray(results));
        assert.equal(results.length, 1);
        assert.equal(_.first(results).fieldA, 'foo_2');
        assert.equal(_.first(results).fieldB, 'bAr_2');
        done();
      });
    });

    // Look into the bowels of the PG Driver and ensure the Create function handles
    // it's connections properly.
    it('should release it\'s connection when completed', function(done) {
      var manager = Adapter.datastores.test.manager;
      var preConnectionsAvailable = manager.pool.pool.availableObjectsCount();

      Adapter.find('test', 'test_find', {}, function(err) {
        assert(!err);
        var postConnectionsAvailable = manager.pool.pool.availableObjectsCount();
        assert.equal(preConnectionsAvailable, postConnectionsAvailable);
        done();
      });
    });
  });
});
