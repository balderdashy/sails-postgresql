var assert = require('assert');
var _ = require('lodash');
var Adapter = require('../../../lib/adapter');
var Support = require('../../support/bootstrap');

describe('Unit Tests ::', function() {
  describe('Update', function() {
    // Test Setup
    before(function(done) {
      Support.Setup('test_update', function(err) {
        if (err) {
          return done(err);
        }

        // Seed the database with two simple records.
        Support.Seed('test_update', done);
      });
    });

    after(function(done) {
      Support.Teardown('test_update', done);
    });

    it('should update the correct record', function(done) {
      var wlFindQuery = {
        where: {
          fieldA: 'foo'
        }
      };

      Adapter.update('test', 'test_update', wlFindQuery, { fieldA: 'foobar' }, function(err, results) {
        assert(!err);
        assert(_.isArray(results));
        assert.equal(results.length, 1);
        assert.equal(_.first(results).fieldA, 'foobar');
        assert.equal(_.first(results).fieldB, 'bar');
        return done();
      });
    });

    it('should be case sensitive', function(done) {
      var wlFindQuery = {
        where: {
          fieldB: 'bAr_2'
        }
      };

      Adapter.update('test', 'test_update', wlFindQuery, { fieldA: 'FooBar' }, function(err, results) {
        assert(!err);
        assert(_.isArray(results));
        assert.equal(results.length, 1);
        assert.equal(_.first(results).fieldA, 'FooBar');
        assert.equal(_.first(results).fieldB, 'bAr_2');
        done();
      });
    });

    // Look into the bowels of the PG Driver and ensure the Create function handles
    // it's connections properly.
    it('should release it\'s connection when completed', function(done) {
      var manager = Adapter.datastores.test.manager;
      var preConnectionsAvailable = manager.pool.pool.availableObjectsCount();

      Adapter.update('test', 'test_update', {}, {}, function(err) {
        assert(!err);
        var postConnectionsAvailable = manager.pool.pool.availableObjectsCount();
        assert.equal(preConnectionsAvailable, postConnectionsAvailable);
        done();
      });
    });
  });
});
