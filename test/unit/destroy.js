var assert = require('assert');
var _ = require('lodash');
var Adapter = require('../../lib/adapter');
var Support = require('./support/bootstrap');

describe('Unit Tests ::', function() {
  describe('Destroy', function() {
    // Test Setup
    before(function(done) {
      Support.Setup('test_destroy', function(err) {
        if (err) {
          return done(err);
        }

        // Seed the database with two simple records.
        Support.Seed('test_destroy', done);
      });
    });

    after(function(done) {
      Support.Teardown('test_destroy', done);
    });


    it('should remove records from the database and return the count', function(done) {
      var wlQuery = {
        where: {
          fieldA: 'foo'
        }
      };

      Adapter.destroy('test', 'test_destroy', wlQuery, function(err, result) {
        assert(!err);
        assert(_.isPlainObject(result));
        assert(_.isNumber(result.numRecordsDeleted));
        assert(result.numRecordsDeleted);
        assert.equal(result.numRecordsDeleted, 1);

        done();
      });
    });


    it('should ensure the record is actually deleted', function(done) {
      var wlQuery = {
        where: {
          fieldA: 'foo_2'
        }
      };

      Adapter.destroy('test', 'test_destroy', wlQuery, function(err) {
        assert(!err);
        Adapter.find('test', 'test_destroy', wlQuery, function(err, results) {
          assert(!err);
          assert.equal(results.length, 0);
          done();
        });
      });
    });

    // Look into the bowels of the PG Driver and ensure the Create function handles
    // it's connections properly.
    it('should release it\'s connection when completed', function(done) {
      var manager = Adapter._datastores.test.manager;
      var preConnectionsAvailable = manager.pool.pool.availableObjectsCount();

      Adapter.destroy('test', 'test_destroy', {}, function(err) {
        assert(!err);
        var postConnectionsAvailable = manager.pool.pool.availableObjectsCount();
        assert.equal(preConnectionsAvailable, postConnectionsAvailable);
        done();
      });
    });
  });
});
