var assert = require('assert');
var _ = require('lodash');
var Adapter = require('../../../lib/adapter');
var Support = require('../../support/bootstrap');

describe('Unit Tests ::', function() {
  describe('Create', function() {
    // Test Setup
    before(function(done) {
      Support.Setup('test_create', done);
    });

    after(function(done) {
      Support.Teardown('test_create', done);
    });

    // Attributes for the test table
    var attributes = {
      fieldA: 'foo',
      fieldB: 'bar'
    };

    it('should insert a record into the database and return it\'s fields', function(done) {
      Adapter.create('test', 'test_create', attributes, function(err, result) {
        assert(!err);
        assert(_.isPlainObject(result));
        assert.equal(result.fieldA, 'foo');
        assert.equal(result.fieldB, 'bar');
        assert(result.id);

        done();
      });
    });

    // Create Auto-Incremented ID
    it('should create an auto-incremented id field', function(done) {
      Adapter.create('test', 'test_create', attributes, function(err, result) {
        assert(!err);
        assert(_.isPlainObject(result));
        assert(result.id);
        done();
      });
    });

    it('should keep case', function(done) {
      var attributes = {
        fieldA: 'Foo',
        fieldB: 'bAr'
      };

      Adapter.create('test', 'test_create', attributes, function(err, result) {
        assert(!err);
        assert.equal(result.fieldA, 'Foo');
        assert.equal(result.fieldB, 'bAr');
        done();
      });
    });

    // Look into the bowels of the PG Driver and ensure the Create function handles
    // it's connections properly.
    it('should release it\'s connection when completed', function(done) {
      var manager = Adapter.datastores.test.manager;
      var preConnectionsAvailable = manager.pool.pool.availableObjectsCount();

      Adapter.create('test', 'test_create', attributes, function(err) {
        assert(!err);
        var postConnectionsAvailable = manager.pool.pool.availableObjectsCount();
        assert.equal(preConnectionsAvailable, postConnectionsAvailable);
        done();
      });
    });
  });
});
