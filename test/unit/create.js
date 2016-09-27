var assert = require('assert');
var _ = require('lodash');
var Adapter = require('../../lib/adapter');
var Support = require('./support/bootstrap');

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
  });
});
