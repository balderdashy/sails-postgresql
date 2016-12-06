var assert = require('assert');
var _ = require('@sailshq/lodash');
var Adapter = require('../../../lib/adapter');
var Support = require('../../support/bootstrap');

describe('Unit Tests ::', function() {
  describe('Add Attribute', function() {
    // Test Setup
    before(function(done) {
      Support.Setup('test_add_attribute', done);
    });

    after(function(done) {
      Support.Teardown('test_add_attribute', done);
    });

    // Attributes for the test table
    var definition = {
      type: 'string'
    };

    it('should add a column to a table', function(done) {
      Adapter.addAttribute('test', 'test_add_attribute', 'color', definition, function(err) {
        assert(!err);

        Adapter.describe('test', 'test_add_attribute', function(err, result) {
          assert(!err);
          assert(_.isPlainObject(result));
          assert(result.color);
          assert.equal(result.color.type, 'text');

          done();
        });
      });
    });
  });
});
