var assert = require('assert');
var _ = require('lodash');
var Adapter = require('../../lib/adapter');
var Support = require('./support/bootstrap');

describe('Unit Tests ::', function() {
  describe('Drop', function() {
    // Test Setup
    before(function(done) {
      Support.Setup('test_drop', done);
    });

    after(function(done) {
      Support.Teardown('test_drop', done);
    });


    it('should remove a table from the database', function(done) {
      Adapter.drop('test', 'test_drop', function dropCb(err) {
        assert(!err);

        Adapter.describe('test', 'test_drop', function describeCb(err, result) {
          assert(!err);
          assert.equal(_.keys(result), 0);

          return done();
        });
      });
    });
  });
});
