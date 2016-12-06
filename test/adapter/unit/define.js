var assert = require('assert');
var _ = require('@sailshq/lodash');
var Adapter = require('../../../lib/adapter');
var Support = require('../../support/bootstrap');

describe('Unit Tests ::', function() {
  describe('Define', function() {
    // Test Setup
    before(function(done) {
      Support.registerConnection(['test_define'], done);
    });

    after(function(done) {
      Support.Teardown('test_define', done);
    });

    // Attributes for the test table
    var definition = {
      id: {
        type: 'serial',
        autoIncrement: true
      },
      name: {
        type: 'string',
        notNull: true
      },
      email: 'string',
      title: 'string',
      phone: 'string',
      type: 'string',
      favoriteFruit: {
        defaultsTo: 'blueberry',
        type: 'string'
      },
      age: 'integer'
    };

    it('should create a table in the database', function(done) {
      Adapter.define('test', 'test_define', definition, function(err) {
        assert(!err);

        Adapter.describe('test', 'test_define', function(err, result) {
          assert(!err);
          assert(_.isPlainObject(result));

          assert.equal(_.keys(result).length, 8);
          assert(result.id);
          assert(result.name);
          assert(result.email);
          assert(result.title);
          assert(result.phone);
          assert(result.type);
          assert(result.favoriteFruit);
          assert(result.age);

          done();
        });
      });
    });
  });
});
