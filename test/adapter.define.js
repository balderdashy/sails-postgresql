var adapter = require('../lib/adapter'),
    _ = require('underscore'),
    should = require('should'),
    support = require('./support/bootstrap');

describe('adapter', function() {

  /**
   * Setup and Teardown
   */

  after(function(done) {
    support.Teardown('test_define', done);
  });

  // Attributes for the test table
  var definition = {
    id    : {
      type: 'serial',
      autoIncrement: true
    },
    name  : 'string',
    email : 'string',
    title : 'string',
    phone : 'string',
    type  : 'string',
    favoriteFruit : {
      defaultsTo: 'blueberry',
      type: 'string'
    },
    age   : 'integer'
  };

  /**
   * DEFINE
   *
   * Create a new table with a defined set of attributes
   */

  describe('.define()', function() {

    describe('basic usage', function() {

      // Register the collection
      before(function(done) {
        var collection = _.extend({ config: support.Config }, {
          identity: 'test_define'
        });

        adapter.registerCollection(collection, done);
      });

      // Build Table from attributes
      it('should build the table', function(done) {

        adapter.define('test_define', definition, function(err) {
          adapter.describe('test_define', function(err, result) {
            Object.keys(result).length.should.eql(8);
            done();
          });
        });

      });

    });

    describe('reserved words', function() {

      // Register the collection
      before(function(done) {
        var collection = _.extend({ config: support.Config }, {
          identity: 'user'
        });

        adapter.registerCollection(collection, done);
      });

      after(function(done) {
        support.Client(function(err, client) {
          var query = 'DROP TABLE "user";';
          client.query(query, done);
        });
      });

      // Build Table from attributes
      it('should escape reserved words', function(done) {

        adapter.define('user', definition, function(err) {
          adapter.describe('user', function(err, result) {
            Object.keys(result).length.should.eql(8);
            done();
          });
        });

      });

    });

  });
});
