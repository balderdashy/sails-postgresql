var adapter = require('../lib/adapter'),
    _ = require('underscore'),
    should = require('should'),
    support = require('./support/bootstrap');

describe('adapter', function() {

  /**
   * Setup and Teardown
   */

  before(function(done) {
    support.Setup('test_update', done);
  });

  after(function(done) {
    support.Teardown('test_update', done);
  });

  /**
   * UPDATE
   *
   * Update a row in a table
   */

  describe('.update()', function() {

    // Register the collection
    before(function(done) {
      var collection = _.extend({ config: support.Config }, {
        identity: 'test_update'
      });

      adapter.registerCollection(collection, done);
    });


    describe('with options', function() {

      before(function(done) {
        support.Seed('test_update', done);
      });

      it('should update the record', function(done) {

        adapter.update('test_update', { where: { id: 1 }}, { field_1: 'foobar' }, function(err, result) {
          result[0].field_1.should.eql('foobar');
          done();
        });

      });

      it('should keep case', function(done) {

        adapter.update('test_update', { where: { id: 1 }}, { field_1: 'FooBar' }, function(err, result) {
          result[0].field_1.should.eql('FooBar');
          done();
        });

      });

    });
  });
});
