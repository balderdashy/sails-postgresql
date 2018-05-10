var assert = require('assert');
var _ = require('@sailshq/lodash');
var Adapter = require('../../../lib/adapter');
var Support = require('../../support/bootstrap');

describe('Unit Tests ::', function() {
  describe('Register', function() {
    after(function(done) {
      Support.Teardown('test', done);
    });

    it('should throw badConfiguration error', function(done) {
      var connection = _.cloneDeep(Support.Config);
      connection.identity = 'test';

      var badDefinition = {
        id: {
          columnType: 'serial',
          required: true,
          autoIncrement: true
        },
        bad: {
          type: 'number',
          autoMigrations: {
            columnType: 'bigint'
          }
        }
      };
      var collections = {};
      var collection = Support.Model('test', badDefinition);
      collections.test = collection;

      Adapter.registerDatastore(connection, collections, function(err) {
        if (!err) {
          return done(new Error('Should have thrown badConfiguration'));
        }

        assert.equal(err.code, 'badConfiguration');
        return done();
      });
    });

    it('should registerDatastore', function(done) {
      var connection = _.cloneDeep(Support.Config);
      connection.identity = 'test';

      var goodDefinition = {
        id: {
          columnType: 'serial',
          required: true,
          autoIncrement: true
        },
        good: {
          type: 'ref',
          autoMigrations: {
            columnType: 'bigint'
          }
        },
        anotherGood: {
          type: 'number'
        }
      };
      var collections = {};
      var collection = Support.Model('test', goodDefinition);
      collections.test = collection;

      Adapter.registerDatastore(connection, collections, function(err) {
        if (err) {
          console.log('why?', err);
          return done(err);
        }

        return done();
      });
    });
  });
});
