var adapter = require('../../lib/adapter'),
    should = require('should'),
    support = require('./support/bootstrap');

describe('adapter', function() {

  /**
   * Setup and Teardown
   */

  before(function(done) {
    support.Setup('test_createEach', done);
  });

  after(function(done) {
    support.Teardown('test_createEach', done);
  });

  // Attributes for the test table
  var attributes = {
    field_1: 'foo',
    field_2: 'bar'
  };

  /**
   * CREATE EACH
   *
   * Insert an array of rows into a table
   */

  describe('.createEach()', function() {

    // Insert multiple records
    it('should insert multiple records', function(done) {
      adapter.createEach('test', 'test_createEach', [attributes, attributes], function(err, result) {

        // Check records were actually inserted
        support.Client(function(err, client, close) {
          client.query('SELECT * FROM "test_createEach"', function(err, result) {

            // Test 2 rows are returned
            result.rows.length.should.eql(2);

            // close client
            close();

            done();
          });
        });
      });
    });

  });
});
