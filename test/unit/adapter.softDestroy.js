var adapter = require('../../lib/adapter'),
    should = require('should'),
    support = require('./support/bootstrap');

describe('adapter', function() {

  /**
   * Setup and Teardown
   */

  before(function(done) {
    support.Config.softDelete = true;
    support.Setup('test_softdestroy', done);
  });

  after(function(done) {
    support.Teardown('test_softdestroy', done);
  });

  /**
   * SOFT DESTROY
   *
   * Remove a row from a table logically by marking it deleted,
   * but leave the row and do not call DELETE.
   */

  describe('.softdestroy()', function() {

    describe('with options', function() {

      before(function(done) {
        support.Seed('test_softdestroy', done);
      });

      it('should destroy the record', function(done) {
        adapter.destroy('test_softdestroy', { where: { id: 1 }}, function(err, result) {
          // Check record was actually removed
          support.Client(function(err, client, close) {
            //client.query('SELECT * FROM "test_destroy"', function(err, result) {
            adapter.find('test_softdestroy', null, function(err, result) {
              // Test no rows are returned
              result.length.should.eql(0);

              // close client
              close();

              done();
            });
          });

        });
      });

    });
  });
});
