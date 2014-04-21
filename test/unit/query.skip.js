var Query = require('../../lib/query'),
    should = require('should');

describe('query', function() {

  /**
   * SKIP
   *
   * Adds an OFFSET parameter to a sql statement
   */

  describe('.skip()', function() {

    // Lookup criteria
    var criteria = {
      where: {
        name: 'foo'
      },
      skip: 1
    };

    var schema = {
      test: {
        name: { type: 'text' }
      }
    };

    it('should append the SKIP clause to the query', function() {
      var query = new Query({ name: { type: 'text' }}, schema).find('test', criteria);
      var sql = 'SELECT "test"."name" FROM "test" WHERE LOWER("test"."name") = $1 OFFSET 1';
      query.query.should.eql(sql);
    });

  });
});
