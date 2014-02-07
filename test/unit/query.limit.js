var Query = require('../../lib/query'),
    should = require('should');

describe('query', function() {

  /**
   * LIMIT
   *
   * Adds a LIMIT parameter to a sql statement
   */

  describe('.limit()', function() {

    // Lookup criteria
    var criteria = {
      where: {
        name: 'foo'
      },
      limit: 1
    };

    var schema = {
      test: {
        name: { type: 'text' }
      }
    };

    it('should append the LIMIT clause to the query', function() {
      var query = new Query({ name: { type: 'text' }}, schema).find('test', criteria);
      var sql = 'SELECT "test"."name" FROM "test" WHERE LOWER("test"."name") = $1 LIMIT 1';
      query.query.should.eql(sql);
    });

  });
});
