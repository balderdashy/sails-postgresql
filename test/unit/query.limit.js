var Sequel = require('waterline-sequel'), 
    should = require('should'),
    Support = require('./support/bootstrap');

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

    var schema = {'test': Support.Schema('test', { name: { type: 'text' } })};

    it('should append the LIMIT clause to the query', function() {
      var query = new Sequel(schema, Support.SqlOptions).find('test', criteria);
      var sql = 'SELECT "test"."name" FROM "test" AS "test"  WHERE LOWER("test"."name") = $1  LIMIT 1';
      query.query[0].should.eql(sql);
    });

  });
});
