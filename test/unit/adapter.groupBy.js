var Query = require('../../lib/query'),
    should = require('should');

describe('query', function() {

  /**
   * groupBy
   *
   * Adds a Group By statement to a sql statement
   */

  describe('.groupBy()', function() {

    describe('with array', function() {

      // Lookup criteria
      var criteria = {
        where: {
          name: 'foo'
        },
        groupBy: ['name']
      };

      it('should append a Group By clause to the select statement', function() {
        var query = new Query({ name: { type: 'text' }}).find('test', criteria);
        var sql = 'SELECT \"test\".\"name\" FROM \"test\" WHERE LOWER(\"test\".\"name\") = $1 ' +
                  'GROUP BY \"test\".\"name\"';

        query.query.should.eql(sql);
      });
    });

    describe('with string', function() {

      // Lookup criteria
      var criteria = {
        where: {
          name: 'foo'
        },
        groupBy: 'name'
      };

      it('should use the MAX aggregate option in the select statement', function() {
        var query = new Query({ name: { type: 'text' }}).find('test', criteria);
        var sql = 'SELECT \"test\".\"name\" FROM \"test\" WHERE LOWER(\"test\".\"name\") = $1 ' +
                  'GROUP BY \"test\".\"name\"';

        query.query.should.eql(sql);
      });
    });

  });
});
