var Query = require('../../lib/query'),
    should = require('should');

describe('query', function() {

  /**
   * AVG
   *
   * Adds a AVG select parameter to a sql statement
   */

  describe('.avg()', function() {

    describe('with array', function() {

      // Lookup criteria
      var criteria = {
        where: {
          name: 'foo'
        },
        average: ['age']
      };

      it('should use the AVG aggregate option in the select statement', function() {
        var query = new Query({ name: { type: 'text' }}).find('test', criteria);
        var sql = 'SELECT CAST(AVG(\"test\".\"age\") AS float) AS age FROM \"test\" WHERE ' +
                  'LOWER(\"test\".\"name\") = $1';

        query.query.should.eql(sql);
      });
    });

    describe('with string', function() {

      // Lookup criteria
      var criteria = {
        where: {
          name: 'foo'
        },
        average: 'age'
      };

      it('should use the AVG aggregate option in the select statement', function() {
        var query = new Query({ name: { type: 'text' }}).find('test', criteria);
        var sql = 'SELECT CAST(AVG(\"test\".\"age\") AS float) AS age FROM \"test\" WHERE ' +
                  'LOWER(\"test\".\"name\") = $1';

        query.query.should.eql(sql);
      });
    });

  });
});
