var Query = require('../../lib/query'),
    should = require('should');

describe('query', function() {

  /**
   * SUM
   *
   * Adds a SUM select parameter to a sql statement
   */

  describe('.sum()', function() {

    describe('with array', function() {

      // Lookup criteria
      var criteria = {
        where: {
          name: 'foo'
        },
        sum: ['age']
      };

      it('should use the SUM aggregate option in the select statement', function() {
        var query = new Query({ name: { type: 'text' }}).find('test', criteria);
        query.query.should.eql('SELECT CAST(SUM(age) AS float) AS age FROM test WHERE LOWER("name") = $1');
      });
    });

    describe('with string', function() {

      // Lookup criteria
      var criteria = {
        where: {
          name: 'foo'
        },
        sum: 'age'
      };

      it('should use the SUM aggregate option in the select statement', function() {
        var query = new Query({ name: { type: 'text' }}).find('test', criteria);
        query.query.should.eql('SELECT CAST(SUM(age) AS float) AS age FROM test WHERE LOWER("name") = $1');
      });
    });

  });
});
