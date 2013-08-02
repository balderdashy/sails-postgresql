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
        query.query.should.eql('SELECT CAST(AVG(age) AS float) AS age FROM test WHERE LOWER("name") = $1');
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
        query.query.should.eql('SELECT CAST(AVG(age) AS float) AS age FROM test WHERE LOWER("name") = $1');
      });
    });

  });
});
