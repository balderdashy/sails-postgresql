var Query = require('../../lib/query'),
    should = require('should');

describe('query', function() {

  /**
   * MAX
   *
   * Adds a MAX select parameter to a sql statement
   */

  describe('.min()', function() {

    describe('with array', function() {

      // Lookup criteria
      var criteria = {
        where: {
          name: 'foo'
        },
        max: ['age']
      };

      it('should use the max aggregate option in the select statement', function() {
        var query = new Query({ name: { type: 'text' }}).find('test', criteria);
        query.query.should.eql('SELECT MAX(\"age\") AS \"age\" FROM test WHERE LOWER(\"name\") = $1');
      });
    });

    describe('with string', function() {

      // Lookup criteria
      var criteria = {
        where: {
          name: 'foo'
        },
        max: 'age'
      };

      it('should use the MAX aggregate option in the select statement', function() {
        var query = new Query({ name: { type: 'text' }}).find('test', criteria);
        query.query.should.eql('SELECT MAX(\"age\") AS \"age\" FROM test WHERE LOWER(\"name\") = $1');
      });
    });

  });
});
