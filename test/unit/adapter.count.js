var Query = require('../../lib/query'),
    should = require('should');

describe('query', function() {

  /**
   * MAX
   *
   * Adds a MAX select parameter to a sql statement
   */

  describe('.count()', function() {

    describe('without parameters', function() {

      // Lookup criteria
      var criteria = { }

      it('should count the total number of rows in the table', function() {
        var query = new Query().count('test', criteria);
        query.query.should.eql('SELECT COUNT(*) FROM test ');
      });
    });

    describe('with where clause', function() {

      // Lookup criteria
      var criteria = {
        where: {
          name: 'foo'
        }
      };

      it('should count a subset of the table', function() {
        var query = new Query({ name: { type: 'text' }}).find('test', criteria);
        query.query.should.eql('SELECT * FROM test WHERE LOWER("name") = $1');
      });
    });

  });
});
