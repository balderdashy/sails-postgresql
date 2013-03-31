var Query = require('../lib/query'),
    should = require('should');

describe('query', function() {

  /**
   * SORT
   *
   * Adds an ORDER BY parameter to a sql statement
   */

  describe('.skip()', function() {

    it('should append the ORDER BY clause to the query', function() {

      // Lookup criteria
      var criteria = {
        where: {
          name: 'foo'
        },
        sort: {
          name: 1
        }
      };

      var query = new Query().find('test', criteria);
      query.query.should.eql('SELECT * FROM test WHERE LOWER("name") = $1 ORDER BY "name" ASC');

    });

    it('should sort by multiple columns', function() {

      // Lookup criteria
      var criteria = {
        where: {
          name: 'foo'
        },
        sort: {
          name: 1,
          age: 1
        }
      };

      var query = new Query().find('test', criteria);
      query.query.should.eql('SELECT * FROM test WHERE LOWER("name") = $1 ORDER BY "name" ASC, "age" ASC');

    });

    it('should allow desc and asc ordering', function() {

      // Lookup criteria
      var criteria = {
        where: {
          name: 'foo'
        },
        sort: {
          name: 1,
          age: -1
        }
      };

      var query = new Query().find('test', criteria);
      query.query.should.eql('SELECT * FROM test WHERE LOWER("name") = $1 ORDER BY "name" ASC, "age" DESC');

    });

  });

});