var Query = require('../lib/query'),
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

    it('should append the LIMIT clause to the query', function() {
      var query = new Query().find('test', criteria);

      query.query.should.eql('SELECT * FROM test WHERE LOWER("name") = $1 LIMIT 1');
    });

  });
});