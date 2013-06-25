var Query = require('../../lib/query'),
    should = require('should');

describe('query', function() {

  /**
   * SKIP
   *
   * Adds an OFFSET parameter to a sql statement
   */

  describe('.skip()', function() {

    // Lookup criteria
    var criteria = {
      where: {
        name: 'foo'
      },
      skip: 1
    };

    it('should append the SKIP clause to the query', function() {
      var query = new Query({ name: { type: 'text' }}).find('test', criteria);
      query.query.should.eql('SELECT * FROM test WHERE LOWER("name") = $1 OFFSET 1');
    });

  });
});
