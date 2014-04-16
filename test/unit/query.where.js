var Query = require('../../lib/query'),
    should = require('should');

describe('query', function() {

  /**
   * WHERE
   *
   * Build the WHERE part of an sql statement from a js object
   */

  describe('.where()', function() {

    describe('`AND` criteria', function() {

      describe('case insensitivity', function() {

        // Lookup criteria
        var criteria = {
          where: {
            name: 'Foo',
            age: 1
          }
        };

        it('should build a SELECT statement using LOWER() on strings', function() {
          var query = new Query({ name: { type: 'text' }, age: { type: 'integer'}}).find('test', criteria);

          query.query.should.eql('SELECT * FROM test WHERE LOWER("name") = $1 AND "age" = $2');
          query.values[0].should.eql('foo');
        });
      });

      describe('criteria is simple key value lookups', function() {

        // Lookup criteria
        var criteria = {
          where: {
            name: 'foo',
            age: 27
          }
        };

        it('should build a simple SELECT statement', function() {
          var query = new Query({ name: { type: 'text' }, age: { type: 'integer'}}).find('test', criteria);

          query.query.should.eql('SELECT * FROM test WHERE LOWER("name") = $1 AND "age" = $2');
          query.values.length.should.eql(2);
        });

      });

      describe('has multiple comparators', function() {

        // Lookup criteria
        var criteria = {
          where: {
            name: 'foo',
            age: {
              '>' : 27,
              '<' : 30
            }
          }
        };

        it('should build a SELECT statement with comparators', function() {
          var query = new Query({ name: { type: 'text' }, age: { type: 'integer'}}).find('test', criteria);

          query.query.should.eql('SELECT * FROM test WHERE LOWER("name") = $1 AND "age" > $2 AND "age" < $3');
          query.values.length.should.eql(3);
        });

      });

    });

    describe('`LIKE` criteria', function() {

      // Lookup criteria
      var criteria = {
        where: {
          like: {
            type: '%foo%',
            name: 'bar%'
          }
        }
      };

      it('should build a SELECT statement with ILIKE', function() {
        var query = new Query({ type: { type: 'text' }, name: { type: 'text'}}).find('test', criteria);

        query.query.should.eql('SELECT * FROM test WHERE LOWER("type") ILIKE $1 AND LOWER("name") ILIKE $2');
        query.values.length.should.eql(2);
      });

    });

    describe('`OR` criteria', function() {

      // Lookup criteria
      var criteria = {
        where: {
          or: [
            { like: { foo: '%foo%' } },
            { like: { bar: '%bar%' } }
          ]
        }
      };

      it('should build a SELECT statement with multiple like statements', function() {
        var query = new Query({ foo: { type: 'text' }, bar: { type: 'text'}}).find('test', criteria);

        query.query.should.eql('SELECT * FROM test WHERE (LOWER("foo") ILIKE $1 OR LOWER("bar") ILIKE $2)');
        query.values.length.should.eql(2);
      });

    });

    describe('`IN` criteria', function() {

      // Lookup criteria
      var criteria = {
        where: {
          name: [
            'foo',
            'bar',
            'baz'
          ]
        }
      };

      var camelCaseCriteria = {
        where: {
          myId: [
            1,
            2,
            3
          ]
        }
      };

      it('should build a SELECT statement with an IN array', function() {
        var query = new Query({ name: { type: 'text' }}).find('test', criteria);

        query.query.should.eql('SELECT * FROM test WHERE LOWER("name") IN ($1, $2, $3)');
        query.values.length.should.eql(3);
      });

      it('should build a SELECT statememnt with an IN array and camel case column', function() {
        var query = new Query({ myId: { type: 'integer' }}).find('test', camelCaseCriteria);

        query.query.should.eql('SELECT * FROM test WHERE "myId" IN ($1, $2, $3)');
        query.values.length.should.eql(3);
      });

    });

    describe('`NOT` criteria', function() {

      // Lookup criteria
      var criteria = {
        where: {
          age: {
            not: 40
          }
        }
      };

      it('should build a SELECT statement with an NOT clause', function() {
        var query = new Query({age: { type: 'integer'}}).find('test', criteria);

        query.query.should.eql('SELECT * FROM test WHERE "age" <> $1');
        query.values.length.should.eql(1);
      });

    });

  });
});
