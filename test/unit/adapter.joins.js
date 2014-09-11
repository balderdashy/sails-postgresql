var Query = require('../../lib/query');
var _ = require('lodash');
var should = require('should');

describe('query', function() {

  /**
   * Joins
   *
   * Build up SQL queries using joins and subqueries.
   */

  describe('.joins()', function() {

    var schema = {
      pet: {
        name: 'string',
        id: {
          type: 'integer',
          autoIncrement: true,
          primaryKey: true,
          unique: true
        },
        createdAt: { type: 'datetime', default: 'NOW' },
        updatedAt: { type: 'datetime', default: 'NOW' },
        owner: {
          columnName: 'owner',
          type: 'integer',
          foreignKey: true,
          references: 'user',
          on: 'id',
          onKey: 'id'
        }
      },
      user: {
        name: 'string',
        // pets: {
        //   collection: 'pet',
        //   via: 'owner',
        //   references: 'pet',
        //   on: 'owner',
        //   onKey: 'owner'
        // },
        id: {
          type: 'integer',
          autoIncrement: true,
          primaryKey: true,
          unique: true
        },
        createdAt: { type: 'datetime', default: 'NOW' },
        updatedAt: { type: 'datetime', default: 'NOW' }
      }
    };

    // Simple populate criteria, ex: .populate('pets')
    describe('populates', function() {

      // Lookup criteria
      var criteria =  {
        where: null,
        limit: 30,
        skip: 0,
        joins: [
          {
            parent: 'user',
            parentKey: 'id',
            child: 'pet',
            childKey: 'owner',
            select: [ 'name', 'id', 'createdAt', 'updatedAt', 'owner' ],
            alias: 'pets',
            removeParentKey: false,
            model: false,
            collection: true,
            criteria: {
              where: {},
              limit: 30
            }
          }
        ]
      };

      it('should build a query using inner joins', function() {
        var query = new Query(schema.user, schema).find('user', criteria);
        var sql = 'SELECT "user"."name", "user"."id", "user"."createdAt", "user"."updatedAt", ' +
                  '"pets_pet"."name" AS "pets_pet__name", "pets_pet"."id" AS "pets_pet__id", ' +
                  '"pets_pet"."createdAt" AS "pets_pet__createdAt", "pets_pet"."updatedAt" AS ' +
                  '"pets_pet__updatedAt", "pets_pet"."owner" AS "pets_pet__owner" FROM "user" ' +
                  'LEFT JOIN "pet" AS "pets_pet" ON "user"."id" = "pets_pet"."owner" WHERE "user"."id" ' +
                  'IN (SELECT "user"."id" FROM "user"  LIMIT 30 OFFSET 0)';

        query.query.should.eql(sql);
      });
    });

  });
});
