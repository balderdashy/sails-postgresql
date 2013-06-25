var Query = require('../../lib/query'),
    assert = require('assert');

describe('query', function() {

  /**
   * CAST
   *
   * Cast values to proper types
   */

  describe('.cast()', function() {

    describe('Array', function() {

      it('should cast to values to array', function() {
        var values = new Query({ list: { type: 'array' }}).cast({ list: "[0,1,2,3]" });
        assert(Array.isArray(values.list));
        assert(values.list.length === 4);
      });

    });

  });
});
