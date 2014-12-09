var assert = require('assert'),
    Support = require('./support/bootstrap'),
    Processor = require('../../lib/processor');

describe('query', function() {

  /**
   * CAST
   *
   * Cast values to proper types
   */

  describe('.cast()', function() {

    describe('Array', function() {

      it('should cast to values to array', function() {
        var schema = {'test': Support.Schema('test', { list: { type: 'array' } })};
        var values = new Processor(schema).cast('test', { list: "[0,1,2,3]" });
        assert(Array.isArray(values.list));
        assert(values.list.length === 4);
      });

    });

  });
});
