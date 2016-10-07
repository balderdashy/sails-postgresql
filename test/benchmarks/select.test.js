var runBenchmarks = require('../support/benchmark-runner');
var Support = require('../support/bootstrap');
var Adapter = require('../../lib/adapter');

//  ╔╗ ╔═╗╔╗╔╔═╗╦ ╦╔╦╗╔═╗╦═╗╦╔═╔═╗
//  ╠╩╗║╣ ║║║║  ╠═╣║║║╠═╣╠╦╝╠╩╗╚═╗
//  ╚═╝╚═╝╝╚╝╚═╝╩ ╩╩ ╩╩ ╩╩╚═╩ ╩╚═╝
describe('Benchmark :: Select', function() {
  // Set "timeout" and "slow" thresholds incredibly high
  // to avoid running into issues.
  this.slow(240000);
  this.timeout(240000);

  // Test Setup
  before(function(done) {
    Support.Setup('test_find', function(err) {
      if (err) {
        return done(err);
      }

      // Seed the database with two simple records.
      Support.Seed('test_find', done);
    });
  });

  // Cleanup
  after(function(done) {
    Support.Teardown('test_find', done);
  });

  it('should be performant enough', function(done) {
    runBenchmarks('Compiler.execSync()', [

      function runSelect(next) {
        var wlQuery = {
          where: {
            fieldA: 'foo'
          }
        };

        Adapter.find('test', 'test_find', wlQuery, function() {
          return next();
        });
      }
    ], done);
  });
});
