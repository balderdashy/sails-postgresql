var _ = require('@sailshq/lodash');
var Benchmark = require('benchmark');

module.exports = function runBenchmarks(name, testFns, done) {
  var suite = new Benchmark.Suite({
    name: name
  });

  _.each(testFns, function buildTest(testFn) {
    suite = suite.add(testFn.name, {
      defer: true,
      async: true,
      fn: function(deferred) {
        testFn(function _afterRunningTestFn() {
          deferred.resolve();
        });
      }
    });
  });

  suite.on('cycle', function(event) {
    console.log(' •', String(event.target));
  })
  .on('complete', function() {
    // Time is measured in microseconds so 1000 = 1ms
    var fastestMean = _.first(this.filter('fastest')).stats.mean * 1000;
    var slowestMean = _.first(this.filter('slowest')).stats.mean * 1000;

    var mean = {
      fastest: Benchmark.formatNumber(fastestMean < 1 ? fastestMean.toFixed(2) : Math.round(fastestMean)),
      slowest: Benchmark.formatNumber(slowestMean < 1 ? slowestMean.toFixed(2) : Math.round(slowestMean))
    };

    console.log('Fastest is ' + this.filter('fastest').map('name') + ' with an average of: ' + mean.fastest + 'ms');
    console.log('Slowest is ' + this.filter('slowest').map('name') + ' with an average of: ' + mean.slowest + 'ms');

    return done(undefined, this);
  })
  .run();
};
