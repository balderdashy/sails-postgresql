//  ███╗   ███╗ ██████╗ ██████╗ ██╗███████╗██╗   ██╗
//  ████╗ ████║██╔═══██╗██╔══██╗██║██╔════╝╚██╗ ██╔╝
//  ██╔████╔██║██║   ██║██║  ██║██║█████╗   ╚████╔╝
//  ██║╚██╔╝██║██║   ██║██║  ██║██║██╔══╝    ╚██╔╝
//  ██║ ╚═╝ ██║╚██████╔╝██████╔╝██║██║        ██║
//  ╚═╝     ╚═╝ ╚═════╝ ╚═════╝ ╚═╝╚═╝        ╚═╝
//
//  ██████╗ ███████╗ ██████╗ ██████╗ ██████╗ ██████╗
//  ██╔══██╗██╔════╝██╔════╝██╔═══██╗██╔══██╗██╔══██╗
//  ██████╔╝█████╗  ██║     ██║   ██║██████╔╝██║  ██║
//  ██╔══██╗██╔══╝  ██║     ██║   ██║██╔══██╗██║  ██║
//  ██║  ██║███████╗╚██████╗╚██████╔╝██║  ██║██████╔╝
//  ╚═╝  ╚═╝╚══════╝ ╚═════╝ ╚═════╝ ╚═╝  ╚═╝╚═════╝
//
// Modify the record(s) and return the values that were modified.

var _ = require('@sailshq/lodash');
var PG = require('machinepack-postgresql');
var runQuery = require('./run-query');
var releaseConnection = require('../connection/release-connection');

module.exports = function modifyRecord(options, cb) {
  //  ╦  ╦╔═╗╦  ╦╔╦╗╔═╗╔╦╗╔═╗  ┌─┐┌─┐┌┬┐┬┌─┐┌┐┌┌─┐
  //  ╚╗╔╝╠═╣║  ║ ║║╠═╣ ║ ║╣   │ │├─┘ │ ││ ││││└─┐
  //   ╚╝ ╩ ╩╩═╝╩═╩╝╩ ╩ ╩ ╚═╝  └─┘┴   ┴ ┴└─┘┘└┘└─┘
  if (_.isUndefined(options) || !_.isPlainObject(options)) {
    throw new Error('Invalid options argument. Options must contain: connection, query, model, schemaName, leased, and tableName.');
  }

  if (!_.has(options, 'connection') || !_.isObject(options.connection)) {
    throw new Error('Invalid option used in options argument. Missing or invalid connection.');
  }

  if (!_.has(options, 'query') || (!_.isPlainObject(options.query) && !_.isString(options.query))) {
    throw new Error('Invalid option used in options argument. Missing or invalid query.');
  }

  if (!_.has(options, 'leased') || !_.isBoolean(options.leased)) {
    throw new Error('Invalid option used in options argument. Missing or invalid leased flag.');
  }


  //  ╦═╗╦ ╦╔╗╔  ┌─┐ ┬ ┬┌─┐┬─┐┬ ┬
  //  ╠╦╝║ ║║║║  │─┼┐│ │├┤ ├┬┘└┬┘
  //  ╩╚═╚═╝╝╚╝  └─┘└└─┘└─┘┴└─ ┴
  runQuery({
    connection: options.connection,
    nativeQuery: options.query,
    valuesToEscape: options.valuesToEscape,
    disconnectOnError: false
  },

  function runQueryCb(err, report) {
    // If the query failed to run, release the connection and return the parsed
    // error footprint.
    if (err) {
      releaseConnection(options.connection, options.leased, function releaseCb() {
        return cb(err);
      });

      return;
    }

    // If the records were fetched, then pretend this was find and parse the
    // native query results.
    if (options.fetchRecords) {
      var parsedResults;
      try {
        parsedResults = PG.parseNativeQueryResult({
          queryType: 'select',
          nativeQueryResult: report
        }).execSync();
      } catch (e) {
        return cb(e);
      }

      return cb(undefined, parsedResults.result);
    }

    // Return the results
    return cb(undefined, report.result);
  });
};
