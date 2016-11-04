//  ██╗███╗   ██╗███████╗███████╗██████╗ ████████╗
//  ██║████╗  ██║██╔════╝██╔════╝██╔══██╗╚══██╔══╝
//  ██║██╔██╗ ██║███████╗█████╗  ██████╔╝   ██║
//  ██║██║╚██╗██║╚════██║██╔══╝  ██╔══██╗   ██║
//  ██║██║ ╚████║███████║███████╗██║  ██║   ██║
//  ╚═╝╚═╝  ╚═══╝╚══════╝╚══════╝╚═╝  ╚═╝   ╚═╝
//
//  ██████╗ ███████╗ ██████╗ ██████╗ ██████╗ ██████╗
//  ██╔══██╗██╔════╝██╔════╝██╔═══██╗██╔══██╗██╔══██╗
//  ██████╔╝█████╗  ██║     ██║   ██║██████╔╝██║  ██║
//  ██╔══██╗██╔══╝  ██║     ██║   ██║██╔══██╗██║  ██║
//  ██║  ██║███████╗╚██████╗╚██████╔╝██║  ██║██████╔╝
//  ╚═╝  ╚═╝╚══════╝ ╚═════╝ ╚═════╝ ╚═╝  ╚═╝╚═════╝
//
// Insert the record and return the values that were inserted.

var util = require('util');
var _ = require('lodash');
var runQuery = require('./run-query');
var compileStatement = require('./compile-statement');
var releaseConnection = require('../connection/release-connection');
var findPrimaryKey = require('../schema/find-primary-key');


module.exports = function insertRecord(options, cb) {
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

  if (!_.has(options, 'model') || !_.isPlainObject(options.model)) {
    throw new Error('Invalid option used in options argument. Missing or invalid model.');
  }

  if (!_.has(options, 'schemaName') || !_.isString(options.schemaName)) {
    throw new Error('Invalid option used in options argument. Missing or invalid schemaName.');
  }

  if (!_.has(options, 'tableName') || !_.isString(options.tableName)) {
    throw new Error('Invalid option used in options argument. Missing or invalid tableName.');
  }

  if (!_.has(options, 'leased') || !_.isBoolean(options.leased)) {
    throw new Error('Invalid option used in options argument. Missing or invalid leased flag.');
  }


  //  ╦╔╗╔╔═╗╔═╗╦═╗╔╦╗  ┌─┐ ┬ ┬┌─┐┬─┐┬ ┬
  //  ║║║║╚═╗║╣ ╠╦╝ ║   │─┼┐│ │├┤ ├┬┘└┬┘
  //  ╩╝╚╝╚═╝╚═╝╩╚═ ╩   └─┘└└─┘└─┘┴└─ ┴
  runQuery({
    connection: options.connection,
    nativeQuery: options.query,
    queryType: 'insert',
    disconnectOnError: false
  },

  function runQueryCb(err, insertReport) {
    // If the query failed to run, release the connection.
    if (err) {
      releaseConnection(options.connection, options.leased, function _rollbackCB() {
        return cb(new Error('There was an error attempting to run the query: ' + '\n\n' + util.inspect(options.query.sql, false, null) + '\nusing values: (' + util.inspect(options.query.bindings, false, null) + ')'));
      });

      return;
    }

    // Hold the results of the insert query
    var insertResults = insertReport.result;


    //  ╔═╗╦╔╗╔╔╦╗  ┬┌┐┌┌─┐┌─┐┬─┐┌┬┐┌─┐┌┬┐  ┬─┐┌─┐┌─┐┌─┐┬─┐┌┬┐┌─┐
    //  ╠╣ ║║║║ ║║  ││││└─┐├┤ ├┬┘ │ ├┤  ││  ├┬┘├┤ │  │ │├┬┘ ││└─┐
    //  ╚  ╩╝╚╝═╩╝  ┴┘└┘└─┘└─┘┴└─ ┴ └─┘─┴┘  ┴└─└─┘└─┘└─┘┴└──┴┘└─┘

    // Find the Primary Key field for the model
    var pk;
    try {
      pk = findPrimaryKey(options.model.definition);
    } catch (e) {
      throw new Error('Could not determine a Primary Key for the model: ' + options.model.tableName + '.');
    }

    // Build up a criteria statement to run
    var criteriaStatement = {
      select: ['*'],
      from: options.tableName,
      where: {},
      opts: {
        schema: options.schemaName
      }
    };

    // Insert dynamic primary key value into query
    criteriaStatement.where[pk] = {
      in: insertResults.inserted
    };

    // Build an IN query from the results of the insert query
    var compiledReport;
    try {
      compiledReport = compileStatement(criteriaStatement);
    } catch (e) {
      return cb(new Error('There was an error compiling the statement into a query.\n\n' + util.inspect(criteriaStatement, false, null)));
    }

    // Run the FIND query
    runQuery({
      connection: options.connection,
      nativeQuery: compiledReport,
      queryType: 'select',
      disconnectOnError: false
    },

    function runFindQueryCb(err, findReport) {
      // If the query failed to run, rollback the transaction and release the connection.
      if (err) {
        releaseConnection(options.connection, options.leased, function rollbackCb() {
          return cb(new Error('There was an error attempting to run the query: ' + '\n\n' + util.inspect(compiledReport.sql) + '\nusing values: (' + util.inspect(compiledReport.bindings) + ')'));
        });

        return;
      }

      // Return the FIND results
      return cb(null, findReport.result);
    });
  });
};
