//  ███████╗███████╗████████╗    ███████╗███████╗ ██████╗ ██╗   ██╗███████╗███╗   ██╗ ██████╗███████╗
//  ██╔════╝██╔════╝╚══██╔══╝    ██╔════╝██╔════╝██╔═══██╗██║   ██║██╔════╝████╗  ██║██╔════╝██╔════╝
//  ███████╗█████╗     ██║       ███████╗█████╗  ██║   ██║██║   ██║█████╗  ██╔██╗ ██║██║     █████╗
//  ╚════██║██╔══╝     ██║       ╚════██║██╔══╝  ██║▄▄ ██║██║   ██║██╔══╝  ██║╚██╗██║██║     ██╔══╝
//  ███████║███████╗   ██║       ███████║███████╗╚██████╔╝╚██████╔╝███████╗██║ ╚████║╚██████╗███████╗
//  ╚══════╝╚══════╝   ╚═╝       ╚══════╝╚══════╝ ╚══▀▀═╝  ╚═════╝ ╚══════╝╚═╝  ╚═══╝ ╚═════╝╚══════╝
//
//  ██╗   ██╗ █████╗ ██╗     ██╗   ██╗███████╗███████╗
//  ██║   ██║██╔══██╗██║     ██║   ██║██╔════╝██╔════╝
//  ██║   ██║███████║██║     ██║   ██║█████╗  ███████╗
//  ╚██╗ ██╔╝██╔══██║██║     ██║   ██║██╔══╝  ╚════██║
//   ╚████╔╝ ██║  ██║███████╗╚██████╔╝███████╗███████║
//    ╚═══╝  ╚═╝  ╚═╝╚══════╝ ╚═════╝ ╚══════╝╚══════╝
//
// Ensures that all Postgres sequences are up to date with the latest values.
// This could happen when a value is manually defined for a sequence value.

var _ = require('lodash');
var async = require('async');
var runQuery = require('../query/run-query');
var rollbackAndRelease = require('../connection/rollback-and-release');

module.exports = function setSequenceValues(options, cb) {
  //  ╦  ╦╔═╗╦  ╦╔╦╗╔═╗╔╦╗╔═╗  ┌─┐┌─┐┌┬┐┬┌─┐┌┐┌┌─┐
  //  ╚╗╔╝╠═╣║  ║ ║║╠═╣ ║ ║╣   │ │├─┘ │ ││ ││││└─┐
  //   ╚╝ ╩ ╩╩═╝╩═╩╝╩ ╩ ╩ ╚═╝  └─┘┴   ┴ ┴└─┘┘└┘└─┘
  if (_.isUndefined(options) || !_.isPlainObject(options)) {
    throw new Error('Invalid options argument. Options must contain: connection, query, model, schemaName, leased, and tableName.');
  }

  if (!_.has(options, 'connection') || !_.isObject(options.connection)) {
    throw new Error('Invalid option used in options argument. Missing or invalid connection.');
  }

  if (!_.has(options, 'sequences') || !_.isArray(options.sequences)) {
    throw new Error('Invalid option used in options argument. Missing or invalid sequences.');
  }

  if (!_.has(options, 'record') || !_.isPlainObject(options.record)) {
    throw new Error('Invalid option used in options argument. Missing or invalid record.');
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


  async.each(options.sequences, function setSequence(item, next) {
    var sequenceName = '\"' + options.schemaName + '\".\"' + options.tableName + '_' + item + '_seq' + '\"';
    var sequenceValue = options.record[item];
    var sequenceQuery = 'SELECT setval(' + sequenceName + ', ' + sequenceValue + ', true)';

    // Run Sequence Query
    runQuery({
      connection: options.connection,
      nativeQuery: sequenceQuery,
      disconnectOnError: false
    }, next);
  },

  function doneWithSequences(err) {
    if (err) {
      rollbackAndRelease(options.connection, options.leased, function rollbackCb(err) {
        if (err) {
          return cb(new Error('There was an error rolling back and releasing the connection.' + err.stack));
        }

        return cb(new Error('There was an error incrementing a sequence on the create.' + err.stack));
      });
      return;
    }

    return cb();
  });
};
