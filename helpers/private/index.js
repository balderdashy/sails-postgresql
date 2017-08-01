module.exports = {
  // Helpers for handling connections
  connection: {
    createManager: require('./connection/create-manager'),
    destroyManager: require('./connection/destroy-manager'),
    releaseConnection: require('./connection/release-connection'),
    spawnConnection: require('./connection/spawn-connection'),
    spawnOrLeaseConnection: require('./connection/spawn-or-lease-connection')
  },

  // Helpers for handling query logic
  query: {
    compileStatement: require('./query/compile-statement'),
    initializeQueryCache: require('./query/initialize-query-cache'),
    modifyRecord: require('./query/modify-record'),
    preProcessEachRecord: require('./query/pre-process-each-record'),
    processEachRecord: require('./query/process-each-record'),
    runNativeQuery: require('./query/run-native-query'),
    runQuery: require('./query/run-query')
  },

  // Helpers for dealing with underlying database schema
  schema: {
    buildIndexes: require('./schema/build-indexes'),
    buildSchema: require('./schema/build-schema'),
    createNamespace: require('./schema/create-namespace'),
    escapeTableName: require('./schema/escape-table-name')
  }
};
