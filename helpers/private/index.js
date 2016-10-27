module.exports = {
  // Helpers for handling connections
  connection: {
    beginTransaction: require('./connection/begin-transaction'),
    commitAndRelease: require('./connection/commit-and-release'),
    createManager: require('./connection/create-manager'),
    destroyManager: require('./connection/destroy-manager'),
    releaseConnection: require('./connection/release-connection'),
    rollbackAndRelease: require('./connection/rollback-and-release'),
    spawnConnection: require('./connection/spawn-connection'),
    spawnTransaction: require('./connection/spawn-transaction')
  },

  // Helpers for handling query logic
  query: {
    compileStatement: require('./query/compile-statement'),
    initializeQueryCache: require('./query/initialize-query-cache'),
    insertRecord: require('./query/insert-record'),
    runNativeQuery: require('./query/run-native-query'),
    runQuery: require('./query/run-query'),
    updateRecord: require('./query/update-record')
  },

  // Helpers for dealing with underlying database schema
  schema: {
    buildIndexes: require('./schema/build-indexes'),
    buildSchema: require('./schema/build-schema'),
    createNamespace: require('./schema/create-namespace'),
    escapeTableName: require('./schema/escape-table-name'),
    findPrimaryKey: require('./schema/find-primary-key'),
    setSequenceValues: require('./schema/set-sequence-values')
  }
};
