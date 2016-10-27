//  ██╗   ██╗██████╗ ██████╗  █████╗ ████████╗███████╗
//  ██║   ██║██╔══██╗██╔══██╗██╔══██╗╚══██╔══╝██╔════╝
//  ██║   ██║██████╔╝██║  ██║███████║   ██║   █████╗
//  ██║   ██║██╔═══╝ ██║  ██║██╔══██║   ██║   ██╔══╝
//  ╚██████╔╝██║     ██████╔╝██║  ██║   ██║   ███████╗
//   ╚═════╝ ╚═╝     ╚═════╝ ╚═╝  ╚═╝   ╚═╝   ╚══════╝
//
//  ██████╗ ███████╗ ██████╗ ██████╗ ██████╗ ██████╗ ███████╗
//  ██╔══██╗██╔════╝██╔════╝██╔═══██╗██╔══██╗██╔══██╗██╔════╝
//  ██████╔╝█████╗  ██║     ██║   ██║██████╔╝██║  ██║███████╗
//  ██╔══██╗██╔══╝  ██║     ██║   ██║██╔══██╗██║  ██║╚════██║
//  ██║  ██║███████╗╚██████╗╚██████╔╝██║  ██║██████╔╝███████║
//  ╚═╝  ╚═╝╚══════╝ ╚═════╝ ╚═════╝ ╚═╝  ╚═╝╚═════╝ ╚══════╝
//
// Updates a set of records based on the criteria. One thing to note is that the
// Waterline API requires the adapter to send back the complete records that
// were updated by the query. In Postgres this is OK because it supports a robust
// "RESTURNING *" feature.

var runNativeQuery = require('./run-native-query');

module.exports = function updateRecords(connection, query, cb) {
  runNativeQuery(connection, query, function runNativeQueryCb(err, report) {
    if (err) {
      return cb(err);
    }

    return cb(null, report);
  });
};
