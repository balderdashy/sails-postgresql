//  ███████╗██████╗  █████╗ ██╗    ██╗███╗   ██╗
//  ██╔════╝██╔══██╗██╔══██╗██║    ██║████╗  ██║
//  ███████╗██████╔╝███████║██║ █╗ ██║██╔██╗ ██║
//  ╚════██║██╔═══╝ ██╔══██║██║███╗██║██║╚██╗██║
//  ███████║██║     ██║  ██║╚███╔███╔╝██║ ╚████║
//  ╚══════╝╚═╝     ╚═╝  ╚═╝ ╚══╝╚══╝ ╚═╝  ╚═══╝
//
//   ██████╗ ██████╗ ███╗   ██╗███╗   ██╗███████╗ ██████╗████████╗██╗ ██████╗ ███╗   ██╗
//  ██╔════╝██╔═══██╗████╗  ██║████╗  ██║██╔════╝██╔════╝╚══██╔══╝██║██╔═══██╗████╗  ██║
//  ██║     ██║   ██║██╔██╗ ██║██╔██╗ ██║█████╗  ██║        ██║   ██║██║   ██║██╔██╗ ██║
//  ██║     ██║   ██║██║╚██╗██║██║╚██╗██║██╔══╝  ██║        ██║   ██║██║   ██║██║╚██╗██║
//  ╚██████╗╚██████╔╝██║ ╚████║██║ ╚████║███████╗╚██████╗   ██║   ██║╚██████╔╝██║ ╚████║
//   ╚═════╝ ╚═════╝ ╚═╝  ╚═══╝╚═╝  ╚═══╝╚══════╝ ╚═════╝   ╚═╝   ╚═╝ ╚═════╝ ╚═╝  ╚═══╝
//
// Instantiate a new connection from the connection manager.

var PG = require('machinepack-postgresql');

module.exports = function spawnConnection(datastore, cb) {
  // Validate datastore
  if (!datastore || !datastore.manager || !datastore.config) {
    return cb(new Error('Spawn Connection requires a valid datastore.'));
  }

  PG.getConnection({
    manager: datastore.manager,
    meta: datastore.config
  })
  .switch({
    error: function error(err) {
      return cb(err);
    },
    failed: function failedToConnect(err) {
      // Setup some basic troubleshooting tips
      console.error('Troubleshooting tips:');
      console.error('');

      // Used below to indicate whether the error is potentially related to config
      // (in which case we'll display a generic message explaining how to configure all the things)
      var isPotentiallyConfigRelated;
      isPotentiallyConfigRelated = true;

      // Determine whether localhost is being used
      var usingLocalhost = !!(function checkForLocalhost() {
        try {
          var LOCALHOST_REGEXP = /(localhost|127\.0\.0\.1)/;
          if (datastore.config.url.match(LOCALHOST_REGEXP)) {
            return true;
          }
        } catch (e) {
          // Ignore error
        }
      })();

      if (usingLocalhost) {
        console.error(
          ' -> You appear to be trying to use a Postgresql install on localhost.',
          'Maybe the database server isn\'t running, or is not installed?'
        );
        console.error('');
      }

      if (isPotentiallyConfigRelated) {
        console.error(
        ' -> Is your Postgresql configuration correct?  Maybe your `poolSize` configuration is set too high?',
        'e.g. If your Postgresql database only supports 20 concurrent connections, you should make sure',
        'you have your `poolSize` set as something < 20 (see http://stackoverflow.com/a/27387928/486547).',
        'The default `poolSize` is 10.',
        'To override default settings, specify the desired properties on the relevant Postgresql',
        '"connection" config object where the host/port/database/etc. are configured.',
        'If you\'re using Sails, this is generally located in `config/datastores.js`,',
        'or wherever your environment-specific database configuration is set.'
        );
        console.error('');
      }

      // TODO: negotiate "Too many connections" error
      var tooManyConnections = true;
      if (tooManyConnections) {
        console.error(
        ' -> Maybe your `poolSize` configuration is set too high?',
        'e.g. If your Postgresql database only supports 20 concurrent connections, you should make sure',
        'you have your `poolSize` set as something < 20 (see http://stackoverflow.com/a/27387928/486547).',
        'The default `poolSize` is 10.');
        console.error('');
      }

      if (tooManyConnections && !usingLocalhost) {
        console.error(
        ' -> Do you have multiple Sails instances sharing the same Postgresql database?',
        'Each Sails instance may use up to the configured `poolSize` # of connections.',
        'Assuming all of the Sails instances are just copies of one another (a reasonable best practice)',
        'we can calculate the actual # of Postgresql connections used (C) by multiplying the configured `poolSize` (P)',
        'by the number of Sails instances (N).',
        'If the actual number of connections (C) exceeds the total # of **AVAILABLE** connections to your',
        'Postgresql database (V), then you have problems.  If this applies to you, try reducing your `poolSize`',
        'configuration. A reasonable `poolSize` setting would be V/N.'
        );
        console.error('');
      }

      // TODO: negotiate the error code here to make the heuristic more helpful
      var isSSLRelated = !usingLocalhost;
      if (isSSLRelated) {
        console.error(' -> Are you using an SSL-enabled Postgresql host like Heroku?',
        'Make sure to set `ssl` to `true` (see http://stackoverflow.com/a/22177218/486547)'
        );
        console.error('');
      }

      console.error('');

      return cb(err);
    },
    success: function success(connection) {
      return cb(null, connection.connection);
    }
  });
};
