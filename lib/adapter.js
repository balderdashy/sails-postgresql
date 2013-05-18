/*---------------------------------------------------------------
  :: sails-postgresql
  -> adapter
---------------------------------------------------------------*/

// Dependencies
var pg = require('pg'),
    _ = require('underscore'),
    async = require('async'),
    Query = require('./query'),
    utils = require('./utils');

module.exports = (function() {

  // Keep track of all the dbs used by the app
  var dbs = {};

  var adapter = {
    identity: 'sails-postgresql',

    syncable: true,

    commitLog: {},

    defaults: {
      host: 'localhost',
      port: 5432,
      pool: false
    },

    /*************************************************************************/
    /* Public Methods for Sails/Waterline Adapter Compatibility              */
    /*************************************************************************/

    // Register a new DB Collection
    registerCollection: function(collection, cb) {
      dbs[collection.identity] = _.find(dbs, function(db) {
        return collection.host === db.host && collection.database === db.database;
      });

      if(!dbs[collection.identity]) {
        dbs[collection.identity] = utils.marshalConfig(collection);
        return cb();
      } else return cb();
    },

    // Teardown
    teardown: function(cb) {
      cb();
    },

    // Raw Query Interface
    query: function(table, query, data, cb) {

      if (_.isFunction(data)) {
        cb = data;
        data = null;
      }

      spawnConnection(function __QUERY__(client, cb) {

        // Run query
        if (data) client.query(query, data, cb);
        else client.query(query, cb);

      }, dbs[table], cb);
    },

    // Describe a table
    describe: function(table, cb) {
      spawnConnection(function __DESCRIBE__(client, cb) {

        // Build Query
        var query = "SELECT column_name, data_type, character_maximum_length FROM information_schema.columns WHERE table_name = '" + table + "';";

        // Run Query
        client.query(query, function(err, result) {
          if(err) return cb();
          if(result.rows.length === 0) return cb();
          cb(null, utils.normalizeSchema(result.rows));
        });

      }, dbs[table], cb);
    },

    // Create a new table
    define: function(table, definition, cb) {
      spawnConnection(function __DEFINE__(client, cb) {

        // Escape Table Name
        table = utils.escapeTable(table);

        // Iterate through each attribute, building a query string
        var schema = utils.buildSchema(definition.attributes);

        // Build Query
        var query = 'CREATE TABLE ' + table + ' (' + schema + ')';

        // Run Query
        client.query(query, function __DEFINE__(err, result) {
          if(err) return cb(err);
          cb(null, result);
        });

      }, dbs[table], cb);
    },

    // Drop a table
    drop: function(table, cb) {
      spawnConnection(function __DROP__(client, cb) {

        // Escape Table Name
        table = utils.escapeTable(table);

        // Build Query
        var query = 'DROP TABLE ' + table + ';';

        // Run Query
        client.query(query, function __DROP__(err, result) {
          if(err) result = null;
          cb(null, result);
        });

      }, dbs[table], cb);
    },

    // Add a column to a table
    addAttribute: function(table, attrName, attrDef, cb) {
      spawnConnection(function __ADD_ATTRIBUTE__(client, cb) {

        // Escape Table Name
        table = utils.escapeTable(table);

        // Setup a Schema Definition
        var attrs = {};
        attrs[attrName] = attrDef;

        var schema = utils.buildSchema(attrs);

        // Build Query
        var query = 'ALTER TABLE ' + table + ' ADD COLUMN ' + schema;

        // Run Query
        client.query(query, function __ADD_ATTRIBUTE__(err, result) {
          if(err) return cb(err);
          cb(null, result.rows);
        });

      }, dbs[table], cb);
    },

    // Remove a column from a table
    removeAttribute: function (table, attrName, cb) {
      spawnConnection(function __REMOVE_ATTRIBUTE__(client, cb) {

        // Escape Table Name
        table = utils.escapeTable(table);

        // Build Query
        var query = 'ALTER TABLE ' + table + ' DROP COLUMN "' + attrName + '" RESTRICT';

        // Run Query
        client.query(query, function __REMOVE_ATTRIBUTE__(err, result) {
          if(err) return cb(err);
          cb(null, result.rows);
        });

      }, dbs[table], cb);
    },

    // Add a new row to the table
    create: function(table, data, cb) {
      spawnConnection(function __CREATE__(client, cb) {

        // Escape Table Name
        table = utils.escapeTable(table);

        // Transform the Data object into arrays used in a parameterized query
        var attributes = utils.mapAttributes(data),
            columnNames = attributes.keys.join(', '),
            paramValues = attributes.params.join(', ');

        // Build Query
        var query = 'INSERT INTO ' + table + ' (' + columnNames + ') values (' + paramValues + ') RETURNING *';

        // Run Query
        client.query(query, attributes.values, function __CREATE__(err, result) {
          if(err) return cb(err);
          cb(null, result.rows[0]);
        });

      }, dbs[table], cb);
    },

    // Add a multiple rows to the table
    createEach: function(table, records, cb) {
      spawnConnection(function __CREATE_EACH__(client, cb) {

        // Escape Table Name
        table = utils.escapeTable(table);

        // Collect Query Results
        var results = [];

        // Simple way for now, in the future make this more awesome
        async.each(records, function(data, cb) {

          // Transform the data object into arrays for parameterized query
          var attributes = utils.mapAttributes(data),
              columnNames = attributes.keys.join(', '),
              paramValues = attributes.params.join(', ');

          // Build Query
          var query = 'INSERT INTO ' + table + ' (' + columnNames + ') values (' + paramValues + ') RETURNING *;';

          // Run Query
          client.query(query, attributes.values, function __CREATE_EACH__(err, result) {
            if(err) return cb(err);
            results.push(result.rows[0]);
            cb();
          });

        }, function(err) {
          if(err) return cb(err);
          cb(null, results);
        });

      }, dbs[table], cb);
    },

    // Select Query Logic
    find: function(table, options, cb) {
      spawnConnection(function __FIND__(client, cb) {

        // Escape Table Name
        table = utils.escapeTable(table);

        // Build Query
        var query = new Query().find(table, options);

        // Run Query
        client.query(query.query, query.values, function __FIND__(err, result) {
          if(err) return cb(err);
          cb(null, result.rows);
        });

      }, dbs[table], cb);
    },

    // Stream one or more models from the collection
    stream: function(table, options, stream) {

      var dbConfig = {
        database: dbs[table].database,
        host: dbs[table].host,
        user: dbs[table].user,
        password: dbs[table].password,
        port: dbs[table].port
      };

      var client = new pg.Client(dbConfig);
      client.connect();

      // Escape Table Name
      table = utils.escapeTable(table);

      // Build Query
      var query = new Query().find(table, options);

      // Run Query
      var dbStream = client.query(query.query, query.values);

      //can stream row results back 1 at a time
      dbStream.on('row', function(row) {
        stream.write(row);
      });

      dbStream.on('error', function(err) {
        stream.end(); // End stream
        client.end(); // Close Connection
      });

      //fired after last row is emitted
      dbStream.on('end', function() {
        stream.end(); // End stream
        client.end(); // Close Connection
      });

    },

    // Update one or more models in the collection
    update: function(table, options, data, cb) {
      spawnConnection(function __UPDATE__(client, cb) {

        // Escape Table Name
        table = utils.escapeTable(table);

        // Build Query
        var query = new Query().update(table, options, data);

        // Run Query
        client.query(query.query, query.values, function __UPDATE__(err, result) {
          if(err) return cb(err);
          cb(null, result.rows);
        });

      }, dbs[table], cb);
    },

    // Delete one or more models from the collection
    destroy: function(table, options, cb) {
      spawnConnection(function __DELETE__(client, cb) {

        // Escape Table Name
        table = utils.escapeTable(table);

        // Build Query
        var query = new Query().destroy(table, options);

        // Run Query
        client.query(query.query, query.values, function __DELETE__(err, result) {
          if(err) return cb(err);
          cb(null, result.rows);
        });

      }, dbs[table], cb);
    }

  };

  /*************************************************************************/
  /* Private Methods
  /*************************************************************************/

  // Wrap a function in the logic necessary to provision a connection
  // (grab from the pool or create a client)
  function spawnConnection(logic, config, cb) {

    // Use a new Client
    if(!config.pool) {

      var dbConfig = {
        database: config.database,
        host: config.host,
        user: config.user,
        password: config.password,
        port: config.port
      };

      var client = new pg.Client(dbConfig);
      client.connect(function(err) {
        after(err, client);
      });
    }

    // Use connection pooling
    else {

    }

    // Run logic using connection, then release/close it
    function after(err, client) {
      if(err) {
        console.error("Error creating a connection to Postgresql: " + err);
        return cb(err);
      }

      logic(client, function(err, result) {
        if(err) console.error("Error running function: " + err);

        // close client connection
        client.end();

        return cb(err, result);
      });
    }
  }

  return adapter;
})();