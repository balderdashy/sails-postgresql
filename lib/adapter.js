/*---------------------------------------------------------------
  :: sails-postgresql
  -> adapter
---------------------------------------------------------------*/

// Dependencies
var pg = require('pg'),
    _ = require('lodash'),
    async = require('async'),
    Query = require('./query'),
    utils = require('./utils');

module.exports = (function() {

  // Keep track of all the connections used by the app
  var connections = {};

  var adapter = {
    identity: 'sails-postgresql',

    // Which type of primary key is used by default
    pkFormat: 'integer',

    syncable: true,

    defaults: {
      host: 'localhost',
      port: 5432,
      schema: true,
      ssl: false
    },

    /*************************************************************************/
    /* Public Methods for Sails/Waterline Adapter Compatibility              */
    /*************************************************************************/

    // Register a new DB Connection
    registerConnection: function(connection, collections, cb) {

      if(!connection.identity) return cb(new Error('Connection is missing an identity'));
      if(connections[connection.identity]) return cb(new Error('Connection is already registered'));

      // Store the connection
      connections[connection.identity] = {
        config: connection,
        collections: collections
      };

      return cb();
    },

    // Teardown
    teardown: function(connectionName, cb) {
      if(!connections[connectionName]) return cb();
      delete connections[connectionName];
      cb();
    },

    // Raw Query Interface
    query: function(connectionName, table, query, data, cb) {

      if (_.isFunction(data)) {
        cb = data;
        data = null;
      }

      spawnConnection(connectionName, function __QUERY__(client, cb) {

        // Run query
        if (data) client.query(query, data, cb);
        else client.query(query, cb);

      }, cb);
    },

    // Describe a table
    describe: function(connectionName, table, cb) {
      spawnConnection(connectionName, function __DESCRIBE__(client, cb) {

        var connectionObject = connections[connectionName];
        var collection = connectionObject.collections[table];

        // Build query to get a bunch of info from the information_schema
        // It's not super important to understand it only that it returns the following fields:
        // [Table, #, Column, Type, Null, Constraint, C, consrc, F Key, Default]
        var query = "SELECT x.nspname || '.' || x.relname as \"Table\", x.attnum as \"#\", x.attname as \"Column\", x.\"Type\"," +
          " case x.attnotnull when true then 'NOT NULL' else '' end as \"NULL\", r.conname as \"Constraint\", r.contype as \"C\", " +
          "r.consrc, fn.nspname || '.' || f.relname as \"F Key\", d.adsrc as \"Default\" FROM (" +
          "SELECT c.oid, a.attrelid, a.attnum, n.nspname, c.relname, a.attname, pg_catalog.format_type(a.atttypid, a.atttypmod) as \"Type\", " +
          "a.attnotnull FROM pg_catalog.pg_attribute a, pg_namespace n, pg_class c WHERE a.attnum > 0 AND NOT a.attisdropped AND a.attrelid = c.oid " +
          "and c.relkind not in ('S','v') and c.relnamespace = n.oid and n.nspname not in ('pg_catalog','pg_toast','information_schema')) x " +
          "left join pg_attrdef d on d.adrelid = x.attrelid and d.adnum = x.attnum " +
          "left join pg_constraint r on r.conrelid = x.oid and r.conkey[1] = x.attnum " +
          "left join pg_class f on r.confrelid = f.oid " +
          "left join pg_namespace fn on f.relnamespace = fn.oid " +
          "where x.relname = '" + table + "' order by 1,2;";

        // Get Sequences to test if column auto-increments
        var autoIncrementQuery = "SELECT t.relname as related_table, a.attname as related_column, s.relname as sequence_name " +
          "FROM pg_class s JOIN pg_depend d ON d.objid = s.oid JOIN pg_class t ON d.objid = s.oid AND d.refobjid = t.oid " +
          "JOIN pg_attribute a ON (d.refobjid, d.refobjsubid) = (a.attrelid, a.attnum) JOIN pg_namespace n ON n.oid = s.relnamespace " +
          "WHERE s.relkind = 'S' AND n.nspname = 'public';";

        // Get Indexes
        var indiciesQuery = "SELECT n.nspname as \"Schema\", c.relname as \"Name\", CASE c.relkind WHEN 'r' THEN 'table' " +
          "WHEN 'v' THEN 'view' WHEN 'i' THEN 'index' WHEN 'S' THEN 'sequence' WHEN 's' THEN 'special' WHEN 'f' THEN " +
          "'foreign table' END as \"Type\", pg_catalog.pg_get_userbyid(c.relowner) as \"Owner\", c2.relname as \"Table\" " +
          "FROM pg_catalog.pg_class c LEFT JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace " +
          "LEFT JOIN pg_catalog.pg_index i ON i.indexrelid = c.oid " +
          "LEFT JOIN pg_catalog.pg_class c2 ON i.indrelid = c2.oid " +
          "WHERE c.relkind IN ('i','') AND n.nspname <> 'pg_catalog' AND n.nspname <> 'information_schema' " +
          "AND n.nspname !~ '^pg_toast' AND pg_catalog.pg_table_is_visible(c.oid) ORDER BY 1,2;";

        // Run Info Query
        client.query(query, function(err, result) {
          if(err) return cb();
          if(result.rows.length === 0) return cb();

          // Run Query to get Auto Incrementing sequences
          client.query(autoIncrementQuery, function(err, aResult) {
            if(err) return cb();

            aResult.rows.forEach(function(row) {
              if(row.related_table !== table) return;

              // Look through query results and see if related_column exists
              result.rows.forEach(function(column) {
                if(column.Column !== row.related_column) return;
                column.autoIncrement = true;
              });
            });

            // Run Query to get Indexed values
            client.query(indiciesQuery, function(err, iResult) {
              if(err) return cb(err);

              // Loop through indicies and see if any match
              iResult.rows.forEach(function(column) {
                var key = column.Name.split('_index_')[1];

                // Look through query results and see if key exists
                result.rows.forEach(function(column) {
                  if(column.Column !== key) return;
                  column.indexed = true;
                });
              });

              // Normalize Schema
              var normalizedSchema = utils.normalizeSchema(result.rows);

              // Set Internal Schema Mapping
              collection.schema = normalizedSchema;

              cb(null, normalizedSchema);
            });
          });

        });

      }, cb);
    },

    // Create a new table
    define: function(connectionName, table, definition, cb) {

      // Create a describe method to run after the define.
      // Ensures the define connection is properly closed.
      var describe = function(err, result) {
        if(err) return cb(err);

        // Describe (sets schema)
        adapter.describe(connectionName, table.replace(/["']/g, ""), cb);
      };

      spawnConnection(connectionName, function __DEFINE__(client, cb) {

        // Escape Table Name
        table = utils.escapeName(table);

        // Iterate through each attribute, building a query string
        var _schema = utils.buildSchema(definition);

        // Check for any Index attributes
        var indexes = utils.buildIndexes(definition);

        // Build Query
        var query = 'CREATE TABLE ' + table + ' (' + _schema + ')';

        // Run Query
        client.query(query, function __DEFINE__(err, result) {
          if(err) return cb(err);

          // Build Indexes
          function buildIndex(name, cb) {

            // Strip slashes from table name, used to namespace index
            var cleanTable = table.replace(/['"]/g, '');

            // Build a query to create a namespaced index tableName_key
            var query = 'CREATE INDEX ' + cleanTable + '_' + name + ' on ' + table + ' (' + name + ');';

            // Run Query
            client.query(query, function(err, result) {
              if(err) return cb(err);
              cb();
            });
          }

          // Build indexes in series
          async.eachSeries(indexes, buildIndex, cb);
        });

      }, describe);
    },

    // Drop a table
    drop: function(connectionName, table, relations, cb) {

      if(typeof relations === 'function') {
        cb = relations;
        relations = [];
      }

      spawnConnection(connectionName, function __DROP__(client, cb) {

        // Drop any relations
        function dropTable(item, next) {

          // Build Query
          var query = 'DROP TABLE ' + utils.escapeName(item) + ';';

          // Run Query
          client.query(query, function __DROP__(err, result) {
            if(err) result = null;
            next(null, result);
          });
        }

        async.eachSeries(relations, dropTable, function(err) {
          if(err) return cb(err);
          dropTable(table, cb);
        });

      }, cb);
    },

    // Add a column to a table
    addAttribute: function(connectionName, table, attrName, attrDef, cb) {
      spawnConnection(connectionName, function __ADD_ATTRIBUTE__(client, cb) {

        // Escape Table Name
        table = utils.escapeName(table);

        // Setup a Schema Definition
        var attrs = {};
        attrs[attrName] = attrDef;

        var _schema = utils.buildSchema(attrs);

        // Build Query
        var query = 'ALTER TABLE ' + table + ' ADD COLUMN ' + _schema;

        // Run Query
        client.query(query, function __ADD_ATTRIBUTE__(err, result) {
          if(err) return cb(err);
          cb(null, result.rows);
        });

      }, cb);
    },

    // Remove a column from a table
    removeAttribute: function (connectionName, table, attrName, cb) {
      spawnConnection(connectionName, function __REMOVE_ATTRIBUTE__(client, cb) {

        // Escape Table Name
        table = utils.escapeName(table);

        // Build Query
        var query = 'ALTER TABLE ' + table + ' DROP COLUMN "' + attrName + '" RESTRICT';

        // Run Query
        client.query(query, function __REMOVE_ATTRIBUTE__(err, result) {
          if(err) return cb(err);
          cb(null, result.rows);
        });

      }, cb);
    },

    // Add a new row to the table
    create: function(connectionName, table, data, cb) {
      spawnConnection(connectionName, function __CREATE__(client, cb) {

        var connectionObject = connections[connectionName];
        var collection = connectionObject.collections[table];

        // Build a Query Object
        var _query = new Query(collection.definition);

        // Escape Table Name
        table = utils.escapeName(table);

        // Transform the Data object into arrays used in a parameterized query
        var attributes = utils.mapAttributes(data),
            columnNames = attributes.keys.join(', '),
            paramValues = attributes.params.join(', ');

        // Build Query
        var query = 'INSERT INTO ' + table + ' (' + columnNames + ') values (' + paramValues + ') RETURNING *';

        // Run Query
        client.query(query, attributes.values, function __CREATE__(err, result) {
          if(err) return cb(err);

          // Cast special values
          var values = _query.cast(result.rows[0]);

          cb(null, values);
        });

      }, cb);
    },

    // Add a multiple rows to the table
    createEach: function(connectionName, table, records, cb) {
      spawnConnection(connectionName, function __CREATE_EACH__(client, cb) {

        var connectionObject = connections[connectionName];
        var collection = connectionObject.collections[table];

        // Build a Query Object
        var _query = new Query(collection.definition);

        // Collect Query Results
        var results = [];

        // Simple way for now, in the future make this more awesome
        async.each(records, function(data, cb) {

          // Transform the data object into arrays for parameterized query
          var attributes = utils.mapAttributes(data),
              columnNames = attributes.keys.join(', '),
              paramValues = attributes.params.join(', ');

          // Build Query
          var query = 'INSERT INTO ' + utils.escapeName(table) + ' (' + columnNames +
            ') values (' + paramValues + ') RETURNING *;';

          // Run Query
          client.query(query, attributes.values, function __CREATE_EACH__(err, result) {
            if(err) return cb(err);

            // Cast special values
            var values = _query.cast(result.rows[0]);

            results.push(values);
            cb();
          });

        }, function(err) {
          if(err) return cb(err);
          cb(null, results);
        });

      }, cb);
    },

    // Select Query Logic
    find: function(connectionName, table, options, cb) {
      spawnConnection(connectionName, function __FIND__(client, cb) {

        // Check if this is an aggregate query and that there is something to return
        if(options.groupBy || options.sum || options.average || options.min || options.max) {
          if(!options.sum && !options.average && !options.min && !options.max) {
            return cb(new Error('Cannot groupBy without a calculation'));
          }
        }

        var connectionObject = connections[connectionName];
        var collection = connectionObject.collections[table];

        // Build a Query Object
        var _query = new Query(collection.definition);

        // Grab Connection Schema
        var schema = {};

        Object.keys(connectionObject.collections).forEach(function(coll) {
          schema[coll] = connectionObject.collections[coll].schema;
        });

        // Build Query
        var _schema = collection.schema;
        var queryObj = new Query(_schema, schema);
        var query = queryObj.find(table, options);

        // Run Query
        client.query(query.query, query.values, function __FIND__(err, result) {
          if(err) return cb(err);

          // Cast special values
          var values = [];

          result.rows.forEach(function(row) {
            values.push(queryObj.cast(row));
          });

          // If a join was used the values should be grouped to normalize the
          // result into objects
          var _values = options.joins ? utils.group(values) : values;

          cb(null, _values);
        });

      }, cb);
    },

    // Stream one or more models from the collection
    stream: function(collectionName, table, options, stream) {

      var connectionObject = connections[connectionName];
      var collection = connectionObject.collections[table];

      var client = new pg.Client(collection.config);
      client.connect();

      // Escape Table Name
      table = utils.escapeName(table);

      // Build Query
      var query = new Query(collection.schema).find(table, options);

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
    update: function(connectionName, table, options, data, cb) {
      spawnConnection(connectionName, function __UPDATE__(client, cb) {

        var connectionObject = connections[connectionName];
        var collection = connectionObject.collections[table];

        // Build a Query Object
        var _query = new Query(collection.definition);

        // Build Query
        var query = new Query(collection.schema).update(table, options, data);

        // Run Query
        client.query(query.query, query.values, function __UPDATE__(err, result) {
          if(err) return cb(err);

          // Cast special values
          var values = [];

          result.rows.forEach(function(row) {
            values.push(_query.cast(row));
          });

          cb(null, values);
        });

      }, cb);
    },

    // Delete one or more models from the collection
    destroy: function(connectionName, table, options, cb) {
      spawnConnection(connectionName, function __DELETE__(client, cb) {

        var connectionObject = connections[connectionName];
        var collection = connectionObject.collections[table];

        // Build Query
        var query = new Query(collection.schema).destroy(table, options);

        // Run Query
        client.query(query.query, query.values, function __DELETE__(err, result) {
          if(err) return cb(err);
          cb(null, result.rows);
        });

      }, cb);
    }

  };

  /*************************************************************************/
  /* Private Methods
  /*************************************************************************/

  // Wrap a function in the logic necessary to provision a connection
  // (grab from the pool or create a client)
  function spawnConnection(connectionName, logic, cb) {

    var connectionObject = connections[connectionName];
    if(!connectionObject) return cb(new Error('Invalid Connection Name'));

    // Grab a client instance from the client pool
    pg.connect(connectionObject.config, function(err, client, done) {
      after(err, client, done);
    });

    // Run logic using connection, then release/close it
    function after(err, client, done) {
      if(err) {
        console.error("Error creating a connection to Postgresql: " + err);

        // be sure to release connection
        done();

        return cb(err);
      }

      logic(client, function(err, result) {

        // release client connection
        done();
        return cb(err, result);
      });
    }
  }

  return adapter;
})();
