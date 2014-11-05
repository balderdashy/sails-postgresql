/*---------------------------------------------------------------
  :: sails-postgresql
  -> adapter
---------------------------------------------------------------*/

// Dependencies
var pg = require('pg.js');
var _ = require('lodash');
var url = require('url');
var async = require('async');
var Errors = require('waterline-errors').adapter;
var Sequel = require('waterline-sequel');
var utils = require('./utils');
var Processor = require('./processor');
var Cursor = require('waterline-cursor');
var hop = utils.object.hasOwnProperty;

module.exports = (function() {

  // Keep track of all the connections used by the app
  var connections = {};

  var sqlOptions = {
    parameterized: true,
    caseSensitive: true,
    escapeCharacter: '"',
    casting: true,
    canReturnValues: true,
    escapeInserts: true,
    declareDeleteAlias: false
  };

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

      var self = this;

      if(!connection.identity) return cb(Errors.IdentityMissing);
      if(connections[connection.identity]) return cb(Errors.IdentityDuplicate);

      // Store the connection
      connections[connection.identity] = {
        config: connection,
        collections: collections
      };

      // Always call describe
      async.map(Object.keys(collections), function(colName, cb){
        self.describe(connection.identity, colName, cb);
      }, cb);
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
          if(err) return cb(handleQueryError(err));
          if(result.rows.length === 0) return cb();

          // Run Query to get Auto Incrementing sequences
          client.query(autoIncrementQuery, function(err, aResult) {
            if(err) return cb(handleQueryError(err));

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
              if(err) return cb(handleQueryError(err));

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
          if(err) return cb(handleQueryError(err));

          // Build Indexes
          function buildIndex(name, cb) {

            // Strip slashes from table name, used to namespace index
            var cleanTable = table.replace(/['"]/g, '');

            // Build a query to create a namespaced index tableName_key
            var query = 'CREATE INDEX ' + utils.escapeName(cleanTable + '_' + name) + ' on ' + table + ' (' + utils.escapeName(name) + ');';

            // Run Query
            client.query(query, function(err, result) {
              if(err) return cb(handleQueryError(err));
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
          if(err) return cb(handleQueryError(err));
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
          if(err) return cb(handleQueryError(err));
          cb(null, result.rows);
        });

      }, cb);
    },

    // Add a new row to the table
    create: function(connectionName, table, data, cb) {
      spawnConnection(connectionName, function __CREATE__(client, cb) {

        var connectionObject = connections[connectionName];
        var collection = connectionObject.collections[table];

        var schemaName = collection.meta && collection.meta.schemaName ? utils.escapeName(collection.meta.schemaName) + '.' : '';
        var tableName = schemaName + utils.escapeName(table);

        // Build up a SQL Query
        var schema = collection.waterline.schema;
        var processor = new Processor(schema);
        var sequel = new Sequel(schema, sqlOptions);

        var incrementSequences = [];
        var query;

        // Build a query for the specific query strategy
        try {
          query = sequel.create(table, data);
        } catch(e) {
          return cb(e);
        }

        // Loop through all the attributes being inserted and check if a sequence was used
        Object.keys(collection.schema).forEach(function(schemaKey) {
          if(!utils.object.hasOwnProperty(collection.schema[schemaKey], 'autoIncrement')) return;
          if(Object.keys(data).indexOf(schemaKey) < 0) return;
          incrementSequences.push(schemaKey);
        });

        // Run Query
        client.query(query.query, query.values, function __CREATE__(err, result) {
          if(err) return cb(handleQueryError(err));

          // Cast special values
          var values = processor.cast(table, result.rows[0]);

          // Set Sequence value to defined value if needed
          if(incrementSequences.length === 0) return cb(null, values);

          function setSequence(item, next) {
            var sequenceName = "'\"" + table + '_' + item + '_seq' + "\"'";
            var sequenceValue = values[item];
            var sequenceQuery = 'SELECT setval(' + sequenceName + ', ' + sequenceValue + ', true)';

            client.query(sequenceQuery, function(err, result) {
              if(err) return next(err);
              next();
            });
          }

          async.each(incrementSequences, setSequence, function(err) {
            if(err) return cb(err);
            cb(null, values);
          });

        });

      }, cb);
    },

    // Add a multiple rows to the table
    createEach: function(connectionName, table, records, cb) {

      // Don't bother if there are no records to create.
      if (records.length === 0) {
        return cb();
      }

      spawnConnection(connectionName, function __CREATE_EACH__(client, cb) {

        var connectionObject = connections[connectionName];
        var collection = connectionObject.collections[table];

        var schemaName = collection.meta && collection.meta.schemaName ? utils.escapeName(collection.meta.schemaName) + '.' : '';
        var tableName = schemaName + utils.escapeName(table);

        // Build up a SQL Query
        var schema = collection.waterline.schema;
        var processor = new Processor(schema);
        var sequel = new Sequel(schema, sqlOptions);
        var incrementSequences = [];

        // Loop through all the attributes being inserted and check if a sequence was used
        Object.keys(collection.schema).forEach(function(schemaKey) {
          if(!utils.object.hasOwnProperty(collection.schema[schemaKey], 'autoIncrement')) return;
          incrementSequences.push({
            key: schemaKey,
            value: 0
          });
        });

        // Collect Query Results
        var results = [];

        // Simple way for now, in the future make this more awesome
        async.each(records, function(data, cb) {

          var query;

          // Build a query for the specific query strategy
          try {
            query = sequel.create(table, data);
          } catch(e) {
            return cb(e);
          }

          // Run Query
          client.query(query.query, query.values, function __CREATE_EACH__(err, result) {
            if(err) return cb(handleQueryError(err));

            // Cast special values
            var values = processor.cast(table, result.rows[0]);

            results.push(values);
            if(incrementSequences.length === 0) return cb(null, values);

            function checkSequence(item, next) {
              var currentValue  = item.value;
              var sequenceValue = values[item.key];

              if(currentValue < sequenceValue) {
                item.value = sequenceValue;
              }
              next();
            }

            async.each(incrementSequences, checkSequence, function(err) {
              if(err) return cb(err);
              cb(null, values);
            });
          });

        }, function(err) {
          if(err) return cb(err);
          if(incrementSequences.length === 0) return cb(null, results);

          function setSequence(item, next) {
            if (sequenceValue === 0) {return next();}
            var sequenceName = "'\"" + table + '_' + item.key + '_seq' + "\"'";
            var sequenceValue = item.value;
            var sequenceQuery = 'SELECT setval(' + sequenceName + ', ' + sequenceValue + ', true)';

            client.query(sequenceQuery, function(err, result) {
              if(err) return next(err);
              next();
            });
          }

          async.each(incrementSequences, setSequence, function(err) {
            if(err) return cb(err);
            cb(null, results);
          });
        });

      }, cb);
    },

    // Native Join Support
    join: function(connectionName, table, options, cb) {

      spawnConnection(connectionName, function __FIND__(client, done) {

        // Populate associated records for each parent result
        // (or do them all at once as an optimization, if possible)
        Cursor({

          instructions: options,
          nativeJoins: true,

          /**
           * Find some records directly (using only this adapter)
           * from the specified collection.
           *
           * @param  {String}   collectionIdentity
           * @param  {Object}   criteria
           * @param  {Function} _cb
           */
          $find: function (collectionName, criteria, _cb) {
            return adapter.find(conn, collectionIdentity, criteria, _cb, client);
          },

          /**
           * Look up the name of the primary key field
           * for the collection with the specified identity.
           *
           * @param  {String}   collectionIdentity
           * @return {String}
           */
          $getPK: function (collectionName) {
            if (!collectionName) return;
            return _getPK(connectionName, collectionName);
          },

          /**
           * Given a strategy type, build up and execute a SQL query for it.
           *
           * @param {}
           */

          $populateBuffers: function populateBuffers(options, next) {

            var buffers = options.buffers;
            var instructions = options.instructions;

            // Grab the collection by looking into the connection
            var connectionObject = connections[connectionName];
            var collection = connectionObject.collections[table];

            var parentRecords = [];
            var cachedChildren = {};

            // Grab Connection Schema
            var schema = {};

            Object.keys(connectionObject.collections).forEach(function(coll) {
              schema[coll] = connectionObject.collections[coll].schema;
            });

            // Build Query
            var _schema = collection.waterline.schema;
            var sequel = new Sequel(_schema, sqlOptions);
            var _query;

            // Build a query for the specific query strategy
            try {
              _query = sequel.find(table, instructions);
            } catch(e) {
              return next(e);
            }

            async.auto({

              processParent: function(next) {

                client.query(_query.query[0], _query.values[0], function __FIND__(err, result) {
                  if(err) return next(handleQueryError(err));

                  parentRecords = result.rows;

                  var splitChildren = function(parent, next) {
                    var cache = {};

                    _.keys(parent).forEach(function(key) {

                      // Check if we can split this on our special alias identifier '___' and if
                      // so put the result in the cache
                      var split = key.split('___');
                      if(split.length < 2) return;

                      if(!hop(cache, split[0])) cache[split[0]] = {};
                      cache[split[0]][split[1]] = parent[key];
                      delete parent[key];
                    });

                    // Combine the local cache into the cachedChildren
                    if(_.keys(cache).length > 0) {
                      _.keys(cache).forEach(function(pop) {
                        if(!hop(cachedChildren, pop)) cachedChildren[pop] = [];
                        cachedChildren[pop] = cachedChildren[pop].concat(cache[pop]);
                      });
                    }

                    next();
                  };


                  // Pull out any aliased child records that have come from a hasFK association
                  async.eachSeries(parentRecords, splitChildren, function(err) {
                    if(err) return next(err);
                    buffers.parents = parentRecords;
                    next();
                  });
                });
              },

              // Build child buffers.
              // For each instruction, loop through the parent records and build up a
              // buffer for the record.
              buildChildBuffers: ['processParent', function(next, results) {
                async.each(_.keys(instructions.instructions), function(population, nextPop) {

                  var populationObject = instructions.instructions[population];
                  var popInstructions = populationObject.instructions;
                  var pk = _getPK(connectionName, popInstructions[0].parent);

                  var alias = populationObject.strategy.strategy === 1 ? popInstructions[0].parentKey : popInstructions[0].alias;

                  // Use eachSeries here to keep ordering
                  async.eachSeries(parentRecords, function(parent, nextParent) {
                    var buffer = {
                      attrName: population,
                      parentPK: parent[pk],
                      pkAttr: pk,
                      keyName: alias
                    };

                    var records = [];

                    // Check for any cached parent records
                    if(hop(cachedChildren, alias)) {
                      cachedChildren[alias].forEach(function(cachedChild) {
                        var childVal = popInstructions[0].childKey;
                        var parentVal = popInstructions[0].parentKey;

                        if(cachedChild[childVal] !== parent[parentVal]) {
                          return;
                        }

                        // If null value for the parentVal, ignore it
                        if(parent[parentVal] === null) return;

                        records.push(cachedChild);
                      });
                    }

                    if(records.length > 0) {
                      buffer.records = records;
                    }

                    buffers.add(buffer);
                    nextParent();
                  }, nextPop);
                }, next);
              }],


              processChildren: ['buildChildBuffers', function(next, results) {

                // Remove the parent query
                _query.query.shift();

                async.each(_query.query, function(q, next) {

                  var qs = '';
                  var pk;

                  if(!Array.isArray(q.instructions)) {
                    pk = _getPK(connectionName, q.instructions.parent);
                  }
                  else if(q.instructions.length > 1) {
                    pk = _getPK(connectionName, q.instructions[0].parent);
                  }

                  parentRecords.forEach(function(parent) {
                    if(_.isNumber(parent[pk])) {
                      qs += q.qs.replace('^?^', parent[pk]) + ' UNION ALL ';
                    } else {
                      qs += q.qs.replace('^?^', "'" + parent[pk] + "'") + ' UNION ALL ';
                    }
                  });

                  // Remove the last UNION ALL
                  qs = qs.slice(0, -11);

                  // Add a final sort to the Union clause for integration
                  if(parentRecords.length > 1) {
                    qs += ' ORDER BY ';

                    if(!Array.isArray(q.instructions)) {
                      _.keys(q.instructions.criteria.sort).forEach(function(sortKey) {
                        var direction = q.instructions.criteria.sort[sortKey] === 1 ? 'ASC' : 'DESC';
                        qs += '"' + sortKey + '"' + ' ' + direction;
                      });
                    }
                    else if(q.instructions.length === 2) {
                      _.keys(q.instructions[1].criteria.sort).forEach(function(sortKey) {
                        var direction = q.instructions[1].criteria.sort[sortKey] === 1 ? 'ASC' : 'DESC';
                        qs += '"' + sortKey + '"' + ' ' + direction;
                      });
                    }
                  }

                  client.query(qs, q.values, function __FIND__(err, result) {
                    if(err) return next(handleQueryError(err));

                    var groupedRecords = {};

                    result.rows.forEach(function(row) {

                      if(!Array.isArray(q.instructions)) {

                        if(!hop(groupedRecords, row[q.instructions.childKey])) {
                          groupedRecords[row[q.instructions.childKey]] = [];
                        }

                        groupedRecords[row[q.instructions.childKey]].push(row);
                      }
                      else {

                        // Grab the special "foreign key" we attach and make sure to remove it
                        var fk = '___' + q.instructions[0].childKey;

                        if(!hop(groupedRecords, row[fk])) {
                          groupedRecords[row[fk]] = [];
                        }

                        var data = _.cloneDeep(row);
                        delete data[fk];
                        groupedRecords[row[fk]].push(data);

                        // Ensure we don't have duplicates in here
                        groupedRecords[row[fk]] = _.uniq(groupedRecords[row[fk]], q.instructions[1].childKey);
                      }
                    });

                    buffers.store.forEach(function(buffer) {
                      if(buffer.attrName !== q.attrName) return;
                      var records = groupedRecords[buffer.belongsToPKValue];
                      if(!records) return;
                      if(!buffer.records) buffer.records = [];
                      buffer.records = buffer.records.concat(records);
                    });

                    next();
                  });
                }, function(err) {
                  next();
                });

              }]

            },
            function(err) {
              if(err) return next(err);
              next();
            });

          }

        }, done);

      }, cb);

    },

    // Select Query Logic
    find: function(connectionName, table, options, cb) {
      spawnConnection(connectionName, function __FIND__(client, cb) {

        // Grab Connection Schema
        var schema = {};
        var connectionObject = connections[connectionName];
        var collection = connectionObject.collections[table];

        Object.keys(connectionObject.collections).forEach(function(coll) {
          schema[coll] = connectionObject.collections[coll].schema;
        });

        // Build Query
        var _schema = collection.waterline.schema;
        var processor = new Processor(_schema);
        var sequel = new Sequel(_schema, sqlOptions);
        var _query;

        // Build a query for the specific query strategy
        try {
          _query = sequel.find(table, options);
        } catch(e) {
          return cb(e);
        }

        client.query(_query.query[0], _query.values[0], function __FIND__(err, result) {
          if(err) return cb(handleQueryError(err));

          // Cast special values
          var values = [];

          result.rows.forEach(function(row) {
            values.push(processor.cast(table, row));
          });

          return cb(null, values);
        });

      }, cb);
    },

    // Stream one or more models from the collection
    stream: function(connectionName, table, options, stream) {

      var connectionObject = connections[connectionName];
      var collection = connectionObject.collections[table];

      var client = new pg.Client(connectionObject.config);
      client.connect();

      var schema = {};

      Object.keys(connectionObject.collections).forEach(function(coll) {
        schema[coll] = connectionObject.collections[coll].schema;
      });

      // Build Query
      var _schema = collection.schema;
      var queryObj = new Query(_schema, schema);
      var query =queryObj.find(table, options);

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

        var _schema = collection.waterline.schema;
        var processor = new Processor(_schema);
        var sequel = new Sequel(_schema, sqlOptions);
        var query;

        // Build a query for the specific query strategy
        try {
          query = sequel.update(table, options, data);
        } catch(e) {
          return cb(e);
        }

        // Run Query
        client.query(query.query, query.values, function __UPDATE__(err, result) {
          if(err) return cb(handleQueryError(err));

          // Cast special values
          var values = [];

          result.rows.forEach(function(row) {
            values.push(processor.cast(table, row));
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

        var _schema = collection.waterline.schema;
        var sequel = new Sequel(_schema, sqlOptions);
        var query;

        // Build a query for the specific query strategy
        try {
          query = sequel.destroy(table, options);
        } catch(e) {
          return cb(e);
        }

        // Run Query
        client.query(query.query, query.values, function __DELETE__(err, result) {
          if(err) return cb(handleQueryError(err));
          cb(null, result.rows);
        });

      }, cb);
    }

  };

  /*************************************************************************/
  /* Private Methods
  /*************************************************************************/

  /**
   * Lookup the primary key for the given collection
   *
   * @param  {String} connectionName
   * @param  {String} collectionName
   * @return {String}
   * @api private
   */
  function _getPK(connectionName, collectionName) {

    var collectionDefinition;

    try {
      collectionDefinition = connections[connectionName].collections[collectionName].definition;
      var pk;

      pk = _.find(Object.keys(collectionDefinition), function _findPK(key) {
        var attrDef = collectionDefinition[key];
        if(attrDef && attrDef.primaryKey) return key;
        else return false;
      });

      if(!pk) pk = 'id';
      return pk;
    }
    catch (e) {
      throw new Error('Unable to determine primary key for collection `'+collectionName+'` because '+
        'an error was encountered acquiring the collection definition:\n'+ require('util').inspect(e,false,null));
    }
  }



  // Wrap a function in the logic necessary to provision a connection
  // (grab from the pool or create a client)
  function spawnConnection(connectionName, logic, cb) {

    var connectionObject = connections[connectionName];
    if(!connectionObject) return cb(Errors.InvalidConnection);

    // If the connection details were supplied as a URL use that. Otherwise,
    // connect using the configuration object as is.
    var connectionConfig = connectionObject.config;
    if(_.has(connectionConfig, 'url')) {
      connectionUrl = url.parse(connectionConfig.url);
      connectionUrl.query = _.omit(connectionConfig, 'url');
      connectionConfig = url.format(connectionUrl);
    }

    // Grab a client instance from the client pool
    pg.connect(connectionConfig, function(err, client, done) {
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

  /**
   *
   * @param  {[type]} err [description]
   * @return {[type]}     [description]
   * @api private
   */
  function handleQueryError (err) {

    var formattedErr;

    // Check for uniqueness constraint violations:
    if (err.code === '23505') {

      // Manually parse the error response and extract the relevant bits,
      // then build the formatted properties that will be passed directly to
      // WLValidationError in Waterline core.
      var matches = err.detail.match(/Key \((.*)\)=\((.*)\) already exists\.$/);
      if (matches && matches.length) {
        formattedErr = {};
        formattedErr.code = 'E_UNIQUE';
        formattedErr.invalidAttributes = {};
        formattedErr.invalidAttributes[matches[1]] = [{
          value: matches[2],
          rule: 'unique'
        }];
      }
    }

    return formattedErr || err;
  }

  return adapter;
})();
