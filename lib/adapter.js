/*---------------------------------------------------------------
  :: sails-postgresql
  -> adapter
---------------------------------------------------------------*/

// Dependencies
var pg = require('pg');
var _ = require('lodash');
var url = require('url');
var async = require('async');
var Errors = require('waterline-errors').adapter;
var Sequel = require('waterline-sequel');
var utils = require('./utils');
var Processor = require('./processor');
var Cursor = require('waterline-cursor');

module.exports = (function() {

  // Keep track of all the connections used by the app
  var connections = {};

  // Connection specific overrides from config
  var connectionOverrides = {};

  var sqlOptions = {
    parameterized: true,
    caseSensitive: true,
    escapeCharacter: '"',
    casting: true,
    canReturnValues: true,
    escapeInserts: true,
    declareDeleteAlias: false,
    schemaName: {}
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

      if(!connection.identity) {
        return cb(Errors.IdentityMissing);
      }

      if(connections[connection.identity]) {
        return cb(Errors.IdentityDuplicate);
      }

      // Set the version of the API
      var version;
      if(connection.version) {
        version = connection.version;
      } else {
        version = 0;
      }

      // Store any connection overrides
      connectionOverrides[connection.identity] = {};

      // Look for the WL Next key
      if(_.has(connection, 'wlNext')) {
        connectionOverrides[connection.identity].wlNext = _.cloneDeep(connection.wlNext);
      }

      // Build up a schema for this connection that can be used throughout the adapter
      var schema = {};

      _.each(_.keys(collections), function(collName) {
        var collection = collections[collName];
        if(!collection) {
          return;
        }

        // Normalize schema into a sane object and discard all the WL context
        var wlSchema = collection.waterline && collection.waterline.schema && collection.waterline.schema[collection.identity];
        var _schema = {};
        _schema.meta = collection.meta || {};
        _schema.tableName = wlSchema.tableName;
        _schema.connection = wlSchema.connection;

        // If a newer Adapter API is in use, the definition key is used to build
        // queries and the attributes property can be ignored.
        //
        // In older api versions SELECT statements were not normalized. Because of
        // this the attributes need to be stored that so SELECTS can be manually
        // normalized in the adapter before sending to the SQL builder.
        if(version > 0) {
          _schema.definition = collection.definition || {};
        } else {
          _schema.definition = collection.definition || {};
          _schema.attributes = wlSchema.attributes || {};
        }

        if(!_schema.tableName) {
          _schema.tableName = collName;
        }

        // If the connection names aren't the same we don't need it in the schema
        if(!_.includes(_schema.connection, connection.identity)) {
          return;
        }

        // If this collection has a schema name set, make sure that it's passed
        // through to the options for waterline-sequel.
        if(_.has(collection, 'meta') && _.has(collection.meta, 'schemaName')) {
          sqlOptions.schemaName[_schema.tableName] = collection.meta.schemaName;
        } else {
          sqlOptions.schemaName[_schema.tableName] = 'public';
        }

        // If the tableName is different from the identity, store the tableName
        // in the schema.
        var schemaKey = collName;
        if(_schema.tableName !== collName) {
          schemaKey = _schema.tableName;
        }
        // Store the normalized schema
        schema[schemaKey] = _schema;
      });

      // Store the connection
      connections[connection.identity] = {
        config: connection,
        schema: schema,
        version: version
      };

      // Always call describe
      async.map(_.keys(collections), function(collName, cb){
        self.describe(connection.identity, collName, cb);
      }, cb);
    },

    // Teardown
    teardown: function (conn, cb) {
      if (typeof conn === 'function') {
        cb = conn;
        conn = null;
      }
      if (conn === null) {
        connections = {};
        return cb();
      }
      if(!connections[conn]) {
        return cb();
      }

      delete connections[conn];
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
        if (data) {
          client.query(query, data, cb);
        }
        else {
          client.query(query, cb);
        }

      }, cb);
    },

    // Describe a table
    describe: function(connectionName, table, cb) {
      spawnConnection(connectionName, function __DESCRIBE__(client, cb) {

        var connectionObject = connections[connectionName];
        var tableName = table;
        var schemaName = getSchema(connectionName, table);

        // Build query to get a bunch of info from the information_schema
        // It's not super important to understand it only that it returns the following fields:
        // [Table, #, Column, Type, Null, Constraint, C, consrc, F Key, Default]
        var query = 'SELECT x.nspname || \'.\' || x.relname as \"Table\", x.attnum as \"#\", x.attname as \"Column\", x.\"Type\",' +
          ' case x.attnotnull when true then \'NOT NULL\' else \'\' end as \"NULL\", r.conname as \"Constraint\", r.contype as \"C\", ' +
          'r.consrc, fn.nspname || \'.\' || f.relname as \"F Key\", d.adsrc as \"Default\" FROM (' +
          'SELECT c.oid, a.attrelid, a.attnum, n.nspname, c.relname, a.attname, pg_catalog.format_type(a.atttypid, a.atttypmod) as \"Type\", ' +
          'a.attnotnull FROM pg_catalog.pg_attribute a, pg_namespace n, pg_class c WHERE a.attnum > 0 AND NOT a.attisdropped AND a.attrelid = c.oid ' +
          'and c.relkind not in (\'S\',\'v\') and c.relnamespace = n.oid and n.nspname not in (\'pg_catalog\',\'pg_toast\',\'information_schema\')) x ' +
          'left join pg_attrdef d on d.adrelid = x.attrelid and d.adnum = x.attnum ' +
          'left join pg_constraint r on r.conrelid = x.oid and r.conkey[1] = x.attnum ' +
          'left join pg_class f on r.confrelid = f.oid ' +
          'left join pg_namespace fn on f.relnamespace = fn.oid ' +
          'where x.relname = \'' + tableName + '\' and x.nspname = \'' + schemaName + '\' order by 1,2;';

        // Get Sequences to test if column auto-increments
        var autoIncrementQuery = 'SELECT t.relname as related_table, a.attname as related_column, s.relname as sequence_name ' +
          'FROM pg_class s JOIN pg_depend d ON d.objid = s.oid JOIN pg_class t ON d.objid = s.oid AND d.refobjid = t.oid ' +
          'JOIN pg_attribute a ON (d.refobjid, d.refobjsubid) = (a.attrelid, a.attnum) JOIN pg_namespace n ON n.oid = s.relnamespace ' +
          'WHERE s.relkind = \'S\' AND n.nspname = \'' + schemaName + '\';';

        // Get Indexes
        var indiciesQuery = 'SELECT n.nspname as \"Schema\", c.relname as \"Name\", CASE c.relkind WHEN \'r\' THEN \'table\' ' +
          'WHEN \'v\' THEN \'view\' WHEN \'i\' THEN \'index\' WHEN \'S\' THEN \'sequence\' WHEN \'s\' THEN \'special\' WHEN \'f\' THEN ' +
          '\'foreign table\' END as \"Type\", pg_catalog.pg_get_userbyid(c.relowner) as \"Owner\", c2.relname as \"Table\" ' +
          'FROM pg_catalog.pg_class c LEFT JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace ' +
          'LEFT JOIN pg_catalog.pg_index i ON i.indexrelid = c.oid ' +
          'LEFT JOIN pg_catalog.pg_class c2 ON i.indrelid = c2.oid ' +
          'WHERE c.relkind IN (\'i\',\'\') AND n.nspname <> \'pg_catalog\' AND n.nspname <> \'information_schema\' ' +
          'AND n.nspname !~ \'^pg_toast\' AND pg_catalog.pg_table_is_visible(c.oid) ORDER BY 1,2;';

        // Run Info Query
        client.query(query, function(err, result) {
          if(err) {
            return cb(handleQueryError(err));
          }

          if(result.rows.length === 0) {
            return cb();
          }

          // Run Query to get Auto Incrementing sequences
          client.query(autoIncrementQuery, function(err, aResult) {
            if(err) {
              return cb(handleQueryError(err));
            }

            _.each(aResult.rows, function(row) {
              if(row.related_table !== table) {
                return;
              }

              // Look through query results and see if related_column exists
              _.each(result.rows, function(column) {
                if(column.Column !== row.related_column) {
                  return;
                }

                column.autoIncrement = true;
              });
            });

            // Run Query to get Indexed values
            client.query(indiciesQuery, function(err, iResult) {
              if(err) {
                return cb(handleQueryError(err));
              }

              // Loop through indicies and see if any match
              _.each(iResult.rows, function(column) {
                var key = column.Name.split('_index_')[1];

                // Look through query results and see if key exists
                _.each(result.rows, function(column) {
                  if(column.Column !== key) {
                    return;
                  }

                  column.indexed = true;
                });
              });

              // Normalize Schema
              var normalizedSchema = utils.normalizeSchema(result.rows);

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
        if(err) {
          return cb(err);
        }

        // Describe (sets schema)
        adapter.describe(connectionName, table.replace(/["']/g, ''), cb);
      };

      var connectionObject = connections[connectionName];

      spawnConnection(connectionName, function __DEFINE__(client, cb) {

        // Get the schema name if any
        var schemaName = getSchema(connectionName, table);

        // If we're being told NOT to create schemas, then skip right to
        // creating the table.
        if (connectionObject.config.createSchemas === false) {
          return _define();
        }

        // If the schema name is "public", just finish creating the table
        if (schemaName === 'public') {
          return _define();
        }

        // If not, attempt to create the schema first.  This will succeed if
        // the schema already exists.
        adapter.createSchema(connectionName, table, schemaName, _define);

        function _define(errCreatingSchema) {

          if (errCreatingSchema) {
            cb(handleQueryError(errCreatingSchema));
          }

          // Escape Table Name
          var tableName = utils.escapeName(table, schemaName);

          // Iterate through each attribute, building a query string
          var _schema = utils.buildSchema(definition);

          // Check for any Index attributes
          var indexes = utils.buildIndexes(definition);

          // Build Query
          var query = 'CREATE TABLE ' + tableName + ' (' + _schema + ')';

          // Run Query
          client.query(query, function __DEFINE__(err, result) {

            if(err) {
              if (err.code === '3F000') {
                err.table = table;
                err.schemaName = schemaName;
              }
              return cb(handleQueryError(err));
            }

            // Build Indexes
            function buildIndex(name, cb) {

              // Strip slashes from table name, used to namespace index
              var cleanTable = table.replace(/['"]/g, '');

              // Build a query to create a namespaced index tableName_key
              var query = 'CREATE INDEX ' + utils.escapeName(cleanTable + '_' + name) + ' on ' + tableName + ' (' + utils.escapeName(name) + ');';
              // Run Query
              client.query(query, function(err, result) {
                if(err) {
                  return cb(handleQueryError(err));
                }

                cb();
              });
            }

            // Build indexes in series
            async.eachSeries(indexes, buildIndex, cb);
          });

        }

      }, describe);
    },

    // Create a schema
    createSchema: function(connectionName, table, schemaName, cb) {

      if ('function' === typeof schemaName) {
        cb = schemaName;
        schemaName = getSchema(connectionName, table);
      }
      if (!schemaName) {
        throw new Error('No schemaName specified, and could not determined schemaname for table `' + table + '`');
      }

      // Build Query
      var query = 'CREATE SCHEMA "' + schemaName + '"';

      spawnConnection(connectionName, function (client, cb) {
        // Run Query
        client.query(query, function (err, result) {
          // If we get a "duplicate schema" error, just silently ignore it
          if (err && err.code === '42P06') {
            return cb();
          }

          // If we get any other type of error, return it
          if (err) {
            err.type = 'CREATING_SCHEMA';
            err.schemaName = schemaName;
            return cb(err);
          }
          // It we get no error, we're all good.
          return cb();
        });
      }, cb);

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

          var tableName = utils.escapeName(item, getSchema(connectionName, item));

          // Build Query
          var query = 'DROP TABLE ' + tableName + ';';

          // Run Query
          client.query(query, function __DROP__(err, result) {
            if(err) {
              result = null;
            }

            next(null, result);
          });
        }

        async.eachSeries(relations, dropTable, function(err) {
          if(err) {
            return cb(err);
          }

          dropTable(table, cb);
        });

      }, cb);
    },

    // Add a column to a table
    addAttribute: function(connectionName, table, attrName, attrDef, cb) {
      spawnConnection(connectionName, function __ADD_ATTRIBUTE__(client, cb) {

        // Escape Table Name
        table = utils.escapeName(table, getSchema(connectionName, table));

        // Setup a Schema Definition
        var attrs = {};
        attrs[attrName] = attrDef;

        var _schema = utils.buildSchema(attrs);

        // Build Query
        var query = 'ALTER TABLE ' + table + ' ADD COLUMN ' + _schema;

        // Run Query
        client.query(query, function __ADD_ATTRIBUTE__(err, result) {
          if(err) {
            return cb(handleQueryError(err));
          }

          cb(null, result.rows);
        });

      }, cb);
    },

    // Remove a column from a table
    removeAttribute: function (connectionName, table, attrName, cb) {
      spawnConnection(connectionName, function __REMOVE_ATTRIBUTE__(client, cb) {

        // Escape Table Name
        table = utils.escapeName(table, getSchema(connectionName, table));

        // Build Query
        var query = 'ALTER TABLE ' + table + ' DROP COLUMN "' + attrName + '" RESTRICT';

        // Run Query
        client.query(query, function __REMOVE_ATTRIBUTE__(err, result) {
          if(err) {
            return cb(handleQueryError(err));
          }

          cb(null, result.rows);
        });

      }, cb);
    },

    // Add a new row to the table
    create: function(connectionName, table, data, cb) {
      spawnConnection(connectionName, function __CREATE__(client, cb) {

        var connectionObject = connections[connectionName];
        var tableName = table;
        var schemaName = getSchema(connectionName, table);

        // Build up a SQL Query
        var schema = connectionObject.schema;
        var processor = new Processor(schema);

        // Mixin WL Next connection overrides to sqlOptions
        var overrides = connectionOverrides[connectionName] || {};
        var options = _.cloneDeep(sqlOptions);
        if(_.has(overrides, 'wlNext')) {
          options.wlNext = overrides.wlNext;
        }

        var sequel = new Sequel(schema, options);

        var incrementSequences = [];
        var query;

        // Build a query for the specific query strategy
        try {
          query = sequel.create(table, data);
        } catch(e) {
          return cb(e);
        }

        // Loop through all the attributes being inserted and check if a sequence was used
        _.each(_.keys(schema[table].definition), function(schemaKey) {
          if(!_.has(schema[table].definition[schemaKey], 'autoIncrement')) {
            return;
          }

          if(_.indexOf(_.keys(data), schemaKey) < 0) {
            return;
          }

          incrementSequences.push(schemaKey);
        });

        // Run Query
        client.query(query.query, query.values, function __CREATE__(err, result) {
          if(err) {
            return cb(handleQueryError(err));
          }

          // Cast special values
          var values = processor.cast(tableName, result.rows[0]);

          // Set Sequence value to defined value if needed
          if(incrementSequences.length === 0) {
            return cb(null, values);
          }

          function setSequence(item, next) {
            var sequenceName = '\'\"' + schemaName + '\".\"' + tableName + '_' + item + '_seq' + '\"\'';
            var sequenceValue = values[item];
            var sequenceQuery = 'SELECT setval(' + sequenceName + ', ' + sequenceValue + ', true)';

            client.query(sequenceQuery, function(err, result) {
              if(err) {
                return next(err);
              }

              next();
            });
          }

          async.each(incrementSequences, setSequence, function(err) {
            if(err) {
              return cb(err);
            }

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
        var tableName = table;
        var schemaName = getSchema(connectionName, table);

        // Build up a SQL Query
        var schema = connectionObject.schema;
        var processor = new Processor(schema);

        // Mixin WL Next connection overrides to sqlOptions
        var overrides = connectionOverrides[connectionName] || {};
        var options = _.cloneDeep(sqlOptions);
        if(_.has(overrides, 'wlNext')) {
          options.wlNext = overrides.wlNext;
        }

        var sequel = new Sequel(schema, options);
        var incrementSequences = [];

        // Loop through all the attributes being inserted and check if a sequence was used
        _.each(_.keys(schema[table].definition), function(schemaKey) {
          if(!_.has(schema[table].definition[schemaKey], 'autoIncrement')) {
            return;
          }

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
            query = sequel.create(tableName, data);
          } catch(e) {
            return cb(e);
          }

          // Run Query
          client.query(query.query, query.values, function __CREATE_EACH__(err, result) {
            if(err) {
              return cb(handleQueryError(err));
            }

            // Cast special values
            var values = processor.cast(tableName, result.rows[0]);

            results.push(values);
            if(incrementSequences.length === 0) {
              return cb(null, values);
            }

            function checkSequence(item, next) {
              var currentValue  = item.value;
              var sequenceValue = values[item.key];

              if(currentValue < sequenceValue) {
                item.value = sequenceValue;
              }
              next();
            }

            async.each(incrementSequences, checkSequence, function(err) {
              if(err) {
                return cb(err);
              }

              cb(null, values);
            });
          });

        }, function(err) {
          if(err) {
            return cb(err);
          }

          if(incrementSequences.length === 0) {
            return cb(null, results);
          }

          function setSequence(item, next) {
            if (sequenceValue === 0) {
              return next();
            }

            var sequenceName = '\'\"' + schemaName + '\".\"' + tableName + '_' + item.key + '_seq' + '\"\'';
            var sequenceValue = item.value;
            var sequenceQuery = 'SELECT setval(' + sequenceName + ', ' + sequenceValue + ', true)';

            client.query(sequenceQuery, function(err, result) {
              if(err) {
                return next(err);
              }

              next();
            });
          }

          async.each(incrementSequences, setSequence, function(err) {
            if(err) {
              return cb(err);
            }

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
            if (!collectionName) {
              return;
            }

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
            var tableName = table;

            var parentRecords = [];
            var cachedChildren = {};

            // Build Query
            var _schema = connectionObject.schema;
            var api_version = connectionObject.version;

            // Mixin WL Next connection overrides to sqlOptions
            var overrides = connectionOverrides[connectionName] || {};
            var _options = _.cloneDeep(sqlOptions);
            if(_.has(overrides, 'wlNext')) {
              _options.wlNext = overrides.wlNext;
            }

            var sequel = new Sequel(_schema, _options);
            var _query;

            // If this is using an older version of the Waterline API and a select
            // modifier was used, normalize it to column_name values before trying
            // to build the query.
            if(api_version < 1 && instructions.select) {
              var _select = [];
              _.each(instructions.select, function(selectKey) {
                var attrs = connectionObject.schema[table] && connectionObject.schema[table].attributes || {};
                var def = attrs[selectKey] || {};
                var colName = _.has(def, 'columnName') ? def.columnName : selectKey;
                _select.push(colName);
              });

              // Replace the select criteria with normalized values
              instructions.select = _select;
            }

            // Build a query for the specific query strategy
            try {
              _query = sequel.find(tableName, instructions);
            } catch(e) {
              return next(e);
            }

            async.auto({

              processParent: function(next) {

                client.query(_query.query[0], _query.values[0], function __FIND__(err, result) {
                  if(err) {
                    return next(handleQueryError(err));
                  }

                  parentRecords = result.rows;

                  var splitChildren = function(parent, next) {
                    var cache = {};

                    _.each(_.keys(parent), function(key) {

                      // Check if we can split this on our special alias identifier '___' and if
                      // so put the result in the cache
                      var split = key.split('___');
                      if(split.length < 2) {
                        return;
                      }

                      if(!_.has(cache, split[0])) {
                        cache[split[0]] = {};
                      }

                      cache[split[0]][split[1]] = parent[key];
                      delete parent[key];
                    });

                    // Combine the local cache into the cachedChildren
                    if(_.keys(cache).length > 0) {
                      _.each(_.keys(cache), function(pop) {
                        if(!_.has(cachedChildren, pop)) {
                          cachedChildren[pop] = [];
                        }

                        cachedChildren[pop] = cachedChildren[pop].concat(cache[pop]);
                      });
                    }

                    next();
                  };


                  // Pull out any aliased child records that have come from a hasFK association
                  async.eachSeries(parentRecords, splitChildren, function(err) {
                    if(err) {
                      return next(err);
                    }

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
                    var recordsMap = {};

                    // Check for any cached parent records
                    if(_.has(cachedChildren, alias)) {
                      _.each(cachedChildren[alias], function(cachedChild) {
                        var childVal = popInstructions[0].childKey;
                        var parentVal = popInstructions[0].parentKey;

                        if(cachedChild[childVal] !== parent[parentVal]) {
                          return;
                        }

                        // If null value for the parentVal, ignore it
                        if(parent[parentVal] === null) {
                          return;
                        }

                        // If the same record is alreay there, ignore it
                        if (!recordsMap[cachedChild[childVal]]) {
                          records.push(cachedChild);
                          recordsMap[cachedChild[childVal]] = true;
                        }
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

                  if(!_.isArray(q.instructions)) {
                    pk = _getPK(connectionName, q.instructions.parent);
                  }
                  else if(q.instructions.length > 1) {
                    pk = _getPK(connectionName, q.instructions[0].parent);
                  }

                  _.each(parentRecords, function(parent) {
                    if(_.isNumber(parent[pk])) {
                      qs += q.qs.replace('^?^', parent[pk]) + ' UNION ALL ';
                    } else {
                      qs += q.qs.replace('^?^', '\'' + parent[pk] + '\'') + ' UNION ALL ';
                    }
                  });

                  // Remove the last UNION ALL
                  qs = qs.slice(0, -11);

                  var addedOrder;
                  function addSort(sortKey, sorts) {
                    // ID sorts are ambiguous
                    if(sortKey === 'id') {
                      return;
                    }

                    if (!addedOrder) {
                      addedOrder = true;
                      qs += ' ORDER BY ';
                    }

                    var direction = sorts[sortKey] === 1 ? 'ASC' : 'DESC';
                    qs += '"' + sortKey + '"' + ' ' + direction + ', ';
                  }

                  // Add a final sort to the Union clause for integration
                  if(parentRecords.length > 1) {

                    if(!_.isArray(q.instructions)) {
                      _.each(_.keys(q.instructions.criteria.sort), function(sortKey) {
                        addSort(sortKey, q.instructions.criteria.sort);
                      });
                    }
                    else if(q.instructions.length === 2) {
                      prefix = q.instructions[1].child;
                      _.each(_.keys(q.instructions[1].criteria.sort), function(sortKey) {
                        addSort(sortKey, q.instructions[1].criteria.sort);
                      });
                    }

                    // Remove the last comma
                    if(addedOrder) {
                      qs = qs.slice(0, -2);
                    }
                  }

                  client.query(qs, q.values, function __FIND__(err, result) {
                    if(err) {
                      return next(handleQueryError(err));
                    }

                    var groupedRecords = {};
                    _.each(result.rows, function(row) {

                      if(!_.isArray(q.instructions)) {

                        if(!_.has(groupedRecords, row[q.instructions.childKey])) {
                          groupedRecords[row[q.instructions.childKey]] = [];
                        }

                        groupedRecords[row[q.instructions.childKey]].push(row);
                      }
                      else {

                        // Grab the special "foreign key" we attach and make sure to remove it
                        var fk = '___' + q.instructions[0].childKey;

                        if(!_.has(groupedRecords, row[fk])) {
                          groupedRecords[row[fk]] = [];
                        }

                        var data = _.cloneDeep(row);
                        delete data[fk];
                        groupedRecords[row[fk]].push(data);

                        // Ensure we don't have duplicates in here
                        groupedRecords[row[fk]] = _.uniq(groupedRecords[row[fk]], q.instructions[1].childKey);
                      }
                    });

                    _.each(buffers.store, function(buffer) {
                      if(buffer.attrName !== q.attrName) {
                        return;
                      }

                      var records = groupedRecords[buffer.belongsToPKValue];
                      if(!records) {
                        return;
                      }

                      if(!buffer.records) {
                        buffer.records = [];
                      }

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
              if(err) {
                return next(err);
              }

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
        var connectionObject = connections[connectionName];
        var api_version = connectionObject.version;
        var tableName = table;

        // Build Query
        var _schema = connectionObject.schema;
        var processor = new Processor(_schema);

        // Mixin WL Next connection overrides to sqlOptions
        var overrides = connectionOverrides[connectionName] || {};
        var _options = _.cloneDeep(sqlOptions);
        if(_.has(overrides, 'wlNext')) {
          _options.wlNext = overrides.wlNext;
        }

        var sequel = new Sequel(_schema, _options);
        var _query;

        // If this is using an older version of the Waterline API and a select
        // modifier was used, normalize it to column_name values before trying
        // to build the query.
        if(api_version < 1 && options.select) {
          var _select = [];
          _.each(options.select, function(selectKey) {
            var attrs = connectionObject.schema[table] && connectionObject.schema[table].attributes || {};
            var def = attrs[selectKey] || {};
            var colName = _.has(def, 'columnName') ? def.columnName : selectKey;
            _select.push(colName);
          });

          // Replace the select criteria with normalized values
          options.select = _select;
        }

        // Build a query for the specific query strategy
        try {
          _query = sequel.find(tableName, options);
        } catch(e) {
          return cb(e);
        }

        client.query(_query.query[0], _query.values[0], function __FIND__(err, result) {
          if(err) {
            return cb(handleQueryError(err));
          }

          // Cast special values
          var values = [];

          _.each(result.rows, function(row) {
            values.push(processor.cast(tableName, row));
          });

          return cb(null, values);
        });

      }, cb);
    },

    // Count Query logic
    count: function(connectionName, table, options, cb) {
      spawnConnection(connectionName, function __COUNT__(client, cb) {

        // Grab Connection Schema
        var connectionObject = connections[connectionName];
        var tableName = table;

        // Build Query
        var _schema = connectionObject.schema;
        var processor = new Processor(_schema);

        // Mixin WL Next connection overrides to sqlOptions
        var overrides = connectionOverrides[connectionName] || {};
        var _options = _.cloneDeep(sqlOptions);
        if(_.has(overrides, 'wlNext')) {
          _options.wlNext = overrides.wlNext;
        }

        var sequel = new Sequel(_schema, _options);
        var _query;

        // Build a query for the specific query strategy
        try {
          _query = sequel.count(tableName, options);
        } catch(e) {
          return cb(e);
        }

        client.query(_query.query[0], _query.values[0], function __COUNT__(err, result) {
          if(err) {
            return cb(handleQueryError(err));
          }

          if(!_.isArray(result.rows) || !result.rows.length) {
            return cb(new Error('Invalid query, no results returned.'));
          }

          var count = result.rows[0] && result.rows[0].count;
          return cb(null, Number(count));
        });

      }, cb);
    },

    // Stream one or more models from the collection
    stream: function(connectionName, table, options, stream) {

      var connectionObject = connections[connectionName];

      var client = new pg.Client(connectionObject.config);
      client.connect();

      // Build Query
      var _schema = connectionObject.schema;

      // Mixin WL Next connection overrides to sqlOptions
      var overrides = connectionOverrides[connectionName] || {};
      var _options = _.cloneDeep(sqlOptions);
      if(_.has(overrides, 'wlNext')) {
        _options.wlNext = overrides.wlNext;
      }

      var sequel = new Sequel(_schema, _options);
      var _query;

      // If this is using an older version of the Waterline API and a select
      // modifier was used, normalize it to column_name values before trying
      // to build the query.
      if(api_version < 1 && options.select) {
        var _select = [];
        _.each(options.select, function(selectKey) {
          var attrs = connectionObject.schema[table] && connectionObject.schema[table].attributes || {};
          var def = attrs[selectKey] || {};
          var colName = _.has(def, 'columnName') ? def.columnName : selectKey;
          _select.push(colName);
        });

        // Replace the select criteria with normalized values
        options.select = _select;
      }

      // Build a query for the specific query strategy
      try {
        _query = sequel.find(tableName, options);
      } catch(e) {
        stream.end(); // End stream
        client.end(); // Close Connection
        return;
      }

      // Run Query
      var dbStream = client.query(_query.query[0], _query.values[0]);

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

      // LIMIT in a postgresql UPDATE command is not valid
      if(_.has(options, 'limit')) {
        return cb(new Error('Your \'LIMIT ' + options.limit + '\' is not allowed in the PostgreSQL UPDATE query.'));
      }

      spawnConnection(connectionName, function __UPDATE__(client, cb) {

        var connectionObject = connections[connectionName];
        var tableName = table;

        var _schema = connectionObject.schema;
        var processor = new Processor(_schema);

        // Mixin WL Next connection overrides to sqlOptions
        var overrides = connectionOverrides[connectionName] || {};
        var _options = _.cloneDeep(sqlOptions);
        if(_.has(overrides, 'wlNext')) {
          _options.wlNext = overrides.wlNext;
        }

        var sequel = new Sequel(_schema, _options);
        var query;

        // Build a query for the specific query strategy
        try {
          query = sequel.update(tableName, options, data);
        } catch(e) {
          return cb(e);
        }

        // Run Query
        client.query(query.query, query.values, function __UPDATE__(err, result) {
          if(err) {
            return cb(handleQueryError(err));
          }

          // Cast special values
          var values = [];

          _.each(result.rows, function(row) {
            values.push(processor.cast(tableName, row));
          });

          cb(null, values);
        });

      }, cb);
    },

    // Delete one or more models from the collection
    destroy: function(connectionName, table, options, cb) {
      spawnConnection(connectionName, function __DELETE__(client, cb) {

        var connectionObject = connections[connectionName];
        var tableName = table;

        var _schema = connectionObject.schema;

        // Mixin WL Next connection overrides to sqlOptions
        var overrides = connectionOverrides[connectionName] || {};
        var _options = _.cloneDeep(sqlOptions);
        if(_.has(overrides, 'wlNext')) {
          _options.wlNext = overrides.wlNext;
        }

        var sequel = new Sequel(_schema, _options);
        var query;

        // Build a query for the specific query strategy
        try {
          query = sequel.destroy(tableName, options);
        } catch(e) {
          return cb(e);
        }

        // Run Query
        client.query(query.query, query.values, function __DELETE__(err, result) {
          if(err) {
            return cb(handleQueryError(err));
          }

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
      collectionDefinition = connections[connectionName].schema[collectionName].definition;
      var pk = _.find(_.keys(collectionDefinition), function _findPK (key) {
        var attrDef = collectionDefinition[key];
        if(attrDef && attrDef.primaryKey) {
          return key;
        }
        else {
          return false;
        }
      }) || 'id';
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
    if(!connectionObject) {
      return cb(Errors.InvalidConnection);
    }

    // If the connection details were supplied as a URL use that. Otherwise,
    // connect using the configuration object as is.
    var connectionConfig = connectionObject.config;
    if(_.has(connectionConfig, 'url')) {
      var connectionUrl = url.parse(connectionConfig.url);
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
        // console.error("Error creating a connection to Postgresql: " + err);

        // Figure out if this user is on Windows
        var isWin = !!process.platform.match(/^win/);
        // Don't mention `sudo` to Windows users-- I hear you guys get touchy about that sort of thing
        var trySudoMsg = (isWin) ? '' : '(you might try `sudo`)';

        // If connection to posgresql fails starting, provide general troubleshooting information,
        // sharpened with a few simple heuristics:

        // Remove password before reporting error so that it doesn't show up in logs
        var connectionValues = _.clone(connectionConfig);
        connectionValues.password = '[redacted]';

        console.error('');
        console.error('Error creating a connection to Postgresql using the following settings:\n',connectionValues);
        console.error('');
        console.error('* * *\nComplete error details:\n',err);
        console.error('');
        console.error('');


        console.error('Troubleshooting tips:');
        console.error('');

        // Used below to indicate whether the error is potentially related to config
        // (in which case we'll display a generic message explaining how to configure all the things)
        var isPotentiallyConfigRelated;
        isPotentiallyConfigRelated = true;

        // Determine whether localhost is being used
        var usingLocalhost = !!(function (){
          try {
            var LOCALHOST_REGEXP = /(localhost|127\.0\.0\.1)/;
            if ((connectionConfig.hostname && connectionConfig.hostname.match(LOCALHOST_REGEXP)) || (connectionConfig.host && connectionConfig.host.match(LOCALHOST_REGEXP))) {
              return true;
            }
          }
          catch (e) {}
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
          'If you\'re using Sails, this is generally located in `config/connections.js`,',
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
        // This is the same property that is set on WLError instances.
        // Accessible as `.originalError` on a WLValidationError instance.
        formattedErr.originalError = err;
        formattedErr.invalidAttributes = {};
        formattedErr.invalidAttributes[matches[1]] = [{
          value: matches[2],
          rule: 'unique'
        }];
      }
    }

    else if (err.code === '3F000') {
      formattedErr = {};
      formattedErr.message = 'Attempted to create a table `' + err.table + '` in a schema `' + err.schemaName + '` that does not exist.  Either create the schema manually or make sure that the `createSchemas` flag in your connection configuration is not set to `false`.';
      delete err.table;
      delete err.schemaName;
      formattedErr.originalError = err;
    }

    else if (err.type === 'CREATING_SCHEMA') {
      formattedErr = {};
      formattedErr.code = 'CREATING_SCHEMA';
      formattedErr.message = 'An error occurred creating the schema "' + err.schemaName + '"; perhaps it contains invalid characters?';
      delete err.type;
      delete err.schemaName;
      formattedErr.originalError = err;
    }

    return formattedErr || err;
  }

  function getSchema(connectionName, collectionName) {
    var collection = connections[connectionName].schema[collectionName];
    return (collection.meta && collection.meta.schemaName) || 'public';
  }

  return adapter;
})();
