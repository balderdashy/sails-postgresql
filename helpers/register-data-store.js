//  ██████╗ ███████╗ ██████╗ ██╗███████╗████████╗███████╗██████╗
//  ██╔══██╗██╔════╝██╔════╝ ██║██╔════╝╚══██╔══╝██╔════╝██╔══██╗
//  ██████╔╝█████╗  ██║  ███╗██║███████╗   ██║   █████╗  ██████╔╝
//  ██╔══██╗██╔══╝  ██║   ██║██║╚════██║   ██║   ██╔══╝  ██╔══██╗
//  ██║  ██║███████╗╚██████╔╝██║███████║   ██║   ███████╗██║  ██║
//  ╚═╝  ╚═╝╚══════╝ ╚═════╝ ╚═╝╚══════╝   ╚═╝   ╚══════╝╚═╝  ╚═╝
//
//  ██████╗  █████╗ ████████╗ █████╗     ███████╗████████╗ ██████╗ ██████╗ ███████╗
//  ██╔══██╗██╔══██╗╚══██╔══╝██╔══██╗    ██╔════╝╚══██╔══╝██╔═══██╗██╔══██╗██╔════╝
//  ██║  ██║███████║   ██║   ███████║    ███████╗   ██║   ██║   ██║██████╔╝█████╗
//  ██║  ██║██╔══██║   ██║   ██╔══██║    ╚════██║   ██║   ██║   ██║██╔══██╗██╔══╝
//  ██████╔╝██║  ██║   ██║   ██║  ██║    ███████║   ██║   ╚██████╔╝██║  ██║███████╗
//  ╚═════╝ ╚═╝  ╚═╝   ╚═╝   ╚═╝  ╚═╝    ╚══════╝   ╚═╝    ╚═════╝ ╚═╝  ╚═╝╚══════╝
//

module.exports = require('machine').build({


  friendlyName: 'Register Data Store',


  description: 'Register a new datastore for making connections.',


  sync: true,


  inputs: {

    identity: {
      description: 'A unique identitifer for the connection.',
      example: 'localPostgres',
      required: true
    },

    config: {
      description: 'The configuration to use for the data store.',
      required: true,
      type: 'ref'
    },

    models: {
      description: 'The Waterline models that will be used with this data store.',
      required: true,
      type: 'ref'
    },

    datastores: {
      description: 'An object containing all of the data stores that have been registered.',
      required: true,
      type: 'ref'
    },

    modelDefinitions: {
      description: 'An object containing all of the model definitions that have been registered.',
      required: true,
      type: 'ref'
    }

  },


  exits: {

    success: {
      description: 'The data store was initialized successfully.'
    },

    badConfiguration: {
      description: 'The configuration was invalid.',
      outputoutputType: 'ref'
    }

  },


  fn: function registerDataStore(inputs, exits) {
    // Dependencies
    var _ = require('@sailshq/lodash');
    var PG = require('machinepack-postgresql');
    var flaverr = require('flaverr');
    var Helpers = require('./private');

    // Validate that the datastore isn't already initialized
    if (inputs.datastores[inputs.identity]) {
      return exits.badConfiguration(new Error('Datastore `' + inputs.identity + '` is already registered.'));
    }

    //  ╦  ╦╔═╗╦  ╦╔╦╗╔═╗╔╦╗╔═╗  ┌─┐┌─┐┌┐┌┌─┐┬┌─┐
    //  ╚╗╔╝╠═╣║  ║ ║║╠═╣ ║ ║╣   │  │ ││││├┤ ││ ┬
    //   ╚╝ ╩ ╩╩═╝╩═╩╝╩ ╩ ╩ ╚═╝  └─┘└─┘┘└┘└  ┴└─┘
    // If a URL config value was not given, ensure that all the various pieces
    // needed to create one exist.
    var hasURL = _.has(inputs.config, 'url');

    // Validate that the connection has a host and database property
    if (!hasURL && !inputs.config.host) {
      return exits.badConfiguration(flaverr('E_MISSING_HOST', new Error('Datastore  `' + inputs.identity + '` config is missing a host value.')));
    }

    if (!hasURL && !inputs.config.database) {
      return exits.badConfiguration(flaverr('E_MISSING_DB_NAME', new Error('Datastore  `' + inputs.identity + '` config is missing a value for the database name.')));
    }

    // Loop through every model assigned to the datastore we're registering,
    // and ensure that each one's primary key is either required or auto-incrementing.
    // Also check for attribute type / column type mismatches.
    try {
      _.each(inputs.models, function checkPrimaryKey(modelDef, modelIdentity) {
        var primaryKeyAttr = modelDef.definition[modelDef.primaryKey];

        // Ensure that the model's primary key has either `autoIncrement` or `required`
        if (primaryKeyAttr.required !== true && (!primaryKeyAttr.autoMigrations || primaryKeyAttr.autoMigrations.autoIncrement !== true)) {
          throw flaverr('E_INVALID_PK', new Error('In model `' + modelIdentity + '`, primary key `' + modelDef.primaryKey + '` must have either `required` or `autoIncrement` set.'));
        }

        _.each(modelDef.definition, function checkAttributes(attribute, attributeName) {

          if (attribute.type === 'number' && attribute.autoMigrations && attribute.autoMigrations.columnType === 'bigint' && !attribute.autoCreatedAt && !attribute.autoUpdatedAt) {
            throw flaverr('E_BIGINT_TYPE_MISMATCH', new Error('\nIn attribute `' + attributeName + '` of model `' + modelIdentity + '`:\nThe `bigint` column type cannot be used with the `number` attribute type.\nSince `bigint` values may be larger than the maximum JavaScript integer size, PostgreSQL will return them as strings.\nTherefore, attributes using this column type must be declared as type `string`, `ref` or `json`.\n'));
          }

        });

      });
    } catch (e) {
      switch (e.code) {
        case 'E_MISSING_HOST':
        case 'E_MISSING_DB_NAME':
        case 'E_INVALID_PK':
        case 'E_BIGINT_TYPE_MISMATCH':
          return exits.badConfiguration(e.message);
        default:
          return exits.error(e);
      }
    }

    //  ╔═╗╔═╗╔╗╔╔═╗╦═╗╔═╗╔╦╗╔═╗  ┌─┐┌─┐┌┐┌┌┐┌┌─┐┌─┐┌┬┐┬┌─┐┌┐┌
    //  ║ ╦║╣ ║║║║╣ ╠╦╝╠═╣ ║ ║╣   │  │ │││││││├┤ │   │ ││ ││││
    //  ╚═╝╚═╝╝╚╝╚═╝╩╚═╩ ╩ ╩ ╚═╝  └─┘└─┘┘└┘┘└┘└─┘└─┘ ┴ ┴└─┘┘└┘
    //  ┌─┐┌┬┐┬─┐┬┌┐┌┌─┐  ┬ ┬┬─┐┬
    //  └─┐ │ ├┬┘│││││ ┬  │ │├┬┘│
    //  └─┘ ┴ ┴└─┴┘└┘└─┘  └─┘┴└─┴─┘
    // If the connection details were not supplied as a URL, make them into one.
    // This is required for the underlying driver in use.
    if (!_.has(inputs.config, 'url')) {
      var url = 'postgres://';
      var port = inputs.config.port || '5432';

      // If authentication is used, add it to the connection string
      if (inputs.config.user && inputs.config.password) {
        url += inputs.config.user + ':' + inputs.config.password + '@';
      }

      url += inputs.config.host + ':' + port + '/' + inputs.config.database;
      inputs.config.url = url;
    }


    //  ╔═╗╦═╗╔═╗╔═╗╔╦╗╔═╗  ┌┬┐┌─┐┌┐┌┌─┐┌─┐┌─┐┬─┐
    //  ║  ╠╦╝║╣ ╠═╣ ║ ║╣   │││├─┤│││├─┤│ ┬├┤ ├┬┘
    //  ╚═╝╩╚═╚═╝╩ ╩ ╩ ╚═╝  ┴ ┴┴ ┴┘└┘┴ ┴└─┘└─┘┴└─
    // Create a manager to handle the datastore connection config
    var report;
    try {
      report = Helpers.connection.createManager(inputs.config.url, inputs.config);
    } catch (e) {
      if (!e.code || e.code === 'error') {
        return exits.error(new Error('There was an error creating a new manager for the connection with a url of: ' + inputs.config.url + '\n\n' + e.stack));
      }

      if (e.code === 'failed') {
        return exits.badConfiguration(new Error('There was an error creating a new manager for the connection with a url of: ' + inputs.config.url + '\n\n' + e.stack));
      }

      if (e.code === 'malformed') {
        return exits.badConfiguration(new Error('There was an error creating a new manager for the connection with a url of: ' + inputs.config.url + '\n\n' + e.stack));
      }

      return exits.error(new Error('There was an error creating a new manager for the connection with a url of: ' + inputs.config.url + '\n\n' + e.stack));
    }


    // Build up a database schema for this connection that can be used
    // throughout the adapter
    var dbSchema = {};

    _.each(inputs.models, function buildSchema(val) {
      var identity = val.identity;
      var tableName = val.tableName;
      var definition = val.definition;

      dbSchema[tableName] = {
        identity: identity,
        tableName: tableName,
        definition: definition,
        attributes: definition,
        primaryKey: val.primaryKey
      };
    });

    // Store the connection
    inputs.datastores[inputs.identity] = {
      manager: report.manager,
      config: inputs.config,
      driver: PG
    };

    // Store the db schema for the connection
    inputs.modelDefinitions[inputs.identity] = dbSchema;

    return exits.success();
  }
});
