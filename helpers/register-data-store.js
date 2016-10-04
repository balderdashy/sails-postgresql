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


  inputs: {

    config: {
      description: 'The configuration to use for the data store.',
      required: true,
      example: '==='
    },

    models: {
      description: 'The Waterline models that will be used with this data store.',
      required: true,
      example: '==='
    },

    datastores: {
      description: 'An object containing all of the data stores that have been registered.',
      required: true,
      example: '==='
    }

  },


  exits: {

    success: {
      description: 'The data store was initialized successfully.'
    },

    badConfiguration: {
      description: 'The configuration was invalid.'
    }

  },


  fn: function registerDataStore(inputs, exits) {
    var _ = require('lodash');
    var PG = require('machinepack-postgresql');

    // Validate that the datastore isn't already initialized
    if (inputs.datastores[inputs.config.identity]) {
      return exits.badConfiguration(new Error('Connection config is already registered.'));
    }

    //  ╦  ╦╔═╗╦  ╦╔╦╗╔═╗╔╦╗╔═╗  ┌─┐┌─┐┌┐┌┌─┐┬┌─┐
    //  ╚╗╔╝╠═╣║  ║ ║║╠═╣ ║ ║╣   │  │ ││││├┤ ││ ┬
    //   ╚╝ ╩ ╩╩═╝╩═╩╝╩ ╩ ╩ ╚═╝  └─┘└─┘┘└┘└  ┴└─┘
    // If a URL config value was not given, ensure that all the various pieces
    // needed to create one exist.
    var hasURL = _.has(inputs.config, 'url');

    // Validate that the connection has an identity property
    if (!inputs.config.identity) {
      return exits.badConfiguration(new Error('Connection config is missing an identity.'));
    }

    // Validate that the connection has a host and database property
    if (!hasURL && !inputs.config.host) {
      return exits.badConfiguration(new Error('Connection config is missing a host value.'));
    }

    if (!hasURL && !inputs.config.database) {
      return exits.badConfiguration(new Error('Connection config is missing a database value.'));
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
    PG.createManager({
      connectionString: inputs.config.url
    })
    .exec({
      error: function error(err) {
        return exits.error(new Error('There was an error creating a new manager for the connection with a url of: ' + inputs.config.url + '\n\n' + err.stack));
      },
      failed: function failed(err) {
        return exits.badConfiguration(new Error('There was an error creating a new manager for the connection with a url of: ' + inputs.config.url + '\n\n' + err.stack));
      },
      malformed: function malformed(err) {
        return exits.badConfiguration(new Error('There was an error creating a new manager for the connection with a url of: ' + inputs.config.url + '\n\n' + err.stack));
      },
      success: function success(report) {
        // Build up a database schema for this connection that can be used
        // throughout the adapter
        var dbSchema = {};

        _.each(inputs.models, function buildSchema(val, key) {
          var _schema = val.waterline && val.waterline.schema && val.waterline.schema[val.identity];
          if (!_schema) {
            return;
          }

          // Set defaults to ensure values are set
          if (!_schema.attributes) {
            _schema.attributes = {};
          }

          if (!_schema.tableName) {
            _schema.tableName = key;
          }

          // If the connection names are't the same we don't need it in the schema
          if (!_.includes(val.connection, inputs.config.identity)) {
            return;
          }

          // If the tableName is different from the identity, store the tableName in the schema
          var schemaKey = key;
          if (_schema.tableName !== key) {
            schemaKey = _schema.tableName;
          }

          dbSchema[schemaKey] = _schema;
        });

        // Store the connection
        inputs.datastores[inputs.config.identity] = {
          manager: report.manager,
          config: inputs.config,
          models: inputs.models,
          dbSchema: dbSchema,
          driver: PG
        };

        // Always call describe on each individual collection table.
        // This adds properties such as auto-increment, indexes, etc.
        // async.map(_.keys(models), function describe(colName, cb) {
        //   self.describe(connectionConfig.identity, colName, cb);
        // }, cb);

        return exits.success();
      }
    });
  }

});
