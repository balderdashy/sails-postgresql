//  ███████╗███████╗██╗     ███████╗ ██████╗████████╗     █████╗  ██████╗████████╗██╗ ██████╗ ███╗   ██╗
//  ██╔════╝██╔════╝██║     ██╔════╝██╔════╝╚══██╔══╝    ██╔══██╗██╔════╝╚══██╔══╝██║██╔═══██╗████╗  ██║
//  ███████╗█████╗  ██║     █████╗  ██║        ██║       ███████║██║        ██║   ██║██║   ██║██╔██╗ ██║
//  ╚════██║██╔══╝  ██║     ██╔══╝  ██║        ██║       ██╔══██║██║        ██║   ██║██║   ██║██║╚██╗██║
//  ███████║███████╗███████╗███████╗╚██████╗   ██║       ██║  ██║╚██████╗   ██║   ██║╚██████╔╝██║ ╚████║
//  ╚══════╝╚══════╝╚══════╝╚══════╝ ╚═════╝   ╚═╝       ╚═╝  ╚═╝ ╚═════╝   ╚═╝   ╚═╝ ╚═════╝ ╚═╝  ╚═══╝
//

module.exports = require('machine').build({


  friendlyName: 'Select',


  description: 'Find record(s) in the database.',


  inputs: {

    datastore: {
      description: 'The datastore to use for connections.',
      extendedDescription: 'Datastores represent the config and manager required to obtain an active database connection.',
      required: true,
      readOnly: true,
      example: '==='
    },

    tableName: {
      description: 'The name of the table to search in.',
      required: true,
      example: 'users'
    },

    criteria: {
      description: 'The Waterline criteria object to use for the query.',
      required: true,
      example: {}
    }

  },


  exits: {

    success: {
      description: 'The results of the select query.',
      outputVariableName: 'records',
      example: {
        records: [{}]
      }
    },

    invalidDatastore: {
      description: 'The datastore used is invalid. It is missing key pieces.'
    },

    badConnection: {
      friendlyName: 'Bad connection',
      description: 'A connection either could not be obtained or there was an error using the connection.'
    }

  },


  fn: function create(inputs, exits) {
    var PG = require('machinepack-postgresql');
    var Converter = require('machinepack-waterline-query-converter');
    var Helpers = require('./private');


    // Ensure that a model can be found on the datastore.
    var model = inputs.datastore.models && inputs.datastore.models[inputs.tableName];
    if (!model) {
      return exits.invalidDatastore();
    }

    // Default the postgres schemaName to "public"
    var schemaName = 'public';

    // Check if a schemaName was manually defined
    if (model.meta && model.meta.schemaName) {
      schemaName = model.meta.schemaName;
    }


    //  ╔═╗╔═╗╔╗╔╦  ╦╔═╗╦═╗╔╦╗  ┌┬┐┌─┐  ┌─┐┌┬┐┌─┐┌┬┐┌─┐┌┬┐┌─┐┌┐┌┌┬┐
    //  ║  ║ ║║║║╚╗╔╝║╣ ╠╦╝ ║    │ │ │  └─┐ │ ├─┤ │ ├┤ │││├┤ │││ │
    //  ╚═╝╚═╝╝╚╝ ╚╝ ╚═╝╩╚═ ╩    ┴ └─┘  └─┘ ┴ ┴ ┴ ┴ └─┘┴ ┴└─┘┘└┘ ┴
    // Convert the Waterline criteria into a Waterline Query Statement. This
    // turns it into something that is declarative and can be easily used to
    // build a SQL query.
    // See: https://github.com/treelinehq/waterline-query-docs for more info
    // on Waterline Query Statements.
    var statement;
    try {
      statement = Converter.convert({
        model: inputs.tableName,
        method: 'find',
        criteria: inputs.criteria
      }).execSync();
    } catch (e) {
      return exits.error(e);
    }


    //  ███╗   ██╗ █████╗ ███╗   ███╗███████╗██████╗
    //  ████╗  ██║██╔══██╗████╗ ████║██╔════╝██╔══██╗
    //  ██╔██╗ ██║███████║██╔████╔██║█████╗  ██║  ██║
    //  ██║╚██╗██║██╔══██║██║╚██╔╝██║██╔══╝  ██║  ██║
    //  ██║ ╚████║██║  ██║██║ ╚═╝ ██║███████╗██████╔╝
    //  ╚═╝  ╚═══╝╚═╝  ╚═╝╚═╝     ╚═╝╚══════╝╚═════╝
    //
    //  ███████╗██╗   ██╗███╗   ██╗ ██████╗████████╗██╗ ██████╗ ███╗   ██╗███████╗
    //  ██╔════╝██║   ██║████╗  ██║██╔════╝╚══██╔══╝██║██╔═══██╗████╗  ██║██╔════╝
    //  █████╗  ██║   ██║██╔██╗ ██║██║        ██║   ██║██║   ██║██╔██╗ ██║███████╗
    //  ██╔══╝  ██║   ██║██║╚██╗██║██║        ██║   ██║██║   ██║██║╚██╗██║╚════██║
    //  ██║     ╚██████╔╝██║ ╚████║╚██████╗   ██║   ██║╚██████╔╝██║ ╚████║███████║
    //  ╚═╝      ╚═════╝ ╚═╝  ╚═══╝ ╚═════╝   ╚═╝   ╚═╝ ╚═════╝ ╚═╝  ╚═══╝╚══════╝
    //
    // Prevent Callback Hell and such.


    //  ╔═╗╔═╗╔╦╗╔═╗╦╦  ╔═╗  ┌─┐┌┬┐┌─┐┌┬┐┌─┐┌┬┐┌─┐┌┐┌┌┬┐
    //  ║  ║ ║║║║╠═╝║║  ║╣   └─┐ │ ├─┤ │ ├┤ │││├┤ │││ │
    //  ╚═╝╚═╝╩ ╩╩  ╩╩═╝╚═╝  └─┘ ┴ ┴ ┴ ┴ └─┘┴ ┴└─┘┘└┘ ┴
    // Transform the Waterline Query Statement into a SQL query.
    var compileStatement = function compileStatement(done) {
      PG.compileStatement({
        statement: statement
      })
      .exec({
        error: function error(err) {
          return done(err);
        },
        success: function success(report) {
          return done(null, report.nativeQuery);
        }
      });
    };


    //  ╔═╗╔═╗╔═╗╦ ╦╔╗╔  ┌─┐┌─┐┌┐┌┌┐┌┌─┐┌─┐┌┬┐┬┌─┐┌┐┌
    //  ╚═╗╠═╝╠═╣║║║║║║  │  │ │││││││├┤ │   │ ││ ││││
    //  ╚═╝╩  ╩ ╩╚╩╝╝╚╝  └─┘└─┘┘└┘┘└┘└─┘└─┘ ┴ ┴└─┘┘└┘
    var spawnConnection = function spawnConnection(done) {
      Helpers.spawnConnection({
        datastore: inputs.datastore
      })
      .exec({
        error: function error(err) {
          return done(err);
        },
        success: function success(connection) {
          return done(null, connection);
        }
      });
    };


    //  ╦═╗╦ ╦╔╗╔  ┌─┐┌─┐┬  ┌─┐┌─┐┌┬┐  ┌─┐ ┬ ┬┌─┐┬─┐┬ ┬
    //  ╠╦╝║ ║║║║  └─┐├┤ │  ├┤ │   │   │─┼┐│ │├┤ ├┬┘└┬┘
    //  ╩╚═╚═╝╝╚╝  └─┘└─┘┴─┘└─┘└─┘ ┴   └─┘└└─┘└─┘┴└─ ┴
    var runSelectQuery = function runSelectQuery(connection, query, done) {
      Helpers.runQuery({
        connection: connection,
        nativeQuery: query,
        queryType: 'select',
        disconnectOnError: true
      })
      .exec({
        // The runQuery helper will automatically release the connection on error.
        error: function error(err) {
          done(err);
        },
        success: function success(report) {
          return done(null, report.result);
        }
      });
    };


    //   █████╗  ██████╗████████╗██╗ ██████╗ ███╗   ██╗
    //  ██╔══██╗██╔════╝╚══██╔══╝██║██╔═══██╗████╗  ██║
    //  ███████║██║        ██║   ██║██║   ██║██╔██╗ ██║
    //  ██╔══██║██║        ██║   ██║██║   ██║██║╚██╗██║
    //  ██║  ██║╚██████╗   ██║   ██║╚██████╔╝██║ ╚████║
    //  ╚═╝  ╚═╝ ╚═════╝   ╚═╝   ╚═╝ ╚═════╝ ╚═╝  ╚═══╝
    //
    //  ██╗      ██████╗  ██████╗ ██╗ ██████╗
    //  ██║     ██╔═══██╗██╔════╝ ██║██╔════╝
    //  ██║     ██║   ██║██║  ███╗██║██║
    //  ██║     ██║   ██║██║   ██║██║██║
    //  ███████╗╚██████╔╝╚██████╔╝██║╚██████╗
    //  ╚══════╝ ╚═════╝  ╚═════╝ ╚═╝ ╚═════╝
    //

    // Compile the original Waterline Query
    compileStatement(function cb(err, query) {
      if (err) {
        return exits.error(err);
      }

      // Spawn a new connection for running queries on.
      spawnConnection(function cb(err, connection) {
        if (err) {
          return exits.badConnection(err);
        }

        // Run the SELECT query
        runSelectQuery(connection, query, function cb(err, foundRecords) {
          if (err) {
            return exits.badConnection(err);
          }

          //  ╔═╗╔═╗╔═╗╔╦╗  ┬  ┬┌─┐┬  ┬ ┬┌─┐┌─┐
          //  ║  ╠═╣╚═╗ ║   └┐┌┘├─┤│  │ │├┤ └─┐
          //  ╚═╝╩ ╩╚═╝ ╩    └┘ ┴ ┴┴─┘└─┘└─┘└─┘
          var castResults = Helpers.normalizeValues({
            schema: model.dbSchema,
            records: foundRecords
          }).execSync();

          return exits.success({ records: castResults });
        }); // </ .runSelectQuery(); >
      }); // </ .spawnTransaction(); >
    }); // </ .compileStatement(); >
  }

});
