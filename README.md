# Sails PostgreSQL Redacted

This tracks [Sails PostgreSQL](https://github.com/balderdashy/sails-postgresql), with the following changes:

* leaking of database passwords into logs is fixed (https://github.com/balderdashy/sails/issues/4595, https://github.com/balderdashy/sails/issues/4606)
* connecting to databases via unix socket is fixed (https://github.com/balderdashy/sails/issues/6888)
* connecting to databases with a pre-initialised `pool` object

Versions track upstream, with an extra `-{increment}`.

# Usage with a pre-defined `PG.pool`

	module.exports.datastores = {
	  default: {
	    adapter: 'sails-postgresql-redacted',
	    pool: yourPoolHere,
	  },
	};

# Usage with Unix Sockets

## General

	const { POSTGRES_HOST, POSTGRES_USER, POSTGRES_PASSWORD, POSTGRES_DATABASE } = process.env;
	module.exports.datastores = {
	  default: {
	    adapter: 'sails-postgresql-redacted',
	    host:     POSTGRES_HOST,
	    user:     POSTGRES_USER,
	    password: POSTGRES_PASSWORD,
	    database: POSTGRES_DATABASE,
	  },
	};

## Google Cloud SQL

	const { GCP_PROJECT, CLOUDSQL_REGION, CLOUDSQL_INSTANCE_NAME } = process.env;
	const { POSTGRES_USER, POSTGRES_PASSWORD, POSTGRES_DATABASE }  = process.env;
	module.exports.datastores = {
	  default: {
	    adapter: 'sails-postgresql-redacted',
	    host:     `/cloudsql/${GCP_PROJECT}:${CLOUDSQL_REGION}:${CLOUDSQL_INSTANCE_NAME}`,
	    user:     POSTGRES_USER,
	    password: POSTGRES_PASSWORD,
	    database: POSTGRES_DATABASE,
	  },
	};
