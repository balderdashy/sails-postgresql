![image_squidhome@2x.png](http://i.imgur.com/RIvu9.png)

# PostgreSQL Sails/Waterline Adapter

[![Build Status](https://travis-ci.org/balderdashy/sails-postgresql.png?branch=master)](https://travis-ci.org/balderdashy/sails-postgresql) [![NPM version](https://badge.fury.io/js/sails-postgresql.png)](http://badge.fury.io/js/sails-postgresql) [![Dependency Status](https://gemnasium.com/balderdashy/sails-postgresql.png)](https://gemnasium.com/balderdashy/sails-postgresql)

A [Waterline](https://github.com/balderdashy/waterline) adapter for PostgreSQL. May be used in a [Sails](https://github.com/balderdashy/sails) app or anything using Waterline for the ORM.

## Install

Install is via NPM.

```bash
$ npm install sails-postgresql
```

## Configuration

The following config options are available along with their default values:

```javascript
config: {
  database: 'databaseName',
  host: 'localhost',
  user: 'root',
  password: '',
  port: 5432,
  pool: false,
  ssl: false
};
```
Alternatively, you can supply the connection information in URL format:
```javascript
config: {
  url: 'postgres://username:password@hostname:port/database',
  pool: false,
  ssl: false
};
```

We are also testing features for future versions of waterline in postgresql. One of these is case sensitive string searching. In order to enable this feature today you can add the following config flag:

```javascript
postgresql: {
  url: 'postgres://username:password@hostname:port/database',
  wlNext: {
    caseSensitive: true
  }
}
```

## Testing

Test are written with mocha. Integration tests are handled by the [waterline-adapter-tests](https://github.com/balderdashy/waterline-adapter-tests) project, which tests adapter methods against the latest Waterline API.

To run tests:

```bash
$ npm test
```

## About Waterline

Waterline is a new kind of storage and retrieval engine.  It provides a uniform API for accessing stuff from different kinds of databases, protocols, and 3rd party APIs.  That means you write the same code to get users, whether they live in mySQL, LDAP, MongoDB, or Facebook.

To learn more visit the project on GitHub at [Waterline](https://github.com/balderdashy/waterline).
