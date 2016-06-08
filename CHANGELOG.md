# Sails PostgreSQL Changelog

### 0.12.2

* [ENHANCEMENT] Update the PG module to work with newer versions of Node. Thanks to [@joaosa](https://github.com/joaosa) for the patch!

* [ENHANCEMENT] Updates the dependencies to the latest versions which should remove any warning messages when installing.

* [BUG] Fixes issues with backwards compatibility to Waterline `0.11.x` and older.

### 0.12.1

* [BUG] Fixes issue with populates due to changes in projections queries coming from Waterline-Sequel. Updated the waterline-sequel dependency to 0.6.2 to fix.

### 0.12.0

* [Enhancement] Upgrades the version of Waterline-Sequel being used to support using projections in join queries. See [#234](https://github.com/balderdashy/sails-postgresql/pull/234) for more details.

* [Enhancement] Adds JSHint and tweaks code style slightly to better support community additions. See [#235](https://github.com/balderdashy/sails-postgresql/pull/235) for more details.

### 0.11.3

* [BUG] Updates [Waterline-Sequel](https://github.com/balderdashy/waterline-sequel) dependency to actually fix the previous date issue.

### 0.11.2

* [BUG] Updates [Waterline-Sequel](https://github.com/balderdashy/waterline-sequel) dependency to gain support for querying dates when they are represented as a string in the criteria.

### 0.11.1

* [DOCS] Added an example of using the `schema` meta key to the readme. See [#223](https://github.com/balderdashy/sails-postgresql/pull/223) for more details.

* [ENHANCEMENT] Locked the dependency versions down to know working versions. Also added a `shrinkwrap.json` file. See [#225](https://github.com/balderdashy/sails-postgresql/pull/225) for more details.

* [ENHANCEMENT] Updated the Travis config to run test on Node 4.0 and 5.0. See [#226](https://github.com/balderdashy/sails-postgresql/pull/226) for more details.

* [PERFORMANCE] And the best for last, merged [#224](https://github.com/balderdashy/sails-postgresql/pull/224) which increases performance on populates ~15x. Thanks a million to [@jianpingw](https://github.com/jianpingw) for spending the time to track this down!
