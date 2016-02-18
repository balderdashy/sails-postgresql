# Sails PostgreSQL Changelog

### 0.11.3

* [BUG] Updates [Waterline-Sequel](https://github.com/balderdashy/waterline-sequel) dependency to actually fix the previous date issue.

### 0.11.2

* [BUG] Updates [Waterline-Sequel](https://github.com/balderdashy/waterline-sequel) dependency to gain support for querying dates when they are represented as a string in the criteria.

### 0.11.1

* [DOCS] Added an example of using the `schema` meta key to the readme. See [#223](https://github.com/balderdashy/sails-postgresql/pull/223) for more details.

* [ENHANCEMENT] Locked the dependency versions down to know working versions. Also added a `shrinkwrap.json` file. See [#225](https://github.com/balderdashy/sails-postgresql/pull/225) for more details.

* [ENHANCEMENT] Updated the Travis config to run test on Node 4.0 and 5.0. See [#226](https://github.com/balderdashy/sails-postgresql/pull/226) for more details.

* [PERFORMANCE] And the best for last, merged [#224](https://github.com/balderdashy/sails-postgresql/pull/224) which increases performance on populates ~15x. Thanks a million to [@jianpingw](https://github.com/jianpingw) for spending the time to track this down!
