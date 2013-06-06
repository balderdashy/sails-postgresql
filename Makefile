
MOCHA_OPTS= --check-leaks
REPORTER = dot

test: test-unit test-integration

test-unit:
	@NODE_ENV=test ./node_modules/.bin/mocha \
		--reporter $(REPORTER) \
		$(MOCHA_OPTS)

test-integration:
	@NODE_ENV=test node test/integration/runner.js
