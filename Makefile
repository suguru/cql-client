MOCHA=./node_modules/mocha/bin/_mocha
ISTANBUL=./node_modules/.bin/istanbul

test:
	@NODE_ENV=test $(MOCHA) -R spec test/*.js

test-cov: clean
	@NODE_ENV=test $(ISTANBUL) cover $(MOCHA) --report lcovonly -- -R spec test/*.js

coveralls:
	cat ./coverage/lcov.info | ./node_modules/codecov.io/bin/codecov.io.js;

clean:
	rm -rf coverage
