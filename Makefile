
MOCHA=node_modules/.bin/mocha
MOCHA_FMT=spec
MOCHA_OPTS=--compilers coffee:coffee-script/register --globals Bling,$$ -R ${MOCHA_FMT} -s 100 --bail

all: test

test: ${MOCHA} test/index.coffee.pass

test/%.coffee: %.coffee

test/%.coffee.pass: test/%.coffee Makefile
	$(MOCHA) $(MOCHA_OPTS) $< && touch $@

${MOCHA}:
	npm install mocha assert bling
