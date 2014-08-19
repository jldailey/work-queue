#!/usr/bin/env coffee

Optimist = require('optimist')
$ = require 'bling'
opts = Optimist \
	.options('c', { \
		alias: 'collection',
		default: 'workQueue',
		describe: 'the collection to hold work orders in'
	}) \
	.options('i', { \
		alias: 'interval',
		default: 100
		describe: 'when idle, how often to look for new work'
	}) \
	.options('r', { \
		alias: 'require',
		default: '',
		describe: 'require this/these module(s), which should export type handlers'
	}) \
	.option('d', { \
		alias: 'demo',
		default: false
		describe: 'DANGEROUS: load an example queue as a test, will flush all jobs in the specified collection'
	}) \
	.demand(1)
	.check( (argv) ->
		unless /^mongodb:/.test argv._[0]
			throw new Error("url must begin with mongodb://")
	)
	.usage("Usage: $0 [options...] mongodb://host:port/db_name")
	.argv
url = opts._[0]
mins = 60*1000

$.log "Options:", opts

Q = require("../index.coffee").connect url, { collection: opts.collection }

if $.is 'array', opts.require
	for r in opts.require
		$.log "Requiring module '#{r}'..."
		for type, handler of require(r)
			$.log "Registering type from '#{r}':", type
			$.assert ($.is 'string', type), "type must be string: #{type}"
			$.assert ($.is 'function', handler), "handler must be function: #{handler}"
			Q.register type, handler

worker = Q.createWorker {
	idle_delay: opts.interval
}

worker.resume()

if opts.demo
	Q.register 'echo', (item, done) ->
		$.log "ECHO:", item.message
		done()
	Q.clear().push(
		type: "echo"
		schedule: { every: .1*mins, maxFail: Infinity }
		message: "Should recur every six seconds"
		_id: "only_one"
	).push(
		type: "echo"
		schedule: { after: .5*mins }
		message: "Once after thirty seconds"
	).push(
		type: "echo"
		schedule: { at: $.now + 3000 }
		message: "Once after three seconds"
	)

	$.delay 34000, ->
		$.log "Ending demo."
		worker.pause()
		Q.close()
