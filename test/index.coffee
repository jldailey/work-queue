$ = require 'bling'
assert = require 'assert'
Q = require '../index.coffee'

describe 'WorkQueue', ->
	it "connects to a database (chainable)", ->
		assert.equal Q.connect("mongodb://localhost:27017/test"), Q
	describe '.clear()', ->
		it "clears all items", (pass) ->
			Q.connect("mongodb://localhost:27017/test").clear().count (err, count) ->
				$.log "clear count:", err ? count
				assert.equal err, null
				assert.equal count, 0
				pass()

	'''
	describe '.push()', ->
		it "adds work items (chainable)", (pass) ->
			Q.clear().push( type: "my-type", schedule: { at: $.now } ).count (err, count) ->
				assert.equal err, null
				assert.equal count, 1
				pass()
		it "rejects items without a type", ->
			assert.throws -> Q.push( notype: "haha" )

	describe '.pop()', ->
		it "yields the next item, and a finalizer", (pass) ->
			Q.clear().push( type: "pop-test", schedule: { at: $.now } ).pop (item, done) ->
				assert.equal item.type, "pop-test"
				done()
				Q.count (err, count) ->
					assert.equal err, null
					assert.equal count, 0
					pass()

	describe ".createWorker()", ->
		it "creates a Worker"
		describe "a Worker", ->
			it "can pause"
			it "can resume"
			it "can register handlers by type"
			it "processes jobs when running"

	'''
	it "can be closed", (pass) ->
		assert $.is 'function', Q.close
		assert $.is 'promise', Q.close().wait pass
