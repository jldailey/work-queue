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

	describe '.push()', ->
		it "adds work items (chainable)", (pass) ->
			Q.clear().push( type: "my-type", schedule: { at: $.now } )
			$.delay 30, ->
				Q.count (err, count) ->
					assert.equal err, null
					assert.equal count, 1
					pass()
		it "rejects items without a type", ->
			assert.throws -> Q.push( notype: "haha" )

	describe '.pop()', ->
		it "yields the next item, and a finalizer", (pass) ->
			Q.clear().push( type: "pop-test", schedule: { at: $.now } )
			$.delay 30, -> # make sure that the new item is really 'due'
				Q.pop (err, item, done) ->
					assert.equal err, null
					assert.notEqual item, null
					assert $.is 'function', done
					assert.equal item.type, "pop-test"
					done()
					$.delay 30, ->
						Q.count (err, count) ->
							assert.equal err, null
							assert.equal count, 0
							pass()

	describe ".register()", ->
		it "can register handlers by type", ->
			Q.register "my-type", (err, item, done) ->
	describe ".createWorker()", ->
		it "creates a Worker", ->
			worker = Q.createWorker()
			assert $.is 'function', worker.pause
			assert $.is 'function', worker.resume

		describe "a Worker", ->
			it "processes jobs when running", (pass) ->
				Q.register "test-type", (item, done) ->
					assert.equal $.type(done), 'function'
					done()
					assert.notEqual item, null
					assert.equal item.type, "test-type"
					pass()

				Q.clear().push( { type: "test-type", schedule: { at: $.now } } )
				worker = Q.createWorker()
				worker.resume()
				$.delay 500, worker.pause

	describe ".close()", ->
		it "is a function", ->
			assert $.is 'function', Q.close
		it "returns a promise", (done) ->
			Q.close().wait (err) -> done()
		it "should pause all workers"
