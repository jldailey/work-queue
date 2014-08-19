$ = require 'bling'
assert = require 'assert'
Q = require '../index.coffee'

test_db = "mongodb://localhost:27017/test"

describe 'WorkQueue', ->
	describe ".connect(url)", ->
		it "connects to a database", ->
			queue = Q.connect(test_db)
			assert.deepEqual $.keysOf(queue), ['close','clear','remove','count','push','pop','register','createWorker']
		it "can connect to different databases", ->
			queueA = Q.connect(test_db + "A")
			queueB = Q.connect(test_db + "B")
			assert.notEqual queueA, queueB
		it "can connect to different collections in one database", ->
			queueA = Q.connect(test_db, { collection: "workQueueA" })
			queueB = Q.connect(test_db, { collection: "workQueueB" })
			assert.notEqual queueA, queueB

	describe '.clear()', ->
		it "clears all items", (pass) ->
			Q.connect(test_db).clear().count (err, count) ->
				assert.equal err, null
				assert.equal count, 0
				pass()

	describe '.push()', ->
		it "adds work items (chainable)", (pass) ->
			q = Q.connect(test_db)
			q.clear().push( type: "my-type", schedule: { at: $.now } )
			$.delay 30, ->
				q.count (err, count) ->
					assert.equal err, null
					assert.equal count, 1
					pass()

	describe '.pop()', ->
		it "yields the next item, and a finalizer", (pass) ->
			q = Q.connect(test_db)
			q.clear().push( type: "pop-test", schedule: { at: $.now } )
			$.delay 30, -> # make sure that the new item is really 'due'
				q.pop (err, item, done) ->
					assert.equal err, null
					assert.notEqual item, null
					assert $.is 'function', done
					assert.equal item.type, "pop-test"
					done()
					$.delay 30, ->
						q.count (err, count) ->
							assert.equal err, null
							assert.equal count, 0
							pass()

	describe ".register()", ->
		it "can register handlers by type", ->
			Q.connect(test_db).register "my-type", (err, item, done) ->
	describe ".createWorker()", ->
		it "creates a Worker", ->
			worker = Q.connect(test_db).createWorker()
			assert $.is 'function', worker.pause
			assert $.is 'function', worker.resume

		describe "a Worker", ->
			it "processes jobs when running", (pass) ->
				q = Q.connect(test_db)
				q.register "test-type", (item, done) ->
					assert.equal $.type(done), 'function'
					done()
					assert.notEqual item, null
					assert.equal item.type, "test-type"
					pass()

				q.clear().push( { type: "test-type", schedule: { at: $.now } } )
				worker = q.createWorker()
				worker.resume()
				$.delay 500, worker.pause

	describe ".close()", ->
		it "is a function", ->
			assert $.is 'function', Q.connect(test_db).close
		it "returns a promise", (done) ->
			Q.connect(test_db).close().wait (err) -> done()
		it "should pause all workers"
