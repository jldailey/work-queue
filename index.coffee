#
# This is a background daemon that run separately from the main application,
# but connects to the same database, and takes care of various house-keeping.
#
# Tasks get inserted into a work-queue collection in mongo, by anyone.
#
# They can be scheduled as
# recurring: { schedule: { every: ms } }
# delayed: { schedule: { after: ms } }
# scheduled: { schedule: { at: timestamp } }
# immediate: # no when clause
#
# This module is intended to be used is two different contexts:
# one process is unlikely to both read and write.
#
# For application processes that want to enqueue new work items, usage looksl ie

$ = require 'bling'
MongoClient = require('mongodb').MongoClient
die = (code, msg) ->
	$.log msg
	process.exit code

$.extend module.exports, {
	connect: (url, opts) ->
		ready = $.Promise().wait (err) ->
			if err then throw new Error("Connection failed to url: #{url} error: #{String(err)}")
		opts = $.extend {
			collection: "workQueue"
			id: [ "reader-", 5 ] # get 5 random characters appended
		}, opts
		unsafe = safe: false
		if $.is 'string', opts.id then opts.id = [ opts.id, 0 ]
		MongoClient.connect url, unsafe, (err, db) ->
			if err then return ready.fail(err)

			q = db.collection(opts.collection)
			q.ensureIndex { status: 1, readyAt: 1 }, unsafe, (err) ->

			getNewReaderId = -> $.random.string opts.id[0].length + opts.id[1], opts.id[0]

			Queue = ->
				readerId = getNewReaderId()
				log = $.logger "[worker #{readerId}]"
				handlers = {}

				ifNotBusy = (cb) ->
					q.find( status: readerId ).count (err, count) ->
						if count > 0 then cb "busy", null
						else cb null

				peekNextItem = (cb) ->
					q.findOne( { status: "new", readyAt: { $lt: $.now } }, { sort: { readyAt: 1 } }, cb )

				getNextItem = (cb) ->
					q.update( # Atomically mark one document as ours
						{ status: "new", readyAt: { $lt: $.now } },
						{ $set: { status: readerId, mtime: $.now } },
						{ multi: false, safe: true }, (err) ->
							if err then cb err, null
							else q.find( # Then retrieve it.
								{ status: readerId },
								null,
								{ safe: true }
							).nextObject cb
					)

				markAsFailed = (doc, cb) ->
					q.update \
						{ _id: doc._id },
						{ $set: { status: "failed", mtime: $.now } },
						{ multi: false, safe: true }, cb

				markAsRetry = (doc, cb) ->
					q.update \
						{ _id: doc._id },
						{ $set: { status: "new", readyAt: $.now + 1000, mtime: $.now }, $inc: { retryCount: 1 } },
						{ multi: false, safe: true }, cb

				markAsComplete = (doc, cb) ->
					status = "complete"
					readyAt = doc.readyAt
					if doc.schedule?.every
						status = "new"
						readyAt = $.now + doc.schedule.every
					q.update { _id: doc._id }, \
						{ $set: {
								status: status
								readyAt: readyAt
								mtime: $.now
							}
						}, { multi: false, safe: true }, cb

				return {
					close: ->
						p = $.Promise()
						db.close ->
							ready.reset()
							ready.wait (err) ->
								if err then throw new Error("Connection failed to url: #{url} error: #{String(err)}")
							p.resolve()
						p
					clear: -> q.remove {}, (err) ->
					remove: (query) -> q.remove query, (err) ->
					count: (cb) -> q.count { status: "new" }, cb
					push: (item) ->
						unless 'type' of item
							$.log "throwing error"
							throw new Error("a 'type' is required on work items")
						# TODO: if item has an _id that is already in the db
						# and the doc in the db is status: "failed", allow overwrite
						q.insert $.extend(item,
							status: "new"
							ctime: $.now
							mtime: $.now
							readyAt: switch
								when not item.schedule? then $.now
								when item.schedule.after? then $.now + parseFloat item.schedule.after
								when item.schedule.at? then item.schedule.at
								else $.now
						), { safe: true }, (err, doc) ->
						@
					pop: (cb) ->
						check = (f) -> (err, data) ->
							if err then cb err, null
							else f data
						ifNotBusy check ->
							getNextItem check (doc) ->
								unless doc? then cb null, null
								else cb null, doc, (err, cbb) ->
									if err
										doc.retryCount ?= 0
										maxFail = doc.schedule?.maxFail ? Infinity
										if doc.retryCount > maxFail
											markAsFailed doc, (err) ->
												cbb? "too many retries", null
										else
											markAsRetry doc, (err) ->
												cbb? "will retry", null
									else
										markAsComplete doc, (err) ->
											cbb? null, doc
						@
					register: (type, handler) ->
						handlers[type] = handler
						@
					createWorker: (worker_opts) ->
						paused = true
						busy_count = 0
						worker_opts = $.extend {
							idle_delay: 100 # polling interval when the queue is empty
							busy_delay: 500 # delay interval if we try to fetch new items while still working on an old one
							busy_max: 10 # give up on a work item if we work on it for longer than (busy_max * busy_delay) ms
						}, worker_opts
						nextItem = (err) =>
							if err then log "error:", err
							return if paused
							@pop (err, item, done) -> switch
								# Failure
								when err then switch err
									# Busy: we still have outstanding items
									when "busy"
										if ++busy_count > worker_opts.busy_max
											readerId = getNewReaderId()
											log = $.logger "worker #{readerId}"
										$.delay worker_opts.busy_delay, nextItem
								# Idle: wait for idle_delay and poll the next item
								when (not item?) and (not done?) then $.delay worker_opts.idle_delay, nextItem
								# Real work! Execute the handler for this type of job
								when item.type of handlers then handlers[item.type] item, -> done null, nextItem
								else done "unknown type: #{JSON.stringify item}"
							null
						return {
							pause: ->
								paused = true
								@
							resume: ->
								if paused
									paused = false
									do nextItem
								@
						}
				}

			ready.finish Queue()

		return ret = {
			close:    ()              ->
				p = $.Promise()
				ready.then  (q) -> q.close().wait (err) -> if err then p.reject(err) else p.resolve()
				p
			clear:    ()              -> ready.then( (q) -> q.clear()); ret
			remove:   (query)         -> ready.then( (q) -> q.remove(query)); ret
			count:    (cb)            -> ready.then( (q) -> q.count(cb)); ret
			push:     (item)          -> ready.then( (q) -> q.push(item)); ret
			pop:      (cb)            -> ready.then( (q) -> q.pop(cb)); ret
			register: (type, handler) -> ready.then( (q) -> q.register(type, handler)); ret
			createWorker: (opts)      ->
				worker = null
				ready.then (q) -> worker = q.createWorker(opts)
				return {
					pause: -> ready.then -> worker.pause()
					resume: -> ready.then -> worker.resume()
				}
		}
}
