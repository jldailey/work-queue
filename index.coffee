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

ready = $.Promise().wait (err) ->
	if err then die 1, err
	else $.log "new connection made"

$.extend module.exports, {
	push: (item) ->
		unless 'type' of item
			throw new Error("a 'type' is required on work items")
		ready.wait( (err, queue) -> queue.push item ); @
	pop:  (cb)      -> ready.wait( (err, queue) -> queue.pop cb ); @
	clear:          -> ready.wait( (err, queue) -> queue.clear() ); @
	remove: (query) -> ready.wait( (err, queue) -> queue.remove(query) ); @
	close:          -> ready.wait (err, queue) -> queue.close()
	count: (cb)     -> ready.wait( (err, queue) -> queue.count cb ); @
	register: (type, handler) ->
		ready.wait (err, queue) -> queue.register type, handler
	createWorker: (opts) ->
		w = $.Promise()
		ready.wait (err, queue) ->
			if err then w.reject err
			else w.resolve queue.createWorker()
		return {
			pause: -> w.then (worker) -> worker.pause()
			resume: -> w.then (worker) -> worker.resume()
		}

	connect: (url, opts) ->
		$.log "connecting to", url
		opts = $.extend {
			collection: "workQueue"
			id: [ "reader-", 5 ] # get 5 random characters appended
		}, opts
		unsafe = safe: false
		if $.is 'string', opts.id then opts.id = [ opts.id, 0 ]
		try return @
		finally MongoClient.connect url, unsafe, (err, db) ->
			if err then return ready.fail(err)

			q = db.collection(opts.collection)
			q.ensureIndex { status: 1, readyAt: 1 }, unsafe, (err) ->

			getNewReaderId = -> $.random.string opts.id[0].length + opts.id[1], opts.id[0]

			Queue = ->
				readerId = getNewReaderId()
				log = $.logger "worker #{readerId}"
				handlers = {}

				ifNotBusy = (cb) ->
					q.find( status: readerId ).count (err, count) ->
						if count > 0 then cb "busy", null
						else cb null

				getNextItem = (cb) ->
					q.update \ # Atomically mark one document as ours
						{ status: "new", readyAt: { $lt: $.now } }, \
						{ $set: {
								status: readerId, # Mark it as owned by us
								mtime: $.now
							}
						}, { multi: false, safe: true }, (err) ->
							if err then cb err, null
							else q.find( # Then retrieve it.
								{ status: readerId },
								null,
								{ safe: true }
							).nextObject cb

				markAsFailed = (doc, cb) ->
					log "too many retries, abandoning #{doc._id}"
					q.update \
						{ _id: doc._id },
						{ $set: {
								status: "failed"
								mtime: $.now
							}
						}, { multi: false, safe: true }, cb

				markAsRetry = (doc, cb) ->
					log "re-queueing item #{doc._id}"
					q.update \
						{ _id: doc._id },
						{ $set: {
								status: "new"
								readyAt: $.now + 1000
								mtime: $.now
							}, "$inc": { retryCount: 1 }
						}, { multi: false, safe: true }, cb

				markAsComplete = (doc, cb) ->
					log "completed item #{doc._id}"
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
						db.close -> $.log "closed"
						ready.reset()
						ready.wait (err) ->
							if err then $.log "connect error:", err
							else $.log "reconnected"

					clear: -> q.remove {}, (err) ->
					remove: (query) -> q.remove query, (err) ->
					count: (cb) -> q.count { status: "new" }, cb
					push: (item) ->
						log "inserting", item
						unless 'type' of item
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
							log "inserted", err ? doc
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
										log "failed item #{doc._id} (count: #{doc.retryCount})", err
										maxFail = doc.schedule?.maxFail ? Infinity
										if doc.retryCount > maxFail
											markAsFailed doc, (err) ->
												if err then log "failed to mark item as 'failed':", err
												else log "marked item as failed"
												cbb? "too many retries", null
										else
											markAsRetry doc, (err) ->
												if err then log "failed to re-queue item #{doc._id}"
												else log "re-queued item #{doc._id}"
												cbb? "will retry", null
									else
										markAsComplete doc, (err) ->
											if err then log "failed to record completion of _id: #{doc._id}", err
											else log "marked item as complete #{doc._id}"
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
							if err then $.log "err", err
							return if paused
							@pop (err, item, done) -> switch
								# Failure
								when err then switch err
									# Busy: we still have outstanding items
									when "busy"
										if ++busy_count > worker_opts.busy_max
											log "busy for too long"
											readerId = getNewReaderId()
											log "starting over with new worker id", readerId
											log = $.logger "worker #{readerId}"
										$.delay worker_opts.busy_delay, nextItem
									# Unknown Error: just log it and move on
									else log "pop error:", err
								# Idle: wait for idle_delay and poll the next item
								when (not item?) and (not done?) then $.delay worker_opts.idle_delay, nextItem
								# Real work! Execute the handler for this type of job
								when item.type of handlers then handlers[item.type] item, -> done null, nextItem
								else done "unknown type: #{JSON.stringify item}"
							null
						pause: ->
							unless paused
								paused = true
							@
						resume: ->
							if paused
								paused = false
								do nextItem
							@
				}

			ready.finish Queue()
}
