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

MongoClient = require('mongodb').MongoClient
$ = require 'bling'
die = (code, msg) ->
	$.log msg
	process.exit code

url = $(process.argv).last()

unless /^mongodb:/.test url
	die 1, "Usage: daemon <mongodb://...>"

MongoClient.connect url, { safe: false }, (err, db) ->
	if err then die 1, err

	q = db.collection("workQueue")
	q.ensureIndex { status: 1, readyAt: 1 }, { safe: false }

	Queue = ->
		reader_id = $.random.string 12, "reader-"
		log = $.logger "worker #{reader_id}"
		ifNotBusy = (cb) ->
			q.find( status: reader_id ).count (err, count) ->
				if count > 0 then cb "busy", null
				else cb null

		getNextItem = (cb) ->
			q.update \ # Atomically mark one document as ours
				{ status: "new", readyAt: { $lt: $.now } }, \
				{ $set: {
						status: reader_id, # Mark it as owned by us
						mtime: $.now
					}
				}, { multi: false, safe: true }, (err) ->
					if err then cb err, null
					else q.find( # Then retrieve it.
						{ status: reader_id },
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
					},
					$inc: { retryCount: 1 }
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
			clear: ->
				q.remove {}, (err) ->
				@
			push: (item) ->
				log "inserting", item
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
		}

	queue = Queue()

	mins = 60*1000

	handlers =
		echo: (item, done) ->
			$.log "ECHO:", item.message
			done()

	$.delay 100, ->
		do nextItem = (err) ->
			if err then $.log "err", err
			queue.pop (err, item, done) -> switch
				when err then switch err
					when "busy" then $.delay 1000, nextItem
					else $.log "pop error:", err
				when (not item?) and (not done?) then $.delay 300, nextItem
				when item.type of handlers then handlers[item.type] item, -> done null, nextItem
				else done "unknown type: #{JSON.stringify item}"

	queue.clear().push(
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

