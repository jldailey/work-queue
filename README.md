The Work Queue
==============

Control a queue of tasks to be scheduled, completed, repeated, etc.

Connecting to the Queue
-----------------------

Use WorkQueue.connect(url, [opts]).

Default opts shown here.

```coffee

WorkQueue = require 'work-queue'

WorkQueue.connect "mongodb://localhost:27017/test", {
	collection: "workQueue",
	readerId: [ "reader-", 5 ], # 5 chars of randomness
	# consider, readerId: "app-server-56"
}

```

Scheduling Jobs
---------------

```

WorkQueue.push {
	type: "my-type"
	schedule: { at: timestamp }
}

```

You can schedule when the job is due using:
 * `at` with an absolute timestamp in ms
 * `every` with an interval in ms, example:
	```coffee
		WorkQueue.push { type: "foo", schedule: { every: 30*1000} }
	```
 * `after` with a delay in ms, example:
	```coffee
		WorkQueue.push { type: "foo", schedule: { after: 5*60*1000 } }
	```

You can combine `every` and `after`, to control when the first iteration occurs.

Working the Queue
-----------------

This is not an abstract Queue.
It is meant to hold items that need to process, complete, fail, retry, and recur.

Documents in the queue always have these fields:

	* type: string
	* ctime: timestamp
	* mtime: timestamp
	* status: "new"|"complete"|"failed"|<worker-id>
	* schedule: object

Plus any fields given when `push`ed.

To turn the current process into a Worker, first you must teach it how
to handle each `type` of item it will find there.

```coffee
	WorkQueue.register 'my-type', (item, done) ->
		# item has all the fields shown above
		doWorkOnItem item, (err) ->
			if err then done(err) # fail
			else done() # all ok!
	
	worker = WorkQueue.createWorker {
		idle_delay: 100 # polling interval if nothing to do
	}
	
	worker.resume()
	# run this example for 10 seconds, then pause
	setTimeout worker.pause, 10000
	
```

A usable example can be found in `bin/queue-reader.coffee`.
