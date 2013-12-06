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

bin/queue-reader.coffee
-----------------------

```
Usage: queue-reader [options...] mongodb://host:port/db_name

Options:
  -c, --collection  the collection to hold work orders in                                                         [default: "workQueue"]
  -i, --interval    when idle, how often to look for new work                                                     [default: 100]
  -r, --require     require this/these module(s), which should export type handlers                               [default: ""]
  -d, --demo        DANGEROUS: load an example queue as a test, will flush all jobs in the specificed collection  [default: false]
```

The `-r` or `--require` option is the most important if you want to do real work.  It can be given multiple times, and each string given to it will be passed to `require()` within the reader script.

Each module required in this way should export an object full of `{ type: handler }` pairs.

Example:
```coffee
module.exports['echo'] = (item, done) ->
	console.log item
	done()
```

