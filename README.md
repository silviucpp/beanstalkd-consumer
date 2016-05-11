# beanstalkd_consumer

Erlang consumer framework for beanstalkd work queue

Idea behind the project
-----------------------

- An easy and configurable app that it's starting a pool of consumers which executes jobs from a beanstalkd server.
- You can tune the concurrency and rate limit the number of jobs that are executed in parallel (todo: rate limiter)
- In case the connection between consumer and server goes down minimise the damage (jobs being cached by another consumer and executed again for example).
- In case the consumer is stopped will wait for all jobs in progress to complete. (todo)

What's the lifetime of a job
----------------------------

- Once a job is reserved before being sent to be processed is buried first. 
- In case the processing was completed fine (processing callback returns `ok`) then the job is deleted 
- In case job processing is failling is left in the buried state. 
- In case for some reason the job was not scheduled (processing limits hit or any other reason) the job is scheduled again for being processed (kick-job).
- All delete/kick operations are taking place on another processes where are queued so in case connection to the server goes down the operations are not lost.

Quick start
-----------

Define a module with a function with arity 2 for example:

```erlang
-module(test).
-export([process/2]).
process(Id, Payload) ->
    io:format(<<"id:~p job:~p ~n">>, [Id, Payload]),
    ok.
```

Use a config similar with:

```erlang
[
    {beanstalkd_consumer, [
        {pools, [
            {default_pool, [
                {connection_info, [{host, {127,0,0,1}}, {port, 11300}, {timeout, 5000}, {tube, undefined}]},
                {queues_number, 1},
                {consumers_number, 1},
                {consumer_callback, {test,process}},
                {consumer_concurrent_jobs, 10}
            ]}
        ]}
    ]
}].
```

Where

- `queues_number` - number of processes that will handle the deletes and kick operations. Those are queued in case the 
connection to the server is not up and are sent again once connection is established.
- `consumers_number` - number of processes that are handle-ing the reserving process
- `consumer_callback` - the module and function that will receive the reserved job
- `consumer_concurrent_jobs` - how many concurrent jobs can run in parallel.

```erlang
application:start(beanstalkd_consumer).
```

