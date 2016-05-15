# beanstalkd-consumer

Erlang consumer framework for beanstalkd work queue

Idea behind the project
-----------------------

- An easy and configurable app that it's starting a pool of consumers which executes jobs from a beanstalkd server.
- You can tune the concurrency of jobs that are executed in parallel
- In case the connection between consumer and server goes down minimise the damage (jobs being cached by another consumer and executed again for example).
- In case the consumer is stopped will wait for all jobs in progress to complete.

What's the lifetime of a job
----------------------------

- Once a job is reserved before being sent to be processed is buried first. 
- In case the processing was completed fine (processing callback returns `ok`) then the job is deleted 
- In case job processing is failing is left in the buried state. 
- In case for some reason the job was not scheduled (processing limits hit or any other reason) the job is scheduled again for being processed (kick-job).
- All delete/kick operations are taking place on another processes where are queued so in case connection to the server goes down the operations are not lost.

Quick start
-----------

Define a module with a function with arity 3 that will process the jobs and one with arity one used for init process for example:

```erlang
-module(test).
-export([init/1, process/2]).
init(Pid) ->
    [{<<"arg1">>, <<"val1">>}, {<<"arg2">>, <<"val2">>}].
process(Id, Payload, State) ->
    io:format(<<"id:~p job:~p state:~p ~n">>, [Id, Payload, State]),
    ok.
```

Use a config similar with:

```erlang
[
    {beanstalkd_consumer, [

        {servers, [
            {default_server, [
                {connection_info, [{host, {127,0,0,1}}, {port, 11300}, {timeout, 5000}]},
                {queues_number, 1},
                {consumers, [
                    {consumer_silviu, [
                        {tubes, <<"silviu">>},
                        {instances, 1},
                        {callbacks,  {test, init, process}},
                        {concurrent_jobs, 100000}
                    ]}
                ]}
            ]}
        ]}
    ]
}].
```

Where

- `connection_info` - connection details
- `queues_number` - number of processes that will handle the deletes and kick operations. Those are queued in case the 
connection to the server is not up and are sent again once connection is established.

For each consumer:

- `tubes` - The tube/list of tubes that will watch
- `instances` - number of consumers
- `callbacks` - `{Module, InitFun/1, ProcessFun/3}`. the module followed by init function and the function that will handle the jobs.
- `concurrent_jobs` - how many concurrent jobs can run in parallel.

```erlang
application:start(beanstalkd_consumer).
```