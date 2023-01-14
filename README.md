# beanstalkd-consumer

[![Build Status](https://travis-ci.com/silviucpp/beanstalkd-consumer.svg?branch=master)](https://travis-ci.com/github/silviucpp/beanstalkd-consumer)
[![GitHub](https://img.shields.io/github/license/silviucpp/beanstalkd-consumer)](https://github.com/silviucpp/beanstalkd-consumer/blob/master/LICENSE)
[![Hex.pm](https://img.shields.io/hexpm/v/beanstalkd-consumer)](https://hex.pm/packages/beanstalkd-consumer)

Erlang consumer framework for beanstalkd work queue.

### Idea behind the project

- An easy and configurable app that it's starting a pool of consumers which executes jobs from a beanstalkd server.
- You can limit the number of concurrent jobs.
- In case the consumer is stopped will wait for all jobs in progress to complete.
- Make sure a job is not executed twice.

### What's the lifetime of a job

- Once a job is reserved before being sent to be processed is buried first. 
- In case the execution completed fine (no exception triggered) then the job is deleted. 
- In case job execution failed, the job is left in the buried state for manual review.
- All delete operations are taking place on another processes where are queued so in case connection to the server goes down the operations are not lost.

### Quick start

All consumers need to implement the `beanstalkd_consumer` behaviour. 

```erlang
-callback init(TubeName::binary()) ->
    {ok, State::any()}.

-callback process(JobId::non_neg_integer(), JobPayload::binary(), State::any()) ->
    any().
```

Example:

```erlang
-module(dummy_consumer).

-export([
    init/1, 
    process/3
]).

init(TubeName) ->
    io:format(<<"~p:init: ~p ~n">>, [?MODULE, TubeName]),
    {ok, #{}}.

process(JobId, JobPayload, State)  ->
    io:format(<<"~p:process: ~p ~n">>, [?MODULE, {JobId, JobPayload, State}]).
```

You can define the consumer pools into `sys.config` as fallow:

```erlang
[
    {beanstalkd_consumer, [
        {servers, [
            {default_server, [
                {start_at_startup, true},
                {connection_info, [{host, {127,0,0,1}}, {port, 11300}, {timeout, 5000}]},
                {queues_number, 1},
                {consumers, [
                    {consumer_silviu, [
                        {instances, 1},
                        {callbacks, [
                            {<<"tube_name">>, dummy_consumer}
                        ]},
                        {concurrent_jobs, 1000}
                    ]}
                ]}
            ]}
        ]}
    ]
}].
```

Where

- `start_at_startup` - specify if the consuming of messages should start right away when the application is started. In case you have the `beanstalkd_consumer` as dependency and you need to load more other stuffs into your current app before starting consuming events, you can put this property on `false` and use `beanstalkd_consumer_app:start_consumers/0` to start the consumers.
- `connection_info` - connection details. See [ebeanstalkd][1] for details.
- `queues_number` - number of processes that will handle the `delete` operations. Those are queued in case the connection to the server is not up and are sent again once connection is established.

For each consumer:

- `instances` - number of consumer instances. 
- `callbacks` - `[{Tube, Module}]`. Each item in list is formed from the tub name and the module that will handle the jobs for that tube.
- `concurrent_jobs` - how many concurrent jobs can run in parallel.

[1]:https://github.com/silviucpp/ebeanstalkd
