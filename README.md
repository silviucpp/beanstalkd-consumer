# beanstalkd-consumer

[![Build Status](https://app.travis-ci.com/silviucpp/beanstalkd-consumer.svg?branch=master)](https://travis-ci.com/github/silviucpp/beanstalkd-consumer)
[![GitHub](https://img.shields.io/github/license/silviucpp/beanstalkd_consumer)](https://github.com/silviucpp/beanstalkd_consumer/blob/master/LICENSE)
[![Hex.pm](https://img.shields.io/hexpm/v/beanstalkd_consumer)](https://hex.pm/packages/beanstalkd_consumer)

Erlang Consumer Framework for Beanstalkd Work Queue.

## Project Overview

This framework provides a simple and configurable solution for managing a pool of consumers that execute jobs from a Beanstalkd server.

## Key Features

- Easily configurable to launch a pool of consumers for job execution.
- Supports limiting the number of concurrent jobs.
- Ensures all in-progress jobs are completed before the consumer shuts down.
- Prevents duplicate execution of jobs.

## Job Lifecycle

- **Reservation**: When a job is reserved, it is first buried before being sent for processing.
- **Successful Completion**: If the job executes successfully without exceptions, it is deleted.
- **Failed Execution**: If the job execution fails (any exception occurred), based on the exception will behave in the following way:
    - If a job throws a `{bad_argument, Reason::any()}` exception, it is deleted (useful for malformed job payloads).
    - If a job throws a `reschedule_job` exception, it is moved back to the ready state to be retried by the consumer.
    - For any other exception, it remains in the buried state for manual review.
- **Delete/Kick Operations**:All job for kick or delete operations are handled in a separate process and queued. If the connection to the server is lost, these operations are preserved and not discarded.
 
## Quick start

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

- `start_at_startup` - specify if the consuming of messages should start right away when the application is started. In case you have the `beanstalkd_consumer` as dependency, and you need to load more other stuffs into your current app before starting consuming events, you can put this property on `false` and use `beanstalkd_consumer_app:start_consumers/0` to start the consumers.
- `connection_info` - connection details. See [ebeanstalkd][1] for details.
- `queues_number` - number of processes that will handle the `delete` operations. Those are queued in case the connection to the server is not up and are sent again once connection is established.

For each consumer:

- `instances` - number of consumer instances. 
- `callbacks` - `[{Tube, Module}]`. Each item in the list is formed from the tub name and the module that will handle the jobs for that tube.
- `concurrent_jobs` - how many concurrent jobs can run in parallel.

[1]:https://github.com/silviucpp/ebeanstalkd
