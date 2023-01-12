-module(beanstalkd_consumer_app).

-include_lib("ebeanstalkd/include/ebeanstalkd.hrl").

-define(BK_POOL_QUEUE(ServerName), binary_to_atom(<<"queue_", (ServerName)/binary>>, utf8)).
-define(BK_POOL_CONSUMER(ServerName, ConsumerName), binary_to_atom(<<"consumer_" , (ServerName)/binary, "_", (ConsumerName)/binary>>, utf8)).

-define(DEFAULT_QUEUES_PER_POOL, 1).
-define(DEFAULT_CONSUMERS_PER_POOL, 1).
-define(DEFAULT_CONCURRENCY, 1).

-behaviour(application).

-export([
    start/2,
    start_consumers/0,
    prep_stop/1,
    stop/1
]).

start(_StartType, _StartArgs) ->
    ok = start_consumers(),
    beanstalkd_consumer_sup:start_link().

start_consumers() ->
    ServersFun = fun({ServerName, Params}) ->
        case beanstalkd_utils:lookup(start_at_startup, Params) of
            true ->
                ServerNameBin = atom_to_binary(ServerName, utf8),
                ConnectionInfo = beanstalkd_utils:lookup(connection_info, Params, []),
                ConsumersList = beanstalkd_utils:lookup(consumers, Params),
                QueuesNrInstances = beanstalkd_utils:lookup(queues_number, Params, ?DEFAULT_QUEUES_PER_POOL),
                {ok, QueuePoolId} = create_queues(ServerNameBin, ConnectionInfo, QueuesNrInstances),
                ok = create_consumer_pool(ServerNameBin, ConnectionInfo, QueuePoolId, ConsumersList);
            _ ->
                ok
        end
    end,

    lists:foreach(ServersFun, beanstalkd_utils:get_env(servers)).

prep_stop(_State) ->
    Servers = beanstalkd_utils:get_env(servers),

    % stop consumers

    StopFun = fun({ServerName, Params}) ->
        ServerNameBin = atom_to_binary(ServerName, utf8),
        Fun = fun({ConsumerName, _}) -> ok = erlpool:stop_pool(?BK_POOL_CONSUMER(ServerNameBin, atom_to_binary(ConsumerName, utf8))) end,
        lists:foreach(Fun, beanstalkd_utils:lookup(consumers, Params))
    end,

    ok = lists:foreach(StopFun, Servers),

    % stop queues

    ok = lists:foreach(fun({Name, _}) -> ok = erlpool:stop_pool(?BK_POOL_QUEUE(atom_to_binary(Name, utf8))) end, Servers).

stop(_State) ->
    ok.

% internals

create_queues(ServerName, ConnectionInfo, QueuesCount) ->
    Args = [
        {size, QueuesCount},
        {start_mfa, {beanstalkd_queue, start_link, [ConnectionInfo]}},
        {supervisor_period, 1},
        {supervisor_intensity, 1000},
        {supervisor_shutdown, infinity}
    ],
    Name = ?BK_POOL_QUEUE(ServerName),
    ok = erlpool:start_pool(Name, Args),
    {ok, Name}.

create_consumer_pool(ServerNameBin, ConnectionInfo, QueuePoolId, Consumers) ->
    FunCreate = fun ({ConsumerName, Params}) ->
        ConsumerNameBin = atom_to_binary(ConsumerName, utf8),
        ConsumerId = ?BK_POOL_CONSUMER(ServerNameBin, ConsumerNameBin),
        Instances = beanstalkd_utils:lookup(instances, Params, ?DEFAULT_CONSUMERS_PER_POOL),
        ConcurrentJobs = beanstalkd_utils:lookup(concurrent_jobs, Params, ?DEFAULT_CONCURRENCY),
        WorkersPerInstance = erlang:min(1, trunc(ConcurrentJobs/Instances)),

        ConsumerArgs = [
            {id, ConsumerId},
            {queue_pool_id, QueuePoolId},
            {concurrent_jobs, WorkersPerInstance},
            {connection_info, ConnectionInfo},
            {callbacks, beanstalkd_utils:lookup(callbacks, Params)}
        ],

        Args = [
            {size, Instances},
            {start_mfa, {beanstalkd_consumer, start_link, [ConsumerArgs]}},
            {supervisor_period, 1},
            {supervisor_intensity, 1000},
            {supervisor_shutdown, infinity}
        ],

        ok = erlpool:start_pool(ConsumerId, Args)
    end,
    lists:foreach(FunCreate, Consumers).
