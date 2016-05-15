-module(beanstalkd_consumer_sup).

-include("beanstalkd_consumer.hrl").

-define(BK_SUPERVISOR_QUEUE(Name), binary_to_atom(<<"bk_sup_queue_", Name/binary>>, utf8)).
-define(BK_SUPERVISOR_CONSUMER(Name), binary_to_atom(<<"bk_sup_consumer_", Name/binary>>, utf8)).

-define(DEFAULT_QUEUES_PER_POOL, 1).
-define(DEFAULT_CONSUMERS_PER_POOL, 1).
-define(DEFAULT_CONCURRENCY, 1).

-behaviour(supervisor).

-export([start_link/0]).
-export([init/1]).

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

init([]) ->
    ServersFun = fun({ServerName, Params}, Acc) ->
        BinServerName = atom_to_binary(ServerName, utf8),
        ConnectionInfo = bk_utils:lookup(connection_info, Params, []),
        NumberOfQueues = bk_utils:lookup(queues_number, Params, ?DEFAULT_QUEUES_PER_POOL),
        ConsumersList = bk_utils:lookup(consumers, Params),

        QueuesSpecs = create_queues(BinServerName, ConnectionInfo, NumberOfQueues),
        ConsumersSpecs = lists:foldl(fun({ConsumerName, Params}, Acc) -> create_consumers(BinServerName, ConsumerName, ConnectionInfo, Params) ++ Acc end, [], ConsumersList),

        QueuesSpecs ++ ConsumersSpecs ++ Acc
    end,

    Servers = bk_utils:get_env(servers),

    {ok, {{one_for_one, 1000, 1}, lists:foldl(ServersFun, [], Servers)}}.

revolver_options() ->
    #{
        min_alive_ratio          => 1,
        reconnect_delay          => 10000,
        max_message_queue_length => undefined,
        connect_at_start         => true
    }.

create_queues(ServerName, ConnectionInfo, Instances) ->
    QueuePoolName = ?BK_POOL_QUEUE(ServerName),
    QueueSupervisorName = ?BK_SUPERVISOR_QUEUE(ServerName),

    QueueChildSpecs =  worker(QueueSupervisorName, beanstalkd_worker_supervisor, [QueueSupervisorName, beanstalkd_queue, ServerName, Instances, ConnectionInfo]),
    QueuePool = worker(<<"queue_revolver_", ServerName/binary>>, revolver, [QueueSupervisorName, QueuePoolName, revolver_options()]),
    [QueueChildSpecs, QueuePool].

create_consumers(ServerName, ConsumerName, ConnectionInfo, Params) ->
    ConsumerNameBin = atom_to_binary(ConsumerName, utf8),
    Identifier = <<ServerName/binary, "_", ConsumerNameBin/binary>>,
    IdentifierAtom = binary_to_atom(Identifier, utf8),

    ConsumerSupervisorName = ?BK_SUPERVISOR_CONSUMER(Identifier),

    Instances = bk_utils:lookup(instances, Params, ?DEFAULT_CONSUMERS_PER_POOL),
    ConcurrentJobs = bk_utils:lookup(concurrent_jobs, Params, ?DEFAULT_CONCURRENCY),
    Callbacks = bk_utils:lookup(callbacks, Params),
    Tubes = bk_utils:lookup(tubes, Params),

    ConsumerArgs = [{callbacks, Callbacks}, {queue_pool_name, ?BK_POOL_QUEUE(ServerName)}, {pool_name, IdentifierAtom}] ++ [{tube, Tubes}|ConnectionInfo],
    RatxArgs = [IdentifierAtom, [{limit, ConcurrentJobs}, {queue, 0}]],

    RatxSpecs = worker(<<"rtx_", Identifier/binary>>, ratx, RatxArgs),
    ConsumerChildSpecs = worker(ConsumerSupervisorName, beanstalkd_worker_supervisor, [ConsumerSupervisorName, beanstalkd_consumer, Identifier, Instances, ConsumerArgs]),
    ConsumerPool = worker(<<"consumer_revolver_", Identifier/binary>>, revolver, [ConsumerSupervisorName, ?BK_POOL_CONSUMER(Identifier), revolver_options()]),
    [RatxSpecs , ConsumerChildSpecs , ConsumerPool].

worker(Name, Module, Args) ->
    worker(Name, Module, 5000, Args).

worker(Name, Module, WaitForClose, Args) ->
    {Name, {Module, start_link, Args}, permanent, WaitForClose, worker, [Module]}.