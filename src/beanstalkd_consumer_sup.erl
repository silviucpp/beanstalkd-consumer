-module(beanstalkd_consumer_sup).

-include("beanstalkd_consumer.hrl").

-define(BK_SUPERVISOR_QUEUE(Name), binary_to_atom(<<"bk_sup_queue_", Name/binary>>, utf8)).
-define(BK_SUPERVISOR_CONSUMER(Name), binary_to_atom(<<"bk_sup_consumer_", Name/binary>>, utf8)).

-behaviour(supervisor).

-export([start_link/0]).
-export([init/1]).

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

init([]) ->
    Pools = bk_utils:get_env(pools),

    RevolverOptions = #{
        min_alive_ratio          => 1,
        reconnect_delay          => 10000,
        max_message_queue_length => undefined,
        connect_at_start         => true
    },

    Fun = fun({AtomName, Params}, Acc) ->

        BinName = atom_to_binary(AtomName, utf8),
        ConsumerPoolName = ?BK_POOL_CONSUMER(BinName),
        QueuePoolName = ?BK_POOL_QUEUE(BinName),

        Arguments = bk_utils:lookup(connection_info, Params, []),
        NumberOfQueues = bk_utils:lookup(queues_number, Params, []),
        NumberOfConsumers = bk_utils:lookup(consumers_number, Params, []),
        ConsumerConcurrentJobs = bk_utils:lookup(consumer_concurrent_jobs, Params, []),
        ConsumerCallback = bk_utils:lookup(consumer_callback, Params, []),
        ConsumerArgs = [{consumer_callback, ConsumerCallback}, {queue_pool_name, QueuePoolName}, {pool_name, AtomName}] ++ Arguments,
        RatxArgs = [AtomName, [{limit, ConsumerConcurrentJobs}, {queue, 0}]],

        QueueSupervisorName = ?BK_SUPERVISOR_QUEUE(BinName),
        ConsumerSupervisorName = ?BK_SUPERVISOR_CONSUMER(BinName),

        RatxSpecs = worker(<<"rtx_", BinName/binary>>, ratx, RatxArgs),
        QueueChildSpecs =  worker(QueueSupervisorName, beanstalkd_worker_supervisor, [QueueSupervisorName, beanstalkd_queue, BinName, NumberOfQueues, Arguments]),
        ConsumerChildSpecs = worker(ConsumerSupervisorName, beanstalkd_worker_supervisor, [ConsumerSupervisorName, beanstalkd_consumer, BinName, NumberOfConsumers, ConsumerArgs]),
        QueuePool = worker(<<"queue_revolver_", BinName/binary>>, revolver, [QueueSupervisorName, QueuePoolName, RevolverOptions]),
        ConsumerPool = worker(<<"consumer_revolver_", BinName/binary>>, revolver, [ConsumerSupervisorName, ConsumerPoolName, RevolverOptions]),
        [RatxSpecs | [QueueChildSpecs | [QueuePool | [ConsumerChildSpecs | [ConsumerPool | Acc]]]]]
    end,

    {ok, {{one_for_one, 1000, 1}, lists:foldl(Fun, [], Pools)}}.

worker(Name, Module, Args) ->
    worker(Name, Module, 5000, Args).

worker(Name, Module, WaitForClose, Args) ->
    {Name, {Module, start_link, Args}, permanent, WaitForClose, worker, [Module]}.