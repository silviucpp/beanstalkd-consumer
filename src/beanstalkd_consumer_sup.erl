-module(beanstalkd_consumer_sup).

-include("beanstalkd_consumer.hrl").

-behaviour(supervisor).

-export([start_link/0]).
-export([init/1]).

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

init([]) ->
    Pools = bk_utils:get_env(pools),

    RevolverOptions = #{
        min_alive_ratio          => 1,
        reconnect_delay          => 1000,
        max_message_queue_length => undefined,
        connect_at_start         => true
    },

    Fun = fun({Name, Params}, Acc) ->

        Arguments = bk_utils:lookup(connection_info, Params, []),
        NumberOfQueues = bk_utils:lookup(queues_number, Params, []),
        NumberOfConsumers = bk_utils:lookup(consumers_number, Params, []),
        ConsumerRateLimiter = bk_utils:lookup(consumer_rate_limiter, Params, []),
        ConsumerCallback = bk_utils:lookup(consumer_callback, Params, []),

        jobs:delete_queue(Name),
        ok = jobs:add_queue(Name, ConsumerRateLimiter),

        QueueChildSpecs = worker(?BK_QUEUE_SUPERVISOR, [Name, NumberOfQueues, Arguments]),
        ConsumerChildSpecs = worker(?BK_CONSUMER_SUPERVISOR, [Name, NumberOfConsumers, [{consumer_callback, ConsumerCallback} | Arguments]]),
        QueuePool = worker(revolver, [?BK_QUEUE_SUPERVISOR, ?BK_QUEUE_POOL, RevolverOptions]),

        [QueueChildSpecs | [QueuePool | [ConsumerChildSpecs | Acc]]]
    end,

    {ok, {{one_for_one, 1000, 1}, lists:foldl(Fun, [], Pools)}}.

worker(Name, Args) ->
    worker(Name, 5000, Args).

worker(Name, WaitForClose, Args) ->
    {Name, {Name, start_link, Args}, permanent, WaitForClose, worker, [Name]}.