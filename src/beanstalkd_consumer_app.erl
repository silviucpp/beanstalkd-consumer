-module(beanstalkd_consumer_app).

-include_lib("ebeanstalkd/include/ebeanstalkd.hrl").
-include("beanstalkd_consumer.hrl").

-behaviour(application).

-export([start/2, stop/1, prep_stop/1]).

start(_StartType, _StartArgs) ->

    case bk_utils:get_env(app_init) of
        undefined ->
            ok;
        {Module, Function} ->
            Module:Function()
    end,

    beanstalkd_consumer_sup:start_link().

prep_stop(_State) ->

    Servers = bk_utils:get_env(servers),

    ExtractIdentifiersFun = fun({ServerName, Params}, AccFinal) ->
        ServerNameBin = atom_to_binary(ServerName, utf8),
        ConsumersList = bk_utils:lookup(consumers, Params),

        Fun = fun({ConsumerName, _}, Acc) ->
            ConsumerNameBin = atom_to_binary(ConsumerName, utf8),
            Identifier = <<ServerNameBin/binary, "_", ConsumerNameBin/binary>>,
            [Identifier | Acc]
        end,

        lists:foldl(Fun, [], ConsumersList) ++ AccFinal
    end,

    Consumers = lists:foldl(ExtractIdentifiersFun, [], Servers),

    %send the stop message to avoid reserving new jobs

    StopFun = fun(ConsumerId) ->
        Pids = revolver:map(?BK_POOL_CONSUMER(ConsumerId), fun(Pid) -> Pid end),
        lists:foreach(fun(Pid) -> beanstalkd_consumer:stop(Pid) end, Pids)
    end,

    lists:foreach(StopFun, Consumers),

    %wait for all consumers to stop

    lists:foreach(fun(ConsumerId) -> wait_for_consumers(?BK_POOL_CONSUMER(ConsumerId)) end , Consumers),

    %%wait until all running jobs will complete

    lists:foreach(fun(ConsumerId) -> wait_for_jobs(binary_to_atom(ConsumerId, utf8)) end, Consumers),

    %%wait all queues to be cleared before stopping

    WaitQueuesFun = fun({Name, _}) ->
        NameBin = atom_to_binary(Name, utf8),
        Pids = revolver:map(?BK_POOL_QUEUE(NameBin), fun(Pid) -> Pid end),
        lists:foreach(fun(Pid) ->  wait_for_queue(Name, Pid) end, Pids)
    end,

    lists:foreach(WaitQueuesFun, Servers).

stop(_State) ->
    ok.

wait_for_jobs(Pool) ->
    {[_, {_,InProgressJobs}],_} = ratx:info(Pool),

    case InProgressJobs of
        0 ->
            ?INFO_MSG(<<"All jobs for pool ~p completed">>, [Pool]),
            ok;
        _ ->
            ?INFO_MSG(<<"Still waiting for ~p jobs in pool ~p">>, [InProgressJobs, Pool]),
            timer:sleep(1000),
            wait_for_jobs(Pool)
    end.

wait_for_consumers(ConsumersPool) ->
    Pids = revolver:map(ConsumersPool, fun(Pid) -> Pid end),

    case Pids of
        [] ->
            ?INFO_MSG(<<"All consumers processes were stopped for: ~p">>, [ConsumersPool]),
            ok;
        List ->
            ?INFO_MSG(<<"Still waiting for ~p consumers to stop in ~p">>, [length(List), ConsumersPool]),
            timer:sleep(1000),
            wait_for_consumers(ConsumersPool)
    end.

wait_for_queue(Pool, Pid) ->
    case beanstalkd_queue:jobs_queued(Pid) of
        {ok, 0} ->
            ?INFO_MSG(<<"All queued jobs for pool ~p completed">>, [Pool]),
            ok;
        {ok, JobsQueued} ->
            ?INFO_MSG(<<"Still waiting for ~p jobs in queue for pool ~p">>, [JobsQueued, Pool]),
            timer:sleep(1000),
            wait_for_queue(Pool, Pid)
    end.

