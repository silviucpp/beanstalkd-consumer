-module(beanstalkd_consumer_app).

-include_lib("beanstalk/include/beanstalk.hrl").
-include("beanstalkd_consumer.hrl").

-behaviour(application).

-export([start/2, stop/1, prep_stop/1]).

start(_StartType, _StartArgs) ->
    load_code(deps),
    lager:start(),
    application:start(poolboy),
    application:start(jobs),
    beanstalkd_consumer_sup:start_link().

prep_stop(_State) ->

    Pools = bk_utils:get_env(pools),

    %send the stop message to avoid reserving new jobs

    StopFun = fun({Name, _}) ->
        NameBin = atom_to_binary(Name, utf8),
        Pids = revolver:map(?BK_POOL_CONSUMER(NameBin), fun(Pid) -> Pid end),

        lists:foreach(fun(Pid) -> beanstalkd_consumer:stop(Pid) end, Pids)
    end,

    lists:foreach(StopFun, Pools),

    %wait for all consumers to stop

    lists:foreach(fun({Name, _}) -> NameBin = atom_to_binary(Name, utf8), wait_for_consumers(Name, ?BK_POOL_CONSUMER(NameBin)) end , Pools),

    %%wait until all running jobs will complete

    lists:foreach(fun({Name, _}) -> wait_for_jobs(Name) end, Pools),

    %%wait all queues to be cleared before stopping

    WaitQueuesFun = fun({Name, _}) ->
        NameBin = atom_to_binary(Name, utf8),
        Pids = revolver:map(?BK_POOL_QUEUE(NameBin), fun(Pid) -> Pid end),
        lists:foreach(fun(Pid) ->  wait_for_queue(Name, Pid) end, Pids)
    end,

    lists:foreach(WaitQueuesFun, Pools).

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

wait_for_consumers(Pool, ConsumersPool) ->
    Pids = revolver:map(ConsumersPool, fun(Pid) -> Pid end),

    case Pids of
        [] ->
            ?INFO_MSG(<<"All consumers processes were stopped for pool: ~p">>, [Pool]),
            ok;
        List ->
            ?INFO_MSG(<<"Still waiting for ~p consumers to stop in pool ~p">>, [length(List), Pool]),
            timer:sleep(1000),
            wait_for_consumers(Pool, ConsumersPool)
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

load_code(Arg) ->
    case init:get_argument(Arg) of
        {ok,[[Dir]]} ->
            case file:list_dir(Dir) of
                {ok, L} ->
                    io:format(<<"Load deps code from ~p ~n">>,[Dir]),
                    lists:foreach(fun(I) -> Path = Dir ++ "/" ++ I ++ "/ebin", code:add_path(Path) end, L),
                    ok;
                _ ->
                    throw(badarg)
            end;
        _ ->
            ok
    end.