-module(stress_test).

-behaviour(beanstalkd_consumer).

-export([
    init/1,
    process/3,
    produce_jobs/3
]).

-define(TUBES, [
    <<"tube1">>,
    <<"tube2">>,
    <<"tube3">>,
    <<"tube4">>,
    <<"tube5">>,
    <<"tube6">>,
    <<"tube7">>,
    <<"tube8">>,
    <<"tube9">>,
    <<"tube10">>,
    <<"tube11">>,
    <<"tube12">>
]).

init(TubeName) ->
    io:format(<<"~p:init: ~p ~n">>, [?MODULE, TubeName]),
    {ok, null}.

process(JobId, _JobPayload, State)  ->
    case JobId rem 1000 =:=0 of
        true ->
            io:format(<<"~p:processed: ~p ~n">>, [?MODULE, {JobId, State}]);
        _ ->
            ok
    end.

produce_jobs(NrProcesses, NrReq, PayloadSize) ->

    ReqPerProcess = round(NrReq/NrProcesses),
    Self = self(),

    FunJobs = fun() ->
        {ok, Conn} = ebeanstalkd:connect(),

        FunPut = fun(_) ->
            Tube = lists:nth(1, ?TUBES),
            Payload = crypto:strong_rand_bytes(PayloadSize),
            {inserted, _} = ebeanstalkd:put_in_tube(Conn, Tube, Payload)
        end,

        ok = lists:foreach(FunPut, lists:seq(1, ReqPerProcess)),
        ok = ebeanstalkd:close(Conn)
    end,

    Pids = [spawn_link(fun() -> FunJobs(), Self ! {self(), done} end) || _ <- lists:seq(1, NrProcesses)],
    [receive {Pid, done} -> ok end || Pid <- Pids],
    io:format(<<"**all jobs sent**~n">>, []).
