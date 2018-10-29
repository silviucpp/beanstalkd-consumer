-module(stress_test).

-export([init/1, process/3, produce_jobs/3]).

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

init(Pid) ->
    io:format(<<"~p:init: ~p ~n">>, [?MODULE, Pid]),
    ok.

process(Id, _Payload, State) ->
    case Id rem 1000 =:=0 of
        true ->
            io:format(<<"~p:processed: ~p ~n">>, [?MODULE, {Id, State}]);
        _ ->
            ok
    end.

produce_jobs(NrProcesses, NrReq, PayloadSize) ->

    ReqPerProcess = round(NrReq/NrProcesses),

    FunJobs = fun() ->

        {ok, Conn} = ebeanstalkd:connect(),

        FunPut = fun(_) ->
            Tube = lists:nth(1, ?TUBES),
            Payload = crypto:strong_rand_bytes(PayloadSize),
            {inserted, _} = ebeanstalkd:put_in_tube(Conn, Tube, Payload)
        end,

        lists:foreach(FunPut, lists:seq(1, ReqPerProcess)),

        ebeanstalkd:close(Conn)
    end,

    multi_spawn:do_work(FunJobs, NrProcesses),
    io:format(<<"**all jobs sent**~n">>, []).
