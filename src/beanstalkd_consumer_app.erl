-module(beanstalkd_consumer_app).

-behaviour(application).

-export([start/2, stop/1]).

start(_StartType, _StartArgs) ->
    load_code(deps),
    ok = lager:start(),
    ok = application:start(poolboy),
    ok = application:start(jobs),
    beanstalkd_consumer_sup:start_link().

stop(_State) ->
    ok.

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