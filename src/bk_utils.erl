-module(bk_utils).
-author("silviu.caragea").

-export([lookup/2, lookup/3, get_env/1]).

lookup(Key, List) ->
    lookup(Key, List, undefined).

lookup(Key, List, Default) ->
    case lists:keyfind(Key, 1, List) of
        {Key, Result} ->
            Result;
        false ->
            Default
    end.

get_env(Key) ->
    {ok, Value} = application:get_env(beanstalkd_consumer, Key),
    Value.
