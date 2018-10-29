-module(beanstalkd_utils).

-export([
    lookup/2,
    lookup/3,
    replace/3,
    get_env/1,
    get_tube/2
]).

lookup(Key, List) ->
    lookup(Key, List, undefined).

lookup(Key, List, Default) ->
    case lists:keyfind(Key, 1, List) of
        {Key, Result} ->
            Result;
        false ->
            Default
    end.

replace(Key, NewValue, List) ->
    lists:keyreplace(Key, 1, List, {Key, NewValue}).

get_tube(_, undefined) ->
    undefined;
get_tube(consumer, Tube) ->
    {watch, Tube};
get_tube(client, Tube) ->
    {use, Tube}.

get_env(Key) ->
    case application:get_env(beanstalkd_consumer, Key) of
        {ok, Value} ->
            Value;
        undefined ->
            undefined
    end.