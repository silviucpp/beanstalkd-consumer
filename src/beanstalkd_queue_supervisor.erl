-module(beanstalkd_queue_supervisor).

-author("silviu").

-behaviour(supervisor).

-export([start_link/3]).

-export([init/1]).

-define(SERVER, ?MODULE).

start_link(PoolName, NumberOfWorkers, Params) ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, [PoolName, NumberOfWorkers, Params]).

init([PoolName, NumberOfWorkers, Params]) ->
    PoolNameBin = atom_to_binary(PoolName, utf8),

    Fun = fun(X, Acc) ->
        Name = binary_to_atom(<<"beanstalkd_queue_", PoolNameBin/binary, (integer_to_binary(X))/binary>>, utf8),
        [children_specs(Name, Params) | Acc]
    end,

    ChildSpecs = lists:foldl(Fun, [], lists:seq(1, NumberOfWorkers)),
    {ok, {{one_for_one, 1000, 1}, ChildSpecs}}.

children_specs(Name, Args) ->
    {Name, {beanstalkd_queue, start_link, [Args]}, transient, 2000, worker, [Name]}.