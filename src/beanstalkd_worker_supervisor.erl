-module(beanstalkd_worker_supervisor).

-author("silviu").

-behaviour(supervisor).

-export([start_link/5]).

-export([init/1]).

-define(SERVER, ?MODULE).

start_link(SupervisorName, Module, PoolName, NumberOfWorkers, Params) ->
    supervisor:start_link({local, SupervisorName}, ?MODULE, [Module, PoolName, NumberOfWorkers, Params]).

init([Module, PoolNameBin, NumberOfWorkers, Params]) ->
    ModuleBin = atom_to_binary(Module, utf8),

    Fun = fun(X, Acc) ->
        Name = <<ModuleBin/binary, "_", PoolNameBin/binary, (integer_to_binary(X))/binary>>,
        [children_specs(Module, Name, Params) | Acc]
    end,

    ChildSpecs = lists:foldl(Fun, [], lists:seq(1, NumberOfWorkers)),
    {ok, {{one_for_one, 1000, 1}, ChildSpecs}}.

children_specs(Module, Name, Args) ->
    {Name, {Module, start_link, [Args]}, transient, 2000, worker, [Module]}.