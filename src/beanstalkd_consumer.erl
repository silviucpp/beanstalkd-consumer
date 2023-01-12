-module(beanstalkd_consumer).

-include_lib("ebeanstalkd/include/ebeanstalkd.hrl").
-include("beanstalkd_consumer.hrl").

-define(RESERVE_TIMEOUT_SECONDS, 1).

-behaviour(gen_server).

-callback init(TubeName::binary()) ->
    {ok, any()}.

-callback process(JobId::non_neg_integer(), JobPayload::binary(), State::any()) ->
    any().

-export([
    start_link/1,

    init/1,
    handle_call/3,
    handle_cast/2,
    handle_info/2,
    terminate/2,
    code_change/3
]).

-record(state, {
    id,
    conn,
    conn_state,
    queue_pool_id,
    is_multi_tube,
    callbacks,

    all_workers,
    idle_workers
}).

-record(worker_state, {
    module,
    state
}).

start_link(Args) ->
    gen_server:start_link(?MODULE, Args, []).

init(Args) ->
    process_flag(trap_exit, true),

    QueuePoolId = beanstalkd_utils:lookup(queue_pool_id, Args),
    ConcurrentJobsCount = beanstalkd_utils:lookup(concurrent_jobs, Args),

    CallbacksList = beanstalkd_utils:lookup(callbacks, Args),

    FunCallbacks = fun({Tube, Module}, Acc) ->
        {ok, WorkerState} = Module:init(Tube),
        [{Tube, #worker_state{module = Module, state = WorkerState}} | Acc]
    end,

    CallbacksMapped = lists:foldl(FunCallbacks, [], CallbacksList),
    TubeList = beanstalkd_utils:get_tube(consumer, lists:map(fun({Tube, _}) -> Tube end, CallbacksMapped)),

    ConnectionInfo = [{tube, TubeList} | beanstalkd_utils:lookup(connection_info, Args)],
    {ok, Q} = ebeanstalkd:connect([{monitor, self()} | ConnectionInfo]),

    % in case it's watching only one tube extract if from list
    JobCallbacks = case CallbacksMapped of
        [{_TubeName, WorkerState}] ->
            WorkerState;
        _ ->
            maps:from_list(CallbacksMapped)
    end,

    ParentPid = self(),
    AllWorkers = sets:from_list(lists:map(fun(_) -> spawn_link(fun() -> worker_loop(QueuePoolId, ParentPid, self()) end) end, lists:seq(1, ConcurrentJobsCount))),

    {ok, #state{
        id = {beanstalkd_utils:lookup(id, Args), ParentPid},
        conn = Q,
        conn_state = down,
        queue_pool_id = QueuePoolId,
        is_multi_tube = is_map(JobCallbacks),
        callbacks = JobCallbacks,

        idle_workers = sets:new(),
        all_workers = AllWorkers
    }}.

handle_call(Request, _From, #state{id = ConsumerId} = State) ->
    ?WARNING_MSG("consumer: ~p received unexpected call msg: ~p", [ConsumerId, Request]),
    {reply, ok, get_state_timeout(State)}.

handle_cast(Request, #state{id = ConsumerId} = State) ->
    ?WARNING_MSG("consumer: ~p received unexpected cast msg: ~p", [ConsumerId, Request]),
    {noreply, State, get_state_timeout(State)}.

handle_info(timeout, #state{
    id = ConsumerId,
    conn_state = ConnectionState,
    conn = ConnectionPid,
    callbacks = Callbacks,
    is_multi_tube = IsMultiTube,
    idle_workers = IdleWorkers} = State) ->
    case ConnectionState == up andalso sets:is_empty(IdleWorkers) == false of
        true ->
            case ebeanstalkd:reserve(ConnectionPid, ?RESERVE_TIMEOUT_SECONDS) of
                {timed_out} ->
                    {noreply, State, 0};
                {reserved, JobId, JobPayload} ->
                    [WorkerPid|_] = sets:to_list(IdleWorkers),

                    case get_worker_for_job(IsMultiTube, ConnectionPid, JobId, Callbacks) of
                        {ok, WorkerState} ->
                            case ebeanstalkd:bury(ConnectionPid, JobId) of
                                {buried} ->
                                    WorkerPid ! {handle_job, JobId, JobPayload, WorkerState},
                                    NewState = State#state{idle_workers = sets:del_element(WorkerPid, IdleWorkers)},
                                    {noreply, NewState, get_state_timeout(NewState)};
                                Error ->
                                    ?ERROR_MSG("consumer: ~p received unexpected bury result for job: ~p error: ~p", [ConsumerId, JobId, Error]),
                                    {noreply, State, 0}
                            end;
                        Error ->
                            ?ERROR_MSG("consumer: ~p received unexpected result for getting worker state job: ~p error: ~p", [ConsumerId, JobId, Error]),
                            {noreply, State, 0}
                    end;
                Error ->
                    ?ERROR_MSG("consumer: ~p received unexpected reserve result: ~p",[ConsumerId, Error]),
                    {noreply, State, 0}
            end;
        _ ->
            {noreply, State}
    end;

handle_info({idle_worker, WorkerPid}, #state{idle_workers = IdleWorkers} = State) ->
    NewState = State#state{idle_workers = sets:add_element(WorkerPid, IdleWorkers)},
    {noreply, NewState, get_state_timeout(NewState)};

handle_info({connection_status, {ConnectionStatus, _Pid}}, #state{id = ConsumerId} = State) ->
    ?INFO_MSG("consumer: ~p received connection status: ~p", [ConsumerId, ConnectionStatus]),
    NewState = State#state{conn_state = ConnectionStatus},
    {noreply, NewState, get_state_timeout(NewState)};

handle_info({'EXIT', FromPid, Reason}, #state{
    id = ConsumerId,
    queue_pool_id = QueuePoolId,
    conn = Connection,
    all_workers = AllWorkers,
    idle_workers = IdleWorkers
} = State) ->
    case FromPid of
        Connection ->
            ?ERROR_MSG("consumer: ~p -> beanstalk connection died with reason: ~p", [ConsumerId, Reason]),
            ok = stop_workers_sync(sets:to_list(AllWorkers), ConsumerId),
            {stop, {error, Reason}, State#state{all_workers = sets:new()}};
        _ ->
            NewAllWorkers = sets:del_element(FromPid, AllWorkers),
            NewIdleWorkers = sets:del_element(FromPid, IdleWorkers),

            case Reason of
                normal ->
                    ?INFO_MSG("consumer: ~p -> workers finished with reason: ~p", [ConsumerId, Reason]),
                    NewState = State#state{all_workers = NewAllWorkers, idle_workers = NewIdleWorkers},
                    {noreply, NewState, get_state_timeout(NewState)};
                _ ->
                    ?ERROR_MSG("consumer: ~p -> workers died with reason: ~p .replacing dead worker ...", [ConsumerId, Reason]),
                    ParentPid = self(),
                    NewWorkerPid = spawn_link(fun() -> worker_loop(QueuePoolId, ParentPid, self()) end),
                    NewState = State#state{all_workers = sets:add_element(NewWorkerPid, NewAllWorkers), idle_workers = NewIdleWorkers},
                    {noreply, NewState, get_state_timeout(NewState)}
            end
    end;

handle_info(Info, #state{id = ConsumerId} = State) ->
    ?WARNING_MSG("consumer: ~p received unexpected info msg: ~p", [ConsumerId, Info]),
    {noreply, State, get_state_timeout(State)}.

terminate(Reason, #state{id = ConsumerId, all_workers = AllWorkers}) ->
    ?INFO_MSG("consumer: ~p -> terminate with reason: ~p", [ConsumerId, Reason]),
    ok = stop_workers_sync(sets:to_list(AllWorkers), ConsumerId).

code_change(_OldVsn, State, _Extra) ->
    {ok, State, get_state_timeout(State)}.

% internals

get_state_timeout(#state{conn_state = ConnState, idle_workers = IdleWorkers}) ->
    case ConnState == up andalso sets:is_empty(IdleWorkers) == false of
        true ->
            0;
        _ ->
            infinity
    end.

stop_workers_sync(WorkersPids, ConsumerId) ->
    lists:foreach(fun(P) -> P ! stop end, WorkersPids),
    wait_processes(WorkersPids, ConsumerId).

wait_processes([H|T] = R, ConsumerId) ->
    case is_process_alive(H) of
        true ->
            ?INFO_MSG("consumer: ~p still waiting for unfinished jobs", [ConsumerId]),
            timer:sleep(1000),
            wait_processes(R, ConsumerId);
        _ ->
            wait_processes(T, ConsumerId)
    end;
wait_processes([], ConsumerId) ->
    ?INFO_MSG("consumer: ~p -> all jobs completed ...", [ConsumerId]),
    ok.

get_worker_for_job(true, ConnectionPid, JobId, Callbacks) ->
    case ebeanstalkd:stats_job(ConnectionPid, JobId) of
        {ok, Stats} ->
            TubeName = beanstalkd_utils:lookup(<<"tube">>, Stats),
            maps:find(TubeName, Callbacks);
        Error ->
            Error
    end;
get_worker_for_job(_IsMultiTube, _ConnectionPid, _JobId, Callbacks) ->
    {ok, Callbacks}.

worker_loop(QueuePool, ConsumerPid, SelfPid) ->

    ConsumerPid ! {idle_worker, SelfPid},

    receive
        {handle_job, JobId, JobPayload, #worker_state{module = Handler, state = HandlerState}} ->
            try
                Handler:process(JobId, JobPayload, HandlerState),
                ok = beanstalkd_queue_pool:delete(QueuePool, JobId)
            catch
                ?EXCEPTION(_, {bad_argument, Reason}, _) ->
                    ?ERROR_MSG("handler: ~p -> delete malformated job payload id: ~p reason: ~p payload: ~p", [Handler, JobId, Reason, JobPayload]),
                    ok = beanstalkd_queue_pool:delete(QueuePool, JobId);
                ?EXCEPTION(_, Response, Stacktrace) ->
                    ?ERROR_MSG("handler: ~p -> job will stay in buried state id: ~p payload: ~p response: ~p stacktrace: ~p", [Handler, JobId, JobPayload, Response, ?GET_STACK(Stacktrace)])
            end,
            worker_loop(QueuePool, ConsumerPid, SelfPid);
        stop ->
            ?INFO_MSG("worker: ~p successfully terminated ...", [SelfPid]),
            ok;
        {'EXIT', ConsumerPid, _Reason} ->
            ?INFO_MSG("worker: ~p will end because consumer pid crashed ...", [SelfPid]),
            ok
    end.

