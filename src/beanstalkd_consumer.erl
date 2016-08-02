-module(beanstalkd_consumer).
-author("silviu").

-include_lib("ebeanstalkd/include/ebeanstalkd.hrl").
-include("beanstalkd_consumer.hrl").

-behaviour(gen_server).

-export([start_link/1, stop/1]).

-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).

-record(state, {
    conn,
    conn_state,
    consumer_pool,
    queue_pool,
    multi_tubes,
    job_callback
}).

stop(Pid) ->
    gen_server:cast(Pid, stop).

start_link(Args) ->
    gen_server:start_link(?MODULE, Args, []).

init(Args) ->
    process_flag(trap_exit, true),

    QPName = beanstalkd_utils:lookup(queue_pool_name, Args),
    CPName = beanstalkd_utils:lookup(pool_name, Args),

    CallbacksList = beanstalkd_utils:lookup(callbacks, Args),

    FunCallbacks = fun(X, Acc) ->
        case X of
            {Tube, Module, InitFun, JobFun} ->
                UserState = Module:InitFun(self()),
                [{Tube, {Module, JobFun, UserState}} | Acc]
        end
    end,

    CallbacksMapped = lists:foldl(FunCallbacks, [], CallbacksList),

    TubeList = beanstalkd_utils:get_tube(consumer, lists:map(fun({Tube, _}) -> Tube end, CallbacksMapped)),

    ArgsNew = beanstalkd_utils:replace(tube, TubeList, Args),
    {ok, Q} = ebeanstalkd:connect([{monitor, self()} | ArgsNew]),

    %in case it's watching only one tube extract if from list
    JobCallback = case CallbacksMapped of
        [{_TubeName, TubePayload}] ->
            TubePayload;
        _ ->
            CallbacksMapped
    end,

    MultiTubes = is_list(JobCallback),

    {ok, #state{conn = Q, conn_state = down, queue_pool = QPName, consumer_pool = CPName, job_callback = JobCallback, multi_tubes = MultiTubes}}.

handle_call(_Request, _From, State) ->
    {reply, ok, State, get_timeout(State)}.

handle_cast(stop, State) ->
    ?INFO_MSG(<<"Consumer for ~p will go in stop state">>, [State#state.consumer_pool]),
    {stop, normal, State};

handle_cast(_Request, State) ->
    {noreply, State, get_timeout(State)}.

handle_info(timeout, State) ->
    case State#state.conn_state of
        up ->
            case get_job(State#state.conn, State#state.multi_tubes) of
                timed_out ->
                    {noreply, State, 0};
                {ok, JobId, JobPayload, TubeName} ->
                    case process_job(State, JobId, JobPayload, TubeName) of
                        sent ->
                            {noreply, State, 0};
                        dropped ->
                            {noreply, State, 3000}
                    end;
                _ ->
                    {noreply, State, 0}
            end;
        _ ->
            {noreply, State}
    end;

handle_info({connection_status, {up, _Pid}}, State) ->
    ?INFO_MSG(<<"received connection up">>,[]),
    {noreply, State#state{conn_state = up}, 0};

handle_info({connection_status, {down, _Pid}}, State) ->
    ?INFO_MSG(<<"received connection down">>,[]),
    {noreply, State#state{conn_state = down}};

handle_info({'EXIT', _FromPid, Reason} , State) ->
    ?ERROR_MSG(<<"beanstalk connection died: ~p">>,[Reason]),
    {stop, {error, Reason}, State};

handle_info(Info, State) ->
    ?WARNING_MSG(<<"received unexpected message: ~p">>,[Info]),
    {noreply, State, get_timeout(State)}.

terminate(_Reason, State) ->
    case State#state.conn of
        undefined ->
            ok;
        _ ->
            catch ebeanstalkd:close(State#state.conn),
            ok
    end.

code_change(_OldVsn, State, _Extra) ->
    {ok, State, get_timeout(State)}.

get_job(Connection, ExtractTubeName) ->
    case ebeanstalkd:reserve(Connection, 1) of
        {timed_out} ->
            timed_out;
        {reserved, JobId, JobPayload} ->
            case get_tube_name(ExtractTubeName, Connection, JobId) of
                {ok, TubeName} ->
                    case ebeanstalkd:bury(Connection, JobId) of
                        {buried} ->
                            {ok, JobId, JobPayload, TubeName};
                        UnexpectedResult ->
                            ?ERROR_MSG(<<"received unexpected bury result job: ~p error: ~p">>,[JobId, UnexpectedResult]),
                            {error, UnexpectedResult}
                    end;
                UnexpectedResult ->
                    ?ERROR_MSG(<<"received unexpected result for getting tube name: ~p error: ~p">>,[JobId, UnexpectedResult]),
                    {error, UnexpectedResult}
            end;
        UnexpectedResult ->
            ?ERROR_MSG(<<"received unexpected reserve result: ~p">>,[UnexpectedResult]),
            {error, UnexpectedResult}
    end.

get_tube_name(true, Connection, JobId) ->
    case ebeanstalkd:stats_job(Connection, JobId) of
        {ok, Stats} ->
            {ok, beanstalkd_utils:lookup(<<"tube">>, Stats)};
        UnexpectedError ->
            UnexpectedError
    end;
get_tube_name(_ , _Connection, _JobId) ->
    {ok, undefined}.

process_job(State, JobId, JobPayload, TubeName) ->
    case ratx:ask(State#state.consumer_pool) of
        drop ->
            ?WARNING_MSG(<<"drop message id: ~p payload: ~p tube:~p">>,[JobId, JobPayload, TubeName]),
            ok = beanstalkd_queue_pool:kick_job(State#state.queue_pool, JobId),
            dropped;
        Ref when is_reference(Ref) ->
            JobFun = fun() ->
                try
                    case State#state.multi_tubes of
                        true ->
                            {Module, Fun, UserState} = beanstalkd_utils:lookup(TubeName, State#state.job_callback),
                            Module:Fun(JobId, JobPayload, UserState);
                        _ ->
                            {Module, Fun, UserState} = State#state.job_callback,
                            Module:Fun(JobId, JobPayload, UserState)
                    end,

                    ok = beanstalkd_queue_pool:delete(State#state.queue_pool, JobId)
                catch
                    _: {bad_argument, Reason} ->
                        ?ERROR_MSG(<<"delete malformated job payload id: ~p reason: ~p payload: ~p">>, [JobId, Reason, JobPayload]),
                        ok = beanstalkd_queue_pool:delete(State#state.queue_pool, JobId);
                    _: Response ->
                        ?ERROR_MSG(<<"Job will stay in buried state id: ~p payload: ~p response: ~p stacktrace: ~p">>, [JobId, JobPayload, Response, erlang:get_stacktrace()])
                after
                    ok = ratx:done(State#state.consumer_pool)
                end
            end,
            spawn(JobFun),
            sent
    end.

get_timeout(State) ->
    case State#state.conn_state of
        up ->
            0;
        _ ->
            infinity
    end.