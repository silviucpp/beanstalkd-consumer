-module(beanstalkd_consumer).
-author("silviu").

-include_lib("beanstalk/include/beanstalk.hrl").
-include("beanstalkd_consumer.hrl").

-behaviour(gen_server).

-export([start_link/1, stop/1]).

-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).

-record(state, {
    conn,
    conn_state,
    consumer_pool,
    queue_pool,
    user_state,
    job_module,
    job_fun}).

stop(Pid) ->
    gen_server:cast(Pid, stop).

start_link(Args) ->
    gen_server:start_link(?MODULE, Args, []).

init(Args) ->
    process_flag(trap_exit, true),

    QueuePoolName = bk_utils:lookup(queue_pool_name, Args),
    PoolName = bk_utils:lookup(pool_name, Args),

    case bk_utils:lookup(callbacks, Args) of
        {Module, InitFun, JobFun} ->
            UserState = Module:InitFun(self()),
            Tube = bk_utils:get_tube(consumer, bk_utils:lookup(tube, Args)),
            ArgsNew = bk_utils:replace(tube, Tube, Args),

            {ok, Q} = beanstalk:connect([{monitor, self()} | ArgsNew]),

            State = #state{
                conn = Q,
                conn_state = down,
                queue_pool = QueuePoolName,
                consumer_pool = PoolName,
                user_state = UserState,
                job_module = Module,
                job_fun = JobFun
            },

            {ok, State};
        _ ->
            throw({error, fun_process_bad_argument})
    end.

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
            case get_job(State#state.conn) of
                timed_out ->
                    {noreply, State, 0};
                {ok, JobId, JobPayload} ->
                    case process_job(State, JobId, JobPayload) of
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
            catch beanstalk:close(State#state.conn),
            ok
    end.

code_change(_OldVsn, State, _Extra) ->
    {ok, State, get_timeout(State)}.

get_job(Connection) ->
    case beanstalk:reserve(Connection, 1) of
        {timed_out} ->
            timed_out;
        {reserved, JobId, JobPayload} ->
            case beanstalk:bury(Connection, JobId) of
                {buried} ->
                    {ok, JobId, JobPayload};
                UnexpectedResult ->
                    ?ERROR_MSG(<<"received unexpected bury result job: ~p error: ~p">>,[JobId, UnexpectedResult]),
                    {error, UnexpectedResult}
            end;
        UnexpectedResult ->
            ?ERROR_MSG(<<"received unexpected reserve result: ~p">>,[UnexpectedResult]),
            {error, UnexpectedResult}
    end.

process_job(State, JobId, JobPayload) ->
    case ratx:ask(State#state.consumer_pool) of
        drop ->
            ?WARNING_MSG(<<"drop message: ~p ~p">>,[JobId, JobPayload]),
            ok = beanstalkd_queue_pool:kick_job(State#state.queue_pool, JobId),
            dropped;
        Ref when is_reference(Ref) ->
            JobFun = fun() ->
                try
                    Module = State#state.job_module,
                    Fun = State#state.job_fun,
                    Module:Fun(JobId, JobPayload, State#state.user_state),
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