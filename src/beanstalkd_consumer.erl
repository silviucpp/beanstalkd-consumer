-module(beanstalkd_consumer).
-author("silviu").

-include_lib("beanstalk/include/beanstalk.hrl").
-include("beanstalkd_consumer.hrl").

-behaviour(gen_server).

-export([start_link/1, stop/1]).

-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).

-record(state, {
    connection,
    connection_state,
    pool_name,
    queue_pool_name,
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

    case bk_utils:lookup(consumer_callback, Args) of
        {Module, Fun} ->

            Tube = bk_utils:get_tube(consumer, bk_utils:lookup(tube, Args)),
            ArgsNew = lists:keyreplace(tube, 1, Args, {tube, Tube}),

            {ok, Q} = beanstalk:connect([{monitor, self()} | ArgsNew]),
            {ok, #state{connection = Q, connection_state = down, queue_pool_name = QueuePoolName, pool_name = PoolName, job_module = Module, job_fun = Fun}};
        _ ->
            throw({error, callback_bad_argument})
    end.

handle_call(_Request, _From, State) ->
    {reply, ok, State}.

handle_cast(stop, State) ->
    ?INFO_MSG(<<"Consumer for ~p will go in stop state">>, [State#state.pool_name]),
    {stop, normal, State};

handle_cast(_Request, State) ->
    {noreply, State}.

handle_info(timeout, State) ->
    case State#state.connection_state of
        up ->
            case get_job(State#state.connection) of
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

handle_info({connection_status, up}, State) ->
    ?INFO_MSG(<<"received connection up">>,[]),
    {noreply, State#state{connection_state = up}, 0};

handle_info({connection_status, down}, State) ->
    ?INFO_MSG(<<"received connection down">>,[]),
    {noreply, State#state{connection_state = down}};

handle_info({'EXIT', _FromPid, Reason} , State) ->
    ?ERROR_MSG(<<"beanstalk connection died: ~p">>,[Reason]),
    {stop, {error, Reason},State};

handle_info(Info, State) ->
    ?WARNING_MSG(<<"received unexpected message: ~p">>,[Info]),
    {noreply, State}.

terminate(_Reason, State) ->
    case State#state.connection of
        undefined ->
            ok;
        _ ->
            catch beanstalk:close(State#state.connection),
            ok
    end.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

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
    case ratx:ask(State#state.pool_name) of
        drop ->
            ?WARNING_MSG(<<"drop message: ~p ~p">>,[JobId, JobPayload]),
            ok = beanstalkd_queue_pool:kick_job(State#state.queue_pool_name, JobId),
            dropped;
        Ref when is_reference(Ref) ->
            JobFun = fun() ->
                try
                    Module = State#state.job_module,
                    Fun = State#state.job_fun,
                    ok = Module:Fun(JobId, JobPayload),
                    ok = beanstalkd_queue_pool:delete(State#state.queue_pool_name, JobId)
                catch
                    _: {bad_argument, Reason} ->
                        ?ERROR_MSG(<<"delete malformated job payload id: ~p reason: ~p payload: ~p">>, [JobId, Reason, JobPayload]),
                        ok = beanstalkd_queue_pool:delete(State#state.queue_pool_name, JobId);
                    _: Response ->
                        ?ERROR_MSG(<<"Job will stay in buried state id: ~p payload: ~p response: ~p">>, [JobId, JobPayload, Response])
                after
                    ok = ratx:done(State#state.pool_name)
                end
            end,
            spawn(JobFun),
            sent
    end.