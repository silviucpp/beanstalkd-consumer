-module(beanstalkd_consumer).
-author("silviu").

-include_lib("beanstalk/include/beanstalk.hrl").
-include("beanstalkd_consumer.hrl").

-behaviour(gen_server).

-export([start_link/1]).

-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).

-record(state, {
    connection,
    connection_state,
    pool_name,
    job_module,
    job_fun}).

start_link(Args) ->
    gen_server:start_link(?MODULE, Args, []).

init(Args) ->
    process_flag(trap_exit, true),

    PoolName = bk_utils:lookup(pool_name, Args),

    case bk_utils:lookup(consumer_callback, Args) of
        {Module, Fun} ->
            {ok, Q} = beanstalk:connect([{monitor, self()} | Args]),
            {ok, #state{connection = Q, connection_state = down, pool_name = PoolName, job_module = Module, job_fun = Fun}};
        _ ->
            throw({error, callback_bad_argument})
    end.

handle_call(_Request, _From, State) ->
    {reply, ok, State}.

handle_cast(_Request, State) ->
    {noreply, State}.

handle_info(timeout, State) ->
    case State#state.connection_state of
        up ->
            case get_job(State#state.connection) of
                {ok, JobId, JobPayload} ->
                    process_job(State, JobId, JobPayload),
                    {noreply, State, 0};
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
    case beanstalk:reserve(Connection) of
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
    Fun = fun() ->
        try
            Module = State#state.job_module,
            Fun = State#state.job_fun,
            ok = Module:Fun(JobId, JobPayload),
            ok = beanstalkd_queue_pool:delete(JobId)
        catch
            _: {error, bad_payload, Reason} ->
                ?ERROR_MSG(<<"delete malformated job payload id: ~p reason: ~p payload: ~p">>, [JobId, Reason, JobPayload]),
                ok = beanstalkd_queue_pool:delete(JobId);
            _: Response ->
                ?ERROR_MSG(<<"Job will stay in buried state id: ~p payload: ~p response: ~p">>, [JobId, JobPayload, Response])
        end
    end,
    spawn(Fun).