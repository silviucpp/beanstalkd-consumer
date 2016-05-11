-module(beanstalkd_queue).
-author("silviu.caragea").

-include_lib("beanstalk/include/beanstalk.hrl").

-behaviour(gen_server).

-define(PUSH_JOB(Job), {push_job, Job}).

-export([start_link/1, delete/2, kick_job/2]).

-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).

-record(state, {job_queue, connection, connection_state}).

start_link(Args) ->
    gen_server:start_link(?MODULE, Args, []).

delete(Pid, JobId) ->
    gen_server:call(Pid, ?PUSH_JOB({delete, JobId})).

kick_job(Pid, JobId)->
    gen_server:call(Pid, ?PUSH_JOB({kick_job, JobId})).

init(Args) ->
    {ok, Connection} = beanstalk:connect([{monitor, self()} | Args]),
    {ok, #state{connection_state = down, connection = Connection, job_queue = []}}.

handle_call({push_job, Job}, _From, State) ->
    NewState = State#state{job_queue = [Job | State#state.job_queue]},
    Timeout = get_timeout(NewState),
    {reply, ok, NewState, Timeout};

handle_call(_Request, _From, State) ->
    {reply, ok, State}.

handle_cast(_Request, State) ->
    {noreply, State}.

handle_info(timeout, State) ->
    consume_job_queue(State);

handle_info({connection_status, up}, State) ->
    ?INFO_MSG(<<"received connection up">>,[]),
    NewState = State#state{connection_state = up},
    {noreply, NewState, get_timeout(NewState)};

handle_info({connection_status, down}, State) ->
    ?INFO_MSG(<<"received connection down">>,[]),
    {noreply, State#state{connection_state = down}};

handle_info({'EXIT', _FromPid, Reason} , State) ->
    ?ERROR_MSG(<<"beanstalk connection died: ~p">>,[Reason]),
    {stop, {error, Reason},State};

handle_info(Info, State) ->
    ?ERROR_MSG(<<"received unexpected message: ~p">>, [Info]),
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

consume_job_queue(State) ->
    case State#state.connection_state of
        up ->
            case State#state.job_queue of
                [H|T] ->
                    case run_job(State#state.connection, H) of
                        true ->
                            {noreply, State#state{job_queue = T}, get_timeout(T)};
                        _ ->
                            {noreply, State, 0}
                    end;
                _ ->
                    {noreply, State}
            end;
        _ ->
            {noreply, State}
    end.

run_job(Connection, {JobType, JobId} = Job) ->
    case run_job(JobType, Connection, JobId) of
        true ->
            ?INFO_MSG(<<"job completed : ~p">>,[Job]),
            true;
        {not_found} ->
            ?WARNING_MSG(<<"job not found: ~p">>,[Job]),
            true;
        UnexpectedResult ->
            ?ERROR_MSG(<<"job failed (send back to queue): ~p error: ~p">>,[Job, UnexpectedResult]),
            false
    end.

run_job(delete, Connection, JobId) ->
    case beanstalk:delete(Connection, JobId) of
        {deleted} ->
            true;
        Result ->
            Result
    end;

run_job(kick_job, Connection, JobId) ->
    case beanstalk:kick_job(Connection, JobId) of
        {kicked} ->
            true;
        Result ->
            Result
    end.

get_timeout(State) when is_record(State, state) ->
    case State#state.connection_state of
        up ->
            get_timeout(State#state.job_queue);
        _ ->
            infinity
    end;
get_timeout([]) ->
    infinity;
get_timeout(_) ->
    0.