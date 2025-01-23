-module(beanstalkd_queue).

-include_lib("ebeanstalkd/include/ebeanstalkd.hrl").

-behaviour(gen_server).

-define(PUSH_JOB(Job), {push_job, Job}).

-export([
    start_link/1,
    jobs_queued/1,
    queue_delete/2,
    queue_kick_job/2,
    queue_kick_job_delay/3,
    queue_kick_job_backoff/4,

    init/1,
    handle_call/3,
    handle_cast/2,
    handle_info/2,
    terminate/2,
    code_change/3
]).

-record(state, {
    queue,
    queue_count,
    connection_pid,
    connection_state
}).

start_link(Args) ->
    gen_server:start_link(?MODULE, Args, []).

jobs_queued(Pid) ->
    gen_server:call(Pid, queue_size).

queue_delete(QueueName, JobId) ->
    push_job(QueueName, {delete, JobId}).

queue_kick_job(QueueName, JobId) ->
    push_job(QueueName, {kick_job, JobId}).

queue_kick_job_delay(QueueName, JobId, Delay) ->
    push_job(QueueName, {kick_job_delay, JobId, Delay}).

queue_kick_job_backoff(QueueName, JobId, Handler, AttemptsLimit) ->
    push_job(QueueName, {kick_job_backoff, JobId, Handler, AttemptsLimit}).

init(Args0) ->
    Tube = beanstalkd_utils:get_tube(client, beanstalkd_utils:lookup(tube, Args0)),
    Args = beanstalkd_utils:replace(tube, Tube, Args0),
    {ok, ConnectionPid} = ebeanstalkd:connect([{monitor, self()} | Args]),
    {ok, #state{
        connection_state = down,
        connection_pid = ConnectionPid,
        queue = queue:new(),
        queue_count =  0
    }}.

handle_call({push_job, Job}, _From, #state{queue = Queue, queue_count = QueueSize} = State) ->
    NewState = State#state{queue = queue:in(Job, Queue), queue_count = QueueSize + 1},
    {reply, ok, NewState, get_timeout_for_state(NewState)};

handle_call(queue_size, _From, #state{queue_count = QueueSize} = State) ->
    {reply, {ok, QueueSize}, State, get_timeout_for_state(State)};

handle_call(_Request, _From, State) ->
    {reply, ok, State, get_timeout_for_state(State)}.

handle_cast(_Request, State) ->
    {noreply, State, get_timeout_for_state(State)}.

handle_info(timeout, State) ->
    consume_job_queue(State);

handle_info({connection_status, {ConnectionStatus, _Pid}}, State) ->
    ?LOG_INFO("queue: ~p received connection ~p ...",[self(), ConnectionStatus]),
    NewState = State#state{connection_state = ConnectionStatus},
    case ConnectionStatus of
        up ->
            {noreply, NewState, get_timeout_for_state(NewState)};
        _ ->
            {noreply, NewState}
    end;

handle_info({'EXIT', _FromPid, Reason} , State) ->
    ?LOG_ERROR("queue: ~p  beanstalk connection died: ~p", [self(), Reason]),
    {stop, {error, Reason}, State};

handle_info(Info, State) ->
    ?LOG_ERROR("queue: ~p received unexpected message: ~p", [self(), Info]),
    {noreply, State, get_timeout_for_state(State)}.

terminate(_Reason, #state{connection_pid = Connection}) ->
    case Connection of
        undefined ->
            ok;
        _ ->
            catch ebeanstalkd:close(Connection),
            ok
    end.

code_change(_OldVsn, State, _Extra) ->
    {ok, State, get_timeout_for_state(State)}.

% internals

push_job(QueueName, Msg) ->
    case erlpool:pid(QueueName) of
        Pid when is_pid(Pid) ->
            gen_server:call(Pid, ?PUSH_JOB(Msg));
        UnexpectedError ->
            {error, UnexpectedError}
    end.

consume_job_queue(#state{connection_state = ConnectionStatus, connection_pid = Connection, queue = Queue, queue_count = QueueSize} =  State) ->
    case ConnectionStatus of
        up ->
            case queue:out(Queue) of
                {{value, Job}, NewQueue} ->
                    case run_job(Connection, Job) of
                        true ->
                            NewQueueSize = QueueSize - 1,
                            {noreply, State#state{queue = NewQueue, queue_count = NewQueueSize}, get_timeout_for_queue_size(NewQueueSize)};
                        _ ->
                            {noreply, State, 50}
                    end;
                _ ->
                    {noreply, State}
            end;
        _ ->
            {noreply, State}
    end.

run_job(Connection, Job) ->
    case internal_run_job(Connection, Job) of
        true ->
            true;
        {not_found} ->
            ?LOG_WARNING("job not found: ~p",[Job]),
            true;
        UnexpectedResult ->
            ?LOG_ERROR("job failed (send back to queue): ~p error: ~p",[Job, UnexpectedResult]),
            false
    end.

internal_run_job(Connection, {delete, JobId}) ->
    case ebeanstalkd:delete(Connection, JobId) of
        {deleted} ->
            true;
        Result ->
            Result
    end;
internal_run_job(Connection, {kick_job, JobId}) ->
    case ebeanstalkd:kick_job(Connection, JobId) of
        {kicked} ->
            true;
        Result ->
            Result
    end;
internal_run_job(Connection, {kick_job_delay, JobId, Delay}) ->
    case ebeanstalkd:kick_job_delay(Connection, JobId, Delay) of
        {kicked} ->
            true;
        Result ->
            Result
    end;
internal_run_job(Connection, {kick_job_backoff, JobId, Handler, AttemptsLimit}) ->
    case ebeanstalkd:stats_job(Connection, JobId) of
        {ok, List} ->
            Kicks = beanstalkd_utils:lookup(<<"kicks">>, List),
            case AttemptsLimit =/= infinity andalso Kicks >= AttemptsLimit of
                true ->
                    ?LOG_WARNING("handler: ~p -> reschedule job (id: ~p) limit reached (~p). will stay in buried state.", [Handler, JobId, AttemptsLimit]),
                    true;
                _ ->
                    Delay = erlang:min(Kicks*1, 4),
                    ?LOG_WARNING("handler: ~p -> reschedule job (id: ~p) after: ~p seconds (attempt: ~p).", [Handler, JobId, Delay, Kicks+1]),
                    internal_run_job(Connection, {kick_job_delay, JobId, Delay})
            end
    end.

get_timeout_for_state(#state{connection_state = ConnectionState, queue_count = QueueCount})->
    case ConnectionState == up andalso QueueCount > 0 of
        true ->
            0;
        _ ->
            infinity
    end.

get_timeout_for_queue_size(Size) when Size >  0 ->
    0;
get_timeout_for_queue_size(_) ->
    infinity.
