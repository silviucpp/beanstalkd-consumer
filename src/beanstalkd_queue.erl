-module(beanstalkd_queue).

-include_lib("ebeanstalkd/include/ebeanstalkd.hrl").

-behaviour(gen_server).

-define(PUSH_JOB(Job), {push_job, Job}).

-export([
    start_link/1,
    jobs_queued/1,
    delete/2,
    kick_job/2,

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

delete(Pid, JobId) ->
    gen_server:call(Pid, ?PUSH_JOB({delete, JobId})).

kick_job(Pid, JobId)->
    gen_server:call(Pid, ?PUSH_JOB({kick_job, JobId})).

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
    ?INFO_MSG("queue: ~p received connection ~p ...",[self(), ConnectionStatus]),
    NewState = State#state{connection_state = ConnectionStatus},
    case ConnectionStatus of
        up ->
            {noreply, NewState, get_timeout_for_state(NewState)};
        _ ->
            {noreply, NewState}
    end;

handle_info({'EXIT', _FromPid, Reason} , State) ->
    ?ERROR_MSG("queue: ~p  beanstalk connection died: ~p", [self(), Reason]),
    {stop, {error, Reason}, State};

handle_info(Info, State) ->
    ?ERROR_MSG("queue: ~p received unexpected message: ~p", [self(), Info]),
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

run_job(Connection, {JobType, JobId} = Job) ->
    case run_job(JobType, Connection, JobId) of
        true ->
            true;
        {not_found} ->
            ?WARNING_MSG("job not found: ~p",[Job]),
            true;
        UnexpectedResult ->
            ?ERROR_MSG("job failed (send back to queue): ~p error: ~p",[Job, UnexpectedResult]),
            false
    end.

run_job(delete, Connection, JobId) ->
    case ebeanstalkd:delete(Connection, JobId) of
        {deleted} ->
            true;
        Result ->
            Result
    end;

run_job(kick_job, Connection, JobId) ->
    case ebeanstalkd:kick_job(Connection, JobId) of
        {kicked} ->
            true;
        Result ->
            Result
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
