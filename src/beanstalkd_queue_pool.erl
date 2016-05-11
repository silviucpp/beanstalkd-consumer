-module(beanstalkd_queue_pool).
-author("silviu.caragea").

-include_lib("beanstalk/include/beanstalk.hrl").
-include("beanstalkd_consumer.hrl").

-export([delete/1, kick_job/1, pool_size/0]).

delete(JobId) ->
    case revolver:pid(?BK_QUEUE_POOL) of
        Pid when is_pid(Pid) ->
            beanstalkd_queue:delete(Pid, JobId);
        UnexpectedError ->
            {error, UnexpectedError}
    end.

kick_job(JobId) ->
    case revolver:pid(?BK_QUEUE_POOL) of
        Pid when is_pid(Pid) ->
            beanstalkd_queue:kick_job(Pid, JobId);
        UnexpectedError ->
            {error, UnexpectedError}
    end.

pool_size() ->
    try
        length(revolver_utils:child_pids(?BK_QUEUE_POOL))
    catch
        _:Err ->
            ?ERROR_MSG(<<"failed to get the supervisor childrens: ~p">>, [Err]),
            0
    end.