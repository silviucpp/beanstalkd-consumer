-module(beanstalkd_queue_pool).

-include_lib("ebeanstalkd/include/ebeanstalkd.hrl").
-include("beanstalkd_consumer.hrl").

-export([delete/2, kick_job/2, pool_size/1]).

delete(QueueName, JobId) ->
    case revolver:pid(QueueName) of
        Pid when is_pid(Pid) ->
            beanstalkd_queue:delete(Pid, JobId);
        UnexpectedError ->
            {error, UnexpectedError}
    end.

kick_job(QueueName, JobId) ->
    case revolver:pid(QueueName) of
        Pid when is_pid(Pid) ->
            beanstalkd_queue:kick_job(Pid, JobId);
        UnexpectedError ->
            {error, UnexpectedError}
    end.

pool_size(QueueName) ->
    try
        length(revolver_utils:child_pids(QueueName))
    catch
        _:Err ->
            ?ERROR_MSG("failed to get the supervisor childrens: ~p", [Err]),
            0
    end.