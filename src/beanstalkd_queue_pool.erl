-module(beanstalkd_queue_pool).

-export([
    delete/2,
    kick_job/2
]).

delete(QueueName, JobId) ->
    case erlpool:pid(QueueName) of
        Pid when is_pid(Pid) ->
            beanstalkd_queue:delete(Pid, JobId);
        UnexpectedError ->
            {error, UnexpectedError}
    end.

kick_job(QueueName, JobId) ->
    case erlpool:pid(QueueName) of
        Pid when is_pid(Pid) ->
            beanstalkd_queue:kick_job(Pid, JobId);
        UnexpectedError ->
            {error, UnexpectedError}
    end.

