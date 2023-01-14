-module(beanstalkd_queue_pool).

-export([
    delete/2
]).

delete(QueueName, JobId) ->
    case erlpool:pid(QueueName) of
        Pid when is_pid(Pid) ->
            beanstalkd_queue:delete(Pid, JobId);
        UnexpectedError ->
            {error, UnexpectedError}
    end.


