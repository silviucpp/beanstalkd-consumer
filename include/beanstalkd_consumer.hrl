
-define(BK_POOL_QUEUE(Name), binary_to_atom(<<"bk_pool_queue_", Name/binary>>, utf8)).
-define(BK_POOL_CONSUMER(Name), binary_to_atom(<<"bk_pool_consumer_", Name/binary>>, utf8)).

% stacktrace

-ifdef(OTP_RELEASE). %% this implies 21 or higher
    -define(EXCEPTION(Class, Reason, Stacktrace), Class:Reason:Stacktrace).
    -define(GET_STACK(Stacktrace), Stacktrace).
-else.
    -define(EXCEPTION(Class, Reason, _), Class:Reason).
    -define(GET_STACK(_), erlang:get_stacktrace()).
-endif.
