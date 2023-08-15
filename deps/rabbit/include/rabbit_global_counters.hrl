-define(NUM_PROTOCOL_COUNTERS, 8).
-define(NUM_PROTOCOL_QUEUE_TYPE_COUNTERS, 8).

%% Dead Letter counters:
%%
%% The following two counters are mutually exclusive because
%% quorum queue dead-letter-strategy at-least-once is incompatible with overflow drop-head.
-define(MESSAGES_DEAD_LETTERED_MAXLEN, 1).
-define(MESSAGES_DEAD_LETTERED_CONFIRMED, 1).
-define(MESSAGES_DEAD_LETTERED_EXPIRED, 2).
-define(MESSAGES_DEAD_LETTERED_REJECTED, 3).
-define(MESSAGES_DEAD_LETTERED_DELIVERY_LIMIT, 4).

-define(MESSAGES_DEAD_LETTERED_MAXLEN_COUNTER,
        {messages_dead_lettered_maxlen_total, ?MESSAGES_DEAD_LETTERED_MAXLEN, counter,
         "Total number of messages dead-lettered due to overflow drop-head or reject-publish-dlx"
        }).

-define(MESSAGES_DEAD_LETTERED_CONFIRMED_COUNTER,
        {
         messages_dead_lettered_confirmed_total, ?MESSAGES_DEAD_LETTERED_CONFIRMED, counter,
         "Total number of messages dead-lettered and confirmed by target queues"
        }).

-define(MESSAGES_DEAD_LETTERED_EXPIRED_COUNTER,
        {
         messages_dead_lettered_expired_total, ?MESSAGES_DEAD_LETTERED_EXPIRED, counter,
         "Total number of messages dead-lettered due to message TTL exceeded"
        }).

-define(MESSAGES_DEAD_LETTERED_REJECTED_COUNTER,
        {
         messages_dead_lettered_rejected_total, ?MESSAGES_DEAD_LETTERED_REJECTED, counter,
         "Total number of messages dead-lettered due to basic.reject or basic.nack"
        }).

-define(MESSAGES_DEAD_LETTERED_DELIVERY_LIMIT_COUNTER,
        {
         messages_dead_lettered_delivery_limit_total, ?MESSAGES_DEAD_LETTERED_DELIVERY_LIMIT, counter,
         "Total number of messages dead-lettered due to delivery-limit exceeded"
        }).
