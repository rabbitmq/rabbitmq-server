-module(rabbit_queue_type).

-export([
         discover/1,
         default/0,
         is_enabled/1,
         declare/2
         ]).

% copied from rabbit_amqqueue
-type absent_reason() :: 'nodedown' | 'crashed' | stopped | timeout.

%% is the queue type feature enabled
-callback is_enabled() -> boolean().

-callback declare(amqqueue:amqqueue(), node()) ->
    {'new' | 'existing' | 'owner_died', amqqueue:amqqueue()} |
    {'absent', amqqueue:amqqueue(), absent_reason()} |
    rabbit_types:channel_exit().


%% TODO: this should be controlled by a registry that is populated on boot
discover(<<"quorum">>) ->
    rabbit_quorum_queue;
discover(<<"classic">>) ->
    rabbit_classic_queue.

default() ->
    rabbit_classic_queue.

-spec is_enabled(module()) -> boolean().
is_enabled(Type) ->
    Type:is_enabled().

-spec declare(amqqueue:amqqueue(), node()) ->
    {'new' | 'existing' | 'owner_died', amqqueue:amqqueue()} |
    {'absent', amqqueue:amqqueue(), absent_reason()} |
    rabbit_types:channel_exit().
declare(Q, Node) ->
    Mod = amqqueue:get_type(Q),
    Mod:declare(Q, Node).

