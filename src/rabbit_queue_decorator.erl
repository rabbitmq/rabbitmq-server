-module(rabbit_queue_decorator).

-include("rabbit.hrl").

-export([select/1, set/1]).

%%----------------------------------------------------------------------------

-ifdef(use_specs).

-callback policy_changed(rabbit_types:amqqueue(), rabbit_types:amqqueue()) ->
    'ok'.

-callback active_for(rabbit_types:amqqueue()) -> boolean().

-else.

-export([behaviour_info/1]).

behaviour_info(callbacks) ->
    [{description, 0}, {active_for, 1}, {policy_changed, 2}];
behaviour_info(_Other) ->
    undefined.

-endif.

%%----------------------------------------------------------------------------

select(Modules) ->
    [M || M <- Modules, code:which(M) =/= non_existing].

set(Q) -> Q#amqqueue{decorators = [D || D <- list(), D:active_for(Q)]}.

list() -> [M || {_, M} <- rabbit_registry:lookup_all(queue_decorator)].
