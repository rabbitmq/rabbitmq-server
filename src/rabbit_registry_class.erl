-module(rabbit_registry_class).

-ifdef(use_specs).

-callback added_to_rabbit_registry(atom(), atom()) -> ok.

-callback removed_from_rabbit_registry(atom()) -> ok.

-else.

-export([behaviour_info/1]).

behaviour_info(callbacks) ->
    [{added_to_rabbit_registry, 2}, {removed_from_rabbit_registry, 1}];
behaviour_info(_Other) ->
    undefined.

-endif.
