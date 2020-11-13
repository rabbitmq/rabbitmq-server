-module(rabbit_registry_class).

-callback added_to_rabbit_registry(atom(), atom()) -> ok.

-callback removed_from_rabbit_registry(atom()) -> ok.
