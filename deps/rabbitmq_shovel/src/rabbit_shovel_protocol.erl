-module(rabbit_shovel_protocol).

-type tag() :: non_neg_integer().

-type uri() :: string() | binary().

-type endpoint_config() :: #{module => atom(),
                             uris => [uri()]}.

-type state() :: #{source => endpoint_config(),
                   dest => endpoint_config()}.

-export_type([state/0, endpoint_config/0, uri/0]).

-callback parse(binary(), {source | destination, Conf :: proplists:proplist()}) ->
    endpoint_config().

-callback connect_source(state()) -> state().
-callback connect_dest(state()) -> state().

-callback init_source(state()) -> state().
-callback init_dest(state()) -> state().

-callback source_uri(state()) -> uri().
-callback dest_uri(state()) -> uri().

-callback close_dest(state()) -> ok.
-callback close_source(state()) -> ok.

-callback handle_source(Msg :: any(), state()) ->
    not_handled | state() | {stop, any()}.
-callback handle_dest(Msg :: any(), state()) ->
    not_handled | state() | {stop, any()}.

-callback ack(Tag :: tag(), Multi :: boolean(), state()) -> state().
-callback nack(Tag :: tag(), Multi :: boolean(), state()) -> state().
-callback forward(Tag :: tag(), Props :: #{atom() => any()},
                  Payload :: binary(), state()) -> state().



