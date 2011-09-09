-module(rabbit_amqp1_0_message).

-export([assemble/1, annotated_message/2]).

-define(TYPE_HEADER, <<"x-amqp-1.0-message-type">>).
-define(SUBJECT_HEADER, <<"x-amqp-1.0-subject">>).

-include_lib("amqp_client/include/amqp_client.hrl").
-include("rabbit_amqp1_0.hrl").

assemble(Msg) ->
    {RKey, Props, Content} = assemble(header, {<<"">>, #'P_basic'{}, undefined},
                                      Msg),
    {RKey, #amqp_msg{props = Props, payload = Content}}.

%% TODO handle delivery-annotations, message-annotations and
%% application-properties

assemble(header, {R, P, C}, [H = #'v1_0.header'{} | Rest]) ->
    assemble(properties, {R, parse_header(H, P), C}, Rest);
assemble(header, {R, P, C}, Rest) ->
    assemble(properties, {R, P, C}, Rest);

assemble(properties, {_R, P, C}, [X = #'v1_0.properties'{} | Rest]) ->
    assemble(properties, {routing_key(X), parse_properties(X, P), C}, Rest);
assemble(properties, {R, P, C}, Rest) ->
    assemble(body, {R, P, C}, Rest);

%% TODO support more than one section

assemble(body, {R, P, _C}, [#'v1_0.data'{content = Content} | Rest]) ->
    assemble(footer, {R, set_1_0_type(<<"data">>, P), Content}, Rest);
assemble(body, {R, P, _C}, [#'v1_0.amqp_value'{content = Content} | Rest]) ->
    assemble(footer, {R, set_1_0_type(<<"amqp-value">>, P),
                      reserialise(Content)}, Rest);
assemble(body, {R, P, _C}, [#'v1_0.amqp_sequence'{content = Content} | Rest]) ->
    assemble(footer, {R, set_1_0_type(<<"amqp-sequence">>, P),
                      reserialise(Content)}, Rest);
assemble(body, {R, P, C}, Rest) ->
    assemble(footer, {R, P, C}, Rest);

assemble(footer, {R, P, C}, [#'v1_0.footer'{}]) ->
    %% TODO parse FOOTER
    {R, P, C};
assemble(footer, {R, P, C}, []) ->
    {R, P, C};
assemble(footer, _, [Left | _]) ->
    exit({unexpected_trailing_sections, Left});

assemble(Expected, _, Actual) ->
    exit({expected_section, Expected, Actual}).

parse_header(Header, Props) ->
    Props#'P_basic'{headers          = undefined, %% TODO
                    delivery_mode    = case Header#'v1_0.header'.durable of
                                           true -> 2;
                                           _    -> 1
                                       end,
                    priority         = unwrap(Header#'v1_0.header'.priority),
                    expiration       = unwrap(Header#'v1_0.header'.ttl),
                    type             = undefined, %% TODO
                    app_id           = undefined, %% TODO
                    cluster_id       = undefined}. %% TODO

parse_properties(Props10, Props = #'P_basic'{headers = Headers}) ->
    Props#'P_basic'{
      content_type     = unwrap(Props10#'v1_0.properties'.content_type),
      content_encoding = undefined, %% TODO parse from 1.0 version
      correlation_id   = unwrap(Props10#'v1_0.properties'.correlation_id),
      reply_to         = unwrap(Props10#'v1_0.properties'.reply_to),
      message_id       = unwrap(Props10#'v1_0.properties'.message_id),
      user_id          = unwrap(Props10#'v1_0.properties'.user_id),
      headers          = set_header(?SUBJECT_HEADER,
                                    unwrap(Props10#'v1_0.properties'.subject),
                                    Headers)}.

routing_key(Props10) ->
    unwrap(Props10#'v1_0.properties'.subject).

%% TODO talk to Mike about this + new codec.
unwrap({utf8, Bin})  -> Bin;
unwrap({ubyte, Num}) -> Num;
unwrap({uint, Num})  -> Num;
unwrap({ulong, Num}) -> Num;
unwrap({long, Num})  -> Num;
unwrap(undefined)    -> undefined.

set_1_0_type(Type, Props = #'P_basic'{headers = Headers}) ->
    Props#'P_basic'{headers = set_header(?TYPE_HEADER, Type, Headers)}.

set_header(Header, Value, undefined) ->
    set_header(Header, Value, []);
set_header(Header, Value, Headers) ->
    rabbit_misc:set_table_value(Headers, Header, longstr, Value).

%% Meh, this is ugly but what else can we do?
reserialise(Content) ->
    iolist_to_binary(rabbit_amqp1_0_binary_generator:generate(Content)).

%%--------------------------------------------------------------------

%% TODO create delivery-annotations, message-annotations and
%% application-properties if we feel like it.

annotated_message(RKey, #amqp_msg{props = Props, payload = Content}) ->
    Header = #'v1_0.header'
      {durable           = case Props#'P_basic'.delivery_mode of
                               2 -> true;
                               _ -> false
                           end,
       priority          = Props#'P_basic'.priority,
       ttl               = Props#'P_basic'.expiration,
       first_acquirer    = undefined, %% TODO
       delivery_count    = undefined}, %% TODO
    Props10 = #'v1_0.properties'{
      message_id     = wrap(Props#'P_basic'.message_id),
      user_id        = wrap(Props#'P_basic'.user_id),
      to             = undefined, %% TODO
      subject        = wrap(get_1_0(?SUBJECT_HEADER, Props, RKey)),
      reply_to       = wrap(Props#'P_basic'.reply_to),
      correlation_id = wrap(Props#'P_basic'.correlation_id),
      content_type   = wrap(Props#'P_basic'.content_type)}, %% TODO encode to 1.0 ver
    Data = case get_1_0(?TYPE_HEADER, Props, <<"data">>) of
               <<"data">> ->
                   #'v1_0.data'{content = Content};
               <<"amqp-value">> ->
                   #'v1_0.amqp_value'{content = unreserialise(Content)};
               <<"amqp-sequence">> ->
                   #'v1_0.amqp_sequence'{content = unreserialise(Content)}
           end,
    [Header, Props10, Data].

get_1_0(Header, #'P_basic'{headers = Headers}, Default) ->
    get_1_0(Header, Headers, Default);
get_1_0(_Header, undefined, Default) ->
    Default;
get_1_0(Header, Headers, Default) ->
    case rabbit_misc:table_lookup(Headers, Header) of
        undefined       -> Default;
        {longstr, Type} -> Type
    end.

unreserialise(Bin) ->
    %% TODO again, multi-section messages
    [Section] = rabbit_amqp1_0_binary_parser:parse_all(Bin),
    Section.

%% TODO again, talk to Mike about this + new codec.
wrap(Bin) when is_binary(Bin) -> {utf8, Bin};
wrap(Num) when is_number(Num) -> {ulong, Num};
wrap(undefined)               -> undefined.
