-module(rabbit_amqp1_0_message).

-export([assemble/1, annotated_message/2]).

-define(TYPE_HEADER, <<"x-amqp-1.0-message-type">>).
-define(SUBJECT_HEADER, <<"x-amqp-1.0-subject">>).

-include_lib("amqp_client/include/amqp_client.hrl").
-include("rabbit_amqp1_0.hrl").

assemble(MsgBin) ->
    {RKey, Props, Content} = assemble(header, {<<"">>, #'P_basic'{}, []},
                                      decode_section(MsgBin), MsgBin),
    {RKey, #amqp_msg{props = Props, payload = Content}}.

%% TODO handle delivery-annotations, message-annotations and
%% application-properties

assemble(header, {R, P, C}, {H = #'v1_0.header'{}, Rest}, _Uneaten) ->
    assemble(properties, {R, parse_header(H, P), C},
             decode_section(Rest), Rest);
assemble(header, {R, P, C}, Else, Uneaten) ->
    assemble(properties, {R, P, C}, Else, Uneaten);

assemble(properties, {_R, P, C}, {X = #'v1_0.properties'{}, Rest}, _Uneaten) ->
    assemble(body, {routing_key(X), parse_properties(X, P), C},
             decode_section(Rest), Rest);
assemble(properties, {R, P, C}, Else, Uneaten) ->
    assemble(body, {R, P, C}, Else, Uneaten);

%% The only 'interoperable' content is a single amqp-data section.
%% Everything else we will leave as-is. We still have to parse the
%% sections one-by-one, however, to see when we hit the footer or
%% whatever comes next.

assemble(body, {R, P, _}, {#'v1_0.data'{content = Content}, Rest}, Uneaten) ->
    Chunk = chunk(Rest, Uneaten),
    assemble(amqp10body, {R, set_1_0_type(<<"binary">>, P), {data, Content, Chunk}},
             decode_section(Rest), Rest);
assemble(body, {R, P, C}, Else, Uneaten) ->
    assemble(amqp10body, {R, P, C}, Else, Uneaten);

assemble(amqp10body, {R, P, C}, {{Type, _}, Rest}, Uneaten)
  when Type =:= 'v1_0.data' orelse
       Type =:= 'v1_0.amqp_sequence' orelse
       Type =:= 'v1_0.amqp_value' ->
    Encoded = chunk(Rest, Uneaten),
    assemble(amqp10body,
             {R, set_1_0_type(<<"amqp-1.0">>, P), add_body_section(Encoded, C)},
             decode_section(Rest), Rest);
assemble(amqp10body, {R, P, C}, Else, Uneaten) ->
    assemble(footer, {R, P, compile_body(C)}, Else, Uneaten);

assemble(footer, {R, P, C}, {#'v1_0.footer'{}, <<>>}, _) ->
    {R, P, C};
assemble(footer, {R, P, C}, none, _) ->
    {R, P, C};
assemble(footer, _, Else, _) ->
    exit({unexpected_trailing_sections, Else});

assemble(Expected, _, Actual, _) ->
    exit({expected_section, Expected, Actual}).

decode_section(<<>>) ->
    none;
decode_section(MsgBin) ->
    {AmqpValue, Rest} = rabbit_amqp1_0_binary_parser:parse(MsgBin),
    {rabbit_amqp1_0_framing:decode(AmqpValue), Rest}.

chunk(Rest, Uneaten) ->
    ChunkLen = size(Uneaten) - size(Rest),
    <<Chunk:ChunkLen/binary, _ActuallyRest/binary>> = Uneaten,
    Chunk.

add_body_section(C, {data, _, Bin}) ->
    [C, Bin];
add_body_section(C, Cs) ->
    [C | Cs].

compile_body({data, Content, _}) ->
    Content;
compile_body(Sections) ->
    lists:reverse(Sections).

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

set_header(Header, Value, undefined) ->
    set_header(Header, Value, []);
set_header(Header, Value, Headers) ->
    rabbit_misc:set_table_value(Headers, Header, longstr, Value).

set_1_0_type(Type, Props = #'P_basic'{}) ->
    Props#'P_basic'{type = Type}.

%%--------------------------------------------------------------------

%% TODO create delivery-annotations, message-annotations and
%% application-properties if we feel like it.

annotated_message(RKey, #amqp_msg{props = Props, payload = Content}) ->
    Header = #'v1_0.header'
      {durable           = case Props#'P_basic'.delivery_mode of
                               2 -> true;
                               _ -> false
                           end,
       priority          = wrap(ubyte, Props#'P_basic'.priority),
       ttl               = wrap(uint, Props#'P_basic'.expiration),
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
    Data = case Props#'P_basic'.type of
               <<"binary">> ->
                   rabbit_amqp1_0_framing:encode_bin(
                     #'v1_0.data'{content = Content});
               <<"amqp-1.0">> ->
                   Content
           end,
    [rabbit_amqp1_0_framing:encode_bin(Header),
     rabbit_amqp1_0_framing:encode_bin(Props10),
     Data].

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

wrap(_Type, undefined) ->
    undefined;
wrap(Type, Val) ->
    {Type, Val}.
