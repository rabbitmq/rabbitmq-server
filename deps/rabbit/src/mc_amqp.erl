-module(mc_amqp).
-behaviour(mc).

-include_lib("amqp10_common/include/amqp10_framing.hrl").
-include("mc.hrl").

-export([
         init/1,
         size/1,
         x_header/2,
         property/2,
         routing_headers/2,
         convert_to/3,
         convert_from/3,
         protocol_state/2,
         prepare/2
        ]).

-import(rabbit_misc,
        [maps_put_truthy/3]).

-define(MESSAGE_ANNOTATIONS_GUESS_SIZE, 100).

-define(IS_SIMPLE_VALUE(V),
        is_binary(V) orelse
        is_number(V) orelse
        is_boolean(V)).

%% §3.2
-define(DESCRIPTOR_CODE_DATA, 16#75).
-define(DESCRIPTOR_CODE_AMQP_SEQUENCE, 16#76).
-define(DESCRIPTOR_CODE_AMQP_VALUE, 16#77).

%% A section that was omitted by the AMQP sender.
%% We use an empty list as it is cheaper to serialize.
-define(OMITTED_SECTION, []).

-type amqp10_data() :: [#'v1_0.amqp_sequence'{} | #'v1_0.data'{}] | #'v1_0.amqp_value'{}.
-type body_descriptor_code() :: ?DESCRIPTOR_CODE_DATA |
                                ?DESCRIPTOR_CODE_AMQP_SEQUENCE |
                                ?DESCRIPTOR_CODE_AMQP_VALUE.
%% §3.2.5
-type application_properties() :: [{Key :: {utf8, binary()},
                                    Val :: term()}].
%% §3.2.10
-type amqp_annotations() :: [{Key :: {symbol, binary()} | {ulong, non_neg_integer()},
                              Val :: term()}].
-type opt(T) :: T | undefined.

%% This representation is used when the message was originally sent with
%% a protocol other than AMQP and the message was not read from a stream.
-record(msg_body_decoded,
        {
         header :: opt(#'v1_0.header'{}),
         message_annotations = [] :: list(),
         properties :: opt(#'v1_0.properties'{}),
         application_properties = [] :: list(),
         data = [] :: amqp10_data(),
         footer = [] :: list()
        }).

%% This representation is used when we received the message from
%% an AMQP client or when we read the message from a stream.
%% This message was parsed only until the start of the body.
-record(msg_body_encoded,
        {
         header :: opt(#'v1_0.header'{}),
         message_annotations = [] :: amqp_annotations(),
         properties :: opt(#'v1_0.properties'{}),
         application_properties = [] :: application_properties(),
         bare_and_footer = uninit :: uninit | binary(),
         bare_and_footer_application_properties_pos = ?OMITTED_SECTION :: non_neg_integer() | ?OMITTED_SECTION,
         bare_and_footer_body_pos = uninit :: uninit | non_neg_integer(),
         body_code = uninit :: uninit | body_descriptor_code()
        }).

%% This representation is how we store the message on disk in classic queues
%% and quorum queues. For better performance and less disk usage, we omit the
%% header because the header fields we're interested in are already set as mc
%% annotations. We store the original bare message unaltered to preserve
%% message hashes on the binary encoding of the bare message [§3.2].
%% We store positions of where the properties, application-properties and body
%% sections start to be able to parse only individual sections after reading
%% the message back from a classic or quorum queue. The record is called v1
%% just in case we ever want to introduce a new v2 on disk representation in
%% the future.
-record(v1,
        {
         message_annotations = [] :: amqp_annotations(),
         bare_and_footer :: binary(),
         bare_and_footer_properties_pos :: 0 | ?OMITTED_SECTION,
         bare_and_footer_application_properties_pos :: non_neg_integer() | ?OMITTED_SECTION,
         bare_and_footer_body_pos :: non_neg_integer(),
         body_code :: body_descriptor_code()
        }).

-opaque state() :: #msg_body_decoded{} | #msg_body_encoded{} | #v1{}.

-export_type([state/0]).

init(Payload) ->
    Sections = amqp10_framing:decode_bin(Payload, [server_mode]),
    Msg = msg_body_encoded(Sections, Payload, #msg_body_encoded{}),
    Anns = essential_properties(Msg),
    {Msg, Anns}.

convert_from(?MODULE, Sections, _Env) when is_list(Sections) ->
    msg_body_decoded(Sections, #msg_body_decoded{});
convert_from(_SourceProto, _, _Env) ->
    not_implemented.

convert_to(?MODULE, Msg, _Env) ->
    Msg;
convert_to(TargetProto, Msg, Env) ->
    TargetProto:convert_from(?MODULE, msg_to_sections(Msg), Env).

size(#v1{message_annotations = MA,
         bare_and_footer = Body}) ->
    MetaSize = case MA of
                   [] -> 0;
                   _ -> ?MESSAGE_ANNOTATIONS_GUESS_SIZE
               end,
    {MetaSize, byte_size(Body)}.

x_header(Key, Msg) ->
    message_annotation(Key, Msg, undefined).

property(_Prop, #msg_body_encoded{properties = undefined}) ->
    undefined;
property(Prop, #msg_body_encoded{properties = Props}) ->
    property0(Prop, Props);
property(_Prop, #v1{bare_and_footer_properties_pos = ?OMITTED_SECTION}) ->
    undefined;
property(Prop, #v1{bare_and_footer = Bin,
                   bare_and_footer_properties_pos = 0,
                   bare_and_footer_application_properties_pos = ApPos,
                   bare_and_footer_body_pos = BodyPos}) ->
    PropsLen = case ApPos of
                   ?OMITTED_SECTION -> BodyPos;
                   _ -> ApPos
               end,
    PropsBin = binary_part(Bin, 0, PropsLen),
    % assertion
    {PropsDescribed, PropsLen} = amqp10_binary_parser:parse(PropsBin),
    Props = amqp10_framing:decode(PropsDescribed),
    property0(Prop, Props).

property0(message_id, #'v1_0.properties'{message_id = Val}) ->
    Val;
property0(user_id, #'v1_0.properties'{user_id = Val}) ->
    Val;
property0(to, #'v1_0.properties'{to = Val}) ->
    Val;
property0(subject, #'v1_0.properties'{subject = Val}) ->
    Val;
property0(reply_to, #'v1_0.properties'{reply_to = Val}) ->
    Val;
property0(correlation_id, #'v1_0.properties'{correlation_id = Val}) ->
    Val;
property0(content_type, #'v1_0.properties'{content_type = Val}) ->
    Val;
property0(content_encoding, #'v1_0.properties'{content_encoding = Val}) ->
    Val;
property0(absolute_expiry_time, #'v1_0.properties'{absolute_expiry_time = Val}) ->
    Val;
property0(creation_time, #'v1_0.properties'{creation_time = Val}) ->
    Val;
property0(group_id, #'v1_0.properties'{group_id = Val}) ->
    Val;
property0(group_sequence, #'v1_0.properties'{group_sequence = Val}) ->
    Val;
property0(reply_to_group_id, #'v1_0.properties'{reply_to_group_id = Val}) ->
    Val;
property0(_Prop, #'v1_0.properties'{}) ->
    undefined.

routing_headers(Msg, Opts) ->
    IncludeX = lists:member(x_headers, Opts),
    X = case IncludeX of
            true ->
                message_annotations_as_simple_map(Msg);
            false ->
                []
        end,
    List = application_properties_as_simple_map(Msg, X),
    maps:from_list(List).

get_property(durable, Msg) ->
    case Msg of
        #msg_body_encoded{header = #'v1_0.header'{durable = Durable}}
          when is_boolean(Durable) ->
            Durable;
        _ ->
            %% fallback in case the source protocol was old AMQP 0.9.1
            case message_annotation(<<"x-basic-delivery-mode">>, Msg, undefined) of
                {ubyte, 1} ->
                    false;
                _ ->
                    true
            end
    end;
get_property(timestamp, Msg) ->
    case Msg of
        #msg_body_encoded{properties = #'v1_0.properties'{creation_time = {timestamp, Ts}}} ->
            Ts;
        _ ->
            undefined
    end;
get_property(ttl, Msg) ->
    case Msg of
        #msg_body_encoded{header = #'v1_0.header'{ttl = {uint, Ttl}}} ->
            Ttl;
        _ ->
            %% fallback in case the source protocol was AMQP 0.9.1
            case message_annotation(<<"x-basic-expiration">>, Msg, undefined) of
                {utf8, Expiration}  ->
                    {ok, Ttl} = rabbit_basic:parse_expiration(Expiration),
                    Ttl;
                _ ->
                    undefined
            end
    end;
get_property(priority, Msg) ->
    case Msg of
        #msg_body_encoded{header = #'v1_0.header'{priority = {ubyte, Priority}}} ->
            Priority;
        _ ->
            %% fallback in case the source protocol was AMQP 0.9.1
            case message_annotation(<<"x-basic-priority">>, Msg, undefined) of
                {_, Priority}  ->
                    Priority;
                _ ->
                    undefined
            end
    end.

%% protocol_state/2 serialises the protocol state outputting an AMQP encoded message.
-spec protocol_state(state(), mc:annotations()) -> iolist().
protocol_state(Msg0 = #msg_body_decoded{header = Header0,
                                        message_annotations = MA0}, Anns) ->
    Header = update_header_from_anns(Header0, Anns),
    MA = protocol_state_message_annotations(MA0, Anns),
    Msg = Msg0#msg_body_decoded{header = Header,
                                message_annotations = MA},
    Sections = msg_to_sections(Msg),
    encode(Sections);
protocol_state(#msg_body_encoded{header = Header0,
                                 message_annotations = MA0,
                                 bare_and_footer = BareAndFooter}, Anns) ->
    Header = update_header_from_anns(Header0, Anns),
    MA = protocol_state_message_annotations(MA0, Anns),
    Sections = to_sections(Header, MA, []),
    [encode(Sections), BareAndFooter];
protocol_state(#v1{message_annotations = MA0,
                   bare_and_footer = BareAndFooter}, Anns) ->
    Durable = case Anns of
                  #{?ANN_DURABLE := D} -> D;
                  _ -> true
              end,
    Priority = case Anns of
                   #{?ANN_PRIORITY := P}
                     when is_integer(P) ->
                       {ubyte, P};
                   _ ->
                       undefined
               end,
    Ttl = case Anns of
              #{ttl := T}
                when is_integer(T) ->
                  {uint, T};
              _ ->
                  undefined
          end,
    Header = update_header_from_anns(#'v1_0.header'{durable = Durable,
                                                    priority = Priority,
                                                    ttl = Ttl}, Anns),
    MA = protocol_state_message_annotations(MA0, Anns),
    Sections = to_sections(Header, MA, []),
    [encode(Sections), BareAndFooter].

prepare(read, Msg) ->
    Msg;
prepare(store, Msg = #v1{}) ->
    Msg;
prepare(store, #msg_body_encoded{
                  message_annotations = MA,
                  properties = Props,
                  bare_and_footer = BF,
                  bare_and_footer_application_properties_pos = AppPropsPos,
                  bare_and_footer_body_pos = BodyPos,
                  body_code = BodyCode})
  when is_integer(BodyPos) ->
    PropsPos = case Props of
                   undefined -> ?OMITTED_SECTION;
                   #'v1_0.properties'{} -> 0
               end,
    #v1{message_annotations = MA,
        bare_and_footer = BF,
        bare_and_footer_properties_pos = PropsPos,
        bare_and_footer_application_properties_pos = AppPropsPos,
        bare_and_footer_body_pos = BodyPos,
        body_code = BodyCode
       }.

%% internal

msg_to_sections(#msg_body_decoded{header = H,
                                  message_annotations = MAC,
                                  properties = P,
                                  application_properties = APC,
                                  data = Data,
                                  footer = FC}) ->
    S0 = case Data of
             #'v1_0.amqp_value'{} ->
                 [Data];
             _ when is_list(Data) ->
                 Data
         end,
    S = case FC of
            [] ->
                S0;
            _ ->
                S0 ++ [#'v1_0.footer'{content = FC}]
        end,
    to_sections(H, MAC, P, APC, S);
msg_to_sections(#msg_body_encoded{header = H,
                                  message_annotations = MAC,
                                  properties = P,
                                  application_properties = APC,
                                  bare_and_footer = BareAndFooter,
                                  bare_and_footer_body_pos = BodyPos,
                                  body_code = BodyCode}) ->
    BodyAndFooterBin = binary_part(BareAndFooter,
                                   BodyPos,
                                   byte_size(BareAndFooter) - BodyPos),
    BodyAndFooter = case BodyCode of
                        ?DESCRIPTOR_CODE_DATA ->
                            amqp10_framing:decode_bin(BodyAndFooterBin);
                        _ ->
                            [{amqp_encoded_body_and_footer, BodyAndFooterBin}]
                    end,
    to_sections(H, MAC, P, APC, BodyAndFooter);
msg_to_sections(#v1{message_annotations = MAC,
                    bare_and_footer = BareAndFooterBin,
                    body_code = ?DESCRIPTOR_CODE_DATA}) ->
    BareAndFooter = amqp10_framing:decode_bin(BareAndFooterBin),
    to_sections(undefined, MAC, BareAndFooter);
msg_to_sections(#v1{message_annotations = MAC,
                    bare_and_footer = BareAndFooter,
                    bare_and_footer_body_pos = BodyPos
                   }) ->
    Tail = case BodyPos =:= 0 of
               true ->
                   [{amqp_encoded_body_and_footer, BareAndFooter}];
               false ->
                   {Bin, BodyAndFooterBin} = split_binary(BareAndFooter, BodyPos),
                   Sections = amqp10_framing:decode_bin(Bin),
                   Sections ++ [{amqp_encoded_body_and_footer, BodyAndFooterBin}]
           end,
    to_sections(undefined, MAC, Tail).

to_sections(H, MAC, P, APC, Tail) ->
    S0 = case APC of
             [] ->
                 Tail;
             _ ->
                 [#'v1_0.application_properties'{content = APC} | Tail]
         end,
    S = case P of
            undefined ->
                S0;
            _ ->
                [P | S0]
        end,
    to_sections(H, MAC, S).

to_sections(H, MAC, Tail) ->
    S = case MAC of
            [] ->
                Tail;
            _ ->
                [#'v1_0.message_annotations'{content = MAC} | Tail]
        end,
    case H of
        undefined ->
            S;
        _ ->
            [H | S]
    end.

-spec protocol_state_message_annotations(amqp_annotations(), mc:annotations()) ->
    amqp_annotations().
protocol_state_message_annotations(MA, Anns) ->
    maps:fold(
      fun(?ANN_EXCHANGE, Exchange, L) ->
              maps_upsert(<<"x-exchange">>, {utf8, Exchange}, L);
         (?ANN_ROUTING_KEYS, RKeys, L) ->
              RKey = hd(RKeys),
              maps_upsert(<<"x-routing-key">>, {utf8, RKey}, L);
         (<<"x-", _/binary>> = K, V, L)
           when V =/= undefined ->
              %% any x-* annotations get added as message annotations
              maps_upsert(K, mc_util:infer_type(V), L);
         (<<"timestamp_in_ms">>, V, L) ->
              maps_upsert(<<"x-opt-rabbitmq-received-time">>, {timestamp, V}, L);
         (deaths, Deaths, L)
           when is_list(Deaths) ->
              Maps = encode_deaths(Deaths),
              maps_upsert(<<"x-opt-deaths">>, {array, map, Maps}, L);
         (_, _, Acc) ->
              Acc
      end, MA, Anns).

maps_upsert(Key, TaggedVal, KVList) ->
    TaggedKey = {symbol, Key},
    Elem = {TaggedKey, TaggedVal},
    lists:keystore(TaggedKey, 1, KVList, Elem).

encode(Sections) when is_list(Sections) ->
    [amqp10_framing:encode_bin(Section) || Section <- Sections,
                                           not is_empty(Section)].

is_empty(#'v1_0.header'{durable = undefined,
                        priority = undefined,
                        ttl = undefined,
                        first_acquirer = undefined,
                        delivery_count = undefined}) ->
    true;
is_empty(#'v1_0.delivery_annotations'{content = []}) ->
    true;
is_empty(#'v1_0.message_annotations'{content = []}) ->
    true;
is_empty(#'v1_0.properties'{message_id = undefined,
                            user_id = undefined,
                            to = undefined,
                            subject = undefined,
                            reply_to = undefined,
                            correlation_id = undefined,
                            content_type = undefined,
                            content_encoding = undefined,
                            absolute_expiry_time = undefined,
                            creation_time = undefined,
                            group_id = undefined,
                            group_sequence = undefined,
                            reply_to_group_id = undefined}) ->
    true;
is_empty(#'v1_0.application_properties'{content = []}) ->
    true;
is_empty(#'v1_0.footer'{content = []}) ->
    true;
is_empty(_) ->
    false.

message_annotation(Key, State, Default)
  when is_binary(Key) ->
    case message_annotations(State) of
        [] -> Default;
        MA -> mc_util:amqp_map_get(Key, MA, Default)
    end.

message_annotations(#msg_body_decoded{message_annotations = L}) -> L;
message_annotations(#msg_body_encoded{message_annotations = L}) -> L;
message_annotations(#v1{message_annotations = L}) -> L.

message_annotations_as_simple_map(#msg_body_encoded{message_annotations = Content}) ->
    message_annotations_as_simple_map0(Content);
message_annotations_as_simple_map(#v1{message_annotations = Content}) ->
    message_annotations_as_simple_map0(Content).

message_annotations_as_simple_map0(Content) ->
    %% the section record format really is terrible
    lists:filtermap(fun({{symbol, K}, {_T, V}})
                          when ?IS_SIMPLE_VALUE(V) ->
                            {true, {K, V}};
                       (_) ->
                            false
                    end, Content).

application_properties_as_simple_map(
  #msg_body_encoded{application_properties = Content}, L) ->
    application_properties_as_simple_map0(Content, L);
application_properties_as_simple_map(
  #v1{bare_and_footer_application_properties_pos = ?OMITTED_SECTION}, L) ->
    L;
application_properties_as_simple_map(
  #v1{bare_and_footer = Bin,
      bare_and_footer_application_properties_pos = ApPos,
      bare_and_footer_body_pos = BodyPos}, L) ->
    ApLen = BodyPos - ApPos,
    ApBin = binary_part(Bin, ApPos, ApLen),
    % assertion
    {ApDescribed, ApLen} = amqp10_binary_parser:parse(ApBin),
    #'v1_0.application_properties'{content = Content} = amqp10_framing:decode(ApDescribed),
    application_properties_as_simple_map0(Content, L).

application_properties_as_simple_map0(Content, L) ->
    %% the section record format really is terrible
    lists:foldl(fun({{utf8, K}, {_T, V}}, Acc)
                      when ?IS_SIMPLE_VALUE(V) ->
                        [{K, V} | Acc];
                   ({{utf8, K}, V}, Acc)
                     when V =:= undefined orelse is_boolean(V) ->
                        [{K, V} | Acc];
                   (_, Acc)->
                        Acc
                end, L, Content).

msg_body_decoded([], Acc) ->
    Acc;
msg_body_decoded([#'v1_0.header'{} = H | Rem], Msg) ->
    msg_body_decoded(Rem, Msg#msg_body_decoded{header = H});
msg_body_decoded([_Ignore = #'v1_0.delivery_annotations'{} | Rem], Msg) ->
    msg_body_decoded(Rem, Msg);
msg_body_decoded([#'v1_0.message_annotations'{content = MAC} | Rem], Msg) ->
    msg_body_decoded(Rem, Msg#msg_body_decoded{message_annotations = MAC});
msg_body_decoded([#'v1_0.properties'{} = P | Rem], Msg) ->
    msg_body_decoded(Rem, Msg#msg_body_decoded{properties = P});
msg_body_decoded([#'v1_0.application_properties'{content = APC} | Rem], Msg) ->
    msg_body_decoded(Rem, Msg#msg_body_decoded{application_properties = APC});
msg_body_decoded([#'v1_0.data'{} = D | Rem], #msg_body_decoded{data = Body} = Msg)
  when is_list(Body) ->
    msg_body_decoded(Rem, Msg#msg_body_decoded{data = Body ++ [D]});
msg_body_decoded([#'v1_0.amqp_sequence'{} = D | Rem], #msg_body_decoded{data = Body} = Msg)
  when is_list(Body) ->
    msg_body_decoded(Rem, Msg#msg_body_decoded{data = Body ++ [D]});
msg_body_decoded([#'v1_0.amqp_value'{} = B | Rem], #msg_body_decoded{} = Msg) ->
    %% an amqp value can only be a singleton
    msg_body_decoded(Rem, Msg#msg_body_decoded{data = B});
msg_body_decoded([#'v1_0.footer'{content = FC} | Rem], Msg) ->
    msg_body_decoded(Rem, Msg#msg_body_decoded{footer = FC}).

msg_body_encoded([#'v1_0.header'{} = H | Rem], Payload, Msg) ->
    msg_body_encoded(Rem, Payload, Msg#msg_body_encoded{header = H});
msg_body_encoded([_Ignore = #'v1_0.delivery_annotations'{} | Rem], Payload, Msg) ->
    msg_body_encoded(Rem, Payload, Msg);
msg_body_encoded([#'v1_0.message_annotations'{content = MAC} | Rem], Payload, Msg) ->
    msg_body_encoded(Rem, Payload, Msg#msg_body_encoded{message_annotations = MAC});
msg_body_encoded([{{pos, Pos}, #'v1_0.properties'{} = Props} | Rem], Payload, Msg) ->
    %% properties is the first bare message section.
    Bin = binary_part_bare_and_footer(Payload, Pos),
    msg_body_encoded(Rem, Pos, Msg#msg_body_encoded{properties = Props,
                                                    bare_and_footer = Bin});
msg_body_encoded([{{pos, Pos}, #'v1_0.application_properties'{content = APC}} | Rem], Payload, Msg)
  when is_binary(Payload) ->
    %% AMQP sender omitted properties section.
    %% application-properties is the first bare message section.
    Bin = binary_part_bare_and_footer(Payload, Pos),
    msg_body_encoded(Rem, Pos, Msg#msg_body_encoded{application_properties = APC,
                                                    bare_and_footer_application_properties_pos = 0,
                                                    bare_and_footer = Bin});
msg_body_encoded([{{pos, Pos}, #'v1_0.application_properties'{content = APC}} | Rem], BarePos, Msg)
  when is_integer(BarePos) ->
    %% properties is the first bare message section.
    %% application-properties is the second bare message section.
    msg_body_encoded(Rem, BarePos, Msg#msg_body_encoded{
                                     application_properties = APC,
                                     bare_and_footer_application_properties_pos = Pos - BarePos
                                    });
%% Base case: we assert the last part contains the mandatory body:
msg_body_encoded([{{pos, Pos}, {body, Code}}], Payload, Msg)
  when is_binary(Payload) ->
    %% AMQP sender omitted properties and application-properties sections.
    %% The body is the first bare message section.
    Bin = binary_part_bare_and_footer(Payload, Pos),
    Msg#msg_body_encoded{bare_and_footer = Bin,
                         bare_and_footer_body_pos = 0,
                         body_code = Code};
msg_body_encoded([{{pos, Pos}, {body, Code}}], BarePos, Msg)
  when is_integer(BarePos) ->
    Msg#msg_body_encoded{bare_and_footer_body_pos = Pos - BarePos,
                         body_code = Code}.

%% We extract the binary part of the payload exactly once when the bare message starts.
binary_part_bare_and_footer(Payload, Start) ->
    binary_part(Payload, Start, byte_size(Payload) - Start).

update_header_from_anns(undefined, Anns) ->
    update_header_from_anns(#'v1_0.header'{durable = true}, Anns);
update_header_from_anns(Header, Anns) ->
    DeliveryCount = case Anns of
                        #{delivery_count := C} -> C;
                        _ -> 0
                    end,
    Redelivered = case Anns of
                      #{redelivered := R} -> R;
                      _ -> false
                  end,
    FirstAcq = not Redelivered andalso
               DeliveryCount =:= 0 andalso
               not is_map_key(deaths, Anns),
    Header#'v1_0.header'{first_acquirer = FirstAcq,
                         delivery_count = {uint, DeliveryCount}}.

encode_deaths(Deaths) ->
    lists:map(
      fun({{Queue, Reason},
           #death{exchange = Exchange,
                  routing_keys = RoutingKeys,
                  count = Count,
                  anns = Anns = #{first_time := FirstTime,
                                  last_time := LastTime}}}) ->
              RKeys = [{utf8, Rk} || Rk <- RoutingKeys],
              Map0 = [
                      {{symbol, <<"queue">>}, {utf8, Queue}},
                      {{symbol, <<"reason">>}, {symbol, atom_to_binary(Reason)}},
                      {{symbol, <<"count">>}, {ulong, Count}},
                      {{symbol, <<"first-time">>}, {timestamp, FirstTime}},
                      {{symbol, <<"last-time">>}, {timestamp, LastTime}},
                      {{symbol, <<"exchange">>}, {utf8, Exchange}},
                      {{symbol, <<"routing-keys">>}, {array, utf8, RKeys}}
                     ],
              Map = case Anns of
                        #{ttl := Ttl} ->
                            [{{symbol, <<"ttl">>}, {uint, Ttl}} | Map0];
                        _ ->
                            Map0
                    end,
              {map, Map}
      end, Deaths).

essential_properties(#msg_body_encoded{message_annotations = MA} = Msg) ->
    Durable = get_property(durable, Msg),
    Priority = get_property(priority, Msg),
    Timestamp = get_property(timestamp, Msg),
    Ttl = get_property(ttl, Msg),
    Anns0 = #{?ANN_DURABLE => Durable},
    Anns = maps_put_truthy(
             ?ANN_PRIORITY, Priority,
             maps_put_truthy(
               ?ANN_TIMESTAMP, Timestamp,
               maps_put_truthy(
                 ttl, Ttl,
                 Anns0))),
    case MA of
        [] ->
            Anns;
        _ ->
            lists:foldl(
              fun ({{symbol, <<"x-routing-key">>},
                    {utf8, Key}}, Acc) ->
                      maps:update_with(?ANN_ROUTING_KEYS,
                                       fun(L) -> [Key | L] end,
                                       [Key],
                                       Acc);
                  ({{symbol, <<"x-cc">>},
                    {list, CCs0}}, Acc) ->
                      CCs = [CC || {_T, CC} <- CCs0],
                      maps:update_with(?ANN_ROUTING_KEYS,
                                       fun(L) -> L ++ CCs end,
                                       CCs,
                                       Acc);
                  ({{symbol, <<"x-exchange">>},
                    {utf8, Exchange}}, Acc) ->
                      Acc#{?ANN_EXCHANGE => Exchange};
                  (_, Acc) ->
                      Acc
              end, Anns, MA)
    end.
