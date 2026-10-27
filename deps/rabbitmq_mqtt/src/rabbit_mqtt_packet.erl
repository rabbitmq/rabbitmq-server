%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2026 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
%%

-module(rabbit_mqtt_packet).

-include("rabbit_mqtt_packet.hrl").
-include_lib("kernel/include/logger.hrl").

-export([init_state/0, parse/2, serialise/2]).
-export_type([state/0]).

-ifdef(TEST).
-export([serialise_variable_byte_integer/1]).
-endif.

-opaque state() :: unauthenticated |
                   protocol_version() |
                   More :: fun().

-define(RESERVED, 0).
-define(HIGHBIT, 2#10000000).
-define(LOWBITS, 2#01111111).
-define(MAX_MULTIPLIER, (?HIGHBIT * ?HIGHBIT * ?HIGHBIT)).

-spec init_state() -> state().
init_state() -> unauthenticated.

-spec parse(binary(), state()) ->
    {more, state()} |
    {ok, mqtt_packet(), Rest :: binary(), state()} |
    {error, any()}.
parse(Bin, State) ->
    try parse0(Bin, State)
    catch throw:{disconnect_reason_code, _} = Reason ->
              {error, Reason}
    end.

parse0(<<?CONNECT:4, 0:4, Rest/binary>>, unauthenticated) ->
    parse_remaining_len(Rest, #mqtt_packet_fixed{type = ?CONNECT}, proto_ver_uninitialised);
parse0(<<>>, ProtoVer)
  when is_number(ProtoVer) ->
    {more, fun(Bin) -> parse(Bin, ProtoVer) end};
parse0(<<MessageType:4, Dup:1, QoS:2, Retain:1, Rest/binary>>, ProtoVer)
  when is_number(ProtoVer) ->
    ok = validate_fixed_header(MessageType, Dup, QoS, Retain, ProtoVer),
    Fixed = #mqtt_packet_fixed{type = MessageType,
                               dup = int_to_bool(Dup),
                               qos = QoS,
                               retain = int_to_bool(Retain)},
    parse_remaining_len(Rest, Fixed, ProtoVer);
parse0(Bin, Cont)
  when is_function(Cont) ->
    Cont(Bin).

validate_fixed_header(_Type, _Dup, _QoS, _Retain, 3) ->
    ok;
validate_fixed_header(?PUBLISH, Dup, QoS, _Retain, _)
  when QoS < 3 andalso (QoS > 0 orelse Dup =:= 0) ->
    ok;
validate_fixed_header(Type, 0, 1, 0, _)
  when Type =:= ?SUBSCRIBE; Type =:= ?UNSUBSCRIBE ->
    ok;
validate_fixed_header(Type, 0, 0, 0, _)
  when Type =:= ?PUBACK; Type =:= ?PINGREQ; Type =:= ?DISCONNECT; Type =:= ?AUTH ->
    ok;
validate_fixed_header(_, _, _, _, _) ->
    throw(invalid_fixed_header).

parse_remaining_len(<<>>, Fixed, ProtoVer) ->
    {more, fun(Bin) -> parse_remaining_len(Bin, Fixed, ProtoVer) end};
parse_remaining_len(Rest, Fixed, ProtoVer) ->
    parse_remaining_len(Rest, Fixed, 1, 0, ProtoVer).

parse_remaining_len(_Bin, _Fixed, Multiplier, _Length, _ProtoVer)
  when Multiplier > ?MAX_MULTIPLIER ->
    {error, malformed_remaining_length};
parse_remaining_len(_Bin, _Fixed, _Multiplier, Length, _ProtoVer)
  when Length > ?VARIABLE_BYTE_INTEGER_MAX ->
    {error, invalid_mqtt_packet_length};
parse_remaining_len(<<>>, Fixed, Multiplier, Length, ProtoVer) ->
    {more, fun(Bin) -> parse_remaining_len(Bin, Fixed, Multiplier, Length, ProtoVer) end};
parse_remaining_len(<<1:1, Len:7, Rest/binary>>, Fixed, Multiplier, Value, ProtoVer) ->
    parse_remaining_len(Rest, Fixed, Multiplier * ?HIGHBIT, Value + Len * Multiplier, ProtoVer);
parse_remaining_len(<<0:1, Len:7, Rest/binary>>, Fixed,  Multiplier, Value, ProtoVer) ->
    RemainingLen = Value + Len * Multiplier,
    case check_max_packet_size(Fixed#mqtt_packet_fixed.type, RemainingLen) of
        ok ->
            parse_packet(Rest, Fixed, RemainingLen, ProtoVer);
        Err ->
            Err
    end.

-spec check_max_packet_size(packet_type(), non_neg_integer()) ->
    ok | {error, {disconnect_reason_code, ?RC_PACKET_TOO_LARGE} | connect_packet_too_large}.
check_max_packet_size(?CONNECT, Length) ->
    MaxSize = persistent_term:get(?PERSISTENT_TERM_MAX_PACKET_SIZE_UNAUTHENTICATED),
    if Length =< MaxSize ->
           ok;
       true ->
           ?LOG_ERROR("CONNECT packet size (~b bytes) exceeds "
                      "mqtt.max_packet_size_unauthenticated (~b bytes)",
                      [Length, MaxSize]),
           {error, connect_packet_too_large}
    end;
check_max_packet_size(Type, Length) ->
    MaxSize = persistent_term:get(?PERSISTENT_TERM_MAX_PACKET_SIZE_AUTHENTICATED),
    if Length =< MaxSize ->
           ok;
       true ->
           ?LOG_ERROR("MQTT packet size (~b bytes, type ~b) exceeds "
                      "mqtt.max_packet_size_authenticated (~p bytes)",
                      [Length, Type, MaxSize]),
           {error, {disconnect_reason_code, ?RC_PACKET_TOO_LARGE}}
    end.

parse_packet(Bin, #mqtt_packet_fixed{type = ?CONNECT} = Fixed, Length, proto_ver_uninitialised) ->
    parse_connect(Bin, Fixed, Length);
parse_packet(Bin, #mqtt_packet_fixed{type = Type,
                                     qos  = Qos} = Fixed, Length, ProtoVer) ->
    case {Type, Bin} of
        {?PUBLISH, <<TopicLen:16, TopicName:TopicLen/binary,
                     PacketBin:(Length-2-TopicLen)/binary,
                     Rest/binary>>} ->
            ok = validate_topic_name(TopicName, ProtoVer =:= 5, Type),
            {PacketId, Rest1} = case Qos of
                                    ?QOS_0 ->
                                        %% "The Packet Identifier field is only present in
                                        %% PUBLISH packets where the QoS level is 1 or 2."
                                        {undefined, PacketBin};
                                    _ ->
                                        <<Id:16, R/binary>> = PacketBin,
                                        {Id, R}
                                end,
            {Props, Payload} = parse_props(Rest1, ProtoVer, Type),
            true = TopicName =/= <<>> orelse maps:is_key('Topic-Alias', Props),
            Publish = #mqtt_packet_publish{topic_name = TopicName,
                                           packet_id = PacketId,
                                           props = Props},
            wrap(Fixed, Publish, Payload, Rest, ProtoVer);
        {?PUBACK, <<PacketId:16, PacketBin:(Length-2)/binary, Rest/binary>>} ->
            {ReasonCode, Props} = case PacketBin of
                                      <<>> ->
                                          {?RC_SUCCESS, #{}};
                                      <<Rc>> when ProtoVer =:= 5 ->
                                          %% "If the Remaining Length is less than 4 there is
                                          %% no Property Length and the value of 0 is used."
                                          %% [v5 3.4.2.2.1]
                                          {Rc, #{}};
                                      <<Rc, PropsBin/binary>> when ProtoVer =:= 5 ->
                                          {Props0, <<>>} = parse_props(PropsBin, ProtoVer, Type),
                                          {Rc, Props0}
                                  end,
            PubAck = #mqtt_packet_puback{packet_id = PacketId,
                                         reason_code = ReasonCode,
                                         props = Props},
            wrap(Fixed, PubAck, Rest, ProtoVer);
        {?SUBSCRIBE, <<PacketId:16, PacketBin:(Length-2)/binary, Rest/binary>>} ->
            %% "SUBSCRIBE messages use QoS level 1 to acknowledge multiple subscription requests."
            1 = Qos,
            {Props, Rest1} = parse_props(PacketBin, ProtoVer, Type),
            Id = maps:get('Subscription-Identifier', Props, undefined),
            [_ | _] = Subscriptions = parse_subscriptions(Rest1, ProtoVer, Id, max_topic_filters(), []),
            Subscribe = #mqtt_packet_subscribe{packet_id = PacketId,
                                               props = Props,
                                               subscriptions = Subscriptions},
            wrap(Fixed, Subscribe, Rest, ProtoVer);
        {?UNSUBSCRIBE, <<PacketId:16, PacketBin:(Length-2)/binary, Rest/binary>>} ->
            %% "UNSUBSCRIBE messages use QoS level 1 to acknowledge multiple unsubscribe requests."
            1 = Qos,
            {Props, Rest1} = parse_props(PacketBin, ProtoVer, Type),
            [_ | _] = Topics = parse_topic_filters(Rest1, max_topic_filters(), []),
            Unsubscribe = #mqtt_packet_unsubscribe{packet_id = PacketId,
                                                   props = Props,
                                                   topic_filters = Topics},
            wrap(Fixed, Unsubscribe, Rest, ProtoVer);
        {?PINGREQ, Rest} ->
            0 = Length,
            wrap(Fixed, Rest, ProtoVer);
        {?DISCONNECT, <<ReasonCode, Rest/binary>>}
          when ProtoVer =:= 5 andalso Length =:= 1 ->
            %% "3.14.2.2.1 Property Length
            %% The length of Properties in the DISCONNECT packet Variable Header encoded as a
            %% Variable Byte Integer. If the Remaining Length is less than 2, a value of 0 is used."
            %% This branch is a bit special because the MQTT v5 protocol spec is not very
            %% precise: It does suggest that a remaining length of 1 is valid.
            Disconnect = #mqtt_packet_disconnect{reason_code = ReasonCode},
            wrap(Fixed, Disconnect, Rest, ProtoVer);
        {?DISCONNECT, <<ReasonCode, PacketBin:(Length-1)/binary, Rest/binary>>}
          when ProtoVer =:= 5 andalso Length > 1 ->
            {Props, <<>>} = parse_props(PacketBin, ProtoVer, Type),
            Disconnect = #mqtt_packet_disconnect{reason_code = ReasonCode,
                                                 props = Props},
            wrap(Fixed, Disconnect, Rest, ProtoVer);
        {?DISCONNECT, Rest} ->
            %% v3, v4, or v5 because for v5:
            %% "The Reason Code and Property Length can be omitted if the Reason Code is 0x00
            %% (Normal disconnecton) and there are no Properties. In this case the DISCONNECT
            %% has a Remaining Length of 0."
            0 = Length,
            wrap(Fixed, #mqtt_packet_disconnect{}, Rest, ProtoVer);
        {?AUTH, _Rest}
          when ProtoVer =:= 5 ->
            throw(extended_authentication_unsupported);
        {_, TooShortBin}
          when byte_size(TooShortBin) < Length ->
            more_fragments([TooShortBin], byte_size(TooShortBin), Length,
                           fun(Joined) ->
                                   parse_packet(Joined, Fixed, Length, ProtoVer)
                           end)
    end.

max_topic_filters() ->
    persistent_term:get(?PERSISTENT_TERM_MAX_TOPIC_FILTERS_PER_PACKET).

parse_subscriptions(<<>>, _ProtoVer, _Id, _Remaining, Acc) ->
    lists:reverse(Acc);
parse_subscriptions(<<Len:16, _:Len/binary, _:8, _/binary>>, _ProtoVer, _Id, 0, _Acc) ->
    topic_filter_limit_exceeded();
parse_subscriptions(<<Len:16, Topic:Len/binary, Options0, Rest/binary>>,
                    ProtoVer, Id, Remaining, Acc) ->
    ok = validate_topic_filter(Topic),
    Options = case ProtoVer of
                  3 -> Options0 band 3;
                  _ -> Options0
              end,
    <<0:2, Rh:2, Rap:1, Nl:1, QoS:2>> = <<Options>>,
    true = QoS < 3 andalso Rh < 3,
    true = ProtoVer =:= 5 orelse Options < 3,
    Sub = #mqtt_subscription{
             topic_filter = binary:copy(Topic),
             options = #mqtt_subscription_opts{
                          qos = QoS,
                          no_local = int_to_bool(Nl),
                          retain_as_published = int_to_bool(Rap),
                          retain_handling = Rh,
                          id = Id}},
    parse_subscriptions(Rest, ProtoVer, Id, Remaining - 1, [Sub | Acc]).

parse_topic_filters(<<>>, _Remaining, Acc) ->
    lists:reverse(Acc);
parse_topic_filters(<<Len:16, _:Len/binary, _/binary>>, 0, _Acc) ->
    topic_filter_limit_exceeded();
parse_topic_filters(<<Len:16, Topic:Len/binary, Rest/binary>>, Remaining, Acc) ->
    ok = validate_topic_filter(Topic),
    parse_topic_filters(Rest, Remaining - 1, [Topic | Acc]).

-spec topic_filter_limit_exceeded() -> no_return().
topic_filter_limit_exceeded() ->
    ?LOG_ERROR("MQTT packet contains more topic filters than "
               "mqtt.max_topic_filters_per_packet (~b)",
               [max_topic_filters()]),
    throw({disconnect_reason_code, ?RC_QUOTA_EXCEEDED}).

parse_connect(Bin, Fixed, Length) ->
    case Bin of
        <<PacketBin:Length/binary, Rest/binary>> ->
            try parse_connect(PacketBin) of
                #mqtt_packet_connect{proto_ver = ProtoVer} = Connect ->
                    wrap(Fixed, Connect, Rest, ProtoVer);
                {error, _} = Error ->
                    Error
            catch throw:Reason when Reason =:= invalid_utf8; Reason =:= invalid_topic_name ->
                      {error, Reason}
            end;
        TooShortBin
          when byte_size(TooShortBin) < Length ->
            more_fragments([TooShortBin], byte_size(TooShortBin), Length,
                           fun(Joined) ->
                                   parse_connect(Joined, Fixed, Length)
                           end)
    end.

-spec parse_connect(binary()) ->
    #mqtt_packet_connect{} |
    {error, reserved_connect_flag | invalid_will_flags | password_without_username}.
parse_connect(<<Len:16, _ProtoName:Len/binary, _ProtoVer, _Flags:7, 1:1, _/binary>>) ->
    {error, reserved_connect_flag};
parse_connect(<<Len:16, _ProtoName:Len/binary, _ProtoVer,
                _Credentials:2, WillRetain:1, WillQos:2, WillFlag:1, _Flags:2, _/binary>>)
  when WillQos =:= 3;
       WillFlag =:= 0 andalso (WillQos =/= 0 orelse WillRetain =/= 0) ->
    {error, invalid_will_flags};
parse_connect(<<Len:16, _ProtoName:Len/binary, ProtoVer, 0:1, 1:1, _Flags:6, _/binary>>)
  when ProtoVer =/= 5 ->
    {error, password_without_username};
parse_connect(<<Len:16, ProtoName:Len/binary,
                ProtoVer,
                UsernameFlag:1,
                PasswordFlag:1,
                WillRetain:1,
                WillQos:2,
                WillFlag:1,
                CleanStart:1,
                0:1, %% ReservedFlag, must be zero
                KeepAlive:16,
                Rest0/binary>>) ->
    true = protocol_name_approved(ProtoVer, ProtoName),
    {Props, Rest1} = parse_props(Rest0, ProtoVer, ?CONNECT),
    {ClientId, Rest2} = parse_utf8(Rest1),
    {WillProps, WillTopic, WillPayload, Rest3} =
    case WillFlag of
        0 ->
            {#{}, undefined, undefined, Rest2};
        1 ->
            {WillProps0, R0} = parse_props(Rest2, ProtoVer, will_props),
            {WillTopic0, R1} = parse_bin(R0),
            ok = validate_topic_name(WillTopic0, false, will_props),
            {WillPayload0, R2} = parse_bin(R1),
            {WillProps0, WillTopic0, WillPayload0, R2}
    end,
    {UserName, Rest} = parse_bin(Rest3, UsernameFlag),
    case UserName of
        undefined -> ok;
        _ -> ok = validate_utf8(UserName)
    end,
    {PasssWord, <<>>} = parse_bin(Rest, PasswordFlag),
    #mqtt_packet_connect{
       proto_ver = ProtoVer,
       will_retain = int_to_bool(WillRetain),
       will_qos = WillQos,
       will_flag = int_to_bool(WillFlag),
       clean_start = int_to_bool(CleanStart),
       keep_alive = KeepAlive,
       props = Props,
       client_id = ClientId,
       will_props = WillProps,
       will_topic = WillTopic,
       will_payload = WillPayload,
       username = UserName,
       password = PasssWord}.

more_fragments(Acc, Size, Length, Parse) when Size < Length ->
    {more, fun(Bin) ->
                   more_fragments([Bin | Acc], Size + byte_size(Bin), Length, Parse)
           end};
more_fragments(Acc, _Size, _Length, Parse) ->
    Parse(iolist_to_binary(lists:reverse(Acc))).

wrap(Fixed, Variable, Payload, Rest, ProtoVer) ->
    {ok, #mqtt_packet { variable = Variable, fixed = Fixed, payload = Payload }, Rest, ProtoVer}.

wrap(Fixed, Variable, Rest, ProtoVer) ->
    {ok, #mqtt_packet { variable = Variable, fixed = Fixed }, Rest, ProtoVer}.

wrap(Fixed, Rest, ProtoVer) ->
    {ok, #mqtt_packet { fixed = Fixed }, Rest, ProtoVer}.

parse_utf8(Bin) ->
    {String, Rest} = parse_bin(Bin),
    ok = validate_utf8(String),
    {String, Rest}.

validate_topic_name(<<>>, false, _Type) ->
    throw(invalid_topic_name);
validate_topic_name(<<>>, true, _Type) ->
    ok;
validate_topic_name(Topic, _AllowEmpty, Type) ->
    ok = validate_utf8(Topic, Type),
    case {binary:match(Topic, <<"+">>), binary:match(Topic, <<"#">>)} of
        {nomatch, nomatch} -> ok;
        _ -> throw(invalid_topic_name)
    end.

validate_topic_filter(Topic) ->
    true = Topic =/= <<>>,
    ok = validate_utf8(Topic),
    validate_topic_levels(binary:split(Topic, <<"/">>, [global])).

validate_topic_levels([]) ->
    ok;
validate_topic_levels([<<"#">>]) ->
    ok;
validate_topic_levels([<<"+">> | Rest]) ->
    validate_topic_levels(Rest);
validate_topic_levels([Level | Rest]) ->
    nomatch = binary:match(Level, [<<"+">>, <<"#">>]),
    validate_topic_levels(Rest).

%% Skip UTF-8 validation in PUBLISH packets to avoid per-message scans.
validate_utf8(_String, ?PUBLISH) ->
    ok;
validate_utf8(String, _Type) ->
    validate_utf8(String).

validate_utf8(<<>>) ->
    ok;
validate_utf8(<<C/utf8, Rest/binary>>) when C =/= 0 ->
    validate_utf8(Rest);
validate_utf8(_) ->
    throw(invalid_utf8).

-spec parse_bin(binary()) ->
    {binary(), binary()}.
parse_bin(<<Len:16, Bin:Len/binary, Rest/binary>>) ->
    {Bin, Rest}.

-spec parse_bin(binary(), 0 | 1) ->
    {option(binary()), binary()}.
parse_bin(Bin, 0) ->
    {undefined, Bin};
parse_bin(Bin, 1) ->
    parse_bin(Bin).

protocol_name_approved(Ver, Name) ->
    lists:member({Ver, Name}, ?PROTOCOL_NAMES).

-spec int_to_bool(0 | 1) -> boolean().
int_to_bool(0) -> false;
int_to_bool(1) -> true.

-spec bool_to_int(boolean()) -> 0 | 1.
bool_to_int(false) -> 0;
bool_to_int(true) -> 1.

-spec parse_props(binary(), protocol_version(), packet_type() | will_props) ->
    {properties(), binary()}.
parse_props(Bin, Vsn, _Type)
  when Vsn < 5 ->
    {#{}, Bin};
parse_props(<<0, Rest/binary>>, 5, _Type) ->
    %% "If there are no properties, this MUST be indicated by
    %% including a Property Length of zero." [MQTT-2.2.2-1]
    {#{}, Rest};
parse_props(Bin, 5, Type) ->
    {Len, Rest0} = parse_variable_byte_integer(Bin),
    <<PropsBin:Len/binary, Rest/binary>> = Rest0,
    Props = parse_prop(PropsBin, Type, #{}),
    {Props, Rest}.

-spec parse_prop(binary(), packet_type() | will_props, properties()) ->
    properties().
parse_prop(<<>>, _Type, #{'User-Property' := {_Remaining, UserProps}} = Props) ->
    %% "The Server MUST maintain the order of User Properties"
    maps:update('User-Property', lists:reverse(UserProps), Props);
parse_prop(<<>>, _Type, Props) ->
    Props;
parse_prop(<<16#01, Val, Bin/binary>>, Type, Props)
  when Type =:= ?PUBLISH orelse Type =:= will_props ->
    parse_prop(Bin, Type, Props#{'Payload-Format-Indicator' => Val});
parse_prop(<<16#02, Val:32, Bin/binary>>, Type, Props)
  when Type =:= ?PUBLISH orelse Type =:= will_props ->
    parse_prop(Bin, Type, Props#{'Message-Expiry-Interval' => Val});
parse_prop(<<16#03, Len:16, Val:Len/binary, Rest/binary>>, Type, Props)
  when Type =:= ?PUBLISH orelse Type =:= will_props ->
    ok = validate_utf8(Val, Type),
    parse_prop(Rest, Type, Props#{'Content-Type' => Val});
parse_prop(<<16#08, Len:16, Val:Len/binary, Rest/binary>>, Type, Props)
  when Type =:= ?PUBLISH orelse Type =:= will_props ->
    ok = validate_utf8(Val, Type),
    parse_prop(Rest, Type, Props#{'Response-Topic' => Val});
parse_prop(<<16#09, Len:16, Val:Len/binary, Bin/binary>>, Type, Props)
  when Type =:= ?PUBLISH orelse Type =:= will_props ->
    parse_prop(Bin, Type, Props#{'Correlation-Data' => Val});
parse_prop(<<16#0B, Bin/binary>>, ?SUBSCRIBE = Type, Props) ->
    {Val, Rest} = parse_variable_byte_integer(Bin),
    true = Val > 0,
    %% Client sends at most one Subscription-Identifier to the server (in SUBSCRIBE packet).
    %% "It is a Protocol Error to include the Subscription Identifier more than once." [v5 3.8.2.1.2]
    parse_prop(Rest, Type, Props#{'Subscription-Identifier' => Val});
parse_prop(<<16#11, Val:32, Bin/binary>>, Type, Props)
  when Type =:= ?CONNECT orelse Type =:= ?DISCONNECT ->
    parse_prop(Bin, Type, Props#{'Session-Expiry-Interval' => Val});
parse_prop(<<16#15, Len:16, Val:Len/binary, Rest/binary>>, ?CONNECT = Type, Props) ->
    ok = validate_utf8(Val),
    parse_prop(Rest, Type, Props#{'Authentication-Method' => Val});
parse_prop(<<16#16, Len:16, Val:Len/binary, Bin/binary>>, ?CONNECT = Type, Props) ->
    parse_prop(Bin, Type, Props#{'Authentication-Data' => Val});
parse_prop(<<16#17, Val, Bin/binary>>, ?CONNECT = Type, Props) ->
    parse_prop(Bin, Type, Props#{'Request-Problem-Information' => Val});
parse_prop(<<16#18, Val:32, Bin/binary>>, will_props = Type, Props) ->
    parse_prop(Bin, Type, Props#{'Will-Delay-Interval' => Val});
parse_prop(<<16#19, Val, Bin/binary>>, ?CONNECT = Type, Props) ->
    parse_prop(Bin, Type, Props#{'Request-Response-Information' => Val});
parse_prop(<<16#1C, Len:16, Val:Len/binary, Rest/binary>>, ?DISCONNECT = Type, Props) ->
    ok = validate_utf8(Val),
    parse_prop(Rest, Type, Props#{'Server-Reference' => Val});
parse_prop(<<16#1F, Len:16, Val:Len/binary, Rest/binary>>, Type, Props)
  when Type =:= ?PUBACK orelse Type =:= ?DISCONNECT ->
    ok = validate_utf8(Val),
    parse_prop(Rest, Type, Props#{'Reason-String' => Val});
parse_prop(<<16#21, Val:16, Bin/binary>>, ?CONNECT = Type, Props) ->
    case Val of
        0 ->
            %% "It is a Protocol Error [...] for it to have the value 0."
            %% [MQTT 5.0 3.1.2.11.3]
            throw({invalid_receive_maximum, Val});
        _ ->
            parse_prop(Bin, Type, Props#{'Receive-Maximum' => Val})
    end;
parse_prop(<<16#22, Val:16, Bin/binary>>, ?CONNECT = Type, Props) ->
    parse_prop(Bin, Type, Props#{'Topic-Alias-Maximum' => Val});
parse_prop(<<16#23, Val:16, Bin/binary>>, ?PUBLISH = Type, Props) ->
    parse_prop(Bin, Type, Props#{'Topic-Alias' => Val});
parse_prop(<<16#26, LenName:16, Name:LenName/binary, LenVal:16, Val:LenVal/binary, Rest/binary>>,
           Type, Props0) ->
    %% "The User Property is allowed to appear multiple times to represent multiple
    %% name, value pairs. The same name is allowed to appear more than once."
    ok = validate_utf8(Name, Type),
    ok = validate_utf8(Val, Type),
    Pair = {Name, Val},
    UserProps = case Props0 of
                    #{'User-Property' := {0, _}} ->
                        ?LOG_ERROR("MQTT packet contains more User Properties than "
                                   "mqtt.max_user_properties_per_packet (~b)",
                                   [max_user_properties()]),
                        throw({disconnect_reason_code, ?RC_QUOTA_EXCEEDED});
                    #{'User-Property' := {Remaining, L}} ->
                        {Remaining - 1, [Pair | L]};
                    _ ->
                        {max_user_properties() - 1, [Pair]}
                end,
    parse_prop(Rest, Type, Props0#{'User-Property' => UserProps});
parse_prop(<<16#27, Val:32, Bin/binary>>, ?CONNECT = Type, Props) ->
    %% "It is a Protocol Error [...] for the value to be set to zero." [MQTT 5.0 3.1.2.11.4]
    true = Val > 0,
    parse_prop(Bin, Type, Props#{'Maximum-Packet-Size' => Val});
parse_prop(<<PropId, _Rest/binary>>, Type, _Props) ->
    throw({invalid_property_id, PropId, Type}).

max_user_properties() ->
    persistent_term:get(?PERSISTENT_TERM_MAX_USER_PROPERTIES_PER_PACKET).

-spec parse_variable_byte_integer(binary()) ->
    {integer(), Rest :: binary()}.
parse_variable_byte_integer(Bin) ->
    parse_variable_byte_integer(Bin, 1, 0).

parse_variable_byte_integer(<<_:8, _Rest/binary>>, Multiplier, _Value)
  when Multiplier > ?MAX_MULTIPLIER ->
    throw(malformed_variable_byte_integer);
parse_variable_byte_integer(<<1:1, Len:7, Rest/binary>>, Multiplier, Value) ->
    parse_variable_byte_integer(Rest, Multiplier * ?HIGHBIT, Value + Len * Multiplier);
parse_variable_byte_integer(<<0:1, Len:7, Rest/binary>>, Multiplier, Value) ->
    {Value + Len * Multiplier, Rest}.

-spec serialise(mqtt_packet(), protocol_version()) ->
    iodata().
serialise(#mqtt_packet{fixed = Fixed,
                       variable = Variable,
                       payload = Payload}, Vsn) ->
    serialise(Fixed, Variable, Payload, Vsn).

-spec serialise(#mqtt_packet_fixed{},
                VariableHeader :: option(tuple()),
                Payload :: option(iodata()),
                protocol_version()) ->
    iodata().
serialise(#mqtt_packet_fixed{type = ?CONNACK} = Fixed,
          #mqtt_packet_connack{session_present = SessionPresent,
                               code = ConnectCode,
                               props = Props},
          undefined, Vsn) ->
    Variable = [bool_to_int(SessionPresent),
                ConnectCode,
                serialise_props(Props, Vsn)],
    serialise_fixed(Fixed, Variable, []);
serialise(#mqtt_packet_fixed{type = ?SUBACK} = Fixed,
          #mqtt_packet_suback{packet_id = PacketId,
                              props = Props,
                              reason_codes = ReasonCodes},
          undefined, Vsn)
  when is_list(ReasonCodes) ->
    Variable = [<<PacketId:16>>,
                serialise_props(Props, Vsn)],
    Payload = if Vsn =:= 3 ->
                     %% Disallow error code (0x80) in the MQTT SUBACK message.
                     << <<?RESERVED:6, QoS:2>> || QoS <- ReasonCodes >>;
                 Vsn > 3 ->
                     ReasonCodes
              end,
    serialise_fixed(Fixed, Variable, Payload);
serialise(#mqtt_packet_fixed{type = ?UNSUBACK} = Fixed,
          #mqtt_packet_unsuback{packet_id = PacketId,
                                props = Props,
                                reason_codes = ReasonCodes},
          undefined, Vsn)
  when is_list(ReasonCodes) ->
    Variable = [<<PacketId:16>>,
                serialise_props(Props, Vsn)],
    Payload = if Vsn < 5 ->
                     %% "The UNSUBACK Packet has no payload." [v4 3.11.3]
                     [];
                 Vsn =:= 5 ->
                     ReasonCodes
              end,
    serialise_fixed(Fixed, Variable, Payload);
serialise(#mqtt_packet_fixed{type = ?PUBLISH,
                             qos = Qos} = Fixed,
          #mqtt_packet_publish{topic_name = TopicName,
                               packet_id = PacketId,
                               props = Props},
          Payload, Vsn) ->
    PacketIdIoData = if Qos =:= 0 -> [];
                        Qos > 0 -> <<PacketId:16>>
                     end,
    Variable = [serialise_binary(TopicName),
                PacketIdIoData,
                serialise_props(Props, Vsn)],
    serialise_fixed(Fixed, Variable, Payload);
serialise(#mqtt_packet_fixed{type = ?PUBACK} = Fixed,
          #mqtt_packet_puback{packet_id = PacketId,
                              reason_code = ReasonCode,
                              props = Props},
          undefined, Vsn) ->
    PacketIdBin = <<PacketId:16>>,
    Variable = case Vsn of
                   V when V =< 4 orelse
                          %% "The Reason Code and Property Length can be omitted if the Reason
                          %% Code is 0x00 (Success) and there are no Properties." [v5 3.4.2.1]
                          V =:= 5 andalso
                          reason_code =:= ?RC_SUCCESS andalso
                          map_size(Props) =:= 0 ->
                       PacketIdBin;
                   5 ->
                       [PacketIdBin,
                        ReasonCode,
                        serialise_props(Props, Vsn)]
               end,
    serialise_fixed(Fixed, Variable, []);
serialise(#mqtt_packet_fixed{type = ?PINGRESP} = Fixed,
          undefined, undefined, _Vsn) ->
    serialise_fixed(Fixed, [], []);
serialise(#mqtt_packet_fixed{type = ?DISCONNECT} = Fixed,
          #mqtt_packet_disconnect{reason_code = ?RC_NORMAL_DISCONNECTION,
                                  props = Props},
          undefined, 5)
  when map_size(Props) =:= 0 ->
    %% "The Reason Code and Property Length can be omitted if the Reason
    %% Code is 0x00 (Normal disconnecton) and there are no Properties."
    serialise_fixed(Fixed, [], []);
serialise(#mqtt_packet_fixed{type = ?DISCONNECT} = Fixed,
          #mqtt_packet_disconnect{reason_code = ReasonCode,
                                  props = Props},
          undefined, Vsn = 5) ->
    Variable = [ReasonCode,
                serialise_props(Props, Vsn)],
    serialise_fixed(Fixed, Variable, []).

-spec serialise_fixed(#mqtt_packet_fixed{},
                      VariableHeader :: iodata(),
                      Payload :: iodata()) ->
    iodata().
serialise_fixed(#mqtt_packet_fixed{type = Type,
                                   dup = Dup,
                                   qos = Qos,
                                   retain = Retain}, Variable, Payload)
  when is_integer(Type) andalso ?CONNECT =< Type andalso Type =< ?AUTH andalso
       is_integer(Qos) andalso 0 =< Qos andalso Qos =< 2 ->
    Len = iolist_size(Variable) + iolist_size(Payload),
    [<<Type:4, (bool_to_int(Dup)):1, Qos:2, (bool_to_int(Retain)):1>>,
     serialise_variable_byte_integer(Len),
     Variable,
     Payload].

%% 1.5.6 Binary Data
%% "Binary Data is represented by a Two Byte Integer length which indicates the number of
%% data bytes, followed by that number of bytes. Thus, the length of Binary Data is
%% limited to the range of 0 to 65,535 Bytes."
-spec serialise_binary(binary()) -> iodata().
serialise_binary(Bin) ->
    Len = byte_size(Bin),
    Len =< 16#ffff orelse throw(mqtt_binary_too_large),
    LenBin = <<Len:16>>,
    [LenBin, Bin].

-spec serialise_variable_byte_integer(non_neg_integer()) -> iodata().
serialise_variable_byte_integer(N)
  when N =< ?LOWBITS ->
    %% Optimisation: Prevent binary construction.
    [N];
serialise_variable_byte_integer(N)
  when N =< ?VARIABLE_BYTE_INTEGER_MAX ->
    serialise_variable_byte_integer0(N);
serialise_variable_byte_integer(_N) ->
    throw(mqtt_remaining_length_too_large).

-spec serialise_variable_byte_integer0(non_neg_integer()) -> binary().
serialise_variable_byte_integer0(N)
  when N =< ?LOWBITS ->
    <<0:1, N:7>>;
serialise_variable_byte_integer0(N) ->
    <<1:1, (N rem ?HIGHBIT):7, (serialise_variable_byte_integer0(N div ?HIGHBIT))/binary>>.

-spec serialise_props(properties(), protocol_version()) ->
    iodata().
serialise_props(_Props, Vsn)
  when Vsn < 5 ->
    [];
serialise_props(Props, 5)
  when map_size(Props) =:= 0 ->
    %% "If there are no properties, this MUST be indicated by
    %% including a Property Length of zero." [MQTT-2.2.2-1]
    [0];
serialise_props(Props, 5) ->
    IoList = maps:fold(fun(Id, Val, Acc) ->
                               [serialise_prop(Id, Val) | Acc]
                       end, [], Props),
    [serialise_variable_byte_integer(iolist_size(IoList)),
     IoList].

-spec serialise_prop(property_name(), property_value()) ->
    iodata().
serialise_prop('Payload-Format-Indicator', Val) ->
    [16#01, Val];
serialise_prop('Message-Expiry-Interval', Val) ->
    <<16#02, Val:32>>;
serialise_prop('Content-Type', Val) ->
    [16#03, serialise_binary(Val)];
serialise_prop('Response-Topic', Val) ->
    [16#08, serialise_binary(Val)];
serialise_prop('Correlation-Data', Val) ->
    [16#09, serialise_binary(Val)];
serialise_prop('Subscription-Identifier', Ids)
%% Server can send multiple Subscription-Identifiers to the client (in PUBLISH packet).
%% "Multiple Subscription Identifiers will be included if the publication is the result
%% of a match to more than one subscription, in this case their order is not significant."
%% [v5 3.3.2.3.8]
  when is_list(Ids) ->
    [[16#0B, serialise_variable_byte_integer(Id)] || Id <- Ids];
serialise_prop('Session-Expiry-Interval', Val) ->
    <<16#11, Val:32>>;
serialise_prop('Assigned-Client-Identifier', Val) ->
    [16#12, serialise_binary(Val)];
serialise_prop('Server-Keep-Alive', Val) ->
    <<16#13, Val:16>>;
serialise_prop('Authentication-Method', Val) ->
    [16#15, serialise_binary(Val)];
serialise_prop('Authentication-Data', Val) ->
    [16#16, serialise_binary(Val)];
serialise_prop('Reason-String', Val) ->
    [16#1F, serialise_binary(Val)];
serialise_prop('Receive-Maximum', Val) ->
    <<16#21, Val:16>>;
serialise_prop('Topic-Alias-Maximum', Val) ->
    <<16#22, Val:16>>;
serialise_prop('Topic-Alias', Val) ->
    <<16#23, Val:16>>;
serialise_prop('Maximum-QoS', Val) ->
    [16#24, Val];
serialise_prop('Retain-Available', Val) ->
    [16#25, Val];
serialise_prop('User-Property', Props) ->
    lists:map(fun({Name, Val}) ->
                      [16#26,
                       serialise_binary(Name),
                       serialise_binary(Val)]
              end, Props);
serialise_prop('Maximum-Packet-Size', Val) ->
    <<16#27, Val:32>>;
serialise_prop('Wildcard-Subscription-Available', Val) ->
    [16#28, Val];
serialise_prop('Subscription-Identifier-Available', Val) ->
    [16#29, Val];
serialise_prop('Shared-Subscription-Available', Val) ->
    [16#2A, Val].
