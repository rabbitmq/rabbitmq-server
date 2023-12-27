%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2023 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries.  All rights reserved.
%%

-module(rabbit_mqtt_packet).

-include("rabbit_mqtt_packet.hrl").
-include_lib("kernel/include/logger.hrl").

-export([init_state/0, parse/2, serialise/2]).
-export_type([state/0]).

-opaque state() :: unauthenticated |
                   protocol_version() |
                   More :: fun().

-define(RESERVED, 0).
-define(HIGHBIT, 2#10000000).
-define(LOWBITS, 2#01111111).
-define(MAX_MULTIPLIER, ?HIGHBIT * ?HIGHBIT * ?HIGHBIT).

-spec init_state() -> state().
init_state() -> unauthenticated.

-spec parse(binary(), state()) ->
    {more, state()} |
    {ok, mqtt_packet(), Rest :: binary(), state()} |
    {error, any()}.
parse(<<?CONNECT:4, 0:4, Rest/binary>>, unauthenticated) ->
    parse_remaining_len(Rest, #mqtt_packet_fixed{type = ?CONNECT}, proto_ver_uninitialised);
parse(<<>>, ProtoVer)
  when is_number(ProtoVer) ->
    {more, fun(Bin) -> parse(Bin, ProtoVer) end};
parse(<<MessageType:4, Dup:1, QoS:2, Retain:1, Rest/binary>>, ProtoVer)
  when is_number(ProtoVer) ->
    Fixed = #mqtt_packet_fixed{type = MessageType,
                               dup = int_to_bool(Dup),
                               qos = QoS,
                               retain = int_to_bool(Retain)},
    parse_remaining_len(Rest, Fixed, ProtoVer);
parse(Bin, Cont)
  when is_function(Cont) ->
    Cont(Bin).

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
            {PacketId, Rest1} = case Qos of
                                    ?QOS_0 ->
                                        %% "The Packet Identifier field is only present in
                                        %% PUBLISH packets where the QoS level is 1 or 2."
                                        {undefined, PacketBin};
                                    _ ->
                                        <<Id:16, R/binary>> = PacketBin,
                                        {Id, R}
                                end,
            {Props, Payload} = parse_props(Rest1, ProtoVer),
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
                                          {Props0, <<>>} = parse_props(PropsBin, ProtoVer),
                                          {Rc, Props0}
                                  end,
            PubAck = #mqtt_packet_puback{packet_id = PacketId,
                                         reason_code = ReasonCode,
                                         props = Props},
            wrap(Fixed, PubAck, Rest, ProtoVer);
        {?SUBSCRIBE, <<PacketId:16, PacketBin:(Length-2)/binary, Rest/binary>>} ->
            %% "SUBSCRIBE messages use QoS level 1 to acknowledge multiple subscription requests."
            1 = Qos,
            {Props, Rest1} = parse_props(PacketBin, ProtoVer),
            Id = maps:get('Subscription-Identifier', Props, undefined),
            Subscriptions = [#mqtt_subscription{
                                topic_filter = Topic,
                                options = #mqtt_subscription_opts{
                                             qos = QoS,
                                             no_local = int_to_bool(Nl),
                                             retain_as_published = int_to_bool(Rap),
                                             retain_handling = Rh,
                                             id = Id}} ||
                             <<Len:16, Topic:Len/binary, _Reserved:2, Rh:2, Rap:1, Nl:1, QoS:2>> <= Rest1],
            Subscribe = #mqtt_packet_subscribe{packet_id = PacketId,
                                               props = Props,
                                               subscriptions = Subscriptions},
            wrap(Fixed, Subscribe, Rest, ProtoVer);
        {?UNSUBSCRIBE, <<PacketId:16, PacketBin:(Length-2)/binary, Rest/binary>>} ->
            %% "UNSUBSCRIBE messages use QoS level 1 to acknowledge multiple unsubscribe requests."
            1 = Qos,
            {Props, Rest1} = parse_props(PacketBin, ProtoVer),
            Topics = [Topic || <<Len:16, Topic:Len/binary>> <= Rest1],
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
            {Props, <<>>} = parse_props(PacketBin, ProtoVer),
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
            {more, fun(BinMore) ->
                           parse_packet(<<TooShortBin/binary, BinMore/binary>>,
                                        Fixed, Length, ProtoVer)
                   end}
    end.

parse_connect(Bin, Fixed, Length) ->
    case Bin of
        <<PacketBin:Length/binary, Rest/binary>> ->
            Connect = #mqtt_packet_connect{proto_ver = ProtoVer} = parse_connect(PacketBin),
            wrap(Fixed, Connect, Rest, ProtoVer);
        TooShortBin
          when byte_size(TooShortBin) < Length ->
            {more, fun(BinMore) ->
                           parse_connect(<<TooShortBin/binary, BinMore/binary>>,
                                         Fixed, Length)
                   end}
    end.

-spec parse_connect(binary()) ->
    #mqtt_packet_connect{}.
parse_connect(<<Len:16, ProtoName:Len/binary,
                ProtoVer,
                UsernameFlag:1,
                PasswordFlag:1,
                WillRetain:1,
                WillQos:2,
                WillFlag:1,
                CleanStart:1,
                _Reserved:1,
                KeepAlive:16,
                Rest0/binary>>) ->
    true = protocol_name_approved(ProtoVer, ProtoName),
    {Props, Rest1} = parse_props(Rest0, ProtoVer),
    {ClientId, Rest2} = parse_bin(Rest1),
    {WillProps, WillTopic, WillPayload, Rest3} =
    case WillFlag of
        0 ->
            {#{}, undefined, undefined, Rest2};
        1 ->
            {WillProps0, R0} = parse_props(Rest2, ProtoVer),
            {WillTopic0, R1} = parse_bin(R0),
            {WillPayload0, R2} = parse_bin(R1),
            {WillProps0, WillTopic0, WillPayload0, R2}
    end,
    {UserName, Rest} = parse_bin(Rest3, UsernameFlag),
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

wrap(Fixed, Variable, Payload, Rest, ProtoVer) ->
    {ok, #mqtt_packet { variable = Variable, fixed = Fixed, payload = Payload }, Rest, ProtoVer}.

wrap(Fixed, Variable, Rest, ProtoVer) ->
    {ok, #mqtt_packet { variable = Variable, fixed = Fixed }, Rest, ProtoVer}.

wrap(Fixed, Rest, ProtoVer) ->
    {ok, #mqtt_packet { fixed = Fixed }, Rest, ProtoVer}.

%% parse_bin/1 is used for parsing both binary data and UTF-8 encoded strings.
%% Our parser opts out of UTF-8 validation for performance reasons.
%% Some UTF-8 encoded strings are blindly forwarded from MQTT publisher to MQTT subscriber.
%% Other UTF-8 encoded strings are processed by the server and checked for UTF-8
%% well-formedness later on.
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

-spec parse_props(binary(), protocol_version()) ->
    {properties(), binary()}.
parse_props(Bin, Vsn)
  when Vsn < 5 ->
    {#{}, Bin};
parse_props(<<0, Rest/binary>>, 5) ->
    %% "If there are no properties, this MUST be indicated by
    %% including a Property Length of zero." [MQTT-2.2.2-1]
    {#{}, Rest};
parse_props(Bin, 5) ->
    {Len, Rest0} = parse_variable_byte_integer(Bin),
    <<PropsBin:Len/binary, Rest/binary>> = Rest0,
    Props = parse_prop(PropsBin, #{}),
    {Props, Rest}.

-spec parse_prop(binary(), properties()) ->
    properties().
parse_prop(<<>>, #{'User-Property' := UserProps} = Props) ->
    %% "The Server MUST maintain the order of User Properties"
    maps:update('User-Property', lists:reverse(UserProps), Props);
parse_prop(<<>>, Props) ->
    Props;
parse_prop(<<16#01, Val, Bin/binary>>, Props) ->
    parse_prop(Bin, Props#{'Payload-Format-Indicator' => Val});
parse_prop(<<16#02, Val:32, Bin/binary>>, Props) ->
    parse_prop(Bin, Props#{'Message-Expiry-Interval' => Val});
parse_prop(<<16#03, Len:16, Val:Len/binary, Rest/binary>>, Props) ->
    parse_prop(Rest, Props#{'Content-Type' => Val});
parse_prop(<<16#08, Len:16, Val:Len/binary, Rest/binary>>, Props) ->
    parse_prop(Rest, Props#{'Response-Topic' => Val});
parse_prop(<<16#09, Len:16, Val:Len/binary, Bin/binary>>, Props) ->
    parse_prop(Bin, Props#{'Correlation-Data' => Val});
parse_prop(<<16#0B, Bin/binary>>, Props) ->
    {Val, Rest} = parse_variable_byte_integer(Bin),
    %% Client sends at most one Subscription-Identifier to the server (in SUBSCRIBE packet).
    %% "It is a Protocol Error to include the Subscription Identifier more than once." [v5 3.8.2.1.2]
    parse_prop(Rest, Props#{'Subscription-Identifier' => Val});
parse_prop(<<16#11, Val:32, Bin/binary>>, Props) ->
    parse_prop(Bin, Props#{'Session-Expiry-Interval' => Val});
parse_prop(<<16#12, Len:16, Val:Len/binary, Rest/binary>>, Props) ->
    parse_prop(Rest, Props#{'Assigned-Client-Identifier' => Val});
parse_prop(<<16#13, Val:16, Bin/binary>>, Props) ->
    parse_prop(Bin, Props#{'Server-Keep-Alive' => Val});
parse_prop(<<16#15, Len:16, Val:Len/binary, Rest/binary>>, Props) ->
    parse_prop(Rest, Props#{'Authentication-Method' => Val});
parse_prop(<<16#16, Len:16, Val:Len/binary, Bin/binary>>, Props) ->
    parse_prop(Bin, Props#{'Authentication-Data' => Val});
parse_prop(<<16#17, Val, Bin/binary>>, Props) ->
    parse_prop(Bin, Props#{'Request-Problem-Information' => Val});
parse_prop(<<16#18, Val:32, Bin/binary>>, Props) ->
    parse_prop(Bin, Props#{'Will-Delay-Interval' => Val});
parse_prop(<<16#19, Val, Bin/binary>>, Props) ->
    parse_prop(Bin, Props#{'Request-Response-Information' => Val});
parse_prop(<<16#1A, Len:16, Val:Len/binary, Rest/binary>>, Props) ->
    parse_prop(Rest, Props#{'Response-Information' => Val});
parse_prop(<<16#1C, Len:16, Val:Len/binary, Rest/binary>>, Props) ->
    parse_prop(Rest, Props#{'Server-Reference' => Val});
parse_prop(<<16#1F, Len:16, Val:Len/binary, Rest/binary>>, Props) ->
    parse_prop(Rest, Props#{'Reason-String' => Val});
parse_prop(<<16#21, Val:16, Bin/binary>>, Props) ->
    parse_prop(Bin, Props#{'Receive-Maximum' => Val});
parse_prop(<<16#22, Val:16, Bin/binary>>, Props) ->
    parse_prop(Bin, Props#{'Topic-Alias-Maximum' => Val});
parse_prop(<<16#23, Val:16, Bin/binary>>, Props) ->
    parse_prop(Bin, Props#{'Topic-Alias' => Val});
parse_prop(<<16#24, Val, Bin/binary>>, Props) ->
    parse_prop(Bin, Props#{'Maximum-QoS' => Val});
parse_prop(<<16#25, Val, Bin/binary>>, Props) ->
    parse_prop(Bin, Props#{'Retain-Available' => Val});
parse_prop(<<16#26, LenName:16, Name:LenName/binary, LenVal:16, Val:LenVal/binary, Rest/binary>>, Props0) ->
    %% "The User Property is allowed to appear multiple times to represent multiple
    %% name, value pairs. The same name is allowed to appear more than once."
    Pair = {Name, Val},
    Props = maps:update_with('User-Property',
                             fun(UserProps) -> [Pair | UserProps] end,
                             [Pair],
                             Props0),
    parse_prop(Rest, Props);
parse_prop(<<16#27, Val:32, Bin/binary>>, Props) ->
    %% "It is a Protocol Error [...] for the value to be set to zero." [MQTT 5.0 3.1.2.11.4]
    true = Val > 0,
    parse_prop(Bin, Props#{'Maximum-Packet-Size' => Val});
parse_prop(<<16#28, Val, Bin/binary>>, Props) ->
    parse_prop(Bin, Props#{'Wildcard-Subscription-Available' => Val});
parse_prop(<<16#29, Val, Bin/binary>>, Props) ->
    parse_prop(Bin, Props#{'Subscription-Identifier-Available' => Val});
parse_prop(<<16#2A, Val, Bin/binary>>, Props) ->
    parse_prop(Bin, Props#{'Shared-Subscription-Available' => Val});
parse_prop(<<PropId, _Rest/binary>>, _Props) ->
    throw({invalid_property_id, PropId}).

-spec parse_variable_byte_integer(binary()) ->
    {integer(), Rest :: binary()}.
parse_variable_byte_integer(Bin) ->
    parse_variable_byte_integer(Bin, 1, 0).

parse_variable_byte_integer(<<1:1, _Len:7, _Rest/binary>>, Multiplier, _Value)
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
    true = (Len =< 16#ffff),
    LenBin = <<Len:16>>,
    [LenBin, Bin].

-spec serialise_variable_byte_integer(0..?VARIABLE_BYTE_INTEGER_MAX) -> iodata().
serialise_variable_byte_integer(N)
  when N =< ?LOWBITS ->
    %% Optimisation: Prevent binary construction.
    [N];
serialise_variable_byte_integer(N)
  when N =< ?VARIABLE_BYTE_INTEGER_MAX ->
    serialise_variable_byte_integer0(N).

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
