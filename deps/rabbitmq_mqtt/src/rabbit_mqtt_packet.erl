%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2023 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(rabbit_mqtt_packet).

-include("rabbit_mqtt_packet.hrl").
-include("rabbit_mqtt.hrl").

-export([init_state/0, reset_state/0,
         parse/2, serialise/2]).
-export_type([state/0]).

-opaque state() :: unauthenticated | authenticated | fun().

-define(RESERVED, 0).
-define(MAX_LEN, 16#fffffff).
-define(HIGHBIT, 2#10000000).
-define(LOWBITS, 2#01111111).
-define(MAX_MULTIPLIER, ?HIGHBIT * ?HIGHBIT * ?HIGHBIT).
-define(MAX_PACKET_SIZE_CONNECT, 65_536).

-spec init_state() -> state().
init_state() -> unauthenticated.

-spec reset_state() -> state().
reset_state() -> authenticated.

-spec parse(binary(), state()) ->
    {more, state()} |
    {ok, mqtt_packet(), binary()} |
    {error, any()}.
parse(<<>>, authenticated) ->
    {more, fun(Bin) -> parse(Bin, authenticated) end};
parse(<<MessageType:4, Dup:1, QoS:2, Retain:1, Rest/binary>>, authenticated) ->
    parse_remaining_len(Rest, #mqtt_packet_fixed{ type   = MessageType,
                                                  dup    = bool(Dup),
                                                  qos    = QoS,
                                                  retain = bool(Retain) });
parse(<<?CONNECT:4, 0:4, Rest/binary>>, unauthenticated) ->
    parse_remaining_len(Rest, #mqtt_packet_fixed{type = ?CONNECT});
parse(Bin, Cont)
  when is_function(Cont) ->
    Cont(Bin).

parse_remaining_len(<<>>, Fixed) ->
    {more, fun(Bin) -> parse_remaining_len(Bin, Fixed) end};
parse_remaining_len(Rest, Fixed) ->
    parse_remaining_len(Rest, Fixed, 1, 0).

parse_remaining_len(_Bin, _Fixed, Multiplier, _Length)
  when Multiplier > ?MAX_MULTIPLIER ->
    {error, malformed_remaining_length};
parse_remaining_len(_Bin, _Fixed, _Multiplier, Length)
  when Length > ?MAX_LEN ->
    {error, invalid_mqtt_packet_length};
parse_remaining_len(<<>>, Fixed, Multiplier, Length) ->
    {more, fun(Bin) -> parse_remaining_len(Bin, Fixed, Multiplier, Length) end};
parse_remaining_len(<<1:1, Len:7, Rest/binary>>, Fixed, Multiplier, Value) ->
    parse_remaining_len(Rest, Fixed, Multiplier * ?HIGHBIT, Value + Len * Multiplier);
parse_remaining_len(<<0:1, Len:7, Rest/binary>>, Fixed,  Multiplier, Value) ->
    parse_packet(Rest, Fixed, Value + Len * Multiplier).

parse_packet(Bin, #mqtt_packet_fixed{type = ?CONNECT} = Fixed, Length) ->
    parse_connect(Bin, Fixed, Length);
parse_packet(Bin, #mqtt_packet_fixed{type = Type,
                                     qos  = Qos} = Fixed, Length)
  when Length =< ?MAX_LEN ->
    case {Type, Bin} of
        {?PUBLISH, <<PacketBin:Length/binary, Rest/binary>>} ->
            {TopicName, Rest1} = parse_utf(PacketBin),
            {PacketId, Payload} = case Qos of
                                       0 -> {undefined, Rest1};
                                       _ -> <<M:16/big, R/binary>> = Rest1,
                                            {M, R}
                                   end,
            wrap(Fixed, #mqtt_packet_publish { topic_name = TopicName,
                                               packet_id = PacketId },
                 Payload, Rest);
        {?PUBACK, <<PacketBin:Length/binary, Rest/binary>>} ->
            <<PacketId:16/big>> = PacketBin,
            wrap(Fixed, #mqtt_packet_publish { packet_id = PacketId }, Rest);
        {Subs, <<PacketBin:Length/binary, Rest/binary>>}
          when Subs =:= ?SUBSCRIBE orelse Subs =:= ?UNSUBSCRIBE ->
            1 = Qos,
            <<PacketId:16/big, Rest1/binary>> = PacketBin,
            Topics = parse_topics(Subs, Rest1, []),
            wrap(Fixed, #mqtt_packet_subscribe { packet_id  = PacketId,
                                                 topic_table = Topics }, Rest);
        {Minimal, Rest}
          when Minimal =:= ?DISCONNECT orelse Minimal =:= ?PINGREQ ->
            Length = 0,
            wrap(Fixed, Rest);
        {_, TooShortBin}
          when byte_size(TooShortBin) < Length ->
            {more, fun(BinMore) ->
                           parse_packet(<<TooShortBin/binary, BinMore/binary>>,
                                        Fixed, Length)
                   end}
    end.

parse_connect(Bin, Fixed, Length) ->
    MaxSize = application:get_env(?APP_NAME,
                                  max_packet_size_unauthenticated,
                                  ?MAX_PACKET_SIZE_CONNECT),
    case Length =< MaxSize of
        true ->
            case Bin of
                <<PacketBin:Length/binary, Rest/binary>> ->
                    {ProtoName, Rest1} = parse_utf(PacketBin),
                    <<ProtoVersion : 8, Rest2/binary>> = Rest1,
                    <<UsernameFlag : 1,
                      PasswordFlag : 1,
                      WillRetain   : 1,
                      WillQos      : 2,
                      WillFlag     : 1,
                      CleanSession : 1,
                      _Reserved    : 1,
                      KeepAlive    : 16/big,
                      Rest3/binary>>   = Rest2,
                    {ClientId,  Rest4} = parse_utf(Rest3),
                    {WillTopic, Rest5} = parse_utf(Rest4, WillFlag),
                    {WillMsg,   Rest6} = parse_msg(Rest5, WillFlag),
                    {UserName,  Rest7} = parse_utf(Rest6, UsernameFlag),
                    {PasssWord, <<>>}  = parse_utf(Rest7, PasswordFlag),
                    case protocol_name_approved(ProtoVersion, ProtoName) of
                        true ->
                            wrap(Fixed,
                                 #mqtt_packet_connect{
                                    proto_ver   = ProtoVersion,
                                    will_retain = bool(WillRetain),
                                    will_qos    = WillQos,
                                    will_flag   = bool(WillFlag),
                                    clean_sess  = bool(CleanSession),
                                    keep_alive  = KeepAlive,
                                    client_id   = ClientId,
                                    will_topic  = WillTopic,
                                    will_msg    = WillMsg,
                                    username    = UserName,
                                    password    = PasssWord}, Rest);
                        false ->
                            {error, protocol_header_corrupt}
                    end;
                TooShortBin
                  when byte_size(TooShortBin) < Length ->
                    {more, fun(BinMore) ->
                                   parse_connect(<<TooShortBin/binary, BinMore/binary>>,
                                                 Fixed, Length)
                           end}
            end;
        false ->
            {error, connect_packet_too_large}
    end.

parse_topics(_, <<>>, Topics) ->
    Topics;
parse_topics(?SUBSCRIBE = Sub, Bin, Topics) ->
    {Name, <<_:6, QoS:2, Rest/binary>>} = parse_utf(Bin),
    parse_topics(Sub, Rest, [#mqtt_topic { name = Name, qos = QoS } | Topics]);
parse_topics(?UNSUBSCRIBE = Sub, Bin, Topics) ->
    {Name, <<Rest/binary>>} = parse_utf(Bin),
    parse_topics(Sub, Rest, [#mqtt_topic { name = Name } | Topics]).

wrap(Fixed, Variable, Payload, Rest) ->
    {ok, #mqtt_packet { variable = Variable, fixed = Fixed, payload = Payload }, Rest}.
wrap(Fixed, Variable, Rest) ->
    {ok, #mqtt_packet { variable = Variable, fixed = Fixed }, Rest}.
wrap(Fixed, Rest) ->
    {ok, #mqtt_packet { fixed = Fixed }, Rest}.

parse_utf(Bin, 0) ->
    {undefined, Bin};
parse_utf(Bin, _) ->
    parse_utf(Bin).

parse_utf(<<Len:16/big, Str:Len/binary, Rest/binary>>) ->
    {Str, Rest}.

parse_msg(Bin, 0) ->
    {undefined, Bin};
parse_msg(<<Len:16/big, Msg:Len/binary, Rest/binary>>, _) ->
    {Msg, Rest}.

bool(0) -> false;
bool(1) -> true.

%% serialisation

-spec serialise(#mqtt_packet{}, ?MQTT_PROTO_V3 | ?MQTT_PROTO_V4) ->
    iodata().
serialise(#mqtt_packet{fixed    = Fixed,
                       variable = Variable,
                       payload  = Payload}, Vsn) ->
    serialise_variable(Fixed, Variable, serialise_payload(Payload), Vsn).

serialise_payload(undefined) ->
    <<>>;
serialise_payload(P)
  when is_binary(P) orelse is_list(P) ->
    P.

serialise_variable(#mqtt_packet_fixed   { type        = ?CONNACK } = Fixed,
                   #mqtt_packet_connack { session_present = SessionPresent,
                                          return_code = ReturnCode },
                   <<>> = PayloadBin, _Vsn) ->
    VariableBin = <<?RESERVED:7, (opt(SessionPresent)):1, ReturnCode:8>>,
    serialise_fixed(Fixed, VariableBin, PayloadBin);

serialise_variable(#mqtt_packet_fixed  { type       = SubAck } = Fixed,
                   #mqtt_packet_suback { packet_id = PacketId,
                                         qos_table  = Qos },
                   <<>> = _PayloadBin, Vsn)
  when SubAck =:= ?SUBACK orelse SubAck =:= ?UNSUBACK ->
    VariableBin = <<PacketId:16/big>>,
    QosBin = case Vsn of
                 ?MQTT_PROTO_V3 ->
                     << <<?RESERVED:6, Q:2>> || Q <- Qos >>;
                 ?MQTT_PROTO_V4 ->
                     %% Allow error code (0x80) in the MQTT SUBACK message.
                     << <<Q:8>> || Q <- Qos >>
             end,
    serialise_fixed(Fixed, VariableBin, QosBin);

serialise_variable(#mqtt_packet_fixed   { type       = ?PUBLISH,
                                          qos        = Qos } = Fixed,
                   #mqtt_packet_publish { topic_name = TopicName,
                                          packet_id = PacketId },
                   Payload, _Vsn) ->
    TopicBin = serialise_utf(TopicName),
    PacketIdBin = case Qos of
                       0 -> <<>>;
                       1 -> <<PacketId:16/big>>
                   end,
    serialise_fixed(Fixed, <<TopicBin/binary, PacketIdBin/binary>>, Payload);

serialise_variable(#mqtt_packet_fixed   { type       = ?PUBACK } = Fixed,
                   #mqtt_packet_publish { packet_id = PacketId },
                   PayloadBin, _Vsn) ->
    PacketIdBin = <<PacketId:16/big>>,
    serialise_fixed(Fixed, PacketIdBin, PayloadBin);

serialise_variable(#mqtt_packet_fixed {} = Fixed,
                   undefined,
                   <<>> = _PayloadBin, _Vsn) ->
    serialise_fixed(Fixed, <<>>, <<>>).

serialise_fixed(#mqtt_packet_fixed{ type   = Type,
                                    dup    = Dup,
                                    qos    = Qos,
                                    retain = Retain }, VariableBin, Payload)
  when is_integer(Type) andalso ?CONNECT =< Type andalso Type =< ?DISCONNECT ->
    Len = size(VariableBin) + iolist_size(Payload),
    true = (Len =< ?MAX_LEN),
    LenBin = serialise_len(Len),
    [<<Type:4, (opt(Dup)):1, (opt(Qos)):2, (opt(Retain)):1,
       LenBin/binary, VariableBin/binary>>, Payload].

serialise_utf(String) ->
    StringBin = unicode:characters_to_binary(String),
    Len = size(StringBin),
    true = (Len =< 16#ffff),
    <<Len:16/big, StringBin/binary>>.

serialise_len(N) when N =< ?LOWBITS ->
    <<0:1, N:7>>;
serialise_len(N) ->
    <<1:1, (N rem ?HIGHBIT):7, (serialise_len(N div ?HIGHBIT))/binary>>.

opt(undefined)            -> ?RESERVED;
opt(false)                -> 0;
opt(true)                 -> 1;
opt(X) when is_integer(X) -> X.

protocol_name_approved(Ver, Name) ->
    lists:member({Ver, Name}, ?PROTOCOL_NAMES).
