%% The contents of this file are subject to the Mozilla Public License
%% Version 1.1 (the "License"); you may not use this file except in
%% compliance with the License. You may obtain a copy of the License
%% at http://www.mozilla.org/MPL/
%%
%% Software distributed under the License is distributed on an "AS IS"
%% basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See
%% the License for the specific language governing rights and
%% limitations under the License.
%%
%% The Original Code is RabbitMQ.
%%
%% The Initial Developer of the Original Code is VMware, Inc.
%% Copyright (c) 2007-2012 VMware, Inc.  All rights reserved.
%%

-module(rabbit_mqtt_frame).

-export([parse/2, initial_state/0]).
-export([serialise/1]).

-compile(export_all).

-include("include/rabbit_mqtt_frame.hrl").

-define(RESERVED, 0).
-define(PROTOCOL_MAGIC, "MQIsdp").
-define(MAX_LEN, 16#fffffff).
-define(HIGHBIT, 2#10000000).
-define(LOWBITS, 2#01111111).

initial_state() -> none.

parse(<<>>, none) ->
    {more, fun(Bin) -> parse(Bin, none) end};
parse(<<MessageType:4, Dup:1, QoS:2, Retain:1, Rest/binary>> = Bin, none) ->
    parse_remaining_len(Rest, #mqtt_frame_fixed{type   = MessageType,
                                                dup    = Dup,
                                                qos    = QoS,
                                                retain = Retain});
parse(Bin, Cont) -> Cont(Bin).

parse_remaining_len(<<>>, Fixed) ->
    {more, fun(Bin) -> parse_remaining_len(Bin, Fixed) end};
parse_remaining_len(Rest, Fixed) ->
    parse_remaining_len(Rest, Fixed, 1, 0).

parse_remaining_len(_Bin, _Fixed, _Multiplier, Length)
  when Length > ?MAX_LEN ->
    {error, invalid_mqtt_frame_len};
parse_remaining_len(<<>>, Fixed, Multiplier, Length) ->
    {more, fun(Bin) -> parse_remaining_len(Bin, Fixed, Multiplier, Length) end};
parse_remaining_len(<<1:1, Len:7, Rest/binary>>, Fixed, Multiplier, Value) ->
    parse_remaining_len(Rest, Fixed, Multiplier * ?HIGHBIT, Value + Len * Multiplier);
parse_remaining_len(<<0:1, Len:7, Rest/binary>>, Fixed,  Multiplier, Value) ->
    parse_frame(Rest, Fixed, Value + Len * Multiplier).

parse_frame(Bin, #mqtt_frame_fixed{ type = Type,
                                    qos  = QoS } = Fixed, Length) ->
    case {Type, Bin} of
        {?CONNECT, <<FrameBin:Length/binary, Rest/binary>>} ->
            {ProtocolMagic, Rest1} = parse_utf(FrameBin),
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
            {WillMsg,   Rest6} = parse_utf(Rest5, WillFlag),
            {UserName,  Rest7} = parse_utf(Rest6, UsernameFlag),
            {PasssWord, <<>>}  = parse_utf(Rest7, PasswordFlag),
            case ProtocolMagic == ?PROTOCOL_MAGIC of
                true ->
                    wrap(Fixed,
                         #mqtt_frame_connect{proto_ver   = ProtoVersion,
                                             will_retain = WillRetain,
                                             will_qos    = WillQos,
                                             will_flag   = WillMsg,
                                             clean_sess  = CleanSession,
                                             keep_alive  = KeepAlive,
                                             client_id   = ClientId,
                                             will_topic  = WillTopic,
                                             will_msg    = WillMsg,
                                             username    = UserName,
                                             password    = PasssWord}, Rest);
               false ->
                    {error, protocol_header_corrupt}
            end;
        {?PUBLISH, <<FrameBin:Length/binary, Rest/binary>>} ->
            {TopicName, Rest1} = parse_utf(FrameBin),
            {MessageID, Payload} = case QoS of
                                       0 -> {undefined, Rest1};
                                       _ -> <<M:16/big, R/binary>> = Rest1,
                                            {<<M>>, <<R>>}
                                   end,
            wrap(Fixed, #mqtt_frame_publish{topic_name = TopicName,
                                            message_id = MessageID}, Payload, Rest);
        TooShortBin ->
            {more, fun(BinMore) ->
                           parse_frame(<<TooShortBin, BinMore>>, Fixed, Length)
                   end}
     end.

wrap(Fixed, Variable, Payload, Rest) ->
    {ok, #mqtt_frame { variable = Variable, fixed = Fixed, payload = Payload }, Rest}.
wrap(Fixed, Variable, Rest) ->
    {ok, #mqtt_frame { variable = Variable, fixed = Fixed }, Rest}.

parse_utf(Bin, 0) ->
    {undefined, Bin};
parse_utf(Bin, _) ->
    parse_utf(Bin).

parse_utf(<<Len:16/big, Str:Len/binary, Rest/binary>>) ->
    {binary_to_list(Str), Rest}.

%% serialisation

serialise(#mqtt_frame{ fixed    = Fixed,
                       variable = Variable,
                       payload  = Payload }) ->
    serialise_variable(Fixed, Variable, serialise_payload(Payload)).

serialise_payload(_) ->
    <<>>.

serialise_variable(#mqtt_frame_fixed   { type        = ?CONNACK } = Fixed,
                   #mqtt_frame_connack { return_code = ReturnCode },
                   <<>> = PayloadBin) ->
    VariableBin = <<?RESERVED:8, ReturnCode:8>>,
    serialise_fixed(Fixed, VariableBin, PayloadBin).

serialise_fixed(#mqtt_frame_fixed{ type   = Type,
                                   dup    = Dup,
                                   qos    = QoS,
                                   retain = Retain }, VariableBin, PayloadBin)
  when is_integer(Type) andalso ?CONNECT =< Type andalso Type =< ?DISCONNECT ->
    Len = size(VariableBin) + size(PayloadBin),
    true = (Len =< ?MAX_LEN),
    LenBin = serialise_len(Len),
    <<Type:4, (opt(Dup)):1, (opt(QoS)):2, (opt(Retain)):1,
      LenBin/binary, VariableBin/binary, PayloadBin/binary>>.

serialise_len(N) when N =< ?LOWBITS ->
    <<0:1, N:7>>;
serialise_len(N) ->
    <<1:1, (N rem ?HIGHBIT):7, (serialise_len(N div ?HIGHBIT))/binary>>.

opt(undefined)            -> ?RESERVED;
opt(X) when is_integer(X) -> X.
