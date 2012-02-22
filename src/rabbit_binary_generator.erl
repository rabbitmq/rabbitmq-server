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

-module(rabbit_binary_generator).
-include("rabbit_framing.hrl").
-include("rabbit.hrl").

%% EMPTY_CONTENT_BODY_FRAME_SIZE, 8 = 1 + 2 + 4 + 1
%%  - 1 byte of frame type
%%  - 2 bytes of channel number
%%  - 4 bytes of frame payload length
%%  - 1 byte of payload trailer FRAME_END byte
%% See definition of check_empty_content_body_frame_size/0,
%% an assertion called at startup.
-define(EMPTY_CONTENT_BODY_FRAME_SIZE, 8).

-export([build_simple_method_frame/3,
         build_simple_content_frames/4,
         build_heartbeat_frame/0]).
-export([generate_table/1, encode_properties/2]).
-export([check_empty_content_body_frame_size/0]).
-export([ensure_content_encoded/2, clear_encoded_content/1]).
-export([map_exception/3]).

%%----------------------------------------------------------------------------

-ifdef(use_specs).

-type(frame() :: [binary()]).

-spec(build_simple_method_frame/3 ::
        (rabbit_channel:channel_number(), rabbit_framing:amqp_method_record(),
         rabbit_types:protocol())
        -> frame()).
-spec(build_simple_content_frames/4 ::
        (rabbit_channel:channel_number(), rabbit_types:content(),
         non_neg_integer(), rabbit_types:protocol())
        -> [frame()]).
-spec(build_heartbeat_frame/0 :: () -> frame()).
-spec(generate_table/1 :: (rabbit_framing:amqp_table()) -> binary()).
-spec(encode_properties/2 ::
        ([rabbit_framing:amqp_property_type()], [any()]) -> binary()).
-spec(check_empty_content_body_frame_size/0 :: () -> 'ok').
-spec(ensure_content_encoded/2 ::
        (rabbit_types:content(), rabbit_types:protocol()) ->
                                       rabbit_types:encoded_content()).
-spec(clear_encoded_content/1 ::
        (rabbit_types:content()) -> rabbit_types:unencoded_content()).
-spec(map_exception/3 :: (rabbit_channel:channel_number(),
                          rabbit_types:amqp_error() | any(),
                          rabbit_types:protocol()) ->
                              {rabbit_channel:channel_number(),
                               rabbit_framing:amqp_method_record()}).

-endif.

%%----------------------------------------------------------------------------

build_simple_method_frame(ChannelInt, MethodRecord, Protocol) ->
    MethodFields = Protocol:encode_method_fields(MethodRecord),
    MethodName = rabbit_misc:method_record_type(MethodRecord),
    {ClassId, MethodId} = Protocol:method_id(MethodName),
    create_frame(1, ChannelInt, [<<ClassId:16, MethodId:16>>, MethodFields]).

build_simple_content_frames(ChannelInt, Content, FrameMax, Protocol) ->
    #content{class_id = ClassId,
             properties_bin = ContentPropertiesBin,
             payload_fragments_rev = PayloadFragmentsRev} =
        ensure_content_encoded(Content, Protocol),
    {BodySize, ContentFrames} =
        build_content_frames(PayloadFragmentsRev, FrameMax, ChannelInt),
    HeaderFrame = create_frame(2, ChannelInt,
                               [<<ClassId:16, 0:16, BodySize:64>>,
                                ContentPropertiesBin]),
    [HeaderFrame | ContentFrames].

build_content_frames(FragsRev, FrameMax, ChannelInt) ->
    BodyPayloadMax = if FrameMax == 0 ->
                             iolist_size(FragsRev);
                        true ->
                             FrameMax - ?EMPTY_CONTENT_BODY_FRAME_SIZE
                     end,
    build_content_frames(0, [], BodyPayloadMax, [],
                         lists:reverse(FragsRev), BodyPayloadMax, ChannelInt).

build_content_frames(SizeAcc, FramesAcc, _FragSizeRem, [],
                     [], _BodyPayloadMax, _ChannelInt) ->
    {SizeAcc, lists:reverse(FramesAcc)};
build_content_frames(SizeAcc, FramesAcc, FragSizeRem, FragAcc,
                     Frags, BodyPayloadMax, ChannelInt)
  when FragSizeRem == 0 orelse Frags == [] ->
    Frame = create_frame(3, ChannelInt, lists:reverse(FragAcc)),
    FrameSize = BodyPayloadMax - FragSizeRem,
    build_content_frames(SizeAcc + FrameSize, [Frame | FramesAcc],
                         BodyPayloadMax, [], Frags, BodyPayloadMax, ChannelInt);
build_content_frames(SizeAcc, FramesAcc, FragSizeRem, FragAcc,
                     [Frag | Frags], BodyPayloadMax, ChannelInt) ->
    Size = size(Frag),
    {NewFragSizeRem, NewFragAcc, NewFrags} =
        if Size == 0           -> {FragSizeRem, FragAcc, Frags};
           Size =< FragSizeRem -> {FragSizeRem - Size, [Frag | FragAcc], Frags};
           true                -> <<Head:FragSizeRem/binary, Tail/binary>> =
                                      Frag,
                                  {0, [Head | FragAcc], [Tail | Frags]}
        end,
    build_content_frames(SizeAcc, FramesAcc, NewFragSizeRem, NewFragAcc,
                         NewFrags, BodyPayloadMax, ChannelInt).

build_heartbeat_frame() ->
    create_frame(?FRAME_HEARTBEAT, 0, <<>>).

create_frame(TypeInt, ChannelInt, Payload) ->
    [<<TypeInt:8, ChannelInt:16, (iolist_size(Payload)):32>>, Payload,
     ?FRAME_END].

%% table_field_to_binary supports the AMQP 0-8/0-9 standard types, S,
%% I, D, T and F, as well as the QPid extensions b, d, f, l, s, t, x,
%% and V.

table_field_to_binary({FName, Type, Value}) ->
    [short_string_to_binary(FName) | field_value_to_binary(Type, Value)].

field_value_to_binary(longstr, Value) ->
    ["S", long_string_to_binary(Value)];

field_value_to_binary(signedint, Value) ->
    ["I", <<Value:32/signed>>];

field_value_to_binary(decimal, {Before, After}) ->
    ["D", Before, <<After:32>>];

field_value_to_binary(timestamp, Value) ->
    ["T", <<Value:64>>];

field_value_to_binary(table, Value) ->
    ["F", table_to_binary(Value)];

field_value_to_binary(array, Value) ->
    ["A", array_to_binary(Value)];

field_value_to_binary(byte, Value) ->
    ["b", <<Value:8/unsigned>>];

field_value_to_binary(double, Value) ->
    ["d", <<Value:64/float>>];

field_value_to_binary(float, Value) ->
    ["f", <<Value:32/float>>];

field_value_to_binary(long, Value) ->
    ["l", <<Value:64/signed>>];

field_value_to_binary(short, Value) ->
    ["s", <<Value:16/signed>>];

field_value_to_binary(bool, Value) ->
    ["t", if Value -> 1; true -> 0 end];

field_value_to_binary(binary, Value) ->
    ["x", long_string_to_binary(Value)];

field_value_to_binary(void, _Value) ->
    ["V"].

table_to_binary(Table) when is_list(Table) ->
    BinTable = generate_table(Table),
    [<<(size(BinTable)):32>>, BinTable].

array_to_binary(Array) when is_list(Array) ->
    BinArray = generate_array(Array),
    [<<(size(BinArray)):32>>, BinArray].

generate_table(Table) when is_list(Table) ->
    list_to_binary(lists:map(fun table_field_to_binary/1, Table)).

generate_array(Array) when is_list(Array) ->
    list_to_binary(lists:map(
                     fun ({Type, Value}) -> field_value_to_binary(Type, Value) end,
                     Array)).

short_string_to_binary(String) when is_binary(String) ->
    Len = size(String),
    if Len < 256 -> [<<Len:8>>, String];
       true      -> exit(content_properties_shortstr_overflow)
    end;
short_string_to_binary(String) ->
    Len = length(String),
    if Len < 256 -> [<<Len:8>>, String];
       true      -> exit(content_properties_shortstr_overflow)
    end.

long_string_to_binary(String) when is_binary(String) ->
    [<<(size(String)):32>>, String];
long_string_to_binary(String) ->
    [<<(length(String)):32>>, String].

encode_properties([], []) ->
    <<0, 0>>;
encode_properties(TypeList, ValueList) ->
    encode_properties(0, TypeList, ValueList, 0, [], []).

encode_properties(_Bit, [], [], FirstShortAcc, FlagsAcc, PropsAcc) ->
    list_to_binary([lists:reverse(FlagsAcc), <<FirstShortAcc:16>>, lists:reverse(PropsAcc)]);
encode_properties(_Bit, [], _ValueList, _FirstShortAcc, _FlagsAcc, _PropsAcc) ->
    exit(content_properties_values_overflow);
encode_properties(15, TypeList, ValueList, FirstShortAcc, FlagsAcc, PropsAcc) ->
    NewFlagsShort = FirstShortAcc bor 1, % set the continuation low bit
    encode_properties(0, TypeList, ValueList, 0, [<<NewFlagsShort:16>> | FlagsAcc], PropsAcc);
encode_properties(Bit, [bit | TypeList], [Value | ValueList], FirstShortAcc, FlagsAcc, PropsAcc) ->
    case Value of
        true -> encode_properties(Bit + 1, TypeList, ValueList,
                                  FirstShortAcc bor (1 bsl (15 - Bit)), FlagsAcc, PropsAcc);
        false -> encode_properties(Bit + 1, TypeList, ValueList,
                                   FirstShortAcc, FlagsAcc, PropsAcc);
        Other -> exit({content_properties_illegal_bit_value, Other})
    end;
encode_properties(Bit, [T | TypeList], [Value | ValueList], FirstShortAcc, FlagsAcc, PropsAcc) ->
    case Value of
        undefined -> encode_properties(Bit + 1, TypeList, ValueList,
                                       FirstShortAcc, FlagsAcc, PropsAcc);
        _ -> encode_properties(Bit + 1, TypeList, ValueList,
                               FirstShortAcc bor (1 bsl (15 - Bit)),
                               FlagsAcc,
                               [encode_property(T, Value) | PropsAcc])
    end.

encode_property(shortstr, String) ->
    Len = size(String),
    if Len < 256 -> <<Len:8, String:Len/binary>>;
       true      -> exit(content_properties_shortstr_overflow)
    end;
encode_property(longstr, String) ->
    Len = size(String), <<Len:32, String:Len/binary>>;
encode_property(octet, Int) ->
    <<Int:8/unsigned>>;
encode_property(shortint, Int) ->
    <<Int:16/unsigned>>;
encode_property(longint, Int) ->
    <<Int:32/unsigned>>;
encode_property(longlongint, Int) ->
    <<Int:64/unsigned>>;
encode_property(timestamp, Int) ->
    <<Int:64/unsigned>>;
encode_property(table, Table) ->
    table_to_binary(Table).

check_empty_content_body_frame_size() ->
    %% Intended to ensure that EMPTY_CONTENT_BODY_FRAME_SIZE is
    %% defined correctly.
    ComputedSize = iolist_size(create_frame(?FRAME_BODY, 0, <<>>)),
    if ComputedSize == ?EMPTY_CONTENT_BODY_FRAME_SIZE ->
            ok;
       true ->
            exit({incorrect_empty_content_body_frame_size,
                  ComputedSize, ?EMPTY_CONTENT_BODY_FRAME_SIZE})
    end.

ensure_content_encoded(Content = #content{properties_bin = PropBin,
                                          protocol = Protocol}, Protocol)
  when PropBin =/= none ->
    Content;
ensure_content_encoded(Content = #content{properties = none,
                                          properties_bin = PropBin,
                                          protocol = Protocol}, Protocol1)
  when PropBin =/= none ->
    Props = Protocol:decode_properties(Content#content.class_id, PropBin),
    Content#content{properties = Props,
                    properties_bin = Protocol1:encode_properties(Props),
                    protocol = Protocol1};
ensure_content_encoded(Content = #content{properties = Props}, Protocol)
  when Props =/= none ->
    Content#content{properties_bin = Protocol:encode_properties(Props),
                    protocol = Protocol}.

clear_encoded_content(Content = #content{properties_bin = none,
                                         protocol = none}) ->
    Content;
clear_encoded_content(Content = #content{properties = none}) ->
    %% Only clear when we can rebuild the properties_bin later in
    %% accordance to the content record definition comment - maximum
    %% one of properties and properties_bin can be 'none'
    Content;
clear_encoded_content(Content = #content{}) ->
    Content#content{properties_bin = none, protocol = none}.

%% NB: this function is also used by the Erlang client
map_exception(Channel, Reason, Protocol) ->
    {SuggestedClose, ReplyCode, ReplyText, FailedMethod} =
        lookup_amqp_exception(Reason, Protocol),
    {ClassId, MethodId} = case FailedMethod of
                              {_, _} -> FailedMethod;
                              none   -> {0, 0};
                              _      -> Protocol:method_id(FailedMethod)
                          end,
    case SuggestedClose orelse (Channel == 0) of
        true  -> {0, #'connection.close'{reply_code = ReplyCode,
                                         reply_text = ReplyText,
                                         class_id   = ClassId,
                                         method_id  = MethodId}};
        false -> {Channel, #'channel.close'{reply_code = ReplyCode,
                                            reply_text = ReplyText,
                                            class_id   = ClassId,
                                            method_id  = MethodId}}
    end.

lookup_amqp_exception(#amqp_error{name        = Name,
                                  explanation = Expl,
                                  method      = Method},
                      Protocol) ->
    {ShouldClose, Code, Text} = Protocol:lookup_amqp_exception(Name),
    ExplBin = amqp_exception_explanation(Text, Expl),
    {ShouldClose, Code, ExplBin, Method};
lookup_amqp_exception(Other, Protocol) ->
    rabbit_log:warning("Non-AMQP exit reason '~p'~n", [Other]),
    {ShouldClose, Code, Text} = Protocol:lookup_amqp_exception(internal_error),
    {ShouldClose, Code, Text, none}.

amqp_exception_explanation(Text, Expl) ->
    ExplBin = list_to_binary(Expl),
    CompleteTextBin = <<Text/binary, " - ", ExplBin/binary>>,
    if size(CompleteTextBin) > 255 -> <<CompleteTextBin:252/binary, "...">>;
       true                        -> CompleteTextBin
    end.
