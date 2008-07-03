%%   The contents of this file are subject to the Mozilla Public License
%%   Version 1.1 (the "License"); you may not use this file except in
%%   compliance with the License. You may obtain a copy of the License at
%%   http://www.mozilla.org/MPL/
%%
%%   Software distributed under the License is distributed on an "AS IS"
%%   basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See the
%%   License for the specific language governing rights and limitations
%%   under the License.
%%
%%   The Original Code is RabbitMQ.
%%
%%   The Initial Developers of the Original Code are LShift Ltd.,
%%   Cohesive Financial Technologies LLC., and Rabbit Technologies Ltd.
%%
%%   Portions created by LShift Ltd., Cohesive Financial Technologies
%%   LLC., and Rabbit Technologies Ltd. are Copyright (C) 2007-2008
%%   LShift Ltd., Cohesive Financial Technologies LLC., and Rabbit
%%   Technologies Ltd.;
%%
%%   All Rights Reserved.
%%
%%   Contributor(s): ______________________________________.
%%

-module(rabbit_binary_generator).
-include("rabbit_framing.hrl").
-include("rabbit.hrl").

% EMPTY_CONTENT_BODY_FRAME_SIZE, 8 = 1 + 2 + 4 + 1
%  - 1 byte of frame type
%  - 2 bytes of channel number
%  - 4 bytes of frame payload length
%  - 1 byte of payload trailer FRAME_END byte
% See definition of check_empty_content_body_frame_size/0, an assertion called at startup.
-define(EMPTY_CONTENT_BODY_FRAME_SIZE, 8).

-export([build_simple_method_frame/2,
         build_simple_content_frames/3,
         build_heartbeat_frame/0]).
-export([generate_table/1, encode_properties/2]).
-export([check_empty_content_body_frame_size/0]).

-import(lists).

%%----------------------------------------------------------------------------

-ifdef(use_specs).

-type(frame() :: [binary()]).

-spec(build_simple_method_frame/2 ::
      (channel_number(), amqp_method()) -> frame()).
-spec(build_simple_content_frames/3 ::
      (channel_number(), content(), non_neg_integer()) -> [frame()]).
-spec(build_heartbeat_frame/0 :: () -> frame()).
-spec(generate_table/1 :: (amqp_table()) -> binary()). 
-spec(encode_properties/2 :: ([amqp_property_type()], [any()]) -> binary()).
-spec(check_empty_content_body_frame_size/0 :: () -> 'ok').

-endif.

%%----------------------------------------------------------------------------

build_simple_method_frame(ChannelInt, MethodRecord) ->
    MethodFields = rabbit_framing:encode_method_fields(MethodRecord),
    MethodName = rabbit_misc:method_record_type(MethodRecord),
    {ClassId, MethodId} = rabbit_framing:method_id(MethodName),
    create_frame(1, ChannelInt, [<<ClassId:16, MethodId:16>>, MethodFields]).

build_simple_content_frames(ChannelInt,
                            #content{class_id = ClassId,
                                     properties = ContentProperties,
                                     properties_bin = ContentPropertiesBin,
                                     payload_fragments_rev = PayloadFragmentsRev},
                            FrameMax) ->
    {BodySize, ContentFrames} = build_content_frames(PayloadFragmentsRev, FrameMax, ChannelInt),
    HeaderFrame = create_frame(2, ChannelInt,
                               [<<ClassId:16, 0:16, BodySize:64>>,
                                maybe_encode_properties(ContentProperties, ContentPropertiesBin)]),
    [HeaderFrame | ContentFrames].

maybe_encode_properties(_ContentProperties, ContentPropertiesBin)
  when is_binary(ContentPropertiesBin) ->
    ContentPropertiesBin;
maybe_encode_properties(ContentProperties, none) ->
    rabbit_framing:encode_properties(ContentProperties).

build_content_frames(FragmentsRev, FrameMax, ChannelInt) ->
    BodyPayloadMax = if
                         FrameMax == 0 ->
                             none;
                         true ->
                             FrameMax - ?EMPTY_CONTENT_BODY_FRAME_SIZE
                     end,
    build_content_frames(0, [], FragmentsRev, BodyPayloadMax, ChannelInt).

build_content_frames(SizeAcc, FragmentAcc, [], _BodyPayloadMax, _ChannelInt) ->
    {SizeAcc, FragmentAcc};
build_content_frames(SizeAcc, FragmentAcc, [Fragment | FragmentsRev],
                     BodyPayloadMax, ChannelInt)
  when is_number(BodyPayloadMax) and (size(Fragment) > BodyPayloadMax) ->
    <<Head:BodyPayloadMax/binary, Tail/binary>> = Fragment,
    build_content_frames(SizeAcc, FragmentAcc, [Tail, Head | FragmentsRev],
                         BodyPayloadMax, ChannelInt);
build_content_frames(SizeAcc, FragmentAcc, [<<>> | FragmentsRev],
                     BodyPayloadMax, ChannelInt) ->
    build_content_frames(SizeAcc, FragmentAcc, FragmentsRev, BodyPayloadMax, ChannelInt);
build_content_frames(SizeAcc, FragmentAcc, [Fragment | FragmentsRev],
                     BodyPayloadMax, ChannelInt) ->
    build_content_frames(SizeAcc + size(Fragment),
                         [create_frame(3, ChannelInt, Fragment) | FragmentAcc],
                         FragmentsRev,
                         BodyPayloadMax,
                         ChannelInt).

build_heartbeat_frame() ->
    create_frame(?FRAME_HEARTBEAT, 0, <<>>).

create_frame(TypeInt, ChannelInt, PayloadBin) when is_binary(PayloadBin) ->
    [<<TypeInt:8, ChannelInt:16, (size(PayloadBin)):32>>, PayloadBin, <<?FRAME_END>>];
create_frame(TypeInt, ChannelInt, Payload) ->
    create_frame(TypeInt, ChannelInt, list_to_binary(Payload)).

%% table_field_to_binary supports the AMQP 0-8/0-9 standard types, S,
%% I, D, T and F, as well as the QPid extensions b, d, f, l, s, t, x,
%% and V.

table_field_to_binary({FName, longstr, Value}) ->
    [short_string_to_binary(FName), "S", long_string_to_binary(Value)];

table_field_to_binary({FName, signedint, Value}) ->
    [short_string_to_binary(FName), "I", <<Value:32/signed>>];

table_field_to_binary({FName, decimal, {Before, After}}) ->
    [short_string_to_binary(FName), "D", Before, <<After:32>>];

table_field_to_binary({FName, timestamp, Value}) ->
    [short_string_to_binary(FName), "T", <<Value:64>>];

table_field_to_binary({FName, table, Value}) ->
    [short_string_to_binary(FName), "F", table_to_binary(Value)];

table_field_to_binary({FName, byte, Value}) ->
    [short_string_to_binary(FName), "b", <<Value:8/unsigned>>];

table_field_to_binary({FName, double, Value}) ->
    [short_string_to_binary(FName), "d", <<Value:64/float>>];

table_field_to_binary({FName, float, Value}) ->
    [short_string_to_binary(FName), "f", <<Value:32/float>>];

table_field_to_binary({FName, long, Value}) ->
    [short_string_to_binary(FName), "l", <<Value:64/signed>>];

table_field_to_binary({FName, short, Value}) ->
    [short_string_to_binary(FName), "s", <<Value:16/signed>>];

table_field_to_binary({FName, bool, Value}) ->
    [short_string_to_binary(FName), "t", if Value -> 1; true -> 0 end];

table_field_to_binary({FName, binary, Value}) ->
    [short_string_to_binary(FName), "x", long_string_to_binary(Value)];

table_field_to_binary({FName, void, _Value}) ->
    [short_string_to_binary(FName), "V"].

table_to_binary(Table) when is_list(Table) ->
    BinTable = generate_table(Table),
    [<<(size(BinTable)):32>>, BinTable].

generate_table(Table) when is_list(Table) ->
    list_to_binary(lists:map(fun table_field_to_binary/1, Table)).


short_string_to_binary(String) when is_binary(String) and (size(String) < 256) ->
    [<<(size(String)):8>>, String];
short_string_to_binary(String) ->
    StringLength = length(String),
    true = (StringLength < 256), % assertion
    [<<StringLength:8>>, String].


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
    Len = size(String), <<Len:8/unsigned, String:Len/binary>>;
encode_property(longstr, String) ->
    Len = size(String), <<Len:32/unsigned, String:Len/binary>>;
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
    encode_table(Table).


encode_table(Table) ->
    TableBin = list_to_binary(lists:map(fun encode_table_entry/1, Table)),
    Len = size(TableBin),
    <<Len:32/unsigned, TableBin:Len/binary>>.


encode_table_entry({Name, longstr, Value}) ->
    NLen = size(Name),
    VLen = size(Value),
    <<NLen:8/unsigned, Name:NLen/binary, "S", VLen:32/unsigned, Value:VLen/binary>>;
encode_table_entry({Name, signedint, Value}) ->
    NLen = size(Name),
    <<NLen:8/unsigned, Name:NLen/binary, "I", Value:32/signed>>;
encode_table_entry({Name, decimal, {Before, After}}) ->
    NLen = size(Name),
    <<NLen:8/unsigned, Name:NLen/binary, "D", Before:8/unsigned, After:32/unsigned>>;
encode_table_entry({Name, timestamp, Value}) ->
    NLen = size(Name),
    <<NLen:8/unsigned, Name:NLen/binary, "T", Value:64/unsigned>>;
encode_table_entry({Name, table, Value}) ->
    NLen = size(Name),
    TableBin = encode_table(Value),
    <<NLen:8/unsigned, Name:NLen/binary, "F", TableBin/binary>>.

check_empty_content_body_frame_size() ->
    %% Intended to ensure that EMPTY_CONTENT_BODY_FRAME_SIZE is
    %% defined correctly.
    ComputedSize = size(list_to_binary(create_frame(?FRAME_BODY, 0, <<>>))),
    if ComputedSize == ?EMPTY_CONTENT_BODY_FRAME_SIZE ->
            ok;
       true ->
            exit({incorrect_empty_content_body_frame_size,
                  ComputedSize, ?EMPTY_CONTENT_BODY_FRAME_SIZE})
    end.
