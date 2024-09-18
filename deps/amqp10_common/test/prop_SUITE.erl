%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2024 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.

-module(prop_SUITE).
-compile([export_all, nowarn_export_all]).

-include_lib("proper/include/proper.hrl").
-include("amqp10_framing.hrl").

-import(rabbit_ct_proper_helpers, [run_proper/3]).

all() ->
    [{group, tests}].

groups() ->
    [
     {tests, [parallel],
      [
       prop_single_primitive_type_parse,
       prop_single_primitive_type_parse_many,
       prop_many_primitive_types_parse,
       prop_many_primitive_types_parse_many,
       prop_annotated_message,
       prop_server_mode_body,
       prop_server_mode_bare_message
      ]}
    ].

%%%%%%%%%%%%%%%%%%
%%% Properties %%%
%%%%%%%%%%%%%%%%%%

prop_single_primitive_type_parse(_Config) ->
    run_proper(
      fun() -> ?FORALL(Val,
                       oneof(primitive_types()),
                       begin
                           Bin = iolist_to_binary(amqp10_binary_generator:generate(Val)),
                           equals({Val, size(Bin)}, amqp10_binary_parser:parse(Bin))
                       end)
      end, [], 10_000).

prop_single_primitive_type_parse_many(_Config) ->
    run_proper(
      fun() -> ?FORALL(Val,
                       oneof(primitive_types()),
                       begin
                           Bin = iolist_to_binary(amqp10_binary_generator:generate(Val)),
                           equals([Val], amqp10_binary_parser:parse_many(Bin, []))
                       end)
      end, [], 10_000).

prop_many_primitive_types_parse(_Config) ->
    run_proper(
      fun() -> ?FORALL(Vals,
                       list(oneof(primitive_types())),
                       begin
                           Bin = iolist_to_binary([amqp10_binary_generator:generate(V) || V <- Vals]),
                           PosValList = parse(Bin, 0, []),
                           equals(Vals, [Val || {_Pos, Val} <- PosValList])
                       end)
      end, [], 1000).

prop_many_primitive_types_parse_many(_Config) ->
    run_proper(
      fun() -> ?FORALL(Vals,
                       list(oneof(primitive_types())),
                       begin
                           Bin = iolist_to_binary([amqp10_binary_generator:generate(V) || V <- Vals]),
                           equals(Vals, amqp10_binary_parser:parse_many(Bin, []))
                       end)
      end, [], 1000).

prop_annotated_message(_Config) ->
    run_proper(
      fun() -> ?FORALL(Sections,
                       annotated_message(),
                       begin
                           Bin = iolist_to_binary([amqp10_framing:encode_bin(S) || S <- Sections]),
                           equals(Sections, amqp10_framing:decode_bin(Bin))
                       end)
      end, [], 1000).

prop_server_mode_body(_Config) ->
    run_proper(
      fun() -> ?FORALL(Sections,
                       annotated_message(),
                       begin
                           {value,
                            FirstBodySection} = lists:search(
                                                  fun(#'v1_0.data'{}) -> true;
                                                     (#'v1_0.amqp_sequence'{}) -> true;
                                                     (#'v1_0.amqp_value'{}) -> true;
                                                     (_) -> false
                                                  end, Sections),

                           Bin = iolist_to_binary([amqp10_framing:encode_bin(S) || S <- Sections]),
                           %% Invariant 1: Decoder should us return the correct
                           %% byte position of the first body section.
                           Decoded = amqp10_framing:decode_bin(Bin, [server_mode]),
                           {value,
                            {{pos, Pos},
                             {body, Code}}} = lists:search(fun(({{pos, _Pos}, {body, _Code}})) ->
                                                                   true;
                                                              (_) ->
                                                                   false
                                                           end, Decoded),
                           FirstBodySectionBin = binary_part(Bin, Pos, size(Bin) - Pos),
                           {Section, _NumBytes} = amqp10_binary_parser:parse(FirstBodySectionBin),
                           %% Invariant 2: Decoder should have returned the
                           %% correct descriptor code of the first body section.
                           {described, {ulong, Code}, _Val} = Section,
                           equals(FirstBodySection, amqp10_framing:decode(Section))
                       end)
      end, [], 1000).

prop_server_mode_bare_message(_Config) ->
    run_proper(
      fun() -> ?FORALL(Sections,
                       annotated_message(),
                       begin
                           {value,
                            FirstBareMsgSection} = lists:search(
                                                     fun(#'v1_0.properties'{}) -> true;
                                                        (#'v1_0.application_properties'{}) -> true;
                                                        (#'v1_0.data'{}) -> true;
                                                        (#'v1_0.amqp_sequence'{}) -> true;
                                                        (#'v1_0.amqp_value'{}) -> true;
                                                        (_) -> false
                                                     end, Sections),

                           Bin = iolist_to_binary([amqp10_framing:encode_bin(S) || S <- Sections]),
                           %% Invariant: Decoder should us return the correct
                           %% byte position of the first bare message section.
                           Decoded = amqp10_framing:decode_bin(Bin, [server_mode]),
                           {value,
                            {{pos, Pos}, _Sect}} = lists:search(fun(({{pos, _Pos}, _Sect})) ->
                                                                        true;
                                                                   (_) ->
                                                                        false
                                                                end, Decoded),
                           FirstBareMsgSectionBin = binary_part(Bin, Pos, size(Bin) - Pos),
                           {Section, _NumBytes} = amqp10_binary_parser:parse(FirstBareMsgSectionBin),
                           equals(FirstBareMsgSection, amqp10_framing:decode(Section))
                       end)
      end, [], 1000).

%%%%%%%%%%%%%%%
%%% Helpers %%%
%%%%%%%%%%%%%%%

parse(Bin, Parsed, PosVal)
  when size(Bin) =:= Parsed ->
    lists:reverse(PosVal);
parse(Bin, Parsed, PosVal)
  when size(Bin) > Parsed ->
    BinPart = binary_part(Bin, Parsed, size(Bin) - Parsed),
    {Val, NumBytes} = amqp10_binary_parser:parse(BinPart),
    parse(Bin, Parsed + NumBytes, [{Parsed, Val} | PosVal]).

%%%%%%%%%%%%%%%%%%
%%% Generators %%%
%%%%%%%%%%%%%%%%%%

primitive_types() ->
    fixed_and_variable_width_types() ++
    compound_types() ++
    [amqp_array()].

fixed_and_variable_width_types() ->
    [
     amqp_null(),
     amqp_boolean(),
     amqp_ubyte(),
     amqp_ushort(),
     amqp_uint(),
     amqp_ulong(),
     amqp_byte(),
     amqp_short(),
     amqp_int(),
     amqp_long(),
     amqp_float(),
     amqp_double(),
     amqp_char(),
     amqp_timestamp(),
     amqp_uuid(),
     amqp_binary(),
     amqp_string(),
     amqp_symbol()
    ].

compound_types() ->
    [
     amqp_list(),
     amqp_map()
    ].

amqp_null() ->
    null.

amqp_boolean() ->
    boolean().

amqp_ubyte() ->
    {ubyte, integer(0, 16#ff)}.

amqp_ushort() ->
    {ushort, integer(0, 16#ff_ff)}.

amqp_uint() ->
    Lim = 16#ff_ff_ff_ff,
    {uint, oneof([
                  integer(0, Lim),
                  ?SIZED(Size, resize(Size * 100, integer(0, Lim)))
                 ])}.

amqp_ulong() ->
    Lim = 16#ff_ff_ff_ff_ff_ff_ff_ff,
    {ulong, oneof([
                   integer(0, Lim),
                   ?SIZED(Size, resize(Size * 100_000, integer(0, Lim)))
                  ])}.

amqp_byte() ->
    Lim = 16#ff div 2,
    {byte, integer(-Lim - 1, Lim)}.

amqp_short() ->
    Lim = 16#ff_ff div 2,
    {short, integer(-Lim - 1, Lim)}.

amqp_int() ->
    Lim = 16#ff_ff_ff_ff div 2,
    {int, oneof([
                 integer(-Lim - 1, Lim),
                 ?SIZED(Size, resize(Size * 100, integer(-Lim - 1, Lim)))
                ])}.

amqp_long() ->
    Lim = 16#ff_ff_ff_ff_ff_ff_ff_ff div 2,
    {long, oneof([
                  integer(-Lim - 1, Lim),
                  ?SIZED(Size, resize(Size * 100, integer(-Lim - 1, Lim)))
                 ])}.

%% AMQP float is 32-bit whereas Erlang float is 64-bit.
%% Therefore, 32-bit encoding any Erlang float will lose precision.
%% Hence, we use some static floats where we know that they can be represented precisely using 32 bits.
amqp_float() ->
    {float, oneof([-1.5, -1.0, 0.0, 1.0, 1.5, 100.0])}.

%% AMQP double and Erlang float are both 64-bit.
amqp_double() ->
    {double, float()}.

amqp_char() ->
    {char, char()}.

amqp_timestamp() ->
    Now = erlang:system_time(millisecond),
    YearMillis = 1000 * 60 * 60 * 24 * 365,
    TimestampMillis1950 = -631_152_000_000,
    TimestampMillis2200 = 7_258_118_400_000,
    {timestamp, oneof([integer(Now - YearMillis, Now + YearMillis),
                       integer(TimestampMillis1950, TimestampMillis2200)
                      ])}.

amqp_uuid() ->
    {uuid, binary(16)}.

amqp_binary() ->
    {binary, oneof([
                    binary(),
                    ?SIZED(Size, resize(Size * 10, binary()))
                   ])}.

amqp_string() ->
    {utf8, utf8()}.

amqp_symbol() ->
    {symbol, ?LET(L,
                  ?SIZED(Size, resize(Size * 10, list(ascii_char()))),
                  list_to_binary(L))}.

ascii_char() ->
    integer(0, 127).

amqp_list() ->
    {list, list(prefer_simple_type())}.

amqp_map() ->
    {map, ?LET(KvList,
               list({prefer_simple_type(),
                     prefer_simple_type()}),
               lists:uniq(fun({K, _V}) -> K end, KvList)
              )}.

amqp_array() ->
    Gens = fixed_and_variable_width_types(),
    ?LET(N,
         integer(1, length(Gens)),
         begin
             Gen = lists:nth(N, Gens),
             ?LET(Instance,
                  Gen,
                  begin
                      Constructor = case Instance of
                                        {T, _V} -> T;
                                        null -> null;
                                        V when is_boolean(V) -> boolean
                                    end,
                      {array, Constructor, list(Gen)}
                  end)
         end).

prefer_simple_type() ->
    frequency([{200, oneof(fixed_and_variable_width_types())},
               {1, ?LAZY(oneof(compound_types()))},
               {1, ?LAZY(amqp_array())}
              ]).

zero_or_one(Section) ->
    oneof([
           [],
           [Section]
          ]).

optional(Field) ->
    oneof([
           undefined,
           Field
          ]).

annotated_message() ->
    ?LET(H,
         zero_or_one(header_section()),
         ?LET(DA,
              zero_or_one(delivery_annotation_section()),
              ?LET(MA,
                   zero_or_one(message_annotation_section()),
                   ?LET(P,
                        zero_or_one(properties_section()),
                        ?LET(AP,
                             zero_or_one(application_properties_section()),
                             ?LET(B,
                                  body(),
                                  ?LET(F,
                                       zero_or_one(footer_section()),
                                       lists:append([H, DA, MA, P, AP, B, F])
                                      ))))))).

%% "The body consists of one of the following three choices: one or more data sections,
%% one or more amqp-sequence sections, or a single amqp-value section." [§3.2]
body() ->
    oneof([
           non_empty(list(data_section())),
           non_empty(list(amqp_sequence_section())),
           [amqp_value_section()]
          ]).

header_section() ->
    #'v1_0.header'{
       durable = optional(amqp_boolean()),
       priority = optional(amqp_ubyte()),
       ttl = optional(milliseconds()),
       first_acquirer = optional(amqp_boolean()),
       delivery_count = optional(amqp_uint())}.

delivery_annotation_section() ->
    #'v1_0.delivery_annotations'{content = annotations()}.

message_annotation_section() ->
    #'v1_0.message_annotations'{content = annotations()}.

properties_section() ->
    #'v1_0.properties'{
       message_id = optional(message_id()),
       user_id = optional(amqp_binary()),
       to = optional(address_string()),
       subject = optional(amqp_string()),
       reply_to = optional(address_string()),
       correlation_id = optional(message_id()),
       content_type = optional(amqp_symbol()),
       content_encoding = optional(amqp_symbol()),
       absolute_expiry_time = optional(amqp_timestamp()),
       creation_time = optional(amqp_timestamp()),
       group_id = optional(amqp_string()),
       group_sequence = optional(sequence_no()),
       reply_to_group_id = optional(amqp_string())}.

application_properties_section() ->
    Gen = ?LET(KvList,
               list({amqp_string(),
                     oneof(fixed_and_variable_width_types() -- [amqp_null()])}),
               lists:uniq(fun({K, _V}) -> K end, KvList)),
    #'v1_0.application_properties'{content = Gen}.

data_section() ->
    #'v1_0.data'{content = binary()}.

amqp_sequence_section() ->
    #'v1_0.amqp_sequence'{content = list(oneof(primitive_types() -- [amqp_null()]))}.

amqp_value_section() ->
    #'v1_0.amqp_value'{content = oneof(primitive_types())}.

footer_section() ->
    #'v1_0.footer'{content = annotations()}.

annotations() ->
    ?LET(KvList,
         list({non_reserved_annotation_key(),
               prefer_simple_type()}),
         begin
             KvList1 = lists:uniq(fun({K, _V}) -> K end, KvList),
             lists:filter(fun({_K, V}) -> V =/= null end, KvList1)
         end).

non_reserved_annotation_key() ->
    {symbol, ?LET(L,
                  ?SIZED(Size, resize(Size * 10, list(ascii_char()))),
                  begin
                      Bin = list_to_binary(L) ,
                      <<"x-", Bin/binary>>
                  end)}.

sequence_no() ->
    amqp_uint().

milliseconds() ->
    amqp_uint().

message_id() ->
    oneof([amqp_ulong(),
           amqp_uuid(),
           amqp_binary(),
           amqp_string()]).

address_string() ->
    amqp_string().
