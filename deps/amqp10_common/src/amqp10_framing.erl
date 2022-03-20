%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2022 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(amqp10_framing).

-export([encode/1, encode_described/3, decode/1, version/0,
         symbol_for/1, number_for/1, encode_bin/1, decode_bin/1, pprint/1]).

%% debug
-export([fill_from_list/2, fill_from_map/2]).

-include("amqp10_framing.hrl").

-type amqp10_frame() :: #'v1_0.header'{} |
#'v1_0.delivery_annotations'{} |
#'v1_0.message_annotations'{} |
#'v1_0.properties'{} |
#'v1_0.application_properties'{} |
#'v1_0.data'{} |
#'v1_0.amqp_sequence'{} |
#'v1_0.amqp_value'{} |
#'v1_0.footer'{} |
#'v1_0.received'{} |
#'v1_0.accepted'{} |
#'v1_0.rejected'{} |
#'v1_0.released'{} |
#'v1_0.modified'{} |
#'v1_0.source'{} |
#'v1_0.target'{} |
#'v1_0.delete_on_close'{} |
#'v1_0.delete_on_no_links'{} |
#'v1_0.delete_on_no_messages'{} |
#'v1_0.delete_on_no_links_or_messages'{} |
#'v1_0.sasl_mechanisms'{} |
#'v1_0.sasl_init'{} |
#'v1_0.sasl_challenge'{} |
#'v1_0.sasl_response'{} |
#'v1_0.sasl_outcome'{} |
#'v1_0.attach'{} |
#'v1_0.flow'{} |
#'v1_0.transfer'{} |
#'v1_0.disposition'{} |
#'v1_0.detach'{} |
#'v1_0.end'{} |
#'v1_0.close'{} |
#'v1_0.error'{} |
#'v1_0.coordinator'{} |
#'v1_0.declare'{} |
#'v1_0.discharge'{} |
#'v1_0.declared'{} |
#'v1_0.transactional_state'{}.

version() ->
    {1, 0, 0}.

%% These are essentially in lieu of code generation ..

fill_from_list(Record, Fields) ->
    {Res, _} = lists:foldl(
                 fun (Field, {Record1, Num}) ->
                         DecodedField = decode(Field),
                         {setelement(Num, Record1, DecodedField),
                          Num + 1}
                 end,
                 {Record, 2}, Fields),
    Res.

fill_from_map(Record, Fields) ->
    {Res, _} = lists:foldl(
                 fun (Key, {Record1, Num}) ->
                         case proplists:get_value(Key, Fields) of
                             undefined ->
                                 {Record1, Num+1};
                             Value ->
                                 {setelement(Num, Record1, decode(Value)), Num+1}
                         end
                 end,
                 {Record, 2}, keys(Record)),
    Res.

fill_from(F = #'v1_0.data'{}, Field) ->
    F#'v1_0.data'{content = Field};
fill_from(F = #'v1_0.amqp_value'{}, Field) ->
    F#'v1_0.amqp_value'{content = Field}.

keys(Record) ->
    [{symbol, symbolify(K)} || K <- amqp10_framing0:fields(Record)].

symbolify(FieldName) when is_atom(FieldName) ->
    re:replace(atom_to_list(FieldName), "_", "-", [{return,binary}, global]).

%% TODO: in fields of composite types with multiple=true, "a null
%% value and a zero-length array (with a correct type for its
%% elements) both describe an absence of a value and should be treated
%% as semantically identical." (see section 1.3)

%% A sequence comes as an arbitrary list of values; it's not a
%% composite type.
decode({described, Descriptor, {list, Fields}}) ->
    case amqp10_framing0:record_for(Descriptor) of
        #'v1_0.amqp_sequence'{} ->
            #'v1_0.amqp_sequence'{content = [decode(F) || F <- Fields]};
        Else ->
            fill_from_list(Else, Fields)
    end;
decode({described, Descriptor, {map, Fields}}) ->
    case amqp10_framing0:record_for(Descriptor) of
        #'v1_0.application_properties'{} ->
            #'v1_0.application_properties'{content = decode_map(Fields)};
        #'v1_0.delivery_annotations'{} ->
            #'v1_0.delivery_annotations'{content = decode_map(Fields)};
        #'v1_0.message_annotations'{} ->
            #'v1_0.message_annotations'{content = decode_map(Fields)};
        #'v1_0.footer'{} ->
            #'v1_0.footer'{content = decode_map(Fields)};
        Else ->
            fill_from_map(Else, Fields)
    end;
decode({described, Descriptor, {binary, Field}}) ->
    case amqp10_framing0:record_for(Descriptor) of
        #'v1_0.amqp_value'{} ->
            #'v1_0.amqp_value'{content = {binary, Field}};
        #'v1_0.data'{} ->
            #'v1_0.data'{content = Field}
    end;
decode({described, Descriptor, Field}) ->
    fill_from(amqp10_framing0:record_for(Descriptor), Field);
decode(null) ->
    undefined;
decode(Other) ->
     Other.

decode_map(Fields) ->
    [{decode(K), decode(V)} || {K, V} <- Fields].

-spec encode_described(list | map | binary | annotations | '*',
                       non_neg_integer(),
                       amqp10_frame()) ->
    amqp10_binary_generator:amqp10_described().
encode_described(list, CodeNumber,
                 #'v1_0.amqp_sequence'{content = Content}) ->
    {described, {ulong, CodeNumber},
     {list, lists:map(fun encode/1, Content)}};
encode_described(list, CodeNumber, Frame) ->
    {described, {ulong, CodeNumber},
     {list, lists:map(fun encode/1, tl(tuple_to_list(Frame)))}};
encode_described(map, CodeNumber,
                 #'v1_0.application_properties'{content = Content}) ->
    {described, {ulong, CodeNumber}, {map, Content}};
encode_described(map, CodeNumber,
                 #'v1_0.delivery_annotations'{content = Content}) ->
    {described, {ulong, CodeNumber}, {map, Content}};
encode_described(map, CodeNumber,
                 #'v1_0.message_annotations'{content = Content}) ->
    {described, {ulong, CodeNumber}, {map, Content}};
encode_described(map, CodeNumber, #'v1_0.footer'{content = Content}) ->
    {described, {ulong, CodeNumber}, {map, Content}};
encode_described(binary, CodeNumber, #'v1_0.data'{content = Content}) ->
    {described, {ulong, CodeNumber}, {binary, Content}};
encode_described('*', CodeNumber, #'v1_0.amqp_value'{content = Content}) ->
    {described, {ulong, CodeNumber}, Content};
encode_described(annotations, CodeNumber, Frame) ->
    encode_described(map, CodeNumber, Frame).

encode(X) ->
    amqp10_framing0:encode(X).

encode_bin(X) ->
    amqp10_binary_generator:generate(encode(X)).


decode_bin(X) ->
    [decode(PerfDesc) || PerfDesc <- decode_bin0(X)].

decode_bin0(<<>>) -> [];
decode_bin0(X)    -> {PerfDesc, Rest} = amqp10_binary_parser:parse(X),
                     [PerfDesc | decode_bin0(Rest)].

symbol_for(X) ->
    amqp10_framing0:symbol_for(X).

number_for(X) ->
    amqp10_framing0:number_for(X).

pprint(Thing) when is_tuple(Thing) ->
    case amqp10_framing0:fields(Thing) of
        unknown -> Thing;
        Names   -> [T|L] = tuple_to_list(Thing),
                   {T, lists:zip(Names, [pprint(I) || I <- L])}
    end;
pprint(Other) -> Other.

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").

encode_decode_test_() ->
    Data = [{{utf8, <<"k">>}, {binary, <<"v">>}}],
    Test = fun(M) -> [M] = decode_bin(iolist_to_binary(encode_bin(M))) end,
    [
     fun() -> Test(#'v1_0.application_properties'{content = Data}) end,
     fun() -> Test(#'v1_0.delivery_annotations'{content = Data}) end,
     fun() -> Test(#'v1_0.message_annotations'{content = Data}) end,
     fun() -> Test(#'v1_0.footer'{content = Data}) end
    ].

encode_decode_amqp_sequence_test() ->
    L = [{utf8, <<"k">>},
         {binary, <<"v">>}],
    F = #'v1_0.amqp_sequence'{content = L},
    [F] = decode_bin(iolist_to_binary(encode_bin(F))),
    ok.

-endif.
