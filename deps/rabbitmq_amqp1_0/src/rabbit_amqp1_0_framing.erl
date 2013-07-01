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
%% The Initial Developer of the Original Code is GoPivotal, Inc.
%% Copyright (c) 2007-2013 GoPivotal, Inc.  All rights reserved.
%%

-module(rabbit_amqp1_0_framing).

-export([encode/1, encode_described/3, decode/1, version/0,
         symbol_for/1, number_for/1, encode_bin/1, decode_bin/1, pprint/1]).

%% debug
-export([fill_from_list/2, fill_from_map/2]).

-include("rabbit_amqp1_0.hrl").

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

%% TODO should this be part of a more general handler for AMQP values etc?
fill_from_binary(F = #'v1_0.data'{}, Field) ->
    F#'v1_0.data'{content = Field}.

%% TODO so should this?
fill_from_amqp(F = #'v1_0.amqp_value'{}, Field) ->
    F#'v1_0.amqp_value'{content = Field}.

keys(Record) ->
    [{symbol, symbolify(K)} || K <- rabbit_amqp1_0_framing0:fields(Record)].

symbolify(FieldName) when is_atom(FieldName) ->
    re:replace(atom_to_list(FieldName), "_", "-", [{return,list}, global]).

%% TODO: in fields of composite types with multiple=true, "a null
%% value and a zero-length array (with a correct type for its
%% elements) both describe an absence of a value and should be treated
%% as semantically identical." (see section 1.3)

%% A sequence comes as an arbitrary list of values; it's not a
%% composite type.
decode({described, Descriptor, {list, Fields}}) ->
    case rabbit_amqp1_0_framing0:record_for(Descriptor) of
        #'v1_0.amqp_sequence'{} ->
            #'v1_0.amqp_sequence'{content = [decode(F) || F <- Fields]};
        Else ->
            fill_from_list(Else, Fields)
    end;
decode({described, Descriptor, {map, Fields}}) ->
    case rabbit_amqp1_0_framing0:record_for(Descriptor) of
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
    fill_from_binary(rabbit_amqp1_0_framing0:record_for(Descriptor), Field);
decode({described, Descriptor, Field}) ->
    fill_from_amqp(rabbit_amqp1_0_framing0:record_for(Descriptor), Field);
decode(null) ->
    undefined;
decode(Other) ->
     Other.

decode_map(Fields) ->
    [{decode(K), decode(V)} || {K, V} <- Fields].

encode_described(list, ListOrNumber, Frame) ->
    Desc = descriptor(ListOrNumber),
    {described, Desc,
     {list, lists:map(fun encode/1, tl(tuple_to_list(Frame)))}};
encode_described(map, ListOrNumber, Frame) ->
    Desc = descriptor(ListOrNumber),
    {described, Desc,
     {map, lists:zip(keys(Frame),
                     lists:map(fun encode/1, tl(tuple_to_list(Frame))))}};
encode_described(binary, ListOrNumber, #'v1_0.data'{content = Content}) ->
    Desc = descriptor(ListOrNumber),
    {described, Desc, {binary, Content}};
encode_described('*', ListOrNumber, #'v1_0.amqp_value'{content = Content}) ->
    Desc = descriptor(ListOrNumber),
    {described, Desc, Content};
encode_described(annotations, ListOrNumber, Frame) ->
    encode_described(map, ListOrNumber, Frame).

encode(X) ->
    rabbit_amqp1_0_framing0:encode(X).

encode_bin(X) ->
    rabbit_amqp1_0_binary_generator:generate(encode(X)).

decode_bin(X) -> [decode(PerfDesc) || PerfDesc <- decode_bin0(X)].
decode_bin0(<<>>) -> [];
decode_bin0(X)    -> {PerfDesc, Rest} = rabbit_amqp1_0_binary_parser:parse(X),
                     [PerfDesc | decode_bin0(Rest)].

symbol_for(X) ->
    rabbit_amqp1_0_framing0:symbol_for(X).

number_for(X) ->
    rabbit_amqp1_0_framing0:number_for(X).

descriptor(Symbol) when is_list(Symbol) ->
    {symbol, Symbol};
descriptor(Number) when is_number(Number) ->
    {ulong, Number}.


pprint(Thing) when is_tuple(Thing) ->
    case rabbit_amqp1_0_framing0:fields(Thing) of
        unknown -> Thing;
        Names   -> [T|L] = tuple_to_list(Thing),
                   {T, lists:zip(Names, [pprint(I) || I <- L])}
    end;
pprint(Other) -> Other.
