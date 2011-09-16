-module(rabbit_amqp1_0_framing).

-export([encode/1, encode_described/3, decode/1, version/0,
         symbol_for/1, number_for/1, encode_bin/1]).

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

%% TODO this should be part of a more general handler for AMQP values etc.
fill_from_binary(F = #'v1_0.data'{}, Field) ->
    F#'v1_0.data'{content = Field}.

%% TODO so should this?
fill_from_amqp(F = #'v1_0.amqp_value'{}, Field) ->
    F#'v1_0.amqp_value'{content = Field}.

keys(Record) ->
    [{symbol, symbolify(K)} || K <- rabbit_amqp1_0_framing0:fields(Record)].

symbolify(FieldName) when is_atom(FieldName) ->
    re:replace(atom_to_list(FieldName), "_", "-", [{return,list}, global]).

%% Some fields are allowed to be 'multiple', in which case they are
%% either undefined, a single value, or given the descriptor true and a
%% list value. (Yes that is gross)
decode({described, true, {list, Fields}}) ->
    [decode(F) || F <- Fields];
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
            #'v1_0.application_properties'{content = pair_up([decode(F) || F <- Fields])};
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

symbol_for(X) ->
    rabbit_amqp1_0_framing0:symbol_for(X).

number_for(X) ->
    rabbit_amqp1_0_framing0:number_for(X).

descriptor(Symbol) when is_list(Symbol) ->
    {symbol, Symbol};
descriptor(Number) when is_number(Number) ->
    {ulong, Number}.

pair_up(List) ->
    pair_up(List, []).

pair_up([], Pairs) ->
    lists:reverse(Pairs);
pair_up([A, B | Rest], Pairs) ->
    pair_up(Rest, [{A, B} | Pairs]).
