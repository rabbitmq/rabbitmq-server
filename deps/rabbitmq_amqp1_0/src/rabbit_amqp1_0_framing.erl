-module(rabbit_amqp1_0_framing).

-export([encode/1, encode_described/3, decode/1, version/0,
         symbol_for/1, number_for/1]).

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

keys(Record) ->
    [{symbol, symbolify(K)} || K <- rabbit_amqp1_0_framing0:fields(Record)].

symbolify(FieldName) when is_atom(FieldName) ->
    {ok, Symbol, _} = re:replace(atom_to_list(FieldName), "_", "-",
                                 [{return,list}, global]),
    Symbol.

%% Some fields are allowed to be 'multiple', in which case they are
%% either undefined, a single value, or given the descriptor true and a
%% list value. (Yes that is gross)
decode({described, true, {list, Fields}}) ->
    [decode(F) || F <- Fields];
decode({described, Descriptor, {list, Fields}}) ->
    fill_from_list(rabbit_amqp1_0_framing0:record_for(Descriptor), Fields);
decode({described, Descriptor, {map, Fields}}) ->
    fill_from_map(rabbit_amqp1_0_framing0:record_for(Descriptor), Fields);
decode(null) -> undefined;
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
                     lists:map(fun encode/1, tl(tuple_to_list(Frame))))}}.

encode(X) ->
    rabbit_amqp1_0_framing0:encode(X).

symbol_for(X) ->
    rabbit_amqp1_0_framing0:symbol_for(X).

number_for(X) ->
    rabbit_amqp1_0_framing0:number_for(X).

descriptor(Symbol) when is_list(Symbol) ->
    {symbol, Symbol};
descriptor(Number) when is_number(Number) ->
    {ulong, Number}.
