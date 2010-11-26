-module(rabbit_amqp1_0_framing).

-export([encode/1, decode/1, version/0]).

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
    [{symbol, symbolify(K)} || K <- fields(Record)].

symbolify(FieldName) when is_atom(FieldName) ->
    {ok, Symbol, _} = regexp:gsub(atom_to_list(FieldName), "_", "-"),
    Symbol.

%% Some fields are allowed to be 'multiple', in which case they are
%% either null, a single value, or given the descriptor true and a
%% list value. (Yes that is gross)
decode({described, true, {list, Fields}}) ->
    [decode(F) || F <- Fields];
decode({described, Descriptor, {list, Fields}}) ->
    fill_from_list(record_for(Descriptor), Fields);
decode({described, Descriptor, {map, Fields}}) ->
    fill_from_map(record_for(Descriptor), Fields);
decode(Other) ->
     Other.

%% Frame bodies
record_for({symbol, "amqp:open:list"}) ->
    #'v1_0.open'{};
record_for({symbol, "amqp:begin:list"}) ->
    #'v1_0.begin'{};
record_for({symbol, "amqp:attach:list"}) ->
    #'v1_0.attach'{};
record_for({symbol, "amqp:flow:list"}) ->
    #'v1_0.flow'{};
record_for({symbol, "amqp:transfer:list"}) ->
    #'v1_0.transfer'{};
record_for({symbol, "amqp:disposition:list"}) ->
    #'v1_0.disposition'{};
record_for({symbol, "amqp:detach:list"}) ->
    #'v1_0.detach'{};
record_for({symbol, "amqp:end:list"}) ->
    #'v1_0.end'{};
record_for({symbol, "amqp:close:list"}) ->
    #'v1_0.close'{};
%% Other types
record_for({symbol, "amqp:linkage:list"}) ->
    #'v1_0.linkage'{};
record_for({symbol, "amqp:flow-state:list"}) ->
    #'v1_0.flow_state'{};
record_for({symbol, "amqp:target:map"}) ->
    #'v1_0.target'{};
record_for({symbol, "amqp:source:map"}) ->
    #'v1_0.source'{};
record_for({symbol, "amqp:fragment:list"}) ->
    #'v1_0.fragment'{};
record_for({symbol, "amqp:header:list"}) ->
    #'v1_0.header'{};
record_for({symbol, "amqp:properties:list"}) ->
    #'v1_0.properties'{};
record_for({symbol, "amqp:footer:list"}) ->
    #'v1_0.footer'{};
record_for({symbol, "amqp:transfer-state:map"}) ->
    #'v1_0.transfer_state'{};
record_for({symbol, "amqp:accepted:map"}) ->
    #'v1_0.accepted'{};
record_for({symbol, "amqp:rejected:map"}) ->
    #'v1_0.rejected'{}.


fields(#'v1_0.open'{})           -> record_info(fields, 'v1_0.open');
fields(#'v1_0.close'{})          -> record_info(fields, 'v1_0.close');
fields(#'v1_0.begin'{})          -> record_info(fields, 'v1_0.begin');
fields(#'v1_0.end'{})            -> record_info(fields, 'v1_0.end');
fields(#'v1_0.attach'{})         -> record_info(fields, 'v1_0.attach');
fields(#'v1_0.detach'{})         -> record_info(fields, 'v1_0.detach');
fields(#'v1_0.flow'{})           -> record_info(fields, 'v1_0.flow');
fields(#'v1_0.transfer'{})       -> record_info(fields, 'v1_0.transfer');
fields(#'v1_0.disposition'{})    -> record_info(fields, 'v1_0.disposition');
fields(#'v1_0.linkage'{})        -> record_info(fields, 'v1_0.linkage');
fields(#'v1_0.flow_state'{})     -> record_info(fields, 'v1_0.flow_state');
fields(#'v1_0.target'{})         -> record_info(fields, 'v1_0.target');
fields(#'v1_0.source'{})         -> record_info(fields, 'v1_0.source');
fields(#'v1_0.fragment'{})       -> record_info(fields, 'v1_0.fragment');
fields(#'v1_0.header'{})         -> record_info(fields, 'v1_0.header');
fields(#'v1_0.properties'{})     -> record_info(fields, 'v1_0.properties');
fields(#'v1_0.footer'{})         -> record_info(fields, 'v1_0.footer');
fields(#'v1_0.transfer_state'{}) -> record_info(fields, 'v1_0.transfer_state');
fields(#'v1_0.accepted'{})       -> record_info(fields, 'v1_0.accepted');
fields(#'v1_0.rejected'{})       -> record_info(fields, 'v1_0.rejected').

encode_described(list, Symbol, Frame) ->
    {described, {symbol, Symbol},
     {list, lists:map(fun encode/1, tl(tuple_to_list(Frame)))}};
encode_described(map, Symbol, Frame) ->
    {described, {symbol, Symbol},
     {map, lists:zip(keys(Frame),
                     lists:map(fun encode/1, tl(tuple_to_list(Frame))))}}.

encode(Frame = #'v1_0.open'{}) ->
    encode_described(list, "amqp:open:list", Frame);
encode(Frame = #'v1_0.begin'{}) ->
    encode_described(list, "amqp:begin:list", Frame);
encode(Frame = #'v1_0.attach'{}) ->
    encode_described(list, "amqp:attach:list", Frame);
encode(Frame = #'v1_0.flow'{}) ->
    encode_described(list, "amqp:flow:list", Frame);
encode(Frame = #'v1_0.transfer'{}) ->
    encode_described(list, "amqp:transfer:list", Frame);
encode(Frame = #'v1_0.detach'{}) ->
    encode_described(list, "amqp:detach:list", Frame);
encode(Frame = #'v1_0.end'{}) ->
    encode_described(list, "amqp:end:list", Frame);
encode(Frame = #'v1_0.close'{}) ->
    encode_described(list, "amqp:close:list", Frame);
encode(Frame = #'v1_0.linkage'{}) ->
    encode_described(list, "amqp:linkage:list", Frame);
encode(Frame = #'v1_0.flow_state'{}) ->
    encode_described(list, "amqp:flow-state:list", Frame);
encode(Frame = #'v1_0.target'{}) ->
    encode_described(map, "amqp:target:map", Frame);
encode(Frame = #'v1_0.source'{}) ->
    encode_described(map, "amqp:source:map", Frame);
encode(Frame = #'v1_0.fragment'{}) ->
    encode_described(list, "amqp:fragment:list", Frame);
encode(Frame = #'v1_0.header'{}) ->
    encode_described(list, "amqp:header:list", Frame);
encode(Frame = #'v1_0.properties'{}) ->
    encode_described(list, "amqp:properties:list", Frame);
encode(Frame = #'v1_0.footer'{}) ->
    encode_described(list, "amqp:footer:list", Frame);
encode(Frame = #'v1_0.transfer_state'{}) ->
    encode_described(map, "amqp:transfer-state:map", Frame);
encode(Frame = #'v1_0.accepted'{}) ->
    encode_described(map, "amqp:accepted:map", Frame);
encode(Frame = #'v1_0.rejected'{}) ->
    encode_described(map, "amqp:rejected:map", Frame);
encode(L) when is_list(L) ->
    {described, true, {list, [encode(I) || I <- L]}};
encode(Other) -> Other.
