-module(rabbit_amqp1_0_framing).

-export([encode/1, decode/1, version/0]).

%% debug
-export([fill_fields/2]).

-include("rabbit_amqp1_0.hrl").

version() ->
    {1, 0, 0}.

fill_fields(Record, Fields) ->
    {Res, _} = lists:foldl(
                 fun (Field, {Record1, Num}) ->
                         DecodedField = decode(Field),
                         {setelement(Num, Record1, DecodedField),
                          Num + 1}
                 end,
                 {Record, 2}, Fields),
    Res.

%% Some fields are allowed to be 'multiple', in which case they are
%% either null, a single value, or given the descriptor true and a
%% list value. (Yes that is gross)
decode({described, true, {list, Fields}}) ->
    {list, [decode(F) || F <- Fields]};
decode({described, Descriptor, {list, Fields}}) ->
    fill_fields(record_for(Descriptor), Fields);
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
record_for({symbol, "amqp:fragment:list"}) ->
    #'v1_0.fragment'{};
record_for({symbol, "amqp:header:list"}) ->
    #'v1_0.header'{};
record_for({symbol, "amqp:properties:list"}) ->
    #'v1_0.properties'{};
record_for({symbol, "amqp:footer:list"}) ->
    #'v1_0.footer'{}.



encode_described(Symbol, Frame) ->
    {described, {symbol, Symbol},
     {list, lists:map(fun encode/1, tl(tuple_to_list(Frame)))}}.

encode(Frame = #'v1_0.open'{}) ->
    encode_described("amqp:open:list", Frame);
encode(Frame = #'v1_0.begin'{}) ->
    encode_described("amqp:begin:list", Frame);
encode(Frame = #'v1_0.attach'{}) ->
    encode_described("amqp:attach:list", Frame);
encode(Frame = #'v1_0.flow'{}) ->
    encode_described("amqp:flow:list", Frame);
encode(Frame = #'v1_0.transfer'{}) ->
    encode_described("amqp:transfer:list", Frame);
encode(Frame = #'v1_0.detach'{}) ->
    encode_described("amqp:detach:list", Frame);
encode(Frame = #'v1_0.end'{}) ->
    encode_described("amqp:end:list", Frame);
encode(Frame = #'v1_0.close'{}) ->
    encode_described("amqp:close:list", Frame);
encode(Frame = #'v1_0.linkage'{}) ->
    encode_described("amqp:linkage:list", Frame);
encode(Frame = #'v1_0.flow_state'{}) ->
    encode_described("amqp:flow-state:list", Frame);
encode(Frame = #'v1_0.fragment'{}) ->
    encode_described("amqp:fragment:list", Frame);
encode({list, L}) ->
    {list, [encode(I) || I <- L]};
encode(Other) -> Other.
