-module(rabbit_amqp1_0_framing0).

-export([record_for/1, fields/1, encode/1]).

-include("rabbit_amqp1_0.hrl").

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
    #'v1_0.rejected'{};
record_for({symbol, "amqp:extent:list"}) ->
    #'v1_0.extent'{}.


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
fields(#'v1_0.rejected'{})       -> record_info(fields, 'v1_0.rejected');
fields(#'v1_0.extent'{})       -> record_info(fields, 'v1_0.extent').

encode(Frame = #'v1_0.open'{}) ->
    rabbit_amqp1_0_framing:encode_described(list, "amqp:open:list", Frame);
encode(Frame = #'v1_0.begin'{}) ->
    rabbit_amqp1_0_framing:encode_described(list, "amqp:begin:list", Frame);
encode(Frame = #'v1_0.attach'{}) ->
    rabbit_amqp1_0_framing:encode_described(list, "amqp:attach:list", Frame);
encode(Frame = #'v1_0.flow'{}) ->
    rabbit_amqp1_0_framing:encode_described(list, "amqp:flow:list", Frame);
encode(Frame = #'v1_0.transfer'{}) ->
    rabbit_amqp1_0_framing:encode_described(list, "amqp:transfer:list", Frame);
encode(Frame = #'v1_0.detach'{}) ->
    rabbit_amqp1_0_framing:encode_described(list, "amqp:detach:list", Frame);
encode(Frame = #'v1_0.end'{}) ->
    rabbit_amqp1_0_framing:encode_described(list, "amqp:end:list", Frame);
encode(Frame = #'v1_0.close'{}) ->
    rabbit_amqp1_0_framing:encode_described(list, "amqp:close:list", Frame);
encode(Frame = #'v1_0.linkage'{}) ->
    rabbit_amqp1_0_framing:encode_described(list, "amqp:linkage:list", Frame);
encode(Frame = #'v1_0.flow_state'{}) ->
    rabbit_amqp1_0_framing:encode_described(list, "amqp:flow-state:list", Frame);
encode(Frame = #'v1_0.target'{}) ->
    rabbit_amqp1_0_framing:encode_described(map, "amqp:target:map", Frame);
encode(Frame = #'v1_0.source'{}) ->
    rabbit_amqp1_0_framing:encode_described(map, "amqp:source:map", Frame);
encode(Frame = #'v1_0.fragment'{}) ->
    rabbit_amqp1_0_framing:encode_described(list, "amqp:fragment:list", Frame);
encode(Frame = #'v1_0.header'{}) ->
    rabbit_amqp1_0_framing:encode_described(list, "amqp:header:list", Frame);
encode(Frame = #'v1_0.properties'{}) ->
    rabbit_amqp1_0_framing:encode_described(list, "amqp:properties:list", Frame);
encode(Frame = #'v1_0.footer'{}) ->
    rabbit_amqp1_0_framing:encode_described(list, "amqp:footer:list", Frame);
encode(Frame = #'v1_0.transfer_state'{}) ->
    rabbit_amqp1_0_framing:encode_described(map, "amqp:transfer-state:map", Frame);
encode(Frame = #'v1_0.accepted'{}) ->
    rabbit_amqp1_0_framing:encode_described(map, "amqp:accepted:map", Frame);
encode(Frame = #'v1_0.rejected'{}) ->
    rabbit_amqp1_0_framing:encode_described(map, "amqp:rejected:map", Frame);
encode(Frame = #'v1_0.extent'{}) ->
    rabbit_amqp1_0_framing:encode_described(list, "amqp:extent:list", Frame);
encode(Frame = #'v1_0.disposition'{}) ->
    rabbit_amqp1_0_framing:encode_described(list, "amqp:disposition:list", Frame);
encode(L) when is_list(L) ->
    {described, true, {list, [encode(I) || I <- L]}};
encode(undefined) -> null;
encode(Other) -> Other.
