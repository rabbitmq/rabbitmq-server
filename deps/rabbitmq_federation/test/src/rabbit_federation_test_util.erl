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
%% The Original Code is RabbitMQ Federation.
%%
%% The Initial Developer of the Original Code is GoPivotal, Inc.
%% Copyright (c) 2007-2014 GoPivotal, Inc.  All rights reserved.
%%

-module(rabbit_federation_test_util).

-include("rabbit_federation.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("amqp_client/include/amqp_client.hrl").

-compile(export_all).

-import(rabbit_misc, [pget/2]).

expect(Ch, Q, Fun) when is_function(Fun) ->
    amqp_channel:subscribe(Ch, #'basic.consume'{queue  = Q,
                                                no_ack = true}, self()),
    receive
        #'basic.consume_ok'{consumer_tag = CTag} -> ok
    end,
    Fun(),
    amqp_channel:call(Ch, #'basic.cancel'{consumer_tag = CTag});

expect(Ch, Q, Payloads) ->
    expect(Ch, Q, fun() -> expect(Payloads) end).

expect([]) ->
    ok;
expect(Payloads) ->
    receive
        {#'basic.deliver'{}, #amqp_msg{payload = Payload}} ->
            case lists:member(Payload, Payloads) of
                true  -> expect(Payloads -- [Payload]);
                false -> throw({expected, Payloads, actual, Payload})
            end
    end.

expect_empty(Ch, Q) ->
    ?assertMatch(#'basic.get_empty'{},
                 amqp_channel:call(Ch, #'basic.get'{ queue = Q })).

set_upstream(Cfg, Name, URI) ->
    set_upstream(Cfg, Name, URI, []).

set_upstream(Cfg, Name, URI, Extra) ->
    rabbit_test_util:set_param(Cfg, <<"federation-upstream">>, Name,
                               [{<<"uri">>, URI} | Extra]).

clear_upstream(Cfg, Name) ->
    rabbit_test_util:clear_param(Cfg, <<"federation-upstream">>, Name).

set_upstream_set(Cfg, Name, Set) ->
    rabbit_test_util:set_param(
      Cfg, <<"federation-upstream-set">>, Name,
      [[{<<"upstream">>, UStream} | Extra] || {UStream, Extra} <- Set]).

set_policy(Cfg, Name, Pattern, UpstreamSet) ->
    rabbit_test_util:set_policy(Cfg, Name, Pattern, <<"all">>,
                                [{<<"federation-upstream-set">>, UpstreamSet}]).

set_policy1(Cfg, Name, Pattern, Upstream) ->
    rabbit_test_util:set_policy(Cfg, Name, Pattern, <<"all">>,
                                [{<<"federation-upstream">>, Upstream}]).

clear_policy(Cfg, Name) ->
    rabbit_test_util:clear_policy(Cfg, Name).

set_policy_upstream(Cfg, Pattern, URI, Extra) ->
    set_policy_upstreams(Cfg, Pattern, [{URI, Extra}]).

set_policy_upstreams(Cfg, Pattern, URIExtras) ->
    put(upstream_num, 1),
    [set_upstream(Cfg, gen_upstream_name(), URI, Extra)
     || {URI, Extra} <- URIExtras],
    set_policy(Cfg, Pattern, Pattern, <<"all">>).

gen_upstream_name() ->
    list_to_binary("upstream-" ++ integer_to_list(next_upstream_num())).

next_upstream_num() ->
    R = get(upstream_num) + 1,
    put (upstream_num, R),
    R.

%% Make sure that even though multiple nodes are in a single
%% distributed system, we still keep all our process groups separate.
disambiguate(Rest) ->
    [Rest,
     fun (Cfgs) ->
             [rpc:call(pget(node, Cfg), application, set_env,
                       [rabbitmq_federation, pgroup_name_cluster_id, true])
              || Cfg <- Cfgs],
             Cfgs
     end].

no_plugins(Cfg) ->
    [{K, case K of
             plugins -> none;
             _       -> V
         end} || {K, V} <- Cfg].

%% "fake" cfg to let us use various utility functions when running
%% in-broker tests
single_cfg() ->
    [{nodename, 'rabbit-test'},
     {node,     rabbit_nodes:make('rabbit-test')},
     {port,     5672}].

%%----------------------------------------------------------------------------

assert_status(XorQs, Names) ->
    Links = lists:append([links(XorQ) || XorQ <- XorQs]),
    Remaining = lists:foldl(fun (Link, Status) ->
                                    assert_link_status(Link, Status, Names)
                            end, rabbit_federation_status:status(), Links),
    ?assertEqual([], Remaining),
    ok.

assert_link_status({DXorQNameBin, UpstreamName, UXorQNameBin}, Status,
                   {TypeName, UpstreamTypeName}) ->
    {This, Rest} = lists:partition(
                     fun(St) ->
                             pget(upstream, St) =:= UpstreamName andalso
                                 pget(TypeName, St) =:= DXorQNameBin andalso
                                 pget(UpstreamTypeName, St) =:= UXorQNameBin
                     end, Status),
    ?assertMatch([_], This),
    Rest.

links(#'exchange.declare'{exchange = Name}) ->
    case rabbit_policy:get(<<"federation-upstream-set">>, xr(Name)) of
        undefined -> [];
        Set       -> X = #exchange{name = xr(Name)},
                     [{Name, U#upstream.name, U#upstream.exchange_name} ||
                         U <- rabbit_federation_upstream:from_set(Set, X)]
    end;
links(#'queue.declare'{queue = Name}) ->
    case rabbit_policy:get(<<"federation-upstream-set">>, qr(Name)) of
        undefined -> [];
        Set       -> Q = #amqqueue{name = qr(Name)},
                     [{Name, U#upstream.name, U#upstream.queue_name} ||
                         U <- rabbit_federation_upstream:from_set(Set, Q)]
    end.

xr(Name) -> rabbit_misc:r(<<"/">>, exchange, Name).
qr(Name) -> rabbit_misc:r(<<"/">>, queue, Name).
