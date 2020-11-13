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
%% Copyright (c) 2007-2017 Pivotal Software, Inc.  All rights reserved.
%%

-module(rabbit_federation_test_util).

-include("rabbit_federation.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("amqp_client/include/amqp_client.hrl").

-compile(export_all).

-import(rabbit_misc, [pget/2]).

setup_federation(Config) ->
    rabbit_ct_broker_helpers:set_parameter(Config, 0,
      <<"federation-upstream">>, <<"localhost">>, [
        {<<"uri">>, rabbit_ct_broker_helpers:node_uri(Config, 0)}]),

    rabbit_ct_broker_helpers:set_parameter(Config, 0,
      <<"federation-upstream">>, <<"local5673">>, [
        {<<"uri">>, <<"amqp://localhost:1">>}]),

    rabbit_ct_broker_helpers:set_parameter(Config, 0,
      <<"federation-upstream-set">>, <<"upstream">>, [
        [
          {<<"upstream">>, <<"localhost">>},
          {<<"exchange">>, <<"upstream">>},
          {<<"queue">>, <<"upstream">>}
        ]
      ]),

    rabbit_ct_broker_helpers:set_parameter(Config, 0,
      <<"federation-upstream-set">>, <<"upstream2">>, [
        [
          {<<"upstream">>, <<"localhost">>},
          {<<"exchange">>, <<"upstream2">>},
          {<<"queue">>, <<"upstream2">>}
        ]
      ]),

    rabbit_ct_broker_helpers:set_parameter(Config, 0,
      <<"federation-upstream-set">>, <<"localhost">>, [
        [{<<"upstream">>, <<"localhost">>}]
      ]),

    rabbit_ct_broker_helpers:set_parameter(Config, 0,
      <<"federation-upstream-set">>, <<"upstream12">>, [
        [
          {<<"upstream">>, <<"localhost">>},
          {<<"exchange">>, <<"upstream">>},
          {<<"queue">>, <<"upstream">>}
        ], [
          {<<"upstream">>, <<"localhost">>},
          {<<"exchange">>, <<"upstream2">>},
          {<<"queue">>, <<"upstream2">>}
        ]
      ]),

    rabbit_ct_broker_helpers:set_parameter(Config, 0,
      <<"federation-upstream-set">>, <<"one">>, [
        [
          {<<"upstream">>, <<"localhost">>},
          {<<"exchange">>, <<"one">>},
          {<<"queue">>, <<"one">>}
        ]
      ]),

    rabbit_ct_broker_helpers:set_parameter(Config, 0,
      <<"federation-upstream-set">>, <<"two">>, [
        [
          {<<"upstream">>, <<"localhost">>},
          {<<"exchange">>, <<"two">>},
          {<<"queue">>, <<"two">>}
        ]
      ]),

    rabbit_ct_broker_helpers:set_parameter(Config, 0,
      <<"federation-upstream-set">>, <<"upstream5673">>, [
        [
          {<<"upstream">>, <<"local5673">>},
          {<<"exchange">>, <<"upstream">>}
        ]
      ]),

    rabbit_ct_broker_helpers:set_policy(Config, 0,
      <<"fed">>, <<"^fed\.">>, <<"all">>, [
        {<<"federation-upstream-set">>, <<"upstream">>}]),

    rabbit_ct_broker_helpers:set_policy(Config, 0,
      <<"fed12">>, <<"^fed12\.">>, <<"all">>, [
        {<<"federation-upstream-set">>, <<"upstream12">>}]),

    rabbit_ct_broker_helpers:set_policy(Config, 0,
      <<"one">>, <<"^two$">>, <<"all">>, [
        {<<"federation-upstream-set">>, <<"one">>}]),

    rabbit_ct_broker_helpers:set_policy(Config, 0,
      <<"two">>, <<"^one$">>, <<"all">>, [
        {<<"federation-upstream-set">>, <<"two">>}]),

    rabbit_ct_broker_helpers:set_policy(Config, 0,
      <<"hare">>, <<"^hare\.">>, <<"all">>, [
        {<<"federation-upstream-set">>, <<"upstream5673">>}]),

    rabbit_ct_broker_helpers:set_policy(Config, 0,
      <<"all">>, <<"^all\.">>, <<"all">>, [
        {<<"federation-upstream-set">>, <<"all">>}]),

    rabbit_ct_broker_helpers:set_policy(Config, 0,
      <<"new">>, <<"^new\.">>, <<"all">>, [
        {<<"federation-upstream-set">>, <<"new-set">>}]),
    Config.

setup_down_federation(Config) ->
    rabbit_ct_broker_helpers:set_parameter(
      Config, 0, <<"federation-upstream">>, <<"broken-bunny">>,
      [{<<"uri">>, <<"amqp://broken-bunny">>},
       {<<"reconnect-delay">>, 600000}]),
    rabbit_ct_broker_helpers:set_parameter(
      Config, 0, <<"federation-upstream">>, <<"localhost">>,
      [{<<"uri">>, rabbit_ct_broker_helpers:node_uri(Config, 0)}]),
    rabbit_ct_broker_helpers:set_parameter(
      Config, 0,
      <<"federation-upstream-set">>, <<"upstream">>,
      [[{<<"upstream">>, <<"localhost">>},
        {<<"exchange">>, <<"upstream">>},
        {<<"queue">>, <<"upstream">>}],
       [{<<"upstream">>, <<"broken-bunny">>},
        {<<"exchange">>, <<"upstream">>},
        {<<"queue">>, <<"upstream">>}]]),
    rabbit_ct_broker_helpers:set_policy(
      Config, 0,
      <<"fed">>, <<"^fed\.">>, <<"all">>, [{<<"federation-upstream-set">>, <<"upstream">>}]),
    rabbit_ct_broker_helpers:set_policy(
      Config, 0,
      <<"fed">>, <<"^fed\.">>, <<"all">>, [{<<"federation-upstream-set">>, <<"upstream">>}]),
    Config.

wait_for_federation(Retries, Fun) ->
    case Fun() of
        true ->
            ok;
        false when Retries > 0 ->
            timer:sleep(1000),
            wait_for_federation(Retries - 1, Fun);
        false ->
            throw({timeout_while_waiting_for_federation, Fun})
    end.

expect(Ch, Q, Fun) when is_function(Fun) ->
    amqp_channel:subscribe(Ch, #'basic.consume'{queue  = Q,
                                                no_ack = true}, self()),
    CTag = receive
        #'basic.consume_ok'{consumer_tag = CT} -> CT
    end,
    Fun(),
    amqp_channel:call(Ch, #'basic.cancel'{consumer_tag = CTag});

expect(Ch, Q, Payloads) ->
    expect(Ch, Q, fun() -> expect(Payloads) end).

expect(Ch, Q, Payloads, Timeout) ->
    expect(Ch, Q, fun() -> expect(Payloads, Timeout) end).

expect([]) ->
    ok;
expect(Payloads) ->
    expect(Payloads, 60000).

expect(Payloads, Timeout) ->
    receive
        {#'basic.deliver'{}, #amqp_msg{payload = Payload}} ->
            case lists:member(Payload, Payloads) of
                true  -> expect(Payloads -- [Payload]);
                false -> throw({expected, Payloads, actual, Payload})
            end
    after Timeout ->
      throw({timeout, {waiting_for, Payloads}})
    end.

expect_empty(Ch, Q) ->
    ?assertMatch(#'basic.get_empty'{},
                 amqp_channel:call(Ch, #'basic.get'{ queue = Q })).

set_upstream(Config, Node, Name, URI) ->
    set_upstream(Config, Node, Name, URI, []).

set_upstream(Config, Node, Name, URI, Extra) ->
    rabbit_ct_broker_helpers:set_parameter(Config, Node,
      <<"federation-upstream">>, Name, [{<<"uri">>, URI} | Extra]).

clear_upstream(Config, Node, Name) ->
    rabbit_ct_broker_helpers:clear_parameter(Config, Node,
      <<"federation-upstream">>, Name).

set_upstream_set(Config, Node, Name, Set) ->
    rabbit_ct_broker_helpers:set_parameter(Config, Node,
      <<"federation-upstream-set">>, Name,
      [[{<<"upstream">>, UStream} | Extra] || {UStream, Extra} <- Set]).

clear_upstream_set(Config, Node, Name) ->
    rabbit_ct_broker_helpers:clear_parameter(Config, Node,
      <<"federation-upstream-set">>, Name).

set_policy(Config, Node, Name, Pattern, UpstreamSet) ->
    rabbit_ct_broker_helpers:set_policy(Config, Node,
      Name, Pattern, <<"all">>,
      [{<<"federation-upstream-set">>, UpstreamSet}]).

set_policy_pattern(Config, Node, Name, Pattern, Regex) ->
    rabbit_ct_broker_helpers:set_policy(Config, Node,
      Name, Pattern, <<"all">>,
      [{<<"federation-upstream-pattern">>, Regex}]).

clear_policy(Config, Node, Name) ->
    rabbit_ct_broker_helpers:clear_policy(Config, Node, Name).

set_policy_upstream(Config, Node, Pattern, URI, Extra) ->
    set_policy_upstreams(Config, Node, Pattern, [{URI, Extra}]).

set_policy_upstreams(Config, Node, Pattern, URIExtras) ->
    put(upstream_num, 1),
    [set_upstream(Config, Node, gen_upstream_name(), URI, Extra)
     || {URI, Extra} <- URIExtras],
    set_policy(Config, Node, Pattern, Pattern, <<"all">>).

gen_upstream_name() ->
    list_to_binary("upstream-" ++ integer_to_list(next_upstream_num())).

next_upstream_num() ->
    R = get(upstream_num) + 1,
    put(upstream_num, R),
    R.

%% Make sure that even though multiple nodes are in a single
%% distributed system, we still keep all our process groups separate.
disambiguate(Config) ->
    rabbit_ct_broker_helpers:rpc_all(Config,
      application, set_env,
      [rabbitmq_federation, pgroup_name_cluster_id, true]),
    Config.

no_plugins(Cfg) ->
    [{K, case K of
             plugins -> none;
             _       -> V
         end} || {K, V} <- Cfg].

%%----------------------------------------------------------------------------

assert_status(Config, Node, XorQs, Names) ->
    rabbit_ct_broker_helpers:rpc(Config, Node,
      ?MODULE, assert_status1, [XorQs, Names]).

assert_status1(XorQs, Names) ->
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
        undefined ->
            case rabbit_policy:get(<<"federation-upstream-pattern">>, xr(Name)) of
                undefined -> [];
                Regex       ->
                    X = #exchange{name = xr(Name)},
                    [{Name, U#upstream.name, U#upstream.exchange_name} ||
                         U <- rabbit_federation_upstream:from_pattern(Regex, X)]
            end;
        Set       -> X = #exchange{name = xr(Name)},
                     [{Name, U#upstream.name, U#upstream.exchange_name} ||
                         U <- rabbit_federation_upstream:from_set(Set, X)]
    end;
links(#'queue.declare'{queue = Name}) ->
    case rabbit_policy:get(<<"federation-upstream-set">>, qr(Name)) of
        undefined ->
          case rabbit_policy:get(<<"federation-upstream-pattern">>, qr(Name)) of
              undefined -> [];
              Regex       ->
                  Q = amqqueue:new(qr(Name),
                                   self(),
                                   false,
                                   false,
                                   none,
                                   [],
                                   undefined,
                                   #{}),
                  [{Name, U#upstream.name, U#upstream.queue_name} ||
                      U <- rabbit_federation_upstream:from_pattern(Regex, Q)]
          end;
        Set       -> Q = amqqueue:new(qr(Name),
                                      self(),
                                      false,
                                      false,
                                      none,
                                      [],
                                      undefined,
                                      #{}),
                     [{Name, U#upstream.name, U#upstream.queue_name} ||
                         U <- rabbit_federation_upstream:from_set(Set, Q)]
    end.

xr(Name) -> rabbit_misc:r(<<"/">>, exchange, Name).
qr(Name) -> rabbit_misc:r(<<"/">>, queue, Name).

with_ch(Config, Fun, Qs) ->
    Ch = rabbit_ct_client_helpers:open_channel(Config, 0),
    declare_all(Ch, Qs),
    timer:sleep(1000), %% Time for statuses to get updated
    assert_status(Config, 0,
      Qs, {queue, upstream_queue}),
    %% Clean up queues even after test failure.
    try
        Fun(Ch)
    after
        delete_all(Ch, Qs),
        rabbit_ct_client_helpers:close_channel(Ch)
    end,
    ok.

declare_all(Ch, Qs) -> [declare_queue(Ch, Q) || Q <- Qs].
delete_all(Ch, Qs) ->
    [delete_queue(Ch, Q) || #'queue.declare'{queue = Q} <- Qs].

declare_queue(Ch, Q) ->
    amqp_channel:call(Ch, Q).

delete_queue(Ch, Q) ->
    amqp_channel:call(Ch, #'queue.delete'{queue = Q}).

q(Name) ->
    #'queue.declare'{queue   = Name,
                     durable = true}.
