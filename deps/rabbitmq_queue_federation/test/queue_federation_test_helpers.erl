%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2025 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
%%

-module(queue_federation_test_helpers).

-include_lib("rabbitmq_federation_common/include/rabbit_federation.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("amqp_client/include/amqp_client.hrl").

-compile(export_all).

-import(rabbit_misc, [pget/2]).

setup_federation(Config) ->
    setup_federation_with_upstream_params(Config, []).

setup_federation_with_upstream_params(Config, ExtraParams) ->
    rabbit_ct_broker_helpers:set_parameter(Config, 0,
      <<"federation-upstream">>, <<"localhost">>, [
        {<<"uri">>, rabbit_ct_broker_helpers:node_uri(Config, 0)},
        {<<"consumer-tag">>, <<"fed.tag">>}
        ] ++ ExtraParams),

    rabbit_ct_broker_helpers:set_parameter(Config, 0,
      <<"federation-upstream">>, <<"local5673">>, [
        {<<"uri">>, <<"amqp://localhost:1">>}
        ] ++ ExtraParams),

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

    rabbit_ct_broker_helpers:rpc(
      Config, 0, rabbit_policy, set,
      [<<"/">>, <<"fed">>, <<"^fed1\.">>, [{<<"federation-upstream-set">>, <<"upstream">>}],
       0, <<"all">>, <<"acting-user">>]),

    rabbit_ct_broker_helpers:rpc(
      Config, 0, rabbit_policy, set,
      [<<"/">>, <<"fed2">>, <<"^fed2\.">>, [{<<"federation-upstream-set">>, <<"upstream2">>}],
       0, <<"all">>, <<"acting-user">>]),

    rabbit_ct_broker_helpers:rpc(
      Config, 0, rabbit_policy, set,
      [<<"/">>, <<"fed12">>, <<"^fed3\.">>, [{<<"federation-upstream-set">>, <<"upstream12">>}],
       2, <<"all">>, <<"acting-user">>]),

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
      <<"fed">>, <<"^fed1\.">>, <<"all">>, [{<<"federation-upstream-set">>, <<"upstream">>}]),
    rabbit_ct_broker_helpers:set_policy(
      Config, 0,
      <<"fed">>, <<"^fed1\.">>, <<"all">>, [{<<"federation-upstream-set">>, <<"upstream">>}]),
    Config.

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

expect([], _Timeout) ->
    ok;
expect(Payloads, Timeout) ->
    receive
        {#'basic.deliver'{delivery_tag = DTag}, #amqp_msg{payload = Payload}} ->
            case lists:member(Payload, Payloads) of
                true  ->
                    ct:pal("Consumed a message: ~tp ~tp left: ~tp", [Payload, DTag, length(Payloads) - 1]),
                    expect(Payloads -- [Payload], Timeout);
                false -> ?assert(false, rabbit_misc:format("received an unexpected payload ~tp", [Payload]))
            end
    after Timeout ->
              ct:fail("Did not receive expected payloads ~tp in time", [Payloads])
    end.

expect_empty(Ch, Q) ->
    ?assertMatch(#'basic.get_empty'{},
                 amqp_channel:call(Ch, #'basic.get'{ queue = Q })).

set_upstream(Config, Node, Name, URI) ->
    set_upstream(Config, Node, Name, URI, []).

set_upstream(Config, Node, Name, URI, Extra) ->
    rabbit_ct_broker_helpers:set_parameter(Config, Node,
      <<"federation-upstream">>, Name, [{<<"uri">>, URI} | Extra]).

set_upstream_in_vhost(Config, Node, VirtualHost, Name, URI) ->
    set_upstream_in_vhost(Config, Node, VirtualHost, Name, URI, []).

set_upstream_in_vhost(Config, Node, VirtualHost, Name, URI, Extra) ->
    rabbit_ct_broker_helpers:set_parameter(Config, Node, VirtualHost,
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

%%----------------------------------------------------------------------------

with_ch(Config, Fun, Methods) ->
    Ch = rabbit_ct_client_helpers:open_channel(Config),
    declare_all(Config, Ch, Methods),
    %% Clean up queues even after test failure.
    try
        Fun(Ch)
    after
        delete_all(Ch, Methods),
        rabbit_ct_client_helpers:close_channel(Ch)
    end,
    ok.

declare_all(Config, Ch, Methods) -> [maybe_declare_queue(Config, Ch, Op) || Op <- Methods].
delete_all(Ch, Methods) ->
    [delete_queue(Ch, Q) || #'queue.declare'{queue = Q} <- Methods].

maybe_declare_queue(Config, Ch, Method) ->
    OneOffCh = rabbit_ct_client_helpers:open_channel(Config),
    try
        amqp_channel:call(OneOffCh, Method#'queue.declare'{passive = true})
    catch exit:{{shutdown, {server_initiated_close, ?NOT_FOUND, _Message}}, _} ->
        amqp_channel:call(Ch, Method)
    after
        catch rabbit_ct_client_helpers:close_channel(OneOffCh)
    end.

delete_queue(Ch, Q) ->
    amqp_channel:call(Ch, #'queue.delete'{queue = Q}).

q(Name) ->
    q(Name, []).

q(Name, undefined) ->
    q(Name, []);
q(Name, Args) ->
    #'queue.declare'{queue   = Name,
                     durable = true,
                     arguments = Args}.

await_running_federation(Config, Links, Timeout) ->
    await_running_federation(Config, 0, Links, Timeout).

await_running_federation(Config, Node, Links, Timeout) ->
    rabbit_ct_helpers:await_condition(
      fun() ->
              Status = rabbit_ct_broker_helpers:rpc(Config, Node,
                         rabbit_federation_status, status, []),
              lists:all(
                fun({DownQ, UpQ}) ->
                        lists:any(
                          fun(Entry) ->
                                  proplists:get_value(queue, Entry) =:= DownQ andalso
                                  proplists:get_value(upstream_queue, Entry) =:= UpQ andalso
                                  proplists:get_value(status, Entry) =:= running andalso
                                  proplists:get_value(consumer_tag, Entry) =/= undefined
                          end, Status)
                end, Links)
      end, Timeout).
