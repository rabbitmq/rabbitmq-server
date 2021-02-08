%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2011-2021 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(rabbit_runtime_parameters_acl_SUITE).

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").
-include_lib("amqp_client/include/amqp_client.hrl").

-compile(export_all).

-import(quorum_queue_utils, [wait_for_messages/2]).

all() ->
    [
      {group, cluster_size_2}
    ].

groups() ->
    [
     {cluster_size_2, [], [
                           policies_acl
                          ]}
    ].

%% -------------------------------------------------------------------
%% Testsuite setup/teardown.
%% -------------------------------------------------------------------

init_per_suite(Config) ->
    rabbit_ct_helpers:log_environment(),
    rabbit_ct_helpers:run_setup_steps(Config).

end_per_suite(Config) ->
    rabbit_ct_helpers:run_teardown_steps(Config).

init_per_group(cluster_size_2, Config) ->
    Suffix = rabbit_ct_helpers:testcase_absname(Config, "", "-"),
    Config1 = rabbit_ct_helpers:set_config(Config, [
                                                    {rmq_nodes_count, 2},
                                                    {rmq_nodename_suffix, Suffix}
      ]),
    rabbit_ct_helpers:run_steps(Config1,
      rabbit_ct_broker_helpers:setup_steps() ++
      rabbit_ct_client_helpers:setup_steps()).

end_per_group(_Group, Config) ->
    rabbit_ct_helpers:run_steps(Config,
      rabbit_ct_client_helpers:teardown_steps() ++
      rabbit_ct_broker_helpers:teardown_steps()).

init_per_testcase(Testcase, Config) ->
    rabbit_ct_client_helpers:setup_steps(),
    rabbit_ct_helpers:testcase_started(Config, Testcase).

end_per_testcase(Testcase, Config) ->
    rabbit_ct_client_helpers:teardown_steps(),
    rabbit_ct_helpers:testcase_finished(Config, Testcase).

%% -------------------------------------------------------------------
%% Test cases.
%% -------------------------------------------------------------------

policies_acl(Config) ->
    {_Conn, Ch} = rabbit_ct_client_helpers:open_connection_and_channel(Config, 0),
    Q = <<"policy_acl-queue">>,
    declare(Ch, Q),
    publish(Ch, Q, lists:seq(1, 20)),

    disallow_policy(Config, 0, <<"/">>, <<"ha-mode">>),
    ?assertEqual({error_string, "'ha-mode' policy is not allowed in vhost '/'"},
        set_ha_all_policy(Config, 0, <<"/">>, <<".*">>)),
    ?assertEqual(0, count_mirrors(Config, 0, Q, <<"/">>)),
    wait_for_messages(Config, [[Q, <<"20">>, <<"20">>, <<"0">>]]),

    DL0 = get_disallowed_list(Config, 0, <<"/">>, <<"policy">>),
    ?assertEqual(1, length(DL0)),
    ?assertEqual(true, lists:member(<<"ha-mode">>, DL0)),

    allow_policy(Config, 0, <<"/">>, <<"ha-mode">>),
    ?assertEqual(ok, set_ha_all_policy(Config, 0, <<"/">>, <<".*">>)),
    rabbit_ct_helpers:await_condition(
        fun () ->
            count_mirrors(Config, 0, Q, <<"/">>) =:= 1
        end),
    wait_for_messages(Config, [[Q, <<"20">>, <<"20">>, <<"0">>]]),

    DL1 = get_disallowed_list(Config, 0, <<"/">>, <<"policy">>),
    ?assertEqual(0, length(DL1)),
    ?assertEqual(false, lists:member(<<"ha-mode">>, DL1)),

    rabbit_ct_broker_helpers:clear_policy(Config, 0, <<".*">>),

    disallow_policy(Config, 0, <<"/">>, <<"ha-sync-mode">>),
    ?assertEqual({error_string, "'ha-sync-mode' policy is not allowed in vhost '/'"},
        set_ha_all_policy(Config, 0, <<"/">>, <<".*">>,
        [{<<"ha-sync-mode">>, <<"automatic">>}])),
    rabbit_ct_helpers:await_condition(
        fun () ->
            count_mirrors(Config, 0, Q, <<"/">>) =:= 0
        end),
    wait_for_messages(Config, [[Q, <<"20">>, <<"20">>, <<"0">>]]),

    DL2 = get_disallowed_list(Config, 0, <<"/">>, <<"policy">>),
    ?assertEqual(1, length(DL2)),
    ?assertEqual(true, lists:member(<<"ha-sync-mode">>, DL2)),

    ?assertEqual(ok, set_ha_all_policy(Config, 0, <<"/">>, <<".*">>)),
    rabbit_ct_helpers:await_condition(
        fun () ->
            count_mirrors(Config, 0, Q, <<"/">>) =:= 1
        end),
    wait_for_messages(Config, [[Q, <<"20">>, <<"20">>, <<"0">>]]),

    rabbit_ct_broker_helpers:clear_policy(Config, 0, <<".*">>),

    disallow_policy(Config, 0, <<"/">>, <<"ha-mode">>),
    ?assertEqual({error_string, "'ha-mode' policy is not allowed in vhost '/'"},
        set_ha_all_policy(Config, 0, <<"/">>, <<".*">>)),
    rabbit_ct_helpers:await_condition(
        fun () ->
            count_mirrors(Config, 0, Q, <<"/">>) =:= 0
        end),
    wait_for_messages(Config, [[Q, <<"20">>, <<"20">>, <<"0">>]]),

    DL3 = get_disallowed_list(Config, 0, <<"/">>, <<"policy">>),
    ?assertEqual(2, length(DL3)),
    ?assertEqual(true, lists:member(<<"ha-mode">>, DL3)),
    ?assertEqual(true, lists:member(<<"ha-sync-mode">>, DL3)),

    allow_policy(Config, 0, <<"/">>, <<"ha-mode">>),
    allow_policy(Config, 0, <<"/">>, <<"ha-sync-mode">>),
    ?assertEqual(ok, set_ha_all_policy(Config, 0, <<"/">>, <<".*">>,
        [{<<"ha-sync-mode">>, <<"automatic">>}])),
    rabbit_ct_helpers:await_condition(
        fun () ->
            count_mirrors(Config, 0, Q, <<"/">>, get_sync_slave_pids) =:= 1
        end),
    wait_for_messages(Config, [[Q, <<"20">>, <<"20">>, <<"0">>]]),

    DL4 = get_disallowed_list(Config, 0, <<"/">>, <<"policy">>),
    ?assertEqual(0, length(DL4)),
    ?assertEqual(false, lists:member(<<"ha-mode">>, DL4)),
    ?assertEqual(false, lists:member(<<"ha-sync-mode">>, DL4)),

    rabbit_ct_broker_helpers:clear_policy(Config, 0, <<".*">>),

    get_messages(20, Ch, Q),
    delete(Ch, Q),

    passed.

%%----------------------------------------------------------------------------


declare(Ch, Q) ->
    amqp_channel:call(Ch, #'queue.declare'{queue     = Q,
                                           durable   = true}).

delete(Ch, Q) ->
    amqp_channel:call(Ch, #'queue.delete'{queue = Q}).

publish(Ch, Q, Ps) ->
    amqp_channel:call(Ch, #'confirm.select'{}),
    [publish1(Ch, Q, P) || P <- Ps],
    amqp_channel:wait_for_confirms(Ch).

publish1(Ch, Q, P) ->
    amqp_channel:cast(Ch, #'basic.publish'{routing_key = Q},
                      #amqp_msg{props   = props(P),
                                payload = erlang:md5(term_to_binary(P))}).

publish1(Ch, Q, P, Pd) ->
    amqp_channel:cast(Ch, #'basic.publish'{routing_key = Q},
                      #amqp_msg{props   = props(P),
                                payload = Pd}).

disallow_policy(Config, NodeIndex, Vhost, Attribute) ->
    rabbit_ct_broker_helpers:rpc(Config, NodeIndex, rabbit_runtime_parameters_acl,
        disallow, [Vhost, <<"policy">>, Attribute, <<"test-user">>]).

allow_policy(Config, NodeIndex, Vhost, Attribute) ->
    rabbit_ct_broker_helpers:rpc(Config, NodeIndex, rabbit_runtime_parameters_acl,
        allow, [Vhost, <<"policy">>, Attribute, <<"test-user">>]).

count_mirrors(Config, NodeIndex, QueueName, Vhost) ->
    count_mirrors(Config, NodeIndex, QueueName, Vhost, get_slave_pids).

count_mirrors(Config, NodeIndex, QueueName, Vhost, Type) ->
    rabbit_ct_broker_helpers:rpc(Config, NodeIndex, ?MODULE, count_mirrors_remote,
        [QueueName, Vhost, Type]).

count_mirrors_remote(QueueName, Vhost, Type) ->
    {ok, Q} = rabbit_amqqueue:lookup(rabbit_misc:r(Vhost, queue, QueueName)),
    length(amqqueue:Type(Q)).

get_disallowed_list(Config, NodeIndex, Vhost, Component) ->
    rabbit_ct_broker_helpers:rpc(Config, NodeIndex, rabbit_runtime_parameters_acl,
        get_disallowed_list, [Vhost, Component]).

set_ha_all_policy(Config, NodeIndex, Vhost, Pattern) ->
    set_ha_all_policy(Config, NodeIndex, Vhost, Pattern, []).

set_ha_all_policy(Config, NodeIndex, Vhost, Pattern, Extra) ->
    rabbit_ct_broker_helpers:rpc(Config, NodeIndex, rabbit_policy, set,
        [Vhost, Pattern, Pattern, [{<<"ha-mode">>, <<"all">>}]  ++ Extra, 0,
        <<"queues">>, <<"acting-user">>]).

props(undefined) -> #'P_basic'{delivery_mode = 2};
props(P)         -> #'P_basic'{priority      = P,
                               delivery_mode = 2}.

get_empty(Ch, Q) ->
    #'basic.get_empty'{} = amqp_channel:call(Ch, #'basic.get'{queue = Q}).

get_messages(0, Ch, Q) ->
    get_empty(Ch, Q);
get_messages(Number, Ch, Q) ->
    case amqp_channel:call(Ch, #'basic.get'{queue = Q}) of
        {#'basic.get_ok'{}, _} ->
            get_messages(Number - 1, Ch, Q);
        #'basic.get_empty'{} ->
            exit(failed)
    end.

%%----------------------------------------------------------------------------
