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
%% Copyright (c) 2007-2019 Pivotal Software, Inc.  All rights reserved.
%%

-module(eager_sync_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("amqp_client/include/amqp_client.hrl").

-compile(export_all).

-define(QNAME, <<"ha.two.test">>).
-define(QNAME_AUTO, <<"ha.auto.test">>).
-define(MESSAGE_COUNT, 200000).

all() ->
    [
      {group, non_parallel_tests}
    ].

groups() ->
    [
      {non_parallel_tests, [], [
          eager_sync,
          eager_sync_cancel,
          eager_sync_auto,
          eager_sync_auto_on_policy_change,
          eager_sync_requeue
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

init_per_group(_, Config) ->
    Config.

end_per_group(_, Config) ->
    Config.

init_per_testcase(Testcase, Config) ->
    rabbit_ct_helpers:testcase_started(Config, Testcase),
    ClusterSize = 3,
    TestNumber = rabbit_ct_helpers:testcase_number(Config, ?MODULE, Testcase),
    Config1 = rabbit_ct_helpers:set_config(Config, [
        {rmq_nodes_count, ClusterSize},
        {rmq_nodes_clustered, true},
        {rmq_nodename_suffix, Testcase},
        {tcp_ports_base, {skip_n_nodes, TestNumber * ClusterSize}}
      ]),
    rabbit_ct_helpers:run_steps(Config1,
      rabbit_ct_broker_helpers:setup_steps() ++
      rabbit_ct_client_helpers:setup_steps() ++ [
        fun rabbit_ct_broker_helpers:set_ha_policy_two_pos/1,
        fun rabbit_ct_broker_helpers:set_ha_policy_two_pos_batch_sync/1
      ]).

end_per_testcase(Testcase, Config) ->
    Config1 = rabbit_ct_helpers:run_steps(Config,
      rabbit_ct_client_helpers:teardown_steps() ++
      rabbit_ct_broker_helpers:teardown_steps()),
    rabbit_ct_helpers:testcase_finished(Config1, Testcase).

%% -------------------------------------------------------------------
%% Testcases.
%% -------------------------------------------------------------------

eager_sync(Config) ->
    [A, B, C] = rabbit_ct_broker_helpers:get_node_configs(Config, nodename),
    %% Queue is on AB but not C.
    ACh = rabbit_ct_client_helpers:open_channel(Config, A),
    Ch = rabbit_ct_client_helpers:open_channel(Config, C),
    amqp_channel:call(ACh, #'queue.declare'{queue   = ?QNAME,
                                            durable = true}),

    %% Don't sync, lose messages
    rabbit_ct_client_helpers:publish(Ch, ?QNAME, ?MESSAGE_COUNT),
    restart(Config, A),
    restart(Config, B),
    rabbit_ct_client_helpers:consume(Ch, ?QNAME, 0),

    %% Sync, keep messages
    rabbit_ct_client_helpers:publish(Ch, ?QNAME, ?MESSAGE_COUNT),
    restart(Config, A),
    ok = sync(C, ?QNAME),
    restart(Config, B),
    rabbit_ct_client_helpers:consume(Ch, ?QNAME, ?MESSAGE_COUNT),

    %% Check the no-need-to-sync path
    rabbit_ct_client_helpers:publish(Ch, ?QNAME, ?MESSAGE_COUNT),
    ok = sync(C, ?QNAME),
    rabbit_ct_client_helpers:consume(Ch, ?QNAME, ?MESSAGE_COUNT),

    %% keep unacknowledged messages
    rabbit_ct_client_helpers:publish(Ch, ?QNAME, ?MESSAGE_COUNT),
    rabbit_ct_client_helpers:fetch(Ch, ?QNAME, 2),
    restart(Config, A),
    rabbit_ct_client_helpers:fetch(Ch, ?QNAME, 3),
    sync(C, ?QNAME),
    restart(Config, B),
    rabbit_ct_client_helpers:consume(Ch, ?QNAME, ?MESSAGE_COUNT),

    ok.

eager_sync_cancel(Config) ->
    [A, B, C] = rabbit_ct_broker_helpers:get_node_configs(Config, nodename),
    %% Queue is on AB but not C.
    ACh = rabbit_ct_client_helpers:open_channel(Config, A),
    Ch = rabbit_ct_client_helpers:open_channel(Config, C),

    set_app_sync_batch_size(A),
    set_app_sync_batch_size(B),
    set_app_sync_batch_size(C),

    amqp_channel:call(ACh, #'queue.declare'{queue   = ?QNAME,
                                            durable = true}),
    {ok, not_syncing} = sync_cancel(C, ?QNAME), %% Idempotence
    eager_sync_cancel_test2(Config, A, B, C, Ch, 100).

eager_sync_cancel_test2(_, _, _, _, _, 0) ->
    error(no_more_attempts_left);
eager_sync_cancel_test2(Config, A, B, C, Ch, Attempts) ->
    %% Sync then cancel
    rabbit_ct_client_helpers:publish(Ch, ?QNAME, ?MESSAGE_COUNT),
    restart(Config, A),
    set_app_sync_batch_size(A),
    spawn_link(fun() -> ok = sync_nowait(C, ?QNAME) end),
    case wait_for_syncing(C, ?QNAME, 1) of
        ok ->
            case sync_cancel(C, ?QNAME) of
                ok ->
                    wait_for_running(C, ?QNAME),
                    restart(Config, B),
                    set_app_sync_batch_size(B),
                    rabbit_ct_client_helpers:consume(Ch, ?QNAME, 0),

                    {ok, not_syncing} = sync_cancel(C, ?QNAME), %% Idempotence
                    ok;
                {ok, not_syncing} ->
                    %% Damn. Syncing finished between wait_for_syncing/3 and
                    %% sync_cancel/2 above. Start again.
                    amqp_channel:call(Ch, #'queue.purge'{queue = ?QNAME}),
                    eager_sync_cancel_test2(Config, A, B, C, Ch, Attempts - 1)
            end;
        synced_already ->
            %% Damn. Syncing finished before wait_for_syncing/3. Start again.
            amqp_channel:call(Ch, #'queue.purge'{queue = ?QNAME}),
            eager_sync_cancel_test2(Config, A, B, C, Ch, Attempts - 1)
    end.

eager_sync_auto(Config) ->
    [A, B, C] = rabbit_ct_broker_helpers:get_node_configs(Config, nodename),
    ACh = rabbit_ct_client_helpers:open_channel(Config, A),
    Ch = rabbit_ct_client_helpers:open_channel(Config, C),
    amqp_channel:call(ACh, #'queue.declare'{queue   = ?QNAME_AUTO,
                                            durable = true}),

    %% Sync automatically, don't lose messages
    rabbit_ct_client_helpers:publish(Ch, ?QNAME_AUTO, ?MESSAGE_COUNT),
    restart(Config, A),
    wait_for_sync(C, ?QNAME_AUTO),
    restart(Config, B),
    wait_for_sync(C, ?QNAME_AUTO),
    rabbit_ct_client_helpers:consume(Ch, ?QNAME_AUTO, ?MESSAGE_COUNT),

    ok.

eager_sync_auto_on_policy_change(Config) ->
    [A, B, C] = rabbit_ct_broker_helpers:get_node_configs(Config, nodename),
    %% Queue is on AB but not C.
    ACh = rabbit_ct_client_helpers:open_channel(Config, A),
    Ch = rabbit_ct_client_helpers:open_channel(Config, C),
    amqp_channel:call(ACh, #'queue.declare'{queue   = ?QNAME,
                                            durable = true}),

    %% Sync automatically once the policy is changed to tell us to.
    rabbit_ct_client_helpers:publish(Ch, ?QNAME, ?MESSAGE_COUNT),
    restart(Config, A),
    Params = [rabbit_misc:atom_to_binary(N) || N <- [A, B]],
    rabbit_ct_broker_helpers:set_ha_policy(Config,
      A, <<"^ha.two.">>, {<<"nodes">>, Params},
      [{<<"ha-sync-mode">>, <<"automatic">>}]),
    wait_for_sync(C, ?QNAME),

    ok.

eager_sync_requeue(Config) ->
    [A, B, C] = rabbit_ct_broker_helpers:get_node_configs(Config, nodename),
    %% Queue is on AB but not C.
    ACh = rabbit_ct_client_helpers:open_channel(Config, A),
    Ch = rabbit_ct_client_helpers:open_channel(Config, C),
    amqp_channel:call(ACh, #'queue.declare'{queue   = ?QNAME,
                                            durable = true}),

    rabbit_ct_client_helpers:publish(Ch, ?QNAME, 2),
    {#'basic.get_ok'{delivery_tag = TagA}, _} =
        amqp_channel:call(Ch, #'basic.get'{queue = ?QNAME}),
    {#'basic.get_ok'{delivery_tag = TagB}, _} =
        amqp_channel:call(Ch, #'basic.get'{queue = ?QNAME}),
    amqp_channel:cast(Ch, #'basic.reject'{delivery_tag = TagA, requeue = true}),
    restart(Config, B),
    ok = sync(C, ?QNAME),
    amqp_channel:cast(Ch, #'basic.reject'{delivery_tag = TagB, requeue = true}),
    rabbit_ct_client_helpers:consume(Ch, ?QNAME, 2),

    ok.

restart(Config, Node) ->
    rabbit_ct_broker_helpers:restart_broker(Config, Node).

sync(Node, QName) ->
    case sync_nowait(Node, QName) of
        ok -> wait_for_sync(Node, QName),
              ok;
        R  -> R
    end.

sync_nowait(Node, QName) -> action(Node, sync_queue, QName).
sync_cancel(Node, QName) -> action(Node, cancel_sync_queue, QName).

wait_for_sync(Node, QName) ->
    sync_detection_SUITE:wait_for_sync_status(true, Node, QName).

action(Node, Action, QName) ->
    rabbit_control_helper:command_with_output(
        Action, Node, [binary_to_list(QName)], [{"-p", "/"}]).

queue(Node, QName) ->
    QNameRes = rabbit_misc:r(<<"/">>, queue, QName),
    {ok, Q} = rpc:call(Node, rabbit_amqqueue, lookup, [QNameRes]),
    Q.

wait_for_syncing(Node, QName, Target) ->
    case state(Node, QName) of
        {{syncing, _}, _} -> ok;
        {running, Target} -> synced_already;
        _                 -> timer:sleep(100),
                             wait_for_syncing(Node, QName, Target)
    end.

wait_for_running(Node, QName) ->
    case state(Node, QName) of
        {running, _} -> ok;
        _            -> timer:sleep(100),
                        wait_for_running(Node, QName)
    end.

state(Node, QName) ->
    [{state, State}, {synchronised_slave_pids, Pids}] =
        rpc:call(Node, rabbit_amqqueue, info,
                 [queue(Node, QName), [state, synchronised_slave_pids]]),
    {State, length(Pids)}.

%% eager_sync_cancel_test needs a batch size that's < ?MESSAGE_COUNT
%% in order to pass, because a SyncBatchSize >= ?MESSAGE_COUNT will
%% always finish before the test is able to cancel the sync.
set_app_sync_batch_size(Node) ->
    rabbit_control_helper:command(
      eval, Node,
      ["application:set_env(rabbit, mirroring_sync_batch_size, 1)."]).
