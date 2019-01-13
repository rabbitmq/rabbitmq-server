%% The contents of this file are subject to the Mozilla Public License
%% Version 1.1 (the "License"); you may not use this file except in
%% compliance with the License. You may obtain a copy of the License at
%% http://www.mozilla.org/MPL/
%%
%% Software distributed under the License is distributed on an "AS IS"
%% basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See the
%% License for the specific language governing rights and limitations
%% under the License.
%%
%% The Original Code is RabbitMQ.
%%
%% The Initial Developer of the Original Code is GoPivotal, Inc.
%% Copyright (c) 2011-2019 Pivotal Software, Inc.  All rights reserved.
%%

-module(partitions_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("amqp_client/include/amqp_client.hrl").

-compile(export_all).

%% We set ticktime to 1s and setuptime is 7s so to make sure it
%% passes...
-define(DELAY, 8000).

%% We wait for 5 minutes for nodes to be running/blocked.
%% It's a lot, but still better than timetrap_timeout
-define(AWAIT_TIMEOUT, 300000).

suite() ->
    [{timetrap, 5 * 60000}].

all() ->
    [
      {group, net_ticktime_1},
      {group, net_ticktime_10}
    ].

groups() ->
    [
      {net_ticktime_1, [], [
          {cluster_size_2, [], [
              ctl_ticktime_sync,
              prompt_disconnect_detection
            ]},
          {cluster_size_3, [], [
              autoheal,
              autoheal_after_pause_if_all_down,
              autoheal_multiple_partial_partitions,
              autoheal_unexpected_finish,
              ignore,
              pause_if_all_down_on_blocked,
              pause_if_all_down_on_down,
              pause_minority_on_blocked,
              pause_minority_on_down,
              partial_false_positive,
              partial_to_full,
              partial_pause_minority,
              partial_pause_if_all_down
            ]}
        ]},
      {net_ticktime_10, [], [
          {cluster_size_2, [], [
              pause_if_all_down_false_promises_mirrored,
              pause_if_all_down_false_promises_unmirrored,
              pause_minority_false_promises_mirrored,
              pause_minority_false_promises_unmirrored
            ]}
        ]}
    ].

%% -------------------------------------------------------------------
%% Testsuite setup/teardown.
%% -------------------------------------------------------------------

init_per_suite(Config) ->
    rabbit_ct_helpers:log_environment(),
    rabbit_ct_helpers:run_setup_steps(Config, [
        fun rabbit_ct_broker_helpers:enable_dist_proxy_manager/1
      ]).

end_per_suite(Config) ->
    rabbit_ct_helpers:run_teardown_steps(Config).

init_per_group(net_ticktime_1, Config) ->
    rabbit_ct_helpers:set_config(Config, [{net_ticktime, 1}]);
init_per_group(net_ticktime_10, Config) ->
    rabbit_ct_helpers:set_config(Config, [{net_ticktime, 10}]);
init_per_group(cluster_size_2, Config) ->
    rabbit_ct_helpers:set_config(Config, [{rmq_nodes_count, 2}]);
init_per_group(cluster_size_3, Config) ->
    rabbit_ct_helpers:set_config(Config, [{rmq_nodes_count, 3}]).

end_per_group(_, Config) ->
    Config.

init_per_testcase(Testcase, Config) ->
    rabbit_ct_helpers:testcase_started(Config, Testcase),
    ClusterSize = ?config(rmq_nodes_count, Config),
    TestNumber = rabbit_ct_helpers:testcase_number(Config, ?MODULE, Testcase),
    Config1 = rabbit_ct_helpers:set_config(Config, [
        {rmq_nodes_clustered, false},
        {rmq_nodename_suffix, Testcase},
        {tcp_ports_base, {skip_n_nodes, TestNumber * ClusterSize}}
      ]),
    rabbit_ct_helpers:run_steps(Config1,
      rabbit_ct_broker_helpers:setup_steps() ++
      rabbit_ct_client_helpers:setup_steps() ++ [
        fun rabbit_ct_broker_helpers:enable_dist_proxy/1,
        fun rabbit_ct_broker_helpers:cluster_nodes/1
      ]).

end_per_testcase(Testcase, Config) ->
    Config1 = rabbit_ct_helpers:run_steps(Config,
      rabbit_ct_client_helpers:teardown_steps() ++
      rabbit_ct_broker_helpers:teardown_steps()),
    rabbit_ct_helpers:testcase_finished(Config1, Testcase).

%% -------------------------------------------------------------------
%% Testcases.
%% -------------------------------------------------------------------

ignore(Config) ->
    [A, B, C] = rabbit_ct_broker_helpers:get_node_configs(Config, nodename),
    block_unblock([{A, B}, {A, C}]),
    timer:sleep(?DELAY),
    [B, C] = partitions(A),
    [A] = partitions(B),
    [A] = partitions(C),
    ok.

pause_minority_on_down(Config) ->
    [A, B, C] = rabbit_ct_broker_helpers:get_node_configs(Config, nodename),
    set_mode(Config, pause_minority),

    true = is_running(A),

    rabbit_ct_broker_helpers:kill_node(Config, B),
    timer:sleep(?DELAY),
    true = is_running(A),

    rabbit_ct_broker_helpers:kill_node(Config, C),
    await_running(A, false),
    ok.

pause_minority_on_blocked(Config) ->
    [A, B, C] = rabbit_ct_broker_helpers:get_node_configs(Config, nodename),
    set_mode(Config, pause_minority),
    pause_on_blocked(A, B, C).

pause_if_all_down_on_down(Config) ->
    [A, B, C] = rabbit_ct_broker_helpers:get_node_configs(Config, nodename),
    set_mode(Config, {pause_if_all_down, [C], ignore}),
    [(true = is_running(N)) || N <- [A, B, C]],

    rabbit_ct_broker_helpers:kill_node(Config, B),
    timer:sleep(?DELAY),
    [(true = is_running(N)) || N <- [A, C]],

    rabbit_ct_broker_helpers:kill_node(Config, C),
    timer:sleep(?DELAY),
    await_running(A, false),
    ok.

pause_if_all_down_on_blocked(Config) ->
    [A, B, C] = rabbit_ct_broker_helpers:get_node_configs(Config, nodename),
    set_mode(Config, {pause_if_all_down, [C], ignore}),
    pause_on_blocked(A, B, C).

pause_on_blocked(A, B, C) ->
    [(true = is_running(N)) || N <- [A, B, C]],
    block([{A, B}, {A, C}]),
    await_running(A, false),
    [await_running(N, true) || N <- [B, C]],
    unblock([{A, B}, {A, C}]),
    [await_running(N, true) || N <- [A, B, C]],
    Status = rpc:call(B, rabbit_mnesia, status, []),
    [] = rabbit_misc:pget(partitions, Status),
    ok.

%%% Make sure we do not confirm any messages after a partition has
%%% happened but before we pause, since any such confirmations would be
%%% lies.
%%%
%%% This test has to use an AB cluster (not ABC) since GM ends up
%%% taking longer to detect down slaves when there are more nodes and
%%% we close the window by mistake.
%%%
%%% In general there are quite a few ways to accidentally cause this
%%% test to pass since there are a lot of things in the broker that can
%%% suddenly take several seconds to time out when TCP connections
%%% won't establish.

pause_minority_false_promises_mirrored(Config) ->
    rabbit_ct_broker_helpers:set_ha_policy(Config, 0, <<".*">>, <<"all">>),
    pause_false_promises(Config, pause_minority).

pause_minority_false_promises_unmirrored(Config) ->
    pause_false_promises(Config, pause_minority).

pause_if_all_down_false_promises_mirrored(Config) ->
    rabbit_ct_broker_helpers:set_ha_policy(Config, 0, <<".*">>, <<"all">>),
    B = rabbit_ct_broker_helpers:get_node_config(Config, 1, nodename),
    pause_false_promises(Config, {pause_if_all_down, [B], ignore}).

pause_if_all_down_false_promises_unmirrored(Config) ->
    B = rabbit_ct_broker_helpers:get_node_config(Config, 1, nodename),
    pause_false_promises(Config, {pause_if_all_down, [B], ignore}).

pause_false_promises(Config, ClusterPartitionHandling) ->
    [A, B] = rabbit_ct_broker_helpers:get_node_configs(Config, nodename),
    set_mode(Config, [A], ClusterPartitionHandling),
    ChA = rabbit_ct_client_helpers:open_channel(Config, A),
    ChB = rabbit_ct_client_helpers:open_channel(Config, B),
    amqp_channel:call(ChB, #'queue.declare'{queue   = <<"test">>,
                                            durable = true}),
    amqp_channel:call(ChA, #'confirm.select'{}),
    amqp_channel:register_confirm_handler(ChA, self()),

    %% Cause a partition after 1s
    Self = self(),
    spawn_link(fun () ->
                       timer:sleep(1000),
                       %%io:format(user, "~p BLOCK~n", [calendar:local_time()]),
                       block([{A, B}]),
                       unlink(Self)
               end),

    %% Publish large no of messages, see how many we get confirmed
    [amqp_channel:cast(ChA, #'basic.publish'{routing_key = <<"test">>},
                       #amqp_msg{props = #'P_basic'{delivery_mode = 1}}) ||
        _ <- lists:seq(1, 100000)],
    %%io:format(user, "~p finish publish~n", [calendar:local_time()]),

    %% Time for the partition to be detected. We don't put this sleep
    %% in receive_acks since otherwise we'd have another similar sleep
    %% at the end.
    timer:sleep(30000),
    Confirmed = receive_acks(0),
    %%io:format(user, "~p got acks~n", [calendar:local_time()]),
    await_running(A, false),
    %%io:format(user, "~p A stopped~n", [calendar:local_time()]),

    unblock([{A, B}]),
    await_running(A, true),

    %% But how many made it onto the rest of the cluster?
    #'queue.declare_ok'{message_count = Survived} =
        amqp_channel:call(ChB, #'queue.declare'{queue   = <<"test">>,
                                                durable = true}),
    %%io:format(user, "~p queue declared~n", [calendar:local_time()]),
    case Confirmed > Survived of
        true  -> io:format("Confirmed=~p Survived=~p~n", [Confirmed, Survived]);
        false -> ok
    end,
    true = (Confirmed =< Survived),

    rabbit_ct_client_helpers:close_channel(ChB),
    rabbit_ct_client_helpers:close_channel(ChA),
    ok.

receive_acks(Max) ->
    receive
        #'basic.ack'{delivery_tag = DTag} ->
            receive_acks(DTag)
    after ?DELAY ->
            Max
    end.

prompt_disconnect_detection(Config) ->
    [A, B] = rabbit_ct_broker_helpers:get_node_configs(Config, nodename),
    ChB = rabbit_ct_client_helpers:open_channel(Config, B),
    [amqp_channel:call(ChB, #'queue.declare'{}) || _ <- lists:seq(1, 100)],
    block([{A, B}]),
    timer:sleep(?DELAY),
    %% We want to make sure we do not end up waiting for setuptime *
    %% no of queues. Unfortunately that means we need a timeout...
    [] = rabbit_ct_broker_helpers:rpc(Config, A,
      rabbit_amqqueue, info_all, [<<"/">>], ?DELAY),
    rabbit_ct_client_helpers:close_channel(ChB),
    ok.

ctl_ticktime_sync(Config) ->
    %% Server has 1s net_ticktime, make sure ctl doesn't get disconnected
    Cmd = ["eval", "timer:sleep(5000)."],
    {ok, "ok\n"} = rabbit_ct_broker_helpers:rabbitmqctl(Config, 0, Cmd).

%% NB: we test full and partial partitions here.
autoheal(Config) ->
    set_mode(Config, autoheal),
    do_autoheal(Config).

autoheal_after_pause_if_all_down(Config) ->
    [_, B, C] = rabbit_ct_broker_helpers:get_node_configs(Config, nodename),
    set_mode(Config, {pause_if_all_down, [B, C], autoheal}),
    do_autoheal(Config).

do_autoheal(Config) ->
    [A, B, C] = rabbit_ct_broker_helpers:get_node_configs(Config, nodename),
    Test = fun (Pairs) ->
                   block_unblock(Pairs),
                   %% Sleep to make sure all the partitions are noticed
                   %% ?DELAY for the net_tick timeout
                   timer:sleep(?DELAY),
                   [await_listening(N, true) || N <- [A, B, C]],
                   [await_partitions(N, []) || N <- [A, B, C]]
           end,
    Test([{B, C}]),
    Test([{A, C}, {B, C}]),
    Test([{A, B}, {A, C}, {B, C}]),
    ok.

autoheal_multiple_partial_partitions(Config) ->
    set_mode(Config, autoheal),
    [A, B, C] = rabbit_ct_broker_helpers:get_node_configs(Config, nodename),
    block_unblock([{A, B}]),
    block_unblock([{A, C}]),
    block_unblock([{A, B}]),
    block_unblock([{A, C}]),
    block_unblock([{A, B}]),
    block_unblock([{A, C}]),
    [await_listening(N, true) || N <- [A, B, C]],
    [await_partitions(N, []) || N <- [A, B, C]],
    ok.

autoheal_unexpected_finish(Config) ->
    set_mode(Config, autoheal),
    [A, B, _C] = rabbit_ct_broker_helpers:get_node_configs(Config, nodename),
    Pid = rpc:call(A, erlang, whereis, [rabbit_node_monitor]),
    Pid ! {autoheal_msg, {autoheal_finished, B}},
    Pid = rpc:call(A, erlang, whereis, [rabbit_node_monitor]),
    ok.

partial_false_positive(Config) ->
    [A, B, C] = rabbit_ct_broker_helpers:get_node_configs(Config, nodename),
    suspend_node_monitor(Config, C),
    block([{A, B}]),
    timer:sleep(1000),
    block([{A, C}]),
    timer:sleep(?DELAY),
    resume_node_monitor(Config, C),
    timer:sleep(?DELAY),
    unblock([{A, B}, {A, C}]),
    timer:sleep(?DELAY),
    %% When B times out A's connection, it will check with C. C will
    %% not have timed out A yet, but already it can't talk to it. We
    %% need to not consider this a partial partition; B and C should
    %% still talk to each other.
    %%
    %% Because there is a chance that C can still talk to A when B
    %% requests to check for a partial partition, we suspend C's
    %% rabbit_node_monitor at the beginning and resume it after the
    %% link between A and C is blocked. This way, when B asks C about
    %% A, we make sure that the A<->C link is blocked before C's
    %% rabbit_node_monitor processes B's request.
    [B, C] = partitions(A),
    [A] = partitions(B),
    [A] = partitions(C),
    ok.

partial_to_full(Config) ->
    [A, B, C] = rabbit_ct_broker_helpers:get_node_configs(Config, nodename),
    block_unblock([{A, B}]),
    timer:sleep(?DELAY),
    %% There are several valid ways this could go, depending on how
    %% the DOWN messages race: either A gets disconnected first and BC
    %% stay together, or B gets disconnected first and AC stay
    %% together, or both make it through and all three get
    %% disconnected.
    case {partitions(A), partitions(B), partitions(C)} of
        {[B, C], [A],    [A]}    -> ok;
        {[B],    [A, C], [B]}    -> ok;
        {[B, C], [A, C], [A, B]} -> ok;
        Partitions               -> exit({partitions, Partitions})
    end.

partial_pause_minority(Config) ->
    [A, B, C] = rabbit_ct_broker_helpers:get_node_configs(Config, nodename),
    set_mode(Config, pause_minority),
    %% We suspend rabbit_node_monitor on C while we block the link
    %% between A and B. This should make sure C's rabbit_node_monitor
    %% processes both partial partition checks from A and B at about
    %% the same time, and thus increase the chance both A and B decides
    %% there is a partial partition.
    %%
    %% Without this, one node may see the partial partition and stop,
    %% before the other node sees it. In this case, the other node
    %% doesn't stop and this testcase fails.
    suspend_node_monitor(Config, C),
    block([{A, B}]),
    timer:sleep(?DELAY),
    resume_node_monitor(Config, C),
    [await_running(N, false) || N <- [A, B]],
    await_running(C, true),
    unblock([{A, B}]),
    [await_listening(N, true) || N <- [A, B, C]],
    [await_partitions(N, []) || N <- [A, B, C]],
    ok.

partial_pause_if_all_down(Config) ->
    [A, B, C] = rabbit_ct_broker_helpers:get_node_configs(Config, nodename),
    set_mode(Config, {pause_if_all_down, [B], ignore}),
    block([{A, B}]),
    await_running(A, false),
    [await_running(N, true) || N <- [B, C]],
    unblock([{A, B}]),
    [await_listening(N, true) || N <- [A, B, C]],
    [await_partitions(N, []) || N <- [A, B, C]],
    ok.

set_mode(Config, Mode) ->
    rabbit_ct_broker_helpers:set_partition_handling_mode_globally(Config, Mode).

set_mode(Config, Nodes, Mode) ->
    rabbit_ct_broker_helpers:set_partition_handling_mode(Config, Nodes, Mode).

suspend_node_monitor(Config, Node) ->
    rabbit_ct_broker_helpers:rpc(
      Config, Node, ?MODULE, suspend_or_resume_node_monitor, [suspend]).

resume_node_monitor(Config, Node) ->
    rabbit_ct_broker_helpers:rpc(
      Config, Node, ?MODULE, suspend_or_resume_node_monitor, [resume]).

suspend_or_resume_node_monitor(SuspendOrResume) ->
    Action = case SuspendOrResume of
                 suspend -> "Suspending";
                 resume  -> "Resuming"
             end,
    rabbit_log:info("(~s) ~s node monitor~n", [?MODULE, Action]),
    ok = sys:SuspendOrResume(rabbit_node_monitor).

block_unblock(Pairs) ->
    block(Pairs),
    timer:sleep(?DELAY),
    unblock(Pairs).

block(Pairs)   -> [block(X, Y) || {X, Y} <- Pairs].
unblock(Pairs) -> [allow(X, Y) || {X, Y} <- Pairs].

partitions(Node) ->
    case rpc:call(Node, rabbit_node_monitor, partitions, []) of
        {badrpc, {'EXIT', E}} = R -> case rabbit_misc:is_abnormal_exit(E) of
                                         true  -> R;
                                         false -> timer:sleep(1000),
                                                  partitions(Node)
                                     end;
        Partitions                -> Partitions
    end.

block(X, Y) ->
    rabbit_ct_broker_helpers:block_traffic_between(X, Y).

allow(X, Y) ->
    rabbit_ct_broker_helpers:allow_traffic_between(X, Y).

await_running   (Node, Bool)  -> await(Node, Bool,  fun is_running/1,   ?AWAIT_TIMEOUT).
await_listening (Node, Bool)  -> await(Node, Bool,  fun is_listening/1, ?AWAIT_TIMEOUT).
await_partitions(Node, Parts) -> await(Node, Parts, fun partitions/1,   ?AWAIT_TIMEOUT).

await(Node, Res, Fun, Timeout) when Timeout =< 0 ->
    error({await_timeout, Node, Res, Fun});
await(Node, Res, Fun, Timeout) ->
    case Fun(Node) of
        Res -> ok;
        _   -> timer:sleep(100),
               await(Node, Res, Fun, Timeout - 100)
    end.

is_running(Node) -> rpc:call(Node, rabbit, is_running, []).

is_listening(Node) ->
    case rpc:call(Node, rabbit_networking, node_listeners, [Node]) of
        []    -> false;
        [_|_] -> true;
        _     -> false
    end.
