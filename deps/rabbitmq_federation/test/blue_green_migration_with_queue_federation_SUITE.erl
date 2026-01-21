%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2026 Broadcom. All Rights Reserved. The term "Broadcom" refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
%%

-module(blue_green_migration_with_queue_federation_SUITE).

%% Simulates a blue-green deployment using queue federation to transfer messages
%% from classic queues (blue) to quorum queues (green).
%%
%% * Phase 1: Set up queue federation (green federates from blue)
%% * Phase 2: Publish to blue with federation active
%% * Phase 3: Consume federated messages from green
%% * Phase 4: Publish some "late" messages to blue
%% * Phase 5: Verify late messages arrive at green, blue queues empty

-include_lib("amqp_client/include/amqp_client.hrl").

-compile(nowarn_export_all).
-compile(export_all).

-define(BLUE_VHOSTS, [<<"blue1">>, <<"blue2">>, <<"blue3">>]).
-define(GREEN_VHOSTS, [<<"green1">>, <<"green2">>, <<"green3">>]).
-define(ALL_VHOSTS, ?BLUE_VHOSTS ++ ?GREEN_VHOSTS).

-define(QUEUE_NAME, <<"test.queue">>).
-define(INITIAL_BATCH, 50).
-define(LATE_BATCH, 20).

all() ->
    [
     {group, blue_green_migration}
    ].

groups() ->
    [
     {blue_green_migration, [], [
                                 blue_green_migration_simulation
                                ]}
    ].

suite() ->
    [{timetrap, {minutes, 5}}].

%% -------------------------------------------------------------------
%% Setup/teardown
%% -------------------------------------------------------------------

init_per_suite(Config) ->
    rabbit_ct_helpers:log_environment(),
    rabbit_ct_helpers:run_setup_steps(Config).

end_per_suite(Config) ->
    rabbit_ct_helpers:run_teardown_steps(Config).

init_per_group(blue_green_migration, Config) ->
    Suffix = rabbit_ct_helpers:testcase_absname(Config, "", "-"),
    Config1 = rabbit_ct_helpers:set_config(Config, [
        {rmq_nodename_suffix, Suffix},
        {rmq_nodes_count, 1}
    ]),
    rabbit_ct_helpers:run_steps(Config1,
        rabbit_ct_broker_helpers:setup_steps() ++
        rabbit_ct_client_helpers:setup_steps() ++
        [fun setup_vhosts/1,
         fun setup_queues/1]);
init_per_group(_, Config) ->
    Config.

end_per_group(blue_green_migration, Config) ->
    rabbit_ct_helpers:run_steps(Config,
        [fun cleanup_vhosts/1] ++
        rabbit_ct_client_helpers:teardown_steps() ++
        rabbit_ct_broker_helpers:teardown_steps());
end_per_group(_, Config) ->
    Config.

init_per_testcase(Testcase, Config) ->
    rabbit_ct_helpers:testcase_started(Config, Testcase).

end_per_testcase(Testcase, Config) ->
    rabbit_ct_helpers:testcase_finished(Config, Testcase).

%% -------------------------------------------------------------------
%% Setup helpers
%% -------------------------------------------------------------------

setup_vhosts(Config) ->
    [rabbit_ct_broker_helpers:delete_vhost(Config, VHost) || VHost <- ?ALL_VHOSTS],
    [begin
         ok = rabbit_ct_broker_helpers:add_vhost(Config, VHost),
         ok = rabbit_ct_broker_helpers:set_full_permissions(Config, <<"guest">>, VHost)
     end || VHost <- ?ALL_VHOSTS],
    Config.

setup_queues(Config) ->
    lists:foreach(
      fun(BlueVHost) ->
              Ch = open_channel(Config, BlueVHost),
              declare_classic_queue(Ch, ?QUEUE_NAME),
              close_channel_and_conn(BlueVHost)
      end, ?BLUE_VHOSTS),
    lists:foreach(
      fun(GreenVHost) ->
              Ch = open_channel(Config, GreenVHost),
              declare_quorum_queue(Ch, ?QUEUE_NAME),
              close_channel_and_conn(GreenVHost)
      end, ?GREEN_VHOSTS),
    Config.

cleanup_vhosts(Config) ->
    [catch rabbit_ct_broker_helpers:delete_vhost(Config, VHost) || VHost <- ?ALL_VHOSTS],
    Config.

%% -------------------------------------------------------------------
%% Test case
%% -------------------------------------------------------------------

blue_green_migration_simulation(Config) ->
    VHostPairs = lists:zip(?BLUE_VHOSTS, ?GREEN_VHOSTS),

    %% Green vhosts federate from their corresponding blue vhosts.
    ct:pal("Phase 1: set up federation from green to blue"),
    Uri = rabbit_ct_broker_helpers:node_uri(Config, 0),
    lists:foreach(
      fun({BlueVHost, GreenVHost}) ->
              setup_queue_federation(Config, GreenVHost, BlueVHost, Uri)
      end, VHostPairs),
    await_federation_links(Config, length(?GREEN_VHOSTS), 30000),

    %% Publish to blue while federation is active.
    ct:pal("Phase 2: publish to blue with federation active"),
    lists:foreach(
      fun(BlueVHost) ->
              Ch = open_channel(Config, BlueVHost),
              publish_with_confirms(Ch, ?QUEUE_NAME, ?INITIAL_BATCH),
              close_channel_and_conn(BlueVHost)
      end, ?BLUE_VHOSTS),

    %% Messages should be federated to green when consumers subscribe.
    ct:pal("Phase 3: consume federated messages from green"),
    lists:foreach(
      fun(GreenVHost) ->
              Ch = open_channel(Config, GreenVHost),
              consume_with_acks(Ch, ?QUEUE_NAME, ?INITIAL_BATCH, 30000),
              close_channel_and_conn(GreenVHost)
      end, ?GREEN_VHOSTS),

    %% Late messages published to blue after consumers moved to green.
    ct:pal("Phase 4: publish late messages to blue"),
    lists:foreach(
      fun(BlueVHost) ->
              Ch = open_channel(Config, BlueVHost),
              publish_with_confirms(Ch, ?QUEUE_NAME, ?LATE_BATCH),
              close_channel_and_conn(BlueVHost)
      end, ?BLUE_VHOSTS),

    %% Late messages must arrive at green via federation.
    ct:pal("Phase 5: verify late messages federated to green"),
    lists:foreach(
      fun(GreenVHost) ->
              Ch = open_channel(Config, GreenVHost),
              consume_with_acks(Ch, ?QUEUE_NAME, ?LATE_BATCH, 30000),
              close_channel_and_conn(GreenVHost)
      end, ?GREEN_VHOSTS),
    await_empty_queues(Config, ?BLUE_VHOSTS),

    ok.

%% -------------------------------------------------------------------
%% Channel helpers
%% -------------------------------------------------------------------

open_channel(Config, VHost) ->
    close_channel_and_conn(VHost),
    Conn = rabbit_ct_client_helpers:open_unmanaged_connection_direct(Config, 0, VHost),
    {ok, Ch} = amqp_connection:open_channel(Conn),
    put({conn, VHost}, Conn),
    Ch.

close_channel_and_conn(VHost) ->
    case erase({conn, VHost}) of
        undefined -> ok;
        Conn -> catch amqp_connection:close(Conn)
    end.

%% -------------------------------------------------------------------
%% Queue helpers
%% -------------------------------------------------------------------

declare_classic_queue(Ch, Name) ->
    #'queue.declare_ok'{} =
        amqp_channel:call(Ch, #'queue.declare'{
                                 queue = Name,
                                 durable = true,
                                 arguments = [{<<"x-queue-type">>, longstr, <<"classic">>}]}).

declare_quorum_queue(Ch, Name) ->
    #'queue.declare_ok'{} =
        amqp_channel:call(Ch, #'queue.declare'{
                                 queue = Name,
                                 durable = true,
                                 arguments = [{<<"x-queue-type">>, longstr, <<"quorum">>}]}).

%% -------------------------------------------------------------------
%% Publish/consume with confirms and acks
%% -------------------------------------------------------------------

publish_with_confirms(Ch, Queue, Count) ->
    #'confirm.select_ok'{} = amqp_channel:call(Ch, #'confirm.select'{}),
    [amqp_channel:cast(Ch,
                       #'basic.publish'{exchange = <<>>, routing_key = Queue},
                       #amqp_msg{payload = integer_to_binary(N),
                                 props = #'P_basic'{delivery_mode = 2}})
     || N <- lists:seq(1, Count)],
    true = amqp_channel:wait_for_confirms(Ch, 30000).

consume_with_acks(Ch, Queue, Count) ->
    consume_with_acks(Ch, Queue, Count, 60000).

consume_with_acks(Ch, Queue, Count, Timeout) ->
    #'basic.consume_ok'{consumer_tag = CTag} =
        amqp_channel:subscribe(Ch, #'basic.consume'{queue = Queue, no_ack = false}, self()),
    receive_and_ack(Ch, CTag, Count, Timeout),
    #'basic.cancel_ok'{} = amqp_channel:call(Ch, #'basic.cancel'{consumer_tag = CTag}).

receive_and_ack(_Ch, _CTag, 0, _Timeout) ->
    ok;
receive_and_ack(Ch, CTag, Remaining, Timeout) ->
    receive
        {#'basic.deliver'{consumer_tag = CTag, delivery_tag = DTag}, _Msg} ->
            amqp_channel:cast(Ch, #'basic.ack'{delivery_tag = DTag}),
            receive_and_ack(Ch, CTag, Remaining - 1, Timeout)
    after Timeout ->
            ct:fail("Timeout waiting for messages, ~p remaining", [Remaining])
    end.

%% -------------------------------------------------------------------
%% Federation helpers
%% -------------------------------------------------------------------

setup_queue_federation(Config, GreenVHost, BlueVHost, BaseUri) ->
    UpstreamName = upstream_name(BlueVHost),
    BlueUri = <<BaseUri/binary, "/", BlueVHost/binary>>,
    rabbit_ct_broker_helpers:set_parameter(
      Config, 0, GreenVHost,
      <<"federation-upstream">>, UpstreamName,
      [{<<"uri">>, BlueUri},
       {<<"queue">>, ?QUEUE_NAME},
       {<<"prefetch-count">>, 50},
       {<<"reconnect-delay">>, 1},
       {<<"ack-mode">>, <<"on-confirm">>}]),
    rabbit_ct_broker_helpers:set_policy_in_vhost(
      Config, 0, GreenVHost,
      <<"federate-from-blue">>, <<"^test\\.">>, <<"queues">>,
      [{<<"federation-upstream">>, UpstreamName}]).

upstream_name(BlueVHost) ->
    <<"upstream-", BlueVHost/binary>>.

await_federation_links(Config, ExpectedCount, Timeout) ->
    rabbit_ct_helpers:await_condition(
      fun() ->
              Status = rabbit_ct_broker_helpers:rpc(
                         Config, 0, rabbit_federation_status, status, []),
              RunningQueues = [S || S <- Status,
                                    proplists:get_value(status, S) =:= running,
                                    proplists:get_value(type, S) =:= queue],
              length(RunningQueues) >= ExpectedCount
      end, Timeout).

%% -------------------------------------------------------------------
%% Message count helpers
%% -------------------------------------------------------------------

await_empty_queues(Config, VHosts) ->
    rabbit_ct_helpers:await_condition(
      fun() ->
              lists:all(fun(VHost) ->
                                get_queue_messages(Config, VHost, ?QUEUE_NAME) =:= 0
                        end, VHosts)
      end, 30000).

get_queue_messages(Config, VHost, QName) ->
    Resource = rabbit_misc:r(VHost, queue, QName),
    case rabbit_ct_broker_helpers:rpc(Config, 0, rabbit_amqqueue, lookup, [Resource]) of
        {ok, Q} ->
            [{messages, Count}] =
                rabbit_ct_broker_helpers:rpc(Config, 0, rabbit_amqqueue, info, [Q, [messages]]),
            Count;
        {error, not_found} ->
            0
    end.
