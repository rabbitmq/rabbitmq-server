%% The contents of this file are subject to the Mozilla Public License
%% Version 2.0 (the "License"); you may not use this file except in
%% compliance with the License. You may obtain a copy of the License
%% at https://www.mozilla.org/en-US/MPL/2.0/
%%
%% Software distributed under the License is distributed on an "AS IS"
%% basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See
%% the License for the specific language governing rights and
%% limitations under the License.
%%
%% The Original Code is RabbitMQ.
%%
%% The Initial Developer of the Original Code is Pivotal Software, Inc.
%% Copyright (c) 2020-2025 Broadcom. All Rights Reserved.
%% The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
%%

-module(rabbit_stream_partitions_SUITE).

-include_lib("eunit/include/eunit.hrl").
-include_lib("rabbitmq_stream_common/include/rabbit_stream.hrl").
-include_lib("rabbit/src/rabbit_stream_sac_coordinator.hrl").

-compile(nowarn_export_all).
-compile(export_all).

-define(TRSPT, gen_tcp).
-define(CORR_ID, 1).
-define(SAC_STATE, rabbit_stream_sac_coordinator).

all() ->
    [{group, cluster}].

groups() ->
    [{cluster, [],
      [simple_sac_consumer_should_get_disconnected_on_partition]}
    ].

init_per_suite(Config) ->
    case rabbit_ct_helpers:is_mixed_versions() of
        true ->
            {skip, "mixed version clusters are not supported"};
        _ ->
            rabbit_ct_helpers:log_environment(),
            Config
    end.

end_per_suite(Config) ->
    Config.

init_per_group(Group, Config) ->
    Config1 = rabbit_ct_helpers:set_config(
                Config, [{rmq_nodes_clustered, true},
                         {rmq_nodes_count, 3},
                         {rmq_nodename_suffix, Group},
                         {tcp_ports_base}
                        ]),
    rabbit_ct_helpers:run_setup_steps(
      Config1,
      [fun rabbit_ct_broker_helpers:configure_dist_proxy/1,
       fun(StepConfig) ->
               rabbit_ct_helpers:merge_app_env(StepConfig,
                                               {aten,
                                                [{poll_interval,
                                                  1000}]})
       end,
       fun(StepConfig) ->
               rabbit_ct_helpers:merge_app_env(StepConfig,
                                               {rabbit,
                                                [{stream_sac_disconnected_timeout,
                                                  2000}]})
       end]
      ++ rabbit_ct_broker_helpers:setup_steps()).

end_per_group(_, Config) ->
    rabbit_ct_helpers:run_steps(Config,
                                rabbit_ct_broker_helpers:teardown_steps()).

init_per_testcase(TestCase, Config) ->
    rabbit_ct_helpers:testcase_started(Config, TestCase).

end_per_testcase(TestCase, Config) ->
    rabbit_ct_helpers:testcase_finished(Config, TestCase).

simple_sac_consumer_should_get_disconnected_on_partition(Config) ->
    S = rabbit_data_coercion:to_binary(?FUNCTION_NAME),
    {ok, So0, C0_00} = stream_test_utils:connect(Config, 0),
    {ok, So1, C1_00} = stream_test_utils:connect(Config, 1),
    {ok, So2, C2_00} = stream_test_utils:connect(Config, 2),

    create_stream(Config, S),
    wait_for_members(So0, C0_00, S, 3),

    C0_01 = register_sac(So0, C0_00, S, 0),
    C0_02 = receive_consumer_update(So0, C0_01),

    C1_01 = register_sac(So1, C1_00, S, 1),
    C2_01 = register_sac(So2, C2_00, S, 2),
    SubIdToState0 = #{0 => {So0, C0_02},
                      1 => {So1, C1_01},
                      2 => {So2, C2_01}},

    Members = stream_members(Config, S),
    L = leader(Members),
    [F1, F2] = followers(Members),

    Consumers1 = query_consumers(Config, S),
    assertSize(3, Consumers1),
    assertConsumersConnected(Consumers1),

    Isolated = F1,
    {value, DisconnectedConsumer} =
        lists:search(fun(#consumer{pid = ConnPid}) ->
                             rpc(Config, erlang, node, [ConnPid]) =:= Isolated
                     end, Consumers1),
    #consumer{subscription_id = DiscSubId} = DisconnectedConsumer,

    rabbit_ct_broker_helpers:block_traffic_between(Isolated, L),
    rabbit_ct_broker_helpers:block_traffic_between(Isolated, F2),

    wait_for_disconnected_consumer(Config, S),
    wait_for_forgotten_consumer(Config, S),

    rabbit_ct_broker_helpers:allow_traffic_between(Isolated, L),
    rabbit_ct_broker_helpers:allow_traffic_between(Isolated, F2),

    wait_for_all_consumers_connected(Config, S),

    Consumers2 = query_consumers(Config, S),
    %% the disconnected, then forgotten consumer is cancelled,
    %% because the stream member on its node has been restarted
    assertSize(2, Consumers2),
    assertConsumersConnected(Consumers2),
    ?assertMatch([DisconnectedConsumer],
                 Consumers1 -- Consumers2),

    %% assert the cancelled consumer received a metadata update frame
    SubIdToState1 =
        maps:fold(fun(K, {S0, C0}, Acc) when K == DiscSubId ->
                          C1 = receive_metadata_update(S0, C0),
                          Acc#{K => {S0, C1}};
                     (K, {S0, C0}, Acc) ->
                          Acc#{K => {S0, C0}}
                  end, #{}, SubIdToState0),

    delete_stream(Config, S),

    %% online consumers should receive a metadata update frame (stream deleted)
    %% we unqueue the this frame before closing the connection
    %% directly closing the connection of the cancelled consumer
    maps:foreach(fun(K, {S0, C0}) when K /= DiscSubId ->
                         {_, C1} = receive_commands(S0, C0),
                         {ok, _} = stream_test_utils:close(S0, C1);
                    (_, {S0, C0}) ->
                         {ok, _} = stream_test_utils:close(S0, C0)
                 end, SubIdToState1),

    ok.

leader(Members) ->
    maps:fold(fun(Node, {_, writer}, _Acc) ->
                      Node;
                 (_, _, Acc) ->
                      Acc
              end, undefined, Members).

followers(Members) ->
    maps:fold(fun(Node, {_, replica}, Acc) ->
                      [Node | Acc];
                 (_, _, Acc) ->
                      Acc
              end, [], Members).

stream_members(Config, Stream) ->
    {ok, Q} = rpc(Config, rabbit_amqqueue, lookup, [Stream, <<"/">>]),
    #{name := StreamId} = amqqueue:get_type_state(Q),
    State = rpc(Config, rabbit_stream_coordinator, state, []),
    {ok, Members} = rpc(Config, rabbit_stream_coordinator, query_members,
                        [StreamId, State]),
    Members.

create_stream(Config, St) ->
    {ok, S, C0} = stream_test_utils:connect(Config, 0),
    {ok, C1} = stream_test_utils:create_stream(S, C0, St),
    {ok, _} = stream_test_utils:close(S, C1).

delete_stream(Config, St) ->
    {ok, S, C0} = stream_test_utils:connect(Config, 0),
    {ok, C1} = stream_test_utils:delete_stream(S, C0, St),
    {ok, _} = stream_test_utils:close(S, C1).

register_sac(S, C0, St, SubId) ->
    SacSubscribeFrame = request({subscribe, SubId, St,
                                 first, 1,
                                 #{<<"single-active-consumer">> => <<"true">>,
                                   <<"name">> => name()}}),
    T = ?TRSPT,
    ok = T:send(S, SacSubscribeFrame),
    {Cmd1, C1} = receive_commands(T, S, C0),
    ?assertMatch({response, ?CORR_ID, {subscribe, ?RESPONSE_CODE_OK}},
                 Cmd1),
    C1.

receive_consumer_update(S, C0) ->
    {Cmd, C1} = receive_commands(S, C0),
    ?assertMatch({request, _CorrId, {consumer_update, _SubId, _Status}},
                 Cmd),
    C1.

receive_metadata_update(S, C0) ->
    {Cmd, C1} = receive_commands(S, C0),
    ?assertMatch({metadata_update, _, ?RESPONSE_CODE_STREAM_NOT_AVAILABLE},
                 Cmd),
    C1.

unsubscribe(S, C0) ->
    {ok, C1} = stream_test_utils:unsubscribe(S, C0, sub_id()),
    C1.

query_consumers(Config, Stream) ->
    Key = group_key(Stream),
    #?SAC_STATE{groups = #{Key := #group{consumers = Consumers}}} =
    rpc(Config, rabbit_stream_coordinator, sac_state, []),
    Consumers.

coordinator_state(Config) ->
    rpc(Config, rabbit_stream_coordinator, state, []).

rpc(Config, M, F, A) ->
    rabbit_ct_broker_helpers:rpc(Config, 0, M, F, A).

group_key(Stream) ->
    {<<"/">>, Stream, name()}.

request(Cmd) ->
    request(?CORR_ID, Cmd).

request(CorrId, Cmd) ->
    rabbit_stream_core:frame({request, CorrId, Cmd}).

receive_commands(S, C) ->
    receive_commands(?TRSPT, S, C).

receive_commands(Transport, S, C) ->
   stream_test_utils:receive_stream_commands(Transport, S, C).

sub_id() ->
    0.

name() ->
    <<"app">>.

wait_for_members(S, C, St, ExpectedCount) ->
    T = ?TRSPT,
    GetStreamNodes =
        fun() ->
           MetadataFrame = request({metadata, [St]}),
           ok = gen_tcp:send(S, MetadataFrame),
           {CmdMetadata, _} = receive_commands(T, S, C),
           {response, 1,
            {metadata, _Nodes, #{St := {Leader = {_H, _P}, Replicas}}}} =
               CmdMetadata,
           [Leader | Replicas]
        end,
    rabbit_ct_helpers:await_condition(fun() ->
                                         length(GetStreamNodes()) == ExpectedCount
                                      end).

wait_for_disconnected_consumer(Config, Stream) ->
    rabbit_ct_helpers:await_condition(
      fun() ->
              Cs = query_consumers(Config, Stream),
              lists:any(fun(#consumer{status = {disconnected, _}}) ->
                                true;
                           (_) ->
                                false
                        end, Cs)
      end).

wait_for_forgotten_consumer(Config, Stream) ->
    rabbit_ct_helpers:await_condition(
      fun() ->
              Cs = query_consumers(Config, Stream),
              lists:any(fun(#consumer{status = {forgotten, _}}) ->
                                true;
                           (_) ->
                                false
                        end, Cs)
      end).

wait_for_all_consumers_connected(Config, Stream) ->
    rabbit_ct_helpers:await_condition(
      fun() ->
              Cs = query_consumers(Config, Stream),
              lists:all(fun(#consumer{status = {connected, _}}) ->
                                true;
                           (_) ->
                                false
                        end, Cs)
      end).


assertConsumersConnected(Consumers) when length(Consumers) > 0 ->
    lists:foreach(fun(#consumer{status = St}) ->
                          ?assertMatch({connected, _}, St,
                                       "Consumer should be connected")
                  end, Consumers);
assertConsumersConnected(_) ->
    ?assert(false, "The consumer list is empty").


assertSize(Expected, []) ->
    ?assertEqual(Expected, 0);
assertSize(Expected, Map) when is_map(Map) ->
    ?assertEqual(Expected, maps:size(Map));
assertSize(Expected, List) when is_list(List) ->
    ?assertEqual(Expected, length(List)).

assertEmpty(Data) ->
    assertSize(0, Data).

