%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2026 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
%%
-module(retainer_SUITE).
-compile([export_all, nowarn_export_all]).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("rabbitmq_ct_helpers/include/rabbit_assert.hrl").
-import(util, [connect/2, connect/3,
               expect_publishes/3,
               assert_message_expiry_interval/2
              ]).

all() ->
    [
     {group, v4},
     {group, v5},
     {group, cluster}
    ].

groups() ->
    [
     {v4, [], sub_groups()},
     {v5, [], sub_groups()},
     {cluster, [], [{dets, [], cluster_tests()},
                    {ets, [], cluster_tests()}]}
    ].

cluster_tests() ->
    [delete_vhost,
     late_cleanup_after_recreate,
     recreate_vhost_after_failed_cleanup,
     delete_vhost_without_plugin].

sub_groups() ->
    [
     {dets, [shuffle], tests() ++ [recover_after_migration_cleanup]},
     {ets, [shuffle], tests()},
     {noop, [shuffle], [does_not_retain]}
    ].

tests() ->
    [
     coerce_configuration_data,
     should_translate_amqp2mqtt_on_publish,
     should_translate_amqp2mqtt_on_retention,
     should_translate_amqp2mqtt_on_retention_search,
     recover,
     delete_vhost_long_name,
     delete_orphaned_storage,
     recover_with_message_expiry_interval,
     limit_max_messages,
     limit_max_size_bytes,
     limit_max_size_bytes_with_properties
    ].

suite() ->
    [{timetrap, {minutes, 2}}].

%% -------------------------------------------------------------------
%% Testsuite setup/teardown.
%% -------------------------------------------------------------------

init_per_suite(Config) ->
    rabbit_ct_helpers:log_environment(),
    Config.

end_per_suite(Config) ->
    Config.

init_per_group(cluster, Config) ->
    case rabbit_ct_helpers:is_mixed_versions() of
        true ->
            {skip, "nodes of the older version do not delete retained message stores"};
        false ->
            rabbit_ct_helpers:set_config(Config, [{rmq_nodes_count, 3}, {mqtt_version, v4}])
    end;
init_per_group(G, Config)
  when G =:= v4;
       G =:= v5 ->
    rabbit_ct_helpers:set_config(Config, {mqtt_version, G});
init_per_group(Group, Config0) ->
    Suffix = rabbit_ct_helpers:testcase_absname(Config0, "", "-"),
    Config = rabbit_ct_helpers:set_config(
               Config0, [{rmq_nodename_suffix, Suffix},
                         {start_rmq_with_plugins_disabled, true}
                        ]),
    Mod = list_to_atom("rabbit_mqtt_retained_msg_store_" ++ atom_to_list(Group)),
    Env = [{rabbitmq_mqtt, [{retained_message_store, Mod}]},
           {rabbit, [
                     {default_user, "guest"},
                     {default_pass, "guest"},
                     {default_vhost, "/"},
                     {default_permissions, [".*", ".*", ".*"]}
                    ]}],
    Config1 = rabbit_ct_helpers:run_setup_steps(
                Config,
                [fun(Conf) -> rabbit_ct_helpers:merge_app_env(Conf, Env) end] ++
                    rabbit_ct_broker_helpers:setup_steps() ++
                    rabbit_ct_client_helpers:setup_steps()),
    util:enable_plugin(Config1, rabbitmq_mqtt),
    Config1.

end_per_group(G, Config)
  when G =:= v4;
       G =:= v5;
       G =:= cluster ->
    Config;
end_per_group(_, Config) ->
    rabbit_ct_helpers:run_teardown_steps(
      Config,
      rabbit_ct_client_helpers:teardown_steps() ++
      rabbit_ct_broker_helpers:teardown_steps()).

init_per_testcase(recover_with_message_expiry_interval = T, Config) ->
    case ?config(mqtt_version, Config) of
        v4 ->
            {skip, "Message Expiry Interval not supported in MQTT v4"};
        v5 ->
            rabbit_ct_helpers:testcase_started(Config, T)
    end;
init_per_testcase(limit_max_size_bytes_with_properties = T, Config) ->
    case ?config(mqtt_version, Config) of
        v4 ->
            {skip, "Correlation-Data not supported in MQTT v4"};
        v5 ->
            rabbit_ct_helpers:testcase_started(Config, T)
    end;
init_per_testcase(Testcase, Config) ->
    rabbit_ct_helpers:testcase_started(Config, Testcase).

end_per_testcase(Testcase, Config)
  when Testcase =:= limit_max_messages;
       Testcase =:= limit_max_size_bytes;
       Testcase =:= limit_max_size_bytes_with_properties ->
    ok = set_limits(Config, 100_000, 1_073_741_824),
    rabbit_ct_helpers:testcase_finished(Config, Testcase);
end_per_testcase(recover_after_migration_cleanup = Testcase, Config) ->
    _ = rabbit_ct_broker_helpers:delete_vhost(Config, atom_to_binary(Testcase)),
    rabbit_ct_helpers:testcase_finished(Config, Testcase);
end_per_testcase(Testcase, Config) ->
    rabbit_ct_helpers:testcase_finished(Config, Testcase).


%% -------------------------------------------------------------------
%% Testsuite cases
%% -------------------------------------------------------------------

coerce_configuration_data(Config) ->
    C = connect(atom_to_binary(?FUNCTION_NAME), Config, [{ack_timeout, 1}]),

    {ok, _, _} = emqtt:subscribe(C, <<"TopicA">>, qos0),
    ok = emqtt:publish(C, <<"TopicA">>, <<"Payload">>),
    ok = expect_publishes(C, <<"TopicA">>, [<<"Payload">>]),

    ok = emqtt:disconnect(C).

%% -------------------------------------------------------------------
%% When a client is subscribed to TopicA/Device.Field and another
%% client publishes to TopicA/Device.Field the client should be
%% sent messages for the translated topic (TopicA/Device/Field)
%% -------------------------------------------------------------------
should_translate_amqp2mqtt_on_publish(Config) ->
    C = connect(atom_to_binary(?FUNCTION_NAME), Config, [{ack_timeout, 1}]),
    %% there's an active consumer
    {ok, _, _} = emqtt:subscribe(C, <<"TopicA/Device.Field">>, qos1),
    ok = emqtt:publish(C, <<"TopicA/Device.Field">>, #{},  <<"Payload">>, [{retain, true}]),
    ok = expect_publishes(C, <<"TopicA/Device/Field">>, [<<"Payload">>]),
    ok = emqtt:disconnect(C).

%% -------------------------------------------------------------------
%% If a client publishes a retained message to TopicA/Device.Field and another
%% client subscribes to TopicA/Device.Field the client should be
%% sent the retained message for the translated topic (TopicA/Device/Field)
%% -------------------------------------------------------------------
should_translate_amqp2mqtt_on_retention(Config) ->
    C = connect(atom_to_binary(?FUNCTION_NAME), Config, [{ack_timeout, 1}]),
    %% publish with retain = true before a consumer comes around
    ok = emqtt:publish(C, <<"TopicA/Device.Field">>, #{},  <<"Payload">>, [{retain, true}]),
    {ok, _, _} = emqtt:subscribe(C, <<"TopicA/Device.Field">>, qos1),
    ok = expect_publishes(C, <<"TopicA/Device/Field">>, [<<"Payload">>]),
    ok = emqtt:disconnect(C).

%% -------------------------------------------------------------------
%% If a client publishes a retained message to TopicA/Device.Field and another
%% client subscribes to TopicA/Device/Field the client should be
%% sent retained message for the translated topic (TopicA/Device/Field)
%% -------------------------------------------------------------------
should_translate_amqp2mqtt_on_retention_search(Config) ->
    C = connect(atom_to_binary(?FUNCTION_NAME), Config, [{ack_timeout, 1}]),
    ok = emqtt:publish(C, <<"TopicA/Device.Field">>, #{},  <<"Payload">>, [{retain, true}]),
    {ok, _, _} = emqtt:subscribe(C, <<"TopicA/Device/Field">>, qos1),
    ok = expect_publishes(C, <<"TopicA/Device/Field">>, [<<"Payload">>]),
    ok = emqtt:disconnect(C).

does_not_retain(Config) ->
    C = connect(atom_to_binary(?FUNCTION_NAME), Config, [{ack_timeout, 1}]),
    ok = emqtt:publish(C, <<"TopicA/Device.Field">>, #{},  <<"Payload">>, [{retain, true}]),
    {ok, _, _} = emqtt:subscribe(C, <<"TopicA/Device.Field">>, qos1),
    ok = expect_nothing(),
    ok = emqtt:disconnect(C).

recover(Config) ->
    Topic = Payload = ClientId = atom_to_binary(?FUNCTION_NAME),
    C1 = connect(ClientId, Config),
    {ok, _} = emqtt:publish(C1, Topic, Payload, [{retain, true},
                                                 {qos, 1}]),
    ok = emqtt:disconnect(C1),
    ok = rabbit_ct_broker_helpers:restart_node(Config, 0),
    rabbit_ct_broker_helpers:enable_plugin(Config, 0, rabbitmq_mqtt),
    C2 = connect(ClientId, Config),
    {ok, _, _} = emqtt:subscribe(C2, Topic, qos1),
    ok = expect_publishes(C2, Topic, [Payload]),
    ok = emqtt:disconnect(C2).

recover_after_migration_cleanup(Config) ->
    Topic = VHost2 = atom_to_binary(?FUNCTION_NAME),
    ok = rabbit_ct_broker_helpers:add_vhost(Config, VHost2),
    ok = rabbit_ct_broker_helpers:set_full_permissions(Config, <<"guest">>, VHost2),
    Creds2 = [{username, <<VHost2/binary, ":guest">>}, {password, <<"guest">>}],
    Cases = [{[], <<"payload-1">>}, {Creds2, <<"payload-2">>}],
    [begin
         C = connect(Payload, Config, Creds),
         {ok, _} = emqtt:publish(C, Topic, Payload, [{retain, true}, {qos, 1}]),
         ok = emqtt:disconnect(C)
     end || {Creds, Payload} <- Cases],
    ok = rpc(Config, rabbit_khepri, khepri_db_migration_post_enable,
             [#{feature_name => khepri_db, enabled => true}]),
    ok = rabbit_ct_broker_helpers:restart_node(Config, 0),
    rabbit_ct_broker_helpers:enable_plugin(Config, 0, rabbitmq_mqtt),
    [begin
         C = connect(<<"sub-", Payload/binary>>, Config, Creds),
         {ok, _, _} = emqtt:subscribe(C, Topic, qos1),
         ok = expect_publishes(C, Topic, [Payload]),
         ok = emqtt:disconnect(C)
     end || {Creds, Payload} <- Cases].

delete_vhost(Config) ->
    delete_vhost(Config, atom_to_binary(?FUNCTION_NAME)).

delete_vhost_long_name(Config) ->
    delete_vhost(Config, binary:copy(<<"v">>, 256)).

delete_vhost(Config, VHost) ->
    Topic = Payload = ClientId = atom_to_binary(?FUNCTION_NAME),
    Creds = [{username, <<VHost/binary, ":guest">>},
             {password, <<"guest">>}],
    Nodes = rabbit_ct_broker_helpers:get_node_configs(Config, nodename),
    ok = rabbit_ct_broker_helpers:add_vhost(Config, VHost),
    ok = rabbit_ct_broker_helpers:set_full_permissions(Config, <<"guest">>, VHost),
    lists:foreach(
      fun(Node) ->
              C1 = util:connect(<<ClientId/binary, "-", (atom_to_binary(Node))/binary>>,
                                Config, Node, Creds),
              {ok, _} = emqtt:publish(C1, Topic, Payload, [{retain, true}, {qos, 1}]),
              {ok, _, _} = emqtt:subscribe(C1, Topic, qos1),
              ok = expect_publishes(C1, Topic, [Payload]),
              ok = emqtt:disconnect(C1)
      end, Nodes),

    ok = rabbit_ct_broker_helpers:delete_vhost(Config, VHost),
    lists:foreach(
      fun(Node) ->
              ?awaitMatch(false,
                          lists:keymember(VHost, 1, rabbit_ct_broker_helpers:rpc(
                                                      Config, Node, supervisor, which_children,
                                                      [rabbit_mqtt_retainer_sup])),
                          30_000),
              Dir = rabbit_ct_broker_helpers:rpc(Config, Node, rabbit, data_dir, []),
              lists:foreach(
                fun(Suffix) ->
                        Path = rabbit_ct_broker_helpers:rpc(
                                 Config, Node, rabbit_mqtt_util, path_for, [Dir, VHost, Suffix]),
                        ?assertNot(rabbit_ct_broker_helpers:rpc(
                                     Config, Node, filelib, is_file, [Path]))
                end, [".ets", ".dets"])
      end, Nodes),

    ok = rabbit_ct_broker_helpers:add_vhost(Config, VHost),
    ok = rabbit_ct_broker_helpers:set_full_permissions(Config, <<"guest">>, VHost),
    lists:foreach(
      fun(Node) ->
              C2 = util:connect(<<ClientId/binary, "-", (atom_to_binary(Node))/binary>>,
                                Config, Node, Creds),
              {ok, _, _} = emqtt:subscribe(C2, Topic, qos1),
              ok = expect_nothing(),
              ok = emqtt:disconnect(C2)
      end, Nodes),
    ok = rabbit_ct_broker_helpers:delete_vhost(Config, VHost).

delete_orphaned_storage(Config) ->
    Dir = rpc(Config, rabbit, data_dir, []),
    Orphaned = [rabbit_mqtt_util:path_for(Dir, <<"deleted vhost">>, Suffix)
                || Suffix <- [".ets", ".dets"]],
    RepairFile = rabbit_mqtt_util:path_for(Dir, <<"deleted vhost">>, ".dets.TMP"),
    [ok = rpc(Config, file, write_file, [P, <<>>]) || P <- [RepairFile | Orphaned]],
    Kept = [P || P <- [RepairFile | [rabbit_mqtt_util:path_for(Dir, <<"/">>, Suffix)
                                     || Suffix <- [".ets", ".dets"]]],
                 rpc(Config, filelib, is_file, [P])],
    ok = rpc(Config, rabbit_mqtt_retainer_sup, delete_orphaned_storage, []),
    ?assertEqual([], [P || P <- Orphaned, rpc(Config, filelib, is_file, [P])]),
    ?assertEqual(Kept, [P || P <- Kept, rpc(Config, filelib, is_file, [P])]),
    ok = rpc(Config, file, delete, [RepairFile]).

delete_vhost_without_plugin(Config) ->
    VHost = Topic = Payload = ClientId = atom_to_binary(?FUNCTION_NAME),
    Creds = [{username, <<VHost/binary, ":guest">>}, {password, <<"guest">>}],
    ok = rabbit_ct_broker_helpers:add_vhost(Config, VHost),
    ok = rabbit_ct_broker_helpers:set_full_permissions(Config, <<"guest">>, VHost),
    C1 = util:connect(ClientId, Config, 1, Creds),
    {ok, _} = emqtt:publish(C1, Topic, Payload, [{retain, true}, {qos, 1}]),
    ok = emqtt:disconnect(C1),
    ok = rabbit_ct_broker_helpers:disable_plugin(Config, 1, rabbitmq_mqtt),
    try
        ok = rabbit_ct_broker_helpers:delete_vhost(Config, VHost),
        Dir = rabbit_ct_broker_helpers:rpc(Config, 1, rabbit, data_dir, []),
        [?awaitMatch(false, rabbit_ct_broker_helpers:rpc(
                              Config, 1, filelib, is_file,
                              [rabbit_mqtt_util:path_for(Dir, VHost, Suffix)]),
                     30_000) || Suffix <- [".ets", ".dets"]]
    after
        ok = rabbit_ct_broker_helpers:enable_plugin(Config, 1, rabbitmq_mqtt)
    end,
    ok = rabbit_ct_broker_helpers:add_vhost(Config, VHost),
    ok = rabbit_ct_broker_helpers:set_full_permissions(Config, <<"guest">>, VHost),
    C2 = util:connect(ClientId, Config, 1, Creds),
    {ok, _, _} = emqtt:subscribe(C2, Topic, qos1),
    ok = expect_nothing(),
    ok = emqtt:disconnect(C2),
    ok = rabbit_ct_broker_helpers:delete_vhost(Config, VHost).

late_cleanup_after_recreate(Config) ->
    VHost = Topic = ClientId = atom_to_binary(?FUNCTION_NAME),
    Creds = [{username, <<VHost/binary, ":guest">>}, {password, <<"guest">>}],
    ok = rabbit_ct_broker_helpers:add_vhost(Config, VHost),
    ok = rabbit_ct_broker_helpers:set_full_permissions(Config, <<"guest">>, VHost),
    C1 = util:connect(ClientId, Config, 1, Creds),
    {ok, _} = emqtt:publish(C1, Topic, <<"old">>, [{retain, true}, {qos, 1}]),
    ok = emqtt:disconnect(C1),
    ok = rpc(Config, sys, suspend, [rabbit_event]),
    try
        ok = rabbit_ct_broker_helpers:delete_vhost(Config, VHost),
        ok = rabbit_ct_broker_helpers:add_vhost(Config, VHost),
        ok = rabbit_ct_broker_helpers:set_full_permissions(Config, <<"guest">>, VHost)
    after
        ok = rpc(Config, sys, resume, [rabbit_event])
    end,
    ?awaitMatch(false,
                lists:keymember(VHost, 1, rabbit_ct_broker_helpers:rpc(
                                            Config, 1, supervisor, which_children,
                                            [rabbit_mqtt_retainer_sup])),
                30_000),
    C2 = util:connect(ClientId, Config, 1, Creds),
    {ok, _, _} = emqtt:subscribe(C2, Topic, qos1),
    ok = expect_nothing(),
    ok = emqtt:disconnect(C2),
    ok = rabbit_ct_broker_helpers:delete_vhost(Config, VHost).

recreate_vhost_after_failed_cleanup(Config) ->
    VHost = Topic = Payload = ClientId = atom_to_binary(?FUNCTION_NAME),
    Creds = [{username, <<VHost/binary, ":guest">>}, {password, <<"guest">>}],
    ok = rabbit_ct_broker_helpers:add_vhost(Config, VHost),
    ok = rabbit_ct_broker_helpers:set_full_permissions(Config, <<"guest">>, VHost),
    C1 = util:connect(ClientId, Config, 1, Creds),
    {ok, _} = emqtt:publish(C1, Topic, Payload, [{retain, true}, {qos, 1}]),
    ok = emqtt:disconnect(C1),
    rabbit_ct_broker_helpers:setup_meck(Config, [?MODULE]),
    ok = rabbit_ct_broker_helpers:rpc(Config, 1, meck, new, [rabbit_file, [no_link, passthrough]]),
    try
        ok = rabbit_ct_broker_helpers:rpc(Config, 1, meck, expect,
                                          [rabbit_file, recursive_delete,
                                           fun ?MODULE:fail_retained_msg_store_delete/1]),
        ok = rabbit_ct_broker_helpers:delete_vhost(Config, VHost),
        ?awaitMatch({VHost, undefined, _, _},
                    lists:keyfind(VHost, 1, rabbit_ct_broker_helpers:rpc(
                                              Config, 1, supervisor, which_children,
                                              [rabbit_mqtt_retainer_sup])),
                    30_000)
    after
        ok = rabbit_ct_broker_helpers:rpc(Config, 1, meck, unload, [rabbit_file])
    end,
    ok = rabbit_ct_broker_helpers:add_vhost(Config, VHost),
    ok = rabbit_ct_broker_helpers:set_full_permissions(Config, <<"guest">>, VHost),
    C2 = util:connect(ClientId, Config, 1, Creds),
    {ok, _, _} = emqtt:subscribe(C2, Topic, qos1),
    ok = expect_nothing(),
    ok = emqtt:disconnect(C2),
    ok = rabbit_ct_broker_helpers:delete_vhost(Config, VHost).

fail_retained_msg_store_delete(Paths) ->
    case lists:any(fun(P) -> string:find(P, "mqtt_retained_") =/= nomatch end, Paths) of
        true -> {error, {"mqtt_retained", eacces}};
        false -> meck:passthrough([Paths])
    end.

recover_with_message_expiry_interval(Config) ->
    ClientId = atom_to_binary(?FUNCTION_NAME),
    C1 = connect(ClientId, Config),
    {ok, _} = emqtt:publish(C1, <<"topic/1">>,
                            <<"m1">>, [{retain, true}, {qos, 1}]),
    {ok, _} = emqtt:publish(C1, <<"topic/2">>, #{'Message-Expiry-Interval' => 100},
                            <<"m2">>, [{retain, true}, {qos, 1}]),
    %% A value close to the timestamp the retainer records for the publish
    %% above. The retainer handles the retain cast after the PUBACK, so a
    %% reading taken before the publish can be a full round trip too early.
    Start = os:system_time(second),
    {ok, _} = emqtt:publish(C1, <<"topic/3">>, #{'Message-Expiry-Interval' => 3},
                            <<"m3">>, [{retain, true}, {qos, 1}]),
    {ok, _} = emqtt:publish(C1, <<"topic/4">>, #{'Message-Expiry-Interval' => 15},
                            <<"m4">>, [{retain, true}, {qos, 1}]),
    ok = emqtt:disconnect(C1),
    %% Takes around 9 seconds on Linux.
    ok = rabbit_ct_broker_helpers:restart_node(Config, 0),
    rabbit_ct_broker_helpers:enable_plugin(Config, 0, rabbitmq_mqtt),
    C2 = connect(ClientId, Config),

    %% Retained message for topic/3 should have expired during node restart.
    %% Wait for retained message for topic/4 to expire.
    ElapsedSeconds1 = os:system_time(second) - Start,
    SleepMs = max(0, timer:seconds(15 - ElapsedSeconds1 + 1)),
    ct:pal("Sleeping for ~b ms", [SleepMs]),
    timer:sleep(SleepMs),

    {ok, _, [1,1,1,1]} = emqtt:subscribe(C2, [{<<"topic/1">>, qos1},
                                              {<<"topic/2">>, qos1},
                                              {<<"topic/3">>, qos1},
                                              {<<"topic/4">>, qos1}]),
    receive {publish, #{client_pid := C2,
                        retain := true,
                        topic := <<"topic/1">>,
                        payload := <<"m1">>,
                        properties := Props}}
              when map_size(Props) =:= 0 -> ok
    after 30_000 -> ct:fail("did not topic/1")
    end,

    receive {publish, #{client_pid := C2,
                        retain := true,
                        topic := <<"topic/2">>,
                        payload := <<"m2">>,
                        properties :=  #{'Message-Expiry-Interval' := MEI}}} ->
                %% The broker computes the interval at lookup time.
                ElapsedSeconds2 = os:system_time(second) - Start,
                assert_message_expiry_interval(100 - ElapsedSeconds2, MEI)
    after 30_000 -> ct:fail("did not topic/2")
    end,

    receive Unexpected -> ct:fail("Received unexpectedly: ~p", [Unexpected])
    after 0 -> ok
    end,

    ok = emqtt:disconnect(C2).

%% A topic that is not in the store yet is refused once the store holds
%% `max_messages` entries, while overwriting a topic already in it is not.
limit_max_messages(Config) ->
    ok = set_limits(Config, 2, infinity),
    ClientId = atom_to_binary(?FUNCTION_NAME),
    C = connect(ClientId, Config),

    {ok, _} = emqtt:publish(C, <<"limit/1">>, <<"m1">>, [{retain, true}, {qos, 1}]),
    {ok, _} = emqtt:publish(C, <<"limit/2">>, <<"m2">>, [{retain, true}, {qos, 1}]),
    {ok, _} = emqtt:publish(C, <<"limit/3">>, <<"m3">>, [{retain, true}, {qos, 1}]),
    {ok, _} = emqtt:publish(C, <<"limit/1">>, <<"m1b">>, [{retain, true}, {qos, 1}]),

    {ok, _, _} = emqtt:subscribe(C, <<"limit/1">>, qos1),
    ok = expect_publishes(C, <<"limit/1">>, [<<"m1b">>]),
    {ok, _, _} = emqtt:subscribe(C, <<"limit/2">>, qos1),
    ok = expect_publishes(C, <<"limit/2">>, [<<"m2">>]),
    {ok, _, _} = emqtt:subscribe(C, <<"limit/3">>, qos1),
    ok = expect_nothing(),

    ok = emqtt:disconnect(C).

%% The limit is checked against the size of the store, which is never zero even
%% when empty, so a limit of 0 refuses every retained message.
limit_max_size_bytes(Config) ->
    ok = set_limits(Config, infinity, 0),
    ClientId = atom_to_binary(?FUNCTION_NAME),
    C1 = connect(<<ClientId/binary, "-1">>, Config),
    {ok, _} = emqtt:publish(C1, <<"size/1">>, <<"m1">>, [{retain, true}, {qos, 1}]),
    {ok, _, _} = emqtt:subscribe(C1, <<"size/1">>, qos1),
    ok = expect_nothing(),
    ok = emqtt:disconnect(C1),

    ok = set_limits(Config, infinity, infinity),
    %% Use a different client ID than C1: its qos1 subscription queue is
    %% exclusive and torn down asynchronously on disconnect, and reusing
    %% the same client ID here would race that deletion with C2 declaring
    %% a same-named queue, killing C2's connection.
    C2 = connect(<<ClientId/binary, "-2">>, Config),
    {ok, _} = emqtt:publish(C2, <<"size/1">>, <<"m1">>, [{retain, true}, {qos, 1}]),
    {ok, _, _} = emqtt:subscribe(C2, <<"size/1">>, qos1),
    ok = expect_publishes(C2, <<"size/1">>, [<<"m1">>]),
    ok = emqtt:disconnect(C2).

%% Message properties (e.g. MQTT 5.0 Correlation-Data, User-Property) count
%% towards `max_size_bytes`, not just the topic and payload.
limit_max_size_bytes_with_properties(Config) ->
    ok = set_limits(Config, infinity, 10_000),
    ClientId = atom_to_binary(?FUNCTION_NAME),

    C1 = connect(<<ClientId/binary, "-1">>, Config),
    LargeCorrelationData = binary:copy(<<"c">>, 20_000),
    {ok, _} = emqtt:publish(C1, <<"size/props">>,
                            #{'Correlation-Data' => LargeCorrelationData},
                            <<"m1">>, [{retain, true}, {qos, 1}]),
    {ok, _, _} = emqtt:subscribe(C1, <<"size/props">>, qos1),
    ok = expect_nothing(),
    ok = emqtt:disconnect(C1),

    C2 = connect(<<ClientId/binary, "-2">>, Config),
    {ok, _} = emqtt:publish(C2, <<"size/props">>, <<"m2">>, [{retain, true}, {qos, 1}]),
    {ok, _, _} = emqtt:subscribe(C2, <<"size/props">>, qos1),
    ok = expect_publishes(C2, <<"size/props">>, [<<"m2">>]),
    ok = emqtt:disconnect(C2).

%% -------------------------------------------------------------------
%% Helpers
%% -------------------------------------------------------------------

%% The retainer reads its limits when it starts, so it is restarted here.
%%
%% The store is emptied as well: the tests in this group run in random order and
%% the limits are counted against whatever the store already holds.
set_limits(Config, MaxMessages, MaxSizeBytes) ->
    VHost = <<"/">>,
    ok = rpc(Config, application, set_env,
             [rabbitmq_mqtt, retained_message_store_max_messages, MaxMessages]),
    ok = rpc(Config, application, set_env,
             [rabbitmq_mqtt, retained_message_store_max_size_bytes, MaxSizeBytes]),
    _ = rpc(Config, rabbit_mqtt_retainer_sup, start_child_for_vhost, [VHost]),
    ok = rpc(Config, rabbit_mqtt_retainer_sup, delete_child_for_vhost, [VHost]),
    _ = rpc(Config, rabbit_mqtt_retainer_sup, start_child_for_vhost, [VHost]),
    ok.

rpc(Config, Mod, Fun, Args) ->
    rabbit_ct_broker_helpers:rpc(Config, 0, Mod, Fun, Args).

expect_nothing() ->
    receive
        Unexpected ->
            ct:fail("Unexpected message: ~p", [Unexpected])
    after 1000 ->
              ok
    end.
