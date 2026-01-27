%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2026 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
%%

-module(amqp091_dynamic_SUITE).

-include_lib("eunit/include/eunit.hrl").
-include_lib("amqp_client/include/amqp_client.hrl").

-import(rabbit_ct_helpers, [eventually/1]).
-import(shovel_test_utils, [await_autodelete/2,
                            invalid_param/2, invalid_param/3,
                            valid_param/2, valid_param/3,
                            with_amqp091_ch/2, amqp091_publish_expect/5,
                            amqp091_publish/4, amqp091_expect_empty/2,
                            amqp091_expect/3]).

-compile(export_all).

-export([spawn_suspender_proc/1]).

all() ->
    [
      {group, core_tests},
      {group, quorum_queue_tests},
      {group, stream_queue_tests}
    ].

groups() ->
    [
      {core_tests, [], [
          set_properties_using_proplist,
          set_properties_using_map,
          set_empty_properties_using_proplist,
          set_empty_properties_using_map,
          headers,
          restart,
          validation,
          security_validation,
          get_connection_name,
          credit_flow
        ]},
    {quorum_queue_tests, [], [
          quorum_queues
    ]},

    {stream_queue_tests, [], [
          stream_queues
    ]}
    ].

%% -------------------------------------------------------------------
%% Testsuite setup/teardown.
%% -------------------------------------------------------------------

init_per_suite(Config) ->
    rabbit_ct_helpers:log_environment(),
    Config1 = rabbit_ct_helpers:set_config(Config, [
        {rmq_nodename_suffix, ?MODULE},
        {ignored_crashes, [
            "server_initiated_close,404",
            "writer,send_failed,closed"
        ]}
      ]),
    rabbit_ct_helpers:run_setup_steps(Config1,
      rabbit_ct_broker_helpers:setup_steps() ++
      rabbit_ct_client_helpers:setup_steps()).

end_per_suite(Config) ->
    rabbit_ct_helpers:run_teardown_steps(Config,
      rabbit_ct_client_helpers:teardown_steps() ++
      rabbit_ct_broker_helpers:teardown_steps()).

init_per_group(quorum_queue_tests, Config) ->
    case rabbit_ct_helpers:is_mixed_versions() of
        false -> Config;
        _     -> {skip, "quorum queue tests are skipped in mixed mode"}
    end;
init_per_group(stream_queue_tests, Config) ->
    case rabbit_ct_helpers:is_mixed_versions() of
        false -> Config;
        _     -> {skip, "stream queue tests are skipped in mixed mode"}
    end;
init_per_group(_, Config) ->
    Config.

end_per_group(_, Config) ->
    Config.

init_per_testcase(Testcase, Config) ->
    rabbit_ct_helpers:testcase_started(Config, Testcase).

end_per_testcase(Testcase, Config) ->
    rabbit_ct_broker_helpers:rpc(Config, 0, ?MODULE, cleanup1, [Config]),
    rabbit_ct_helpers:testcase_finished(Config, Testcase).

%% -------------------------------------------------------------------
%% Testcases.
%% -------------------------------------------------------------------
quorum_queues(Config) ->
    with_amqp091_ch(Config,
      fun (Ch) ->
              shovel_test_utils:set_param(
                Config,
                <<"test">>, [
                             {<<"src-queue">>,       <<"src">>},
                             {<<"dest-queue">>,      <<"dest">>},
                             {<<"src-queue-args">>,  #{<<"x-queue-type">> => <<"quorum">>}},
                             {<<"dest-queue-args">>, #{<<"x-queue-type">> => <<"quorum">>,
                                                       <<"x-dead-letter-exchange">> => <<"dlx1">>,
                                                       <<"x-dead-letter-routing-key">> => <<"rk1">>}}
                            ]),
              amqp091_publish_expect(Ch, <<>>, <<"src">>, <<"dest">>, <<"hello">>)
      end).

stream_queues(Config) ->
    with_amqp091_ch(Config,
      fun (Ch) ->
              shovel_test_utils:set_param(
                Config,
                <<"test">>, [
                             {<<"src-queue">>,       <<"src">>},
                             {<<"dest-queue">>,      <<"dest">>},
                             {<<"src-queue-args">>,  #{<<"x-queue-type">> => <<"stream">>}},
                             {<<"src-consumer-args">>,  #{<<"x-stream-offset">> => <<"first">>}}
                            ]),
              amqp091_publish_expect(Ch, <<>>, <<"src">>, <<"dest">>, <<"hello">>)
      end).

set_properties_using_map(Config) ->
    with_amqp091_ch(Config,
      fun (Ch) ->
              Ps = [{<<"src-queue">>,      <<"src">>},
                    {<<"dest-queue">>,     <<"dest">>},
                    {<<"publish-properties">>, #{<<"cluster_id">> => <<"x">>}}],
              shovel_test_utils:set_param(Config, <<"test">>, Ps),
              #amqp_msg{props = #'P_basic'{cluster_id = Cluster}} =
                  amqp091_publish_expect(Ch, <<>>, <<"src">>, <<"dest">>, <<"hi">>),
              <<"x">> = Cluster
      end).

set_properties_using_proplist(Config) ->
    with_amqp091_ch(Config,
      fun (Ch) ->
              Ps = [{<<"src-queue">>,      <<"src">>},
                    {<<"dest-queue">>,     <<"dest">>},
                    {<<"publish-properties">>, [{<<"cluster_id">>, <<"x">>}]}],
              shovel_test_utils:set_param(Config, <<"test">>, Ps),
              #amqp_msg{props = #'P_basic'{cluster_id = Cluster}} =
                  amqp091_publish_expect(Ch, <<>>, <<"src">>, <<"dest">>, <<"hi">>),
              <<"x">> = Cluster
      end).

set_empty_properties_using_map(Config) ->
    with_amqp091_ch(Config,
      fun (Ch) ->
              Ps = [{<<"src-queue">>,      <<"src">>},
                    {<<"dest-queue">>,     <<"dest">>},
                    {<<"publish-properties">>, #{}}],
              shovel_test_utils:set_param(Config, <<"test">>, Ps),
              #amqp_msg{props = #'P_basic'{}} =
                  amqp091_publish_expect(Ch, <<>>, <<"src">>, <<"dest">>, <<"hi">>)
      end).

set_empty_properties_using_proplist(Config) ->
    with_amqp091_ch(Config,
      fun (Ch) ->
              Ps = [{<<"src-queue">>,      <<"src">>},
                    {<<"dest-queue">>,     <<"dest">>},
                    {<<"publish-properties">>, []}],
              shovel_test_utils:set_param(Config, <<"test">>, Ps),
              #amqp_msg{props = #'P_basic'{}} =
                  amqp091_publish_expect(Ch, <<>>, <<"src">>, <<"dest">>, <<"hi">>)
      end).

headers(Config) ->
    with_amqp091_ch(Config,
        fun(Ch) ->
            %% No headers by default
            shovel_test_utils:set_param(Config,
                <<"test">>,
                [{<<"src-queue">>,            <<"src">>},
                 {<<"dest-queue">>,           <<"dest">>}]),
            ?assertMatch(#amqp_msg{props = #'P_basic'{headers = H0}}
                           when H0 == undefined orelse H0 == [],
                                amqp091_publish_expect(Ch, <<>>, <<"src">>, <<"dest">>, <<"hi1">>)),

            shovel_test_utils:set_param(Config,
                <<"test">>,
                [{<<"src-queue">>,            <<"src">>},
                 {<<"dest-queue">>,           <<"dest">>},
                 {<<"add-forward-headers">>,  true},
                 {<<"add-timestamp-header">>, true}]),
            Timestmp = os:system_time(seconds),
            #amqp_msg{props = #'P_basic'{headers = Headers}} =
                    amqp091_publish_expect(Ch, <<>>, <<"src">>, <<"dest">>, <<"hi2">>),
            [{<<"x-shovelled">>, _, [{table, ShovelledHeader}]},
             {<<"x-shovelled-timestamp">>, long, TS}] = Headers,
            %% We assume that the message was shovelled within a 2 second
            %% window.
            true = TS >= Timestmp andalso TS =< Timestmp + 2,
            {<<"shovel-type">>, _, <<"dynamic">>} =
                lists:keyfind(<<"shovel-type">>, 1, ShovelledHeader),
            {<<"shovel-vhost">>, _, <<"/">>} =
                lists:keyfind(<<"shovel-vhost">>, 1, ShovelledHeader),
            {<<"shovel-name">>, _, <<"test">>} =
                lists:keyfind(<<"shovel-name">>, 1, ShovelledHeader),

            shovel_test_utils:set_param(Config,
                <<"test">>,
                [{<<"src-queue">>,            <<"src">>},
                 {<<"dest-queue">>,           <<"dest">>},
                 {<<"add-timestamp-header">>, true}]),
            #amqp_msg{props = #'P_basic'{headers = [{<<"x-shovelled-timestamp">>,
                                                    long, _}]}} =
                    amqp091_publish_expect(Ch, <<>>, <<"src">>, <<"dest">>, <<"hi3">>),

            shovel_test_utils:set_param(Config,
                <<"test">>,
                [{<<"src-queue">>,            <<"src">>},
                 {<<"dest-queue">>,           <<"dest">>},
                 {<<"add-forward-headers">>,  true}]),
            #amqp_msg{props = #'P_basic'{headers = [{<<"x-shovelled">>,
                                                     _, _}]}} =
                    amqp091_publish_expect(Ch, <<>>, <<"src">>, <<"dest">>, <<"hi4">>)

        end).

restart(Config) ->
    with_amqp091_ch(Config,
      fun (Ch) ->
              shovel_test_utils:set_param(Config,
                        <<"test">>, [{<<"src-queue">>,  <<"src">>},
                                     {<<"dest-queue">>, <<"dest">>}]),
              %% The catch is because connections link to the shovel,
              %% so one connection will die, kill the shovel, kill
              %% the other connection, then we can't close it
              Conns = rabbit_ct_broker_helpers:rpc(Config, 0,
                rabbit_direct, list, []),
              [catch amqp_connection:close(C) || C <- Conns],
              amqp091_publish_expect(Ch, <<>>, <<"src">>, <<"dest">>, <<"hello">>)
      end).

validation(Config) ->
    URIs = [{<<"src-uri">>,  <<"amqp://">>},
            {<<"dest-uri">>, <<"amqp://">>}],

    %% Need valid src and dest URIs
    invalid_param(Config, []),
    invalid_param(Config,
                  [{<<"src-queue">>, <<"test">>},
                   {<<"src-uri">>,   <<"derp">>},
                   {<<"dest-uri">>,  <<"amqp://">>}]),
    invalid_param(Config,
                  [{<<"src-queue">>, <<"test">>},
                   {<<"src-uri">>,   [<<"derp">>]},
                   {<<"dest-uri">>,  <<"amqp://">>}]),
    invalid_param(Config,
                  [{<<"src-queue">>, <<"test">>},
                   {<<"dest-uri">>,  <<"amqp://">>}]),

    %% Also need src exchange or queue
    invalid_param(Config,
                  URIs),
    valid_param(Config,
                [{<<"src-exchange">>, <<"test">>} | URIs]),
    QURIs =     [{<<"src-queue">>,    <<"test">>} | URIs],
    valid_param(Config, QURIs),

    %% But not both
    invalid_param(Config,
                  [{<<"src-exchange">>, <<"test">>} | QURIs]),

    %% Check these are of right type
    invalid_param(Config,
                  [{<<"prefetch-count">>,  <<"three">>} | QURIs]),
    invalid_param(Config,
                  [{<<"reconnect-delay">>, <<"three">>} | QURIs]),
    invalid_param(Config,
                  [{<<"ack-mode">>,        <<"whenever">>} | QURIs]),
    invalid_param(Config,
                  [{<<"src-delete-after">>,    <<"whenever">>} | QURIs]),

    %% Check properties have to look property-ish
    valid_param(Config,
                [{<<"src-exchange">>, <<"test">>},
                 {<<"publish-properties">>, [{<<"cluster_id">>, <<"rabbit@localhost">>},
                                             {<<"delivery_mode">>, 2}]}
                 | URIs]),
    valid_param(Config,
                #{<<"publish-properties">> => #{<<"cluster_id">>    => <<"rabbit@localhost">>,
                                                <<"delivery_mode">> => 2},
                  <<"src-exchange">> => <<"test">>,
                  <<"src-uri">>      => <<"amqp://">>,
                  <<"dest-uri">>     => <<"amqp://">>}),
    invalid_param(Config,
                  [{<<"publish-properties">>, [{<<"nonexistent">>, <<>>}]}]),
    invalid_param(Config,
                  #{<<"publish-properties">> => #{<<"nonexistent">> => <<>>}}),
    invalid_param(Config,
                  [{<<"publish-properties">>, [{<<"cluster_id">>, 2}]}]),
    invalid_param(Config,
                  [{<<"publish-properties">>, <<"something">>}]),

    %% Can't use explicit message count and no-ack together
    invalid_param(Config,
                  [{<<"src-delete-after">>,    1},
                   {<<"ack-mode">>,        <<"no-ack">>} | QURIs]),
    %% superseded by src-delete-after
    invalid_param(Config,
                  [{<<"delete-after">>,    1},
                   {<<"ack-mode">>,        <<"no-ack">>} | QURIs]),
    ok.

security_validation(Config) ->
    ok = rabbit_ct_broker_helpers:rpc(Config, 0,
      ?MODULE, security_validation_add_user, []),

    Qs = [{<<"src-queue">>, <<"test">>},
          {<<"dest-queue">>, <<"test2">>}],

    A = lookup_user(Config, <<"a">>),
    valid_param(Config, [{<<"src-uri">>,  <<"amqp://localhost:5672/a">>},
                         {<<"dest-uri">>, <<"amqp://localhost:5672/b">>} | Qs], A),
    %% src-uri and dest-uri are not valid URIs
    invalid_param(Config,
                  [{<<"src-uri">>,  <<"an arbitrary string">>},
                   {<<"dest-uri">>, <<"\o/ \o/ \o/">>} | Qs], A),
    %% missing src-queue and dest-queue
    invalid_param(Config,
                  [{<<"src-uri">>,  <<"amqp://localhost/a">>},
                   {<<"dest-uri">>, <<"amqp://localhost/b">>}], A),

    ok = rabbit_ct_broker_helpers:rpc(Config, 0,
      ?MODULE, security_validation_remove_user, []),
    ok.

security_validation_add_user() ->
    [begin
         rabbit_vhost:add(U, <<"acting-user">>),
         rabbit_auth_backend_internal:add_user(U, <<>>, <<"acting-user">>),
         rabbit_auth_backend_internal:set_permissions(
           U, U, <<".*">>, <<".*">>, <<".*">>, <<"acting-user">>)
     end || U <- [<<"a">>, <<"b">>]],
    ok.

security_validation_remove_user() ->
    [begin
         rabbit_vhost:delete(U, <<"acting-user">>),
         rabbit_auth_backend_internal:delete_user(U, <<"acting-user">>)
     end || U <- [<<"a">>, <<"b">>]],
    ok.

get_connection_name(_Config) ->
    <<"Shovel static_shovel_name_as_atom">> = rabbit_shovel_worker:get_connection_name(static_shovel_name_as_atom),
    <<"Shovel dynamic_shovel_name_as_binary">> = rabbit_shovel_worker:get_connection_name({<<"/">>, <<"dynamic_shovel_name_as_binary">>}),
    <<"Shovel">> = rabbit_shovel_worker:get_connection_name({<<"/">>, {unexpected, tuple}}),
    <<"Shovel">> = rabbit_shovel_worker:get_connection_name({one, two, three}),
    <<"Shovel">> = rabbit_shovel_worker:get_connection_name(<<"anything else">>).

credit_flow(Config) ->
    OrigCredit = set_default_credit(Config, {20, 10}),

    with_amqp091_ch(Config,
      fun (Ch) ->
              try
                  shovel_test_utils:set_param_nowait(
                    Config,
                    <<"test">>, [{<<"src-queue">>,    <<"src">>},
                                 {<<"dest-queue">>,   <<"dest">>},
                                 {<<"src-prefetch-count">>, 50},
                                 {<<"ack-mode">>,     <<"on-publish">>},
                                 {<<"src-delete-after">>, <<"never">>}]),
                  shovel_test_utils:await_shovel(Config, <<"test">>),
                  running = shovel_test_utils:get_shovel_status(Config, <<"test">>),

                  ShovelPid = find_shovel_pid(Config),
                  #{dest :=
                        #{current :=
                              {_DestConn, DestChan, _DestUri}}} =
                      get_shovel_state(ShovelPid),
                  WriterPid = find_writer_pid_for_channel(Config, DestChan),

                  %% When the broker-side channel is blocked by flow
                  %% control, it stops reading from the tcp
                  %% socket. After all the OS, BEAM and process buffers
                  %% are full, gen_tcp:send/2 will block the writer
                  %% process. Simulate this by suspending the writer process.
                  true = suspend_process(Config, WriterPid),

                  %% Publish 1000 messages to the src queue
                  amqp_channel:call(Ch, #'confirm.select'{}),
                  publish_count(Ch, <<>>, <<"src">>, <<"hello">>, 1000),
                  amqp_channel:wait_for_confirms(Ch),

                  %% Wait until the shovel is blocked
                  rabbit_ct_helpers:await_condition(
                    fun() ->
                            flow =:= shovel_test_utils:get_shovel_status(Config, <<"test">>)
                    end,
                    5000),

                  %% There should be only one process with a message buildup
                  Top = [{WriterPid, MQLen, _}, {_, P, _} | _] =
                      rabbit_ct_broker_helpers:rpc(
                        Config, 0, recon, proc_count, [message_queue_len, 10]),
                  ct:pal("Top processes by message queue length: ~p", [Top]),
                  ?assert(P < 3),

                  %% The writer process should have only a limited
                  %% message queue. The shovel stops sending messages
                  %% when the channel and shovel process used up all
                  %% their initial credit (that is 20 + 20).
                  2 * 20 = MQLen = proc_info(WriterPid, message_queue_len),

                  %% Most messages should still be in the queue either ready or unacked
                  ExpDestCnt = 0,
                  #{messages := ExpDestCnt} = message_count(Config, <<"dest">>),
                  ExpSrcCnt = 1000 - MQLen,
                  #{messages := ExpSrcCnt,
                    messages_unacknowledged := 50} = message_count(Config, <<"src">>),

                  %% After the writer process is resumed all messages
                  %% should be shoveled to the dest queue, and process
                  %% message queues should be empty
                  resume_process(Config),

                  rabbit_ct_helpers:await_condition(
                    fun() ->
                            #{messages := Cnt} = message_count(Config, <<"src">>),
                            Cnt =:= 0
                    end,
                    5000),
                  #{messages := 1000} = message_count(Config, <<"dest">>),
                  [{_, P, _}] =
                      rabbit_ct_broker_helpers:rpc(
                        Config, 0, recon, proc_count, [message_queue_len, 1]),
                  ?assert(P < 3),

                  rabbit_ct_helpers:await_condition(
                    fun() ->
                            running =:= shovel_test_utils:get_shovel_status(Config, <<"test">>)
                    end,
                    5000)
              after
                  resume_process(Config),
                  set_default_credit(Config, OrigCredit)
              end
      end).

%%----------------------------------------------------------------------------
publish_count(Ch, X, Key, M, Count) ->
    [begin

         amqp091_publish(Ch, X, Key, M)
     end || _ <- lists:seq(1, Count)].

expect_count(Ch, Q, M, Count) ->
    [begin
         amqp091_expect(Ch, Q, M)
     end || _ <- lists:seq(1, Count)],
    amqp091_expect_empty(Ch, Q).

lookup_user(Config, Name) ->
    {ok, User} = rabbit_ct_broker_helpers:rpc(Config, 0,
      rabbit_access_control, check_user_login, [Name, []]),
    User.

set_default_credit(Config, Value) ->
    Key = credit_flow_default_credit,
    OrigValue = rabbit_ct_broker_helpers:rpc(Config, persistent_term, get, [Key]),
    ok = rabbit_ct_broker_helpers:rpc(Config, persistent_term, put, [Key, Value]),
    OrigValue.

message_count(Config, QueueName) ->
    Resource = rabbit_misc:r(<<"/">>, queue, QueueName),
    {ok, Q} = rabbit_ct_broker_helpers:rpc(Config, 0, rabbit_amqqueue, lookup, [Resource]),
    maps:from_list(
      rabbit_ct_broker_helpers:rpc(Config, 0, rabbit_amqqueue, info,
                                   [Q, [messages, messages_unacknowledged]])).

%% A process can be only suspended by another process on the same node
suspend_process(Config, Pid) ->
    true = rabbit_ct_broker_helpers:rpc(Config, 0, ?MODULE, spawn_suspender_proc, [Pid]),
    suspended = proc_info(Pid, status),
    true.

%% When the suspender process terminates, the suspended process is also resumed
resume_process(Config) ->
    case rabbit_ct_broker_helpers:rpc(Config, 0, erlang, whereis, [suspender]) of
        undefined ->
            false;
        SusPid ->
            exit(SusPid, kill)
    end.

spawn_suspender_proc(Pid) ->
    undefined = whereis(suspender),
    ReqPid = self(),
    SusPid =
        spawn(
          fun() ->
                  register(suspender, self()),
                  Res = catch (true = erlang:suspend_process(Pid)),
                  ReqPid ! {suspend_res, self(), Res},
                  %% wait indefinitely
                  receive stop -> ok end
          end),
    receive
        {suspend_res, SusPid, Res} -> Res
    after
        5000 -> timeout
    end.

find_shovel_pid(Config) ->
    [ShovelPid] = [P || P <- rabbit_ct_broker_helpers:rpc(
                               Config, 0, erlang, processes, []),
                        rabbit_shovel_worker ==
                            (catch element(1, erpc:call(node(P), proc_lib, initial_call, [P])))],
    ShovelPid.

get_shovel_state(ShovelPid) ->
    gen_server2:with_state(ShovelPid, fun rabbit_shovel_worker:get_internal_config/1).

find_writer_pid_for_channel(Config, ChanPid) ->
    {amqp_channel, ChanName} = process_name(ChanPid),
    [WriterPid] = [P || P <- rabbit_ct_broker_helpers:rpc(
                               Config, 0, erlang, processes, []),
                        {rabbit_writer, ChanName} == process_name(P)],
    WriterPid.

process_name(Pid) ->
    try proc_info(Pid, dictionary) of
        Dict ->
            proplists:get_value(process_name, Dict)
    catch _:_ ->
            undefined
    end.

proc_info(Pid, Item) ->
    {Item, Value} = erpc:call(node(Pid), erlang, process_info, [Pid, Item]),
    Value.

cleanup1(_Config) ->
    [rabbit_runtime_parameters:clear(rabbit_misc:pget(vhost, P),
                                     rabbit_misc:pget(component, P),
                                     rabbit_misc:pget(name, P),
                                     <<"acting-user">>) ||
        P <- rabbit_runtime_parameters:list()],
    [rabbit_amqqueue:delete(Q, false, false, <<"acting-user">>)
     || Q <- rabbit_amqqueue:list()].
