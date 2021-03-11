%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2021 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(amqp10_dynamic_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("amqp_client/include/amqp_client.hrl").

-compile(export_all).

all() ->
    [
      {group, non_parallel_tests},
      {group, with_map_config}
    ].

groups() ->
    [
      {non_parallel_tests, [], [
          simple,
          change_definition,
          autodelete_amqp091_src_on_confirm,
          autodelete_amqp091_src_on_publish,
          autodelete_amqp091_dest_on_confirm,
          autodelete_amqp091_dest_on_publish,
          simple_amqp10_dest,
          simple_amqp10_src
        ]},
      {with_map_config, [], [
          simple,
          simple_amqp10_dest,
          simple_amqp10_src
      ]}
    ].

%% -------------------------------------------------------------------
%% Testsuite setup/teardown.
%% -------------------------------------------------------------------

init_per_suite(Config0) ->
    {ok, _} = application:ensure_all_started(amqp10_client),
    rabbit_ct_helpers:log_environment(),
    Config1 = rabbit_ct_helpers:set_config(Config0, [
        {rmq_nodename_suffix, ?MODULE}
      ]),
    rabbit_ct_helpers:run_setup_steps(Config1,
      rabbit_ct_broker_helpers:setup_steps() ++
      rabbit_ct_client_helpers:setup_steps()).

end_per_suite(Config) ->
    application:stop(amqp10_client),
    rabbit_ct_helpers:run_teardown_steps(Config,
      rabbit_ct_client_helpers:teardown_steps() ++
      rabbit_ct_broker_helpers:teardown_steps()).

init_per_group(with_map_config, Config) ->
    rabbit_ct_helpers:set_config(Config, [{map_config, true}]);
init_per_group(_, Config) ->
    rabbit_ct_helpers:set_config(Config, [{map_config, false}]).

end_per_group(_, Config) ->
    Config.

init_per_testcase(Testcase, Config0) ->
    SrcQ = list_to_binary(atom_to_list(Testcase) ++ "_src"),
    DestQ = list_to_binary(atom_to_list(Testcase) ++ "_dest"),
    DestQ2 = list_to_binary(atom_to_list(Testcase) ++ "_dest2"),
    Config = [{srcq, SrcQ}, {destq, DestQ}, {destq2, DestQ2} | Config0],
    rabbit_ct_helpers:testcase_started(Config, Testcase).

end_per_testcase(Testcase, Config) ->
    rabbit_ct_helpers:testcase_finished(Config, Testcase).

%% -------------------------------------------------------------------
%% Testcases.
%% -------------------------------------------------------------------

simple(Config) ->
    Src = ?config(srcq, Config),
    Dest = ?config(destq, Config),
    with_session(Config,
      fun (Sess) ->
              test_amqp10_destination(Config, Src, Dest, Sess, <<"amqp10">>,
                                      <<"src-address">>)
      end).

simple_amqp10_dest(Config) ->
    Src = ?config(srcq, Config),
    Dest = ?config(destq, Config),
    with_session(Config,
      fun (Sess) ->
              test_amqp10_destination(Config, Src, Dest, Sess, <<"amqp091">>,
                                      <<"src-queue">>)
      end).

test_amqp10_destination(Config, Src, Dest, Sess, Protocol, ProtocolSrc) ->
    MapConfig = ?config(map_config, Config),
    shovel_test_utils:set_param(Config, <<"test">>,
                                [{<<"src-protocol">>, Protocol},
                                 {ProtocolSrc, Src},
                                 {<<"dest-protocol">>, <<"amqp10">>},
                                 {<<"dest-address">>, Dest},
                                 {<<"dest-add-forward-headers">>, true},
                                 {<<"dest-add-timestamp-header">>, true},
                                 {<<"dest-application-properties">>,
                                  case MapConfig of
                                     true ->
                                        #{<<"app-prop-key">> => <<"app-prop-value">>};
                                     _ ->
                                        [{<<"app-prop-key">>, <<"app-prop-value">>}]
                                  end},
                                 {<<"dest-properties">>,
                                  case MapConfig of
                                     true ->
                                         #{<<"user_id">> => <<"guest">>};
                                     _ ->
                                         [{<<"user_id">>, <<"guest">>}]
                                  end},
                                 {<<"dest-message-annotations">>,
                                  case MapConfig of
                                     true ->
                                         #{<<"message-ann-key">> =>
                                               <<"message-ann-value">>};
                                     _ ->
                                         [{<<"message-ann-key">>,
                                           <<"message-ann-value">>}]
                                  end}]),
    Msg = publish_expect(Sess, Src, Dest, <<"tag1">>, <<"hello">>),
    ?assertMatch((#{user_id := <<"guest">>, creation_time := _}),
                 (amqp10_msg:properties(Msg))),
    ?assertMatch((#{<<"shovel-name">> := <<"test">>,
                    <<"shovel-type">> := <<"dynamic">>, <<"shovelled-by">> := _,
                    <<"app-prop-key">> := <<"app-prop-value">>}),
                 (amqp10_msg:application_properties(Msg))),
    ?assertMatch((#{<<"message-ann-key">> := <<"message-ann-value">>}),
                 (amqp10_msg:message_annotations(Msg))).

simple_amqp10_src(Config) ->
    MapConfig = ?config(map_config, Config),
    Src = ?config(srcq, Config),
    Dest = ?config(destq, Config),
    with_session(Config,
      fun (Sess) ->
              shovel_test_utils:set_param(
                Config,
                <<"test">>, [{<<"src-protocol">>, <<"amqp10">>},
                             {<<"src-address">>,  Src},
                             {<<"dest-protocol">>, <<"amqp091">>},
                             {<<"dest-queue">>, Dest},
                             {<<"add-forward-headers">>, true},
                             {<<"dest-add-timestamp-header">>, true},
                             {<<"publish-properties">>,
                                case MapConfig of
                                    true -> #{<<"cluster_id">> => <<"x">>};
                                    _    -> [{<<"cluster_id">>, <<"x">>}]
                                end}
                            ]),
              _Msg = publish_expect(Sess, Src, Dest, <<"tag1">>,
                                    <<"hello">>),
              % the fidelity loss is quite high when consuming using the amqp10
              % plugin. For example custom headers aren't current translated.
              % This isn't due to the shovel though.
              ok
      end).

change_definition(Config) ->
    Src = ?config(srcq, Config),
    Dest = ?config(destq, Config),
    Dest2 = ?config(destq2, Config),
    with_session(Config,
      fun (Sess) ->
              shovel_test_utils:set_param(Config, <<"test">>,
                                          [{<<"src-address">>,  Src},
                                           {<<"src-protocol">>, <<"amqp10">>},
                                           {<<"dest-protocol">>, <<"amqp10">>},
                                           {<<"dest-address">>, Dest}]),
              publish_expect(Sess, Src, Dest, <<"tag2">>,<<"hello">>),
              shovel_test_utils:set_param(Config, <<"test">>,
                                          [{<<"src-address">>,  Src},
                                           {<<"src-protocol">>, <<"amqp10">>},
                                           {<<"dest-protocol">>, <<"amqp10">>},
                                           {<<"dest-address">>, Dest2}]),
              publish_expect(Sess, Src, Dest2, <<"tag3">>, <<"hello">>),
              expect_empty(Sess, Dest),
              shovel_test_utils:clear_param(Config, <<"test">>),
              publish_expect(Sess, Src, Src, <<"tag4">>, <<"hello2">>),
              expect_empty(Sess, Dest),
              expect_empty(Sess, Dest2)
      end).

autodelete_amqp091_src_on_confirm(Config) ->
    autodelete_case(Config, {<<"on-confirm">>, 50, 50, 50},
                    fun autodelete_amqp091_src/2),
    ok.

autodelete_amqp091_src_on_publish(Config) ->
    autodelete_case(Config, {<<"on-publish">>, 50, 50, 50},
                    fun autodelete_amqp091_src/2),
    ok.

autodelete_amqp091_dest_on_confirm(Config) ->
    autodelete_case(Config, {<<"on-confirm">>, 50, 50, 50},
                    fun autodelete_amqp091_dest/2),
    ok.

autodelete_amqp091_dest_on_publish(Config) ->
    autodelete_case(Config, {<<"on-publish">>, 50, 50, 50},
                    fun autodelete_amqp091_dest/2),
    ok.

autodelete_case(Config, Args, CaseFun) ->
    with_session(Config, CaseFun(Config, Args)).

autodelete_do(Config, {AckMode, After, ExpSrc, ExpDest}) ->
    Src = ?config(srcq, Config),
    Dest = ?config(destq, Config),
    fun (Session) ->
            publish_count(Session, Src, <<"hello">>, 100),
            shovel_test_utils:set_param_nowait(
              Config,
              <<"test">>, [{<<"src-address">>,    Src},
                           {<<"src-protocol">>,   <<"amqp10">>},
                           {<<"src-delete-after">>, After},
                           {<<"src-prefetch-count">>, 5},
                           {<<"dest-address">>,   Dest},
                           {<<"dest-protocol">>,   <<"amqp10">>},
                           {<<"ack-mode">>,     AckMode}
                          ]),
            await_autodelete(Config, <<"test">>),
            expect_count(Session, Dest, <<"hello">>, ExpDest),
            expect_count(Session, Src, <<"hello">>, ExpSrc)
    end.

autodelete_amqp091_src(Config, {AckMode, After, ExpSrc, ExpDest}) ->
    Src = ?config(srcq, Config),
    Dest = ?config(destq, Config),
    fun (Session) ->
            publish_count(Session, Src, <<"hello">>, 100),
            shovel_test_utils:set_param_nowait(
              Config,
              <<"test">>, [{<<"src-queue">>, Src},
                           {<<"src-protocol">>, <<"amqp091">>},
                           {<<"src-delete-after">>, After},
                           {<<"src-prefetch-count">>, 5},
                           {<<"dest-address">>, Dest},
                           {<<"dest-protocol">>, <<"amqp10">>},
                           {<<"ack-mode">>, AckMode}
                          ]),
            await_autodelete(Config, <<"test">>),
            expect_count(Session, Dest, <<"hello">>, ExpDest),
            expect_count(Session, Src, <<"hello">>, ExpSrc)
    end.

autodelete_amqp091_dest(Config, {AckMode, After, ExpSrc, ExpDest}) ->
    Src = ?config(srcq, Config),
    Dest = ?config(destq, Config),
    fun (Session) ->
            publish_count(Session, Src, <<"hello">>, 100),
            shovel_test_utils:set_param_nowait(
              Config,
              <<"test">>, [{<<"src-address">>, Src},
                           {<<"src-protocol">>, <<"amqp10">>},
                           {<<"src-delete-after">>, After},
                           {<<"src-prefetch-count">>, 5},
                           {<<"dest-queue">>, Dest},
                           {<<"dest-protocol">>, <<"amqp091">>},
                           {<<"ack-mode">>, AckMode}
                          ]),
            await_autodelete(Config, <<"test">>),
            expect_count(Session, Dest, <<"hello">>, ExpDest),
            expect_count(Session, Src, <<"hello">>, ExpSrc)
    end.

%%----------------------------------------------------------------------------

with_session(Config, Fun) ->
    Hostname = ?config(rmq_hostname, Config),
    Port = rabbit_ct_broker_helpers:get_node_config(Config, 0, tcp_port_amqp),
    {ok, Conn} = amqp10_client:open_connection(Hostname, Port),
    {ok, Sess} = amqp10_client:begin_session(Conn),
    Fun(Sess),
    amqp10_client:close_connection(Conn),
    ok.

publish(Sender, Tag, Payload) when is_binary(Payload) ->
    Headers = #{durable => true},
    Msg = amqp10_msg:set_headers(Headers,
                                 amqp10_msg:new(Tag, Payload, false)),
    ok = amqp10_client:send_msg(Sender, Msg),
    receive
        {amqp10_disposition, {accepted, Tag}} -> ok
    after 3000 ->
              exit(publish_disposition_not_received)
    end.

publish_expect(Session, Source, Dest, Tag, Payload) ->
    LinkName = <<"dynamic-sender-", Dest/binary>>,
    {ok, Sender} = amqp10_client:attach_sender_link(Session, LinkName, Source,
                                                    unsettled, unsettled_state),
    ok = await_amqp10_event(link, Sender, attached),
    publish(Sender, Tag, Payload),
    amqp10_client:detach_link(Sender),
    expect_one(Session, Dest, Payload).

await_amqp10_event(On, Ref, Evt) ->
    receive
        {amqp10_event, {On, Ref, Evt}} -> ok
    after 5000 ->
          exit({amqp10_event_timeout, On, Ref, Evt})
    end.

expect_one(Session, Dest, Payload) ->
    LinkName = <<"dynamic-receiver-", Dest/binary>>,
    {ok, Receiver} = amqp10_client:attach_receiver_link(Session, LinkName,
                                                        Dest, settled,
                                                        unsettled_state),
    ok = amqp10_client:flow_link_credit(Receiver, 1, never),
    Msg = expect(Receiver, Payload),
    amqp10_client:detach_link(Receiver),
    Msg.

expect(Receiver, _Payload) ->
    receive
        {amqp10_msg, Receiver, InMsg} ->
            InMsg
    after 4000 ->
              throw(timeout_in_expect_waiting_for_delivery)
    end.

expect_empty(Session, Dest) ->
    {ok, Receiver} = amqp10_client:attach_receiver_link(Session,
                                                        <<"dynamic-receiver">>,
                                                        Dest, settled,
                                                        unsettled_state),
    % probably good enough given we don't currently have a means of
    % echoing flow state
    {error, timeout} = amqp10_client:get_msg(Receiver, 250),
    amqp10_client:detach_link(Receiver).

publish_count(Session, Address, Payload, Count) ->
    LinkName = <<"dynamic-sender-", Address/binary>>,
    {ok, Sender} = amqp10_client:attach_sender_link(Session, LinkName,
                                                    Address, unsettled,
                                                    unsettled_state),
    ok = await_amqp10_event(link, Sender, attached),
    [begin
         Tag = rabbit_data_coercion:to_binary(I),
         publish(Sender, Tag, <<Payload/binary, Tag/binary>>)
     end || I <- lists:seq(1, Count)],
     amqp10_client:detach_link(Sender).

expect_count(Session, Address, Payload, Count) ->
    {ok, Receiver} = amqp10_client:attach_receiver_link(Session,
                                                        <<"dynamic-receiver",
                                                          Address/binary>>,
                                                        Address, settled,
                                                        unsettled_state),
    ok = amqp10_client:flow_link_credit(Receiver, Count, never),
    [begin
         expect(Receiver, Payload)
     end || _ <- lists:seq(1, Count)],
    expect_empty(Session, Address),
    amqp10_client:detach_link(Receiver).


invalid_param(Config, Value, User) ->
    {error_string, _} = rabbit_ct_broker_helpers:rpc(Config, 0,
      rabbit_runtime_parameters, set,
      [<<"/">>, <<"shovel">>, <<"invalid">>, Value, User]).

valid_param(Config, Value, User) ->
    rabbit_ct_broker_helpers:rpc(Config, 0,
      ?MODULE, valid_param1, [Config, Value, User]).

valid_param1(_Config, Value, User) ->
    ok = rabbit_runtime_parameters:set(
           <<"/">>, <<"shovel">>, <<"a">>, Value, User),
    ok = rabbit_runtime_parameters:clear(<<"/">>, <<"shovel">>, <<"a">>, <<"acting-user">>).

invalid_param(Config, Value) -> invalid_param(Config, Value, none).
valid_param(Config, Value) -> valid_param(Config, Value, none).

lookup_user(Config, Name) ->
    {ok, User} = rabbit_ct_broker_helpers:rpc(Config, 0,
      rabbit_access_control, check_user_login, [Name, []]),
    User.

await_autodelete(Config, Name) ->
    rabbit_ct_broker_helpers:rpc(Config, 0,
      ?MODULE, await_autodelete1, [Config, Name], 10000).

await_autodelete1(_Config, Name) ->
    shovel_test_utils:await(
      fun () -> not lists:member(Name, shovels_from_parameters()) end),
    shovel_test_utils:await(
      fun () ->
              not lists:member(Name,
                               shovel_test_utils:shovels_from_status())
      end).

shovels_from_parameters() ->
    L = rabbit_runtime_parameters:list(<<"/">>, <<"shovel">>),
    [rabbit_misc:pget(name, Shovel) || Shovel <- L].
