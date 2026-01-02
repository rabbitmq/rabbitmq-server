%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2026 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
%%

-module(amqp10_dynamic_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("rabbitmq_ct_helpers/include/rabbit_assert.hrl").
-compile(export_all).

-import(shovel_test_utils, [with_amqp10_session/2,
                            amqp10_publish/3, amqp10_publish/5,
                            amqp10_expect_empty/2,
                            await_amqp10_event/3, amqp10_expect_one/2,
                            amqp10_expect_count/3, amqp10_publish/4,
                            amqp10_publish_expect/5, amqp10_declare_queue/3,
                            await_autodelete/2]).

-define(PARAM, <<"test">>).

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
          simple_amqp10_dest,
          amqp091_to_amqp10_with_dead_lettering,
          test_amqp10_delete_after_queue_length
        ]},
      {with_map_config, [], [
          simple,
          simple_amqp10_dest
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
    shovel_test_utils:clear_param(Config, ?PARAM),
    rabbit_ct_broker_helpers:rpc(Config, 0, shovel_test_utils, delete_all_queues, []),
    rabbit_ct_helpers:testcase_finished(Config, Testcase).

%% -------------------------------------------------------------------
%% Testcases.
%% -------------------------------------------------------------------

simple(Config) ->
    Src = ?config(srcq, Config),
    Dest = ?config(destq, Config),
    with_amqp10_session(Config,
      fun (Sess) ->
              test_amqp10_destination(Config, Src, Dest, Sess, <<"amqp10">>,
                                      <<"src-address">>)
      end).

simple_amqp10_dest(Config) ->
    Src = ?config(srcq, Config),
    Dest = ?config(destq, Config),
    with_amqp10_session(Config,
      fun (Sess) ->
              test_amqp10_destination(Config, Src, Dest, Sess, <<"amqp091">>,
                                      <<"src-queue">>)
      end).

amqp091_to_amqp10_with_dead_lettering(Config) ->
    Dest = ?config(destq, Config),
    Src = ?config(srcq, Config),
    TmpQ = <<"tmp">>,
    with_amqp10_session(Config,
      fun (Sess) ->
              amqp10_declare_queue(Sess, TmpQ, #{<<"x-max-length">> => {uint, 0},
                                                 <<"x-dead-letter-exchange">> => {utf8, <<"">>},
                                                 <<"x-dead-letter-routing-key">> => {utf8, Src}}),
              {ok, Sender} = amqp10_client:attach_sender_link(Sess,
                                                              <<"sender-tmp">>,
                                                              <<"/queues/", TmpQ/binary>>,
                                                              unsettled,
                                                              unsettled_state),
              ok = await_amqp10_event(link, Sender, attached),
              amqp10_expect_empty(Sess, TmpQ),
              test_amqp10_destination(Config, Src, Dest, Sess, <<"amqp091">>, <<"src-queue">>),
              %% publish to tmp, it should be dead-lettered to src and then shovelled to dest
              _ = amqp10_publish_expect(Sess, TmpQ, Dest, <<"hello">>, 1)
      end).

test_amqp10_destination(Config, Src, Dest, Sess, Protocol, ProtocolSrc) ->
    MapConfig = ?config(map_config, Config),
    shovel_test_utils:set_param(Config, ?PARAM,
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
                                          #{<<"x-message-ann-key">> =>
                                            <<"message-ann-value">>};
                                      _ ->
                                          [{<<"x-message-ann-key">>,
                                            <<"message-ann-value">>}]
                                  end}]),
    [Msg] = amqp10_publish_expect(Sess, Src, Dest, <<"hello">>, 1),
    AppProps = amqp10_msg:application_properties(Msg),
    Anns = amqp10_msg:message_annotations(Msg),
    %% We no longer add/override properties, application properties or
    %% message annotations. Just the forward headers and timestamp as
    %% message annotations. The AMQP 1.0 message is inmutable
    ?assertNot(maps:is_key(user_id, amqp10_msg:properties(Msg))),
    ?assertNot(maps:is_key(<<"app-prop-key">>, AppProps)),
    ?assertEqual(undefined, maps:get(<<"delivery_mode">>, AppProps, undefined)),
    ?assertNot(maps:is_key(<<"x-message-ann-key">>, Anns)),
    ?assertMatch(#{<<"x-opt-shovel-name">> := <<"test">>,
                   <<"x-opt-shovel-type">> := <<"dynamic">>,
                   <<"x-opt-shovelled-by">> := _,
                   <<"x-opt-shovelled-timestamp">> := _
                  }, Anns).

change_definition(Config) ->
    Src = ?config(srcq, Config),
    Dest = ?config(destq, Config),
    Dest2 = ?config(destq2, Config),
    with_amqp10_session(Config,
      fun (Sess) ->
              shovel_test_utils:set_param(Config, ?PARAM,
                                          [{<<"src-address">>,  Src},
                                           {<<"src-protocol">>, <<"amqp10">>},
                                           {<<"dest-protocol">>, <<"amqp10">>},
                                           {<<"dest-address">>, Dest}]),
              amqp10_publish_expect(Sess, Src, Dest, <<"hello1">>, 1),
              shovel_test_utils:set_param(Config, ?PARAM,
                                          [{<<"src-address">>,  Src},
                                           {<<"src-protocol">>, <<"amqp10">>},
                                           {<<"dest-protocol">>, <<"amqp10">>},
                                           {<<"dest-address">>, Dest2}]),
              amqp10_publish_expect(Sess, Src, Dest2, <<"hello2">>, 1),
              amqp10_expect_empty(Sess, Dest),
              shovel_test_utils:clear_param(Config, <<"test">>),
              amqp10_publish_expect(Sess, Src, Src, <<"hello3">>, 1),
              amqp10_expect_empty(Sess, Dest),
              amqp10_expect_empty(Sess, Dest2)
      end).

test_amqp10_delete_after_queue_length(Config) ->
    Src = ?config(srcq, Config),
    Dest = ?config(destq, Config),
    Uri = shovel_test_utils:make_uri(Config, 0),
    Error = rabbit_ct_broker_helpers:rpc(
              Config, 0,
              rabbit_runtime_parameters, set,
              [<<"/">>, <<"shovel">>, <<"test">>, [{<<"src-uri">>,  Uri},
                                                   {<<"dest-uri">>, [Uri]},
                                                   {<<"src-protocol">>, <<"amqp10">>},
                                                   {<<"src-address">>, Src},
                                                   {<<"src-delete-after">>, <<"queue-length">>},
                                                   {<<"dest-protocol">>, <<"amqp10">>},
                                                   {<<"dest-address">>, Dest}],
               none]),
    ?assertMatch({error_string, _}, Error),
    {_, Msg} = Error,
    ?assertMatch(match, re:run(Msg, "Validation failed.*", [{capture, none}])).

%%----------------------------------------------------------------------------
publish(Sender, Msg) ->
    ok = amqp10_client:send_msg(Sender, Msg),
    Tag = amqp10_msg:delivery_tag(Msg),
    receive
        {amqp10_disposition, {accepted, Tag}} -> ok
    after 3000 ->
            exit(publish_disposition_not_received)
    end.

publish_expect_msg(Session, Source, Dest, Msg) ->
    LinkName = <<"dynamic-sender-", Dest/binary>>,
    {ok, Sender} = amqp10_client:attach_sender_link(Session, LinkName, Source,
                                                    unsettled, unsettled_state),
    ok = await_amqp10_event(link, Sender, attached),
    publish(Sender, Msg),
    amqp10_client:detach_link(Sender),
    amqp10_expect_one(Session, Dest).

lookup_user(Config, Name) ->
    {ok, User} = rabbit_ct_broker_helpers:rpc(Config, 0,
      rabbit_access_control, check_user_login, [Name, []]),
    User.
