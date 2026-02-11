%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2025 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
%%

-module(shovel_dynamic_SUITE).
%% Common test cases to all protocols

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("rabbitmq_ct_helpers/include/rabbit_assert.hrl").

-compile(export_all).

-import(rabbit_ct_helpers, [eventually/3]).
-import(shovel_test_utils, [await_autodelete/2,
                            set_param/3,
                            set_param_nowait/3,
                            clear_param/2,
                            with_amqp10_session/2,
                            amqp10_publish_expect/5,
                            amqp10_declare_queue/3,
                            amqp10_publish/4,
                            amqp10_publish_msg/4,
                            amqp10_expect/3,
                            amqp10_expect_one/2,
                            amqp10_expect_count/3,
                            amqp10_expect_empty/2,
                            amqp10_subscribe/2,
                            make_uri/2, make_uri/3,
                            make_uri/5,
                            await_no_shovel/2
                           ]).

all() ->
    [
     {group, amqp091},
     {group, amqp10},
     {group, local},
     {group, amqp091_to_amqp10},
     {group, amqp091_to_local},
     {group, amqp10_to_amqp091},
     {group, amqp10_to_local},
     {group, local_to_amqp091},
     {group, local_to_amqp10}
    ].

groups() ->
    [
     {amqp091, [parallel], tests()},
     {amqp10, [parallel], tests()},
     {local, [parallel], tests()},
     {amqp091_to_amqp10, [parallel], tests()},
     {amqp091_to_local, [parallel], tests()},
     {amqp10_to_amqp091, [parallel], tests()},
     {amqp10_to_local, [parallel], tests()},
     {local_to_amqp091, [parallel], tests()},
     {local_to_amqp10, [parallel], tests()}
    ].

tests() ->
    [
     simple,
     simple_classic_no_ack,
     simple_classic_on_confirm,
     simple_classic_on_publish,
     simple_quorum_no_ack,
     simple_quorum_on_confirm,
     simple_quorum_on_publish,
     simple_stream_on_confirm,
     simple_stream_on_publish,
     %% Credit flow tests are just simple tests that publish a high
     %% number of messages, on the attempt to trigger the different
     %% credit flow mechanisms. Having the same test twice (simple/credit)
     %% helps to isolate the problem.
     credit_flow_classic_no_ack,
     credit_flow_classic_on_confirm,
     credit_flow_classic_on_publish,
     credit_flow_quorum_no_ack,
     credit_flow_quorum_on_confirm,
     credit_flow_quorum_on_publish,
     credit_flow_stream_on_confirm,
     credit_flow_stream_on_publish,
     delete_after_never,
     autodelete_classic_on_confirm,
     autodelete_quorum_on_confirm,
     autodelete_classic_on_publish,
     autodelete_quorum_on_publish,
     autodelete_no_ack,
     autodelete_classic_on_confirm_no_transfer,
     autodelete_quorum_on_confirm_no_transfer,
     autodelete_classic_on_publish_no_transfer,
     autodelete_quorum_on_publish_no_transfer,
     %% AMQP091 and local shovels requeue messages on reject
     %% while AMQP10 discards messages on reject.
     %% These two tests will remain commented out until a decision on
     %% which behavior to adopt for both is made and implemented.
     %% autodelete_classic_on_confirm_with_rejections,
     %% autodelete_quorum_on_confirm_with_rejections,
     autodelete_classic_on_publish_with_rejections,
     autodelete_quorum_on_publish_with_rejections,
     no_vhost_access,
     no_user_access,
     application_properties,
     delete_src_queue,
     shovel_status,
     change_definition,
     disk_alarm
    ].

%% -------------------------------------------------------------------
%% Testsuite setup/teardown.
%% -------------------------------------------------------------------

init_per_suite(Config0) ->
    {ok, _} = application:ensure_all_started(amqp10_client),
    rabbit_ct_helpers:log_environment(),
    Config1 = rabbit_ct_helpers:set_config(Config0, [
      {rmq_nodename_suffix, ?MODULE},
      {ignored_crashes, [
          "server_initiated_close,404",
          "writer,send_failed,closed",
          "source_queue_down",
          "dest_queue_down",
          "inbound_link_detached"
        ]}
      ]),
    rabbit_ct_helpers:run_setup_steps(
      Config1,
      rabbit_ct_broker_helpers:setup_steps() ++
          rabbit_ct_client_helpers:setup_steps()).

end_per_suite(Config) ->
    application:stop(amqp10_client),
    rabbit_ct_helpers:run_teardown_steps(Config,
      rabbit_ct_client_helpers:teardown_steps() ++
      rabbit_ct_broker_helpers:teardown_steps()).

init_per_group(amqp091, Config) ->
    rabbit_ct_helpers:set_config(
      Config,
      [
       {src_protocol, <<"amqp091">>},
       {dest_protocol, <<"amqp091">>},
       {src_address, <<"src-queue">>},
       {dest_address, <<"dest-queue">>}
      ]);
init_per_group(amqp10, Config) ->
    rabbit_ct_helpers:set_config(
      Config,
      [
       {src_protocol, <<"amqp10">>},
       {dest_protocol, <<"amqp10">>},
       {src_address, <<"src-address">>},
       {dest_address, <<"dest-address">>}
      ]);
init_per_group(local, Config0) ->
    Config = rabbit_ct_helpers:set_config(
               Config0,
               [
                {src_protocol, <<"local">>},
                {dest_protocol, <<"local">>},
                {src_address, <<"src-queue">>},
                {dest_address, <<"dest-queue">>}
               ]),
    maybe_skip_local_protocol(Config);
init_per_group(amqp091_to_amqp10, Config) ->
    rabbit_ct_helpers:set_config(
      Config,
      [
       {src_protocol, <<"amqp091">>},
       {dest_protocol, <<"amqp10">>},
       {src_address, <<"src-queue">>},
       {dest_address, <<"dest-address">>}
      ]);
init_per_group(amqp091_to_local, Config0) ->
    Config = rabbit_ct_helpers:set_config(
               Config0,
               [
                {src_protocol, <<"amqp091">>},
                {dest_protocol, <<"local">>},
                {src_address, <<"src-queue">>},
                {dest_address, <<"dest-queue">>}
               ]),
    maybe_skip_local_protocol(Config);
init_per_group(amqp10_to_amqp091, Config) ->
    rabbit_ct_helpers:set_config(
      Config,
      [
       {src_protocol, <<"amqp10">>},
       {dest_protocol, <<"amqp091">>},
       {src_address, <<"src-address">>},
       {dest_address, <<"dest-queue">>}
      ]);
init_per_group(amqp10_to_local, Config0) ->
    Config = rabbit_ct_helpers:set_config(
               Config0,
               [
                {src_protocol, <<"amqp10">>},
                {dest_protocol, <<"local">>},
                {src_address, <<"src-address">>},
                {dest_address, <<"dest-queue">>}
               ]),
    maybe_skip_local_protocol(Config);
init_per_group(local_to_amqp091, Config0) ->
    Config = rabbit_ct_helpers:set_config(
               Config0,
               [
                {src_protocol, <<"local">>},
                {dest_protocol, <<"amqp091">>},
                {src_address, <<"src-queue">>},
                {dest_address, <<"dest-queue">>}
              ]),
    maybe_skip_local_protocol(Config);
init_per_group(local_to_amqp10, Config0) ->
    Config = rabbit_ct_helpers:set_config(
               Config0,
               [
                {src_protocol, <<"local">>},
                {dest_protocol, <<"amqp10">>},
                {src_address, <<"src-queue">>},
                {dest_address, <<"dest-address">>}
               ]),
    maybe_skip_local_protocol(Config).

end_per_group(_, Config) ->
    Config.

init_per_testcase(Testcase, Config0) ->
    SrcQ = list_to_binary(atom_to_list(Testcase) ++ "_src"),
    DestQ = list_to_binary(atom_to_list(Testcase) ++ "_dest"),
    VHost = list_to_binary(atom_to_list(Testcase) ++ "_vhost"),
    Group = proplists:get_value(name, ?config(tc_group_properties, Config0)),
    Param = list_to_binary(atom_to_list(Group) ++ "_" ++ atom_to_list(Testcase)),
    ShovelArgs = [{<<"src-protocol">>, ?config(src_protocol, Config0)},
                  {<<"dest-protocol">>, ?config(dest_protocol, Config0)},
                  {?config(src_address, Config0), SrcQ},
                  {?config(dest_address, Config0), DestQ}],
    Config = rabbit_ct_helpers:set_config(
               Config0,
               [{srcq, SrcQ}, {destq, DestQ}, {shovel_args, ShovelArgs},
                {alt_vhost, VHost}, {param, Param}]),
    rabbit_ct_helpers:testcase_started(Config, Testcase).

end_per_testcase(Testcase, Config) ->
    shovel_test_utils:clear_param(Config, ?config(param, Config)),
    Src = ?config(srcq, Config),
    Dest = ?config(destq, Config),
    Queues = [rabbit_misc:r(<<"/">>, queue, Src), rabbit_misc:r(<<"/">>, queue, Dest)],
    rabbit_ct_broker_helpers:rpc(Config, 0, shovel_test_utils, delete_queues, [Queues]),
    _ = rabbit_ct_broker_helpers:delete_vhost(Config, ?config(alt_vhost, Config)),
    rabbit_ct_helpers:testcase_finished(Config, Testcase).

%% -------------------------------------------------------------------
%% Testcases.
%% -------------------------------------------------------------------
simple(Config) ->
    Src = ?config(srcq, Config),
    Dest = ?config(destq, Config),
    Param = ?config(param, Config),
    with_amqp10_session(
      Config,
      fun (Sess) ->
              set_param(Config, Param, ?config(shovel_args, Config)),
              amqp10_publish_expect(Sess, Src, Dest, <<"hello">>, 1),
              Status = rabbit_ct_broker_helpers:rpc(Config, 0, rabbit_shovel_status, lookup, [{<<"/">>, Param}]),
              ?assertMatch([_|_], Status),
              ?assertMatch(#{metrics := #{forwarded := 1}}, maps:from_list(Status))
      end).

simple_classic_no_ack(Config) ->
    simple_queue_type_ack_mode(Config, <<"classic">>, <<"no-ack">>, 10).

simple_classic_on_confirm(Config) ->
    simple_queue_type_ack_mode(Config, <<"classic">>, <<"on-confirm">>, 10).

simple_classic_on_publish(Config) ->
    simple_queue_type_ack_mode(Config, <<"classic">>, <<"on-publish">>, 10).

simple_quorum_no_ack(Config) ->
    simple_queue_type_ack_mode(Config, <<"quorum">>, <<"no-ack">>, 10).

simple_quorum_on_confirm(Config) ->
    simple_queue_type_ack_mode(Config, <<"quorum">>, <<"on-confirm">>, 10).

simple_quorum_on_publish(Config) ->
    simple_queue_type_ack_mode(Config, <<"quorum">>, <<"on-publish">>, 10).

simple_stream_on_confirm(Config) ->
    simple_stream(Config, <<"on-confirm">>, 10).

simple_stream_on_publish(Config) ->
    simple_stream(Config, <<"on-publish">>, 10).

simple_stream(Config, AckMode, NMsgs) ->
    Src = ?config(srcq, Config),
    Dest = ?config(destq, Config),
    Param = ?config(param, Config),
    with_amqp10_session(Config,
      fun (Sess) ->
              amqp10_declare_queue(Sess, Src, #{<<"x-queue-type">> => {utf8, <<"stream">>}}),
              amqp10_declare_queue(Sess, Dest, #{<<"x-queue-type">> => {utf8, <<"stream">>}}),
              set_param(Config, Param,
                        ?config(shovel_args, Config) ++ [{<<"ack-mode">>, AckMode}]),
              Receiver = amqp10_subscribe(Sess, Dest),
              amqp10_publish(Sess, Src, <<"tag1">>, NMsgs),
              ?awaitMatch({_Name, dynamic, {running, _}, #{forwarded := NMsgs}, _},
                          shovel_status(Config, {<<"/">>, Param}),
                          30000),
              _ = amqp10_expect(Receiver, NMsgs, []),
              amqp10_client:detach_link(Receiver)
      end).

credit_flow_classic_no_ack(Config) ->
    simple_queue_type_ack_mode(Config, <<"classic">>, <<"no-ack">>, 5000).

credit_flow_classic_on_confirm(Config) ->
    simple_queue_type_ack_mode(Config, <<"classic">>, <<"on-confirm">>, 5000).

credit_flow_classic_on_publish(Config) ->
    simple_queue_type_ack_mode(Config, <<"classic">>, <<"on-publish">>, 5000).

credit_flow_quorum_no_ack(Config) ->
    simple_queue_type_ack_mode(Config, <<"quorum">>, <<"no-ack">>, 5000).

credit_flow_quorum_on_confirm(Config) ->
    simple_queue_type_ack_mode(Config, <<"quorum">>, <<"on-confirm">>, 5000).

credit_flow_quorum_on_publish(Config) ->
    simple_queue_type_ack_mode(Config, <<"quorum">>, <<"on-publish">>, 5000).

credit_flow_stream_on_confirm(Config) ->
    simple_stream(Config, <<"on-confirm">>, 5000).

credit_flow_stream_on_publish(Config) ->
    simple_stream(Config, <<"on-publish">>, 5000).

simple_queue_type_ack_mode(Config, Type, AckMode, NMsgs) ->
    Src = ?config(srcq, Config),
    Dest = ?config(destq, Config),
    with_amqp10_session(
      Config,
      fun (Sess) ->
              amqp10_declare_queue(Sess, Src, #{<<"x-queue-type">> => {utf8, Type}}),
              amqp10_declare_queue(Sess, Dest, #{<<"x-queue-type">> => {utf8, Type}}),
              ExtraArgs = [{<<"ack-mode">>, AckMode}],
              ShovelArgs = ?config(shovel_args, Config) ++ ExtraArgs,
              set_param(Config, ?config(param, Config), ShovelArgs),
              amqp10_publish_expect(Sess, Src, Dest, <<"hello">>, NMsgs)
      end).

delete_after_never(Config) ->
    Src = ?config(srcq, Config),
    Dest = ?config(destq, Config),
    Param = ?config(param, Config),
    with_amqp10_session(
      Config,
      fun (Sess) ->
              set_param(Config, Param,
                        ?config(shovel_args, Config) ++
                            [{<<"src-delete-after">>, <<"never">>}]),
              amqp10_publish_expect(Sess, Src, Dest, <<"carrots">>, 5000),
              ?awaitMatch({_Name, dynamic, {running, _}, #{forwarded := 5000}, _},
                          shovel_status(Config, {<<"/">>, Param}),
                          30000)
      end).

autodelete_classic_on_confirm_no_transfer(Config) ->
    autodelete(Config, <<"classic">>, <<"on-confirm">>, 0, 100, 0).

autodelete_quorum_on_confirm_no_transfer(Config) ->
    autodelete(Config, <<"quorum">>, <<"on-confirm">>, 0, 100, 0).

autodelete_classic_on_publish_no_transfer(Config) ->
    autodelete(Config, <<"classic">>, <<"on-publish">>, 0, 100, 0).

autodelete_quorum_on_publish_no_transfer(Config) ->
    autodelete(Config, <<"quorum">>, <<"on-publish">>, 0, 100, 0).

autodelete_classic_on_confirm(Config) ->
    autodelete(Config, <<"classic">>, <<"on-confirm">>, 50, 50, 50).

autodelete_quorum_on_confirm(Config) ->
    autodelete(Config, <<"quorum">>, <<"on-confirm">>, 50, 50, 50).

autodelete_classic_on_publish(Config) ->
    autodelete(Config, <<"classic">>, <<"on-publish">>, 50, 50, 50).

autodelete_quorum_on_publish(Config) ->
    autodelete(Config, <<"quorum">>, <<"on-publish">>, 50, 50, 50).

autodelete_no_ack(Config) ->
    ExtraArgs = [{<<"ack-mode">>, <<"no-ack">>},
                 {<<"src-delete-after">>, 100}],
    ShovelArgs = ?config(shovel_args, Config) ++ ExtraArgs,
    Uri = make_uri(Config, 0),
    ?assertMatch({error_string, _},
                 rabbit_ct_broker_helpers:rpc(
                   Config, 0, rabbit_runtime_parameters, set,
                   [<<"/">>, <<"shovel">>, ?config(param, Config),
                    [{<<"src-uri">>,  Uri},
                     {<<"dest-uri">>, [Uri]}] ++ ShovelArgs,
                    none])).

autodelete(Config, Type, AckMode, After, ExpSrc, ExpDest) ->
    Src = ?config(srcq, Config),
    Dest = ?config(destq, Config),
    Param = ?config(param, Config),
    with_amqp10_session(
      Config,
      fun (Sess) ->
              amqp10_declare_queue(Sess, Src, #{<<"x-queue-type">> => {utf8, Type}}),
              amqp10_declare_queue(Sess, Dest, #{<<"x-queue-type">> => {utf8, Type}}),
              amqp10_publish(Sess, Src, <<"hello">>, 100),
              ExtraArgs = [{<<"ack-mode">>, AckMode},
                           {<<"src-delete-after">>, After}],
              ShovelArgs = ?config(shovel_args, Config) ++ ExtraArgs,
              set_param_nowait(Config, Param, ShovelArgs),
              await_autodelete(Config, Param),
              amqp10_expect_count(Sess, Src, ExpSrc),
              amqp10_expect_count(Sess, Dest, ExpDest)
      end).

autodelete_classic_on_confirm_with_rejections(Config) ->
    autodelete_with_rejections(Config, <<"classic">>, <<"on-confirm">>, 5, 5).

autodelete_quorum_on_confirm_with_rejections(Config) ->
    ExpSrc = fun(ExpDest) -> 100 - ExpDest end,
    autodelete_with_quorum_rejections(Config, <<"on-confirm">>, ExpSrc).

autodelete_classic_on_publish_with_rejections(Config) ->
    autodelete_with_rejections(Config, <<"classic">>, <<"on-publish">>, 1, 5).

autodelete_quorum_on_publish_with_rejections(Config) ->
    ExpSrc = fun(_) -> 0 end,
    autodelete_with_quorum_rejections(Config, <<"on-publish">>, ExpSrc).

autodelete_with_rejections(Config, Type, AckMode, MaxExpSrc, ExpDest) ->
    Src = ?config(srcq, Config),
    Dest = ?config(destq, Config),
    Param = ?config(param, Config),
    with_amqp10_session(
      Config,
      fun (Sess) ->
              amqp10_declare_queue(Sess, Src, #{<<"x-queue-type">> => {utf8, Type}}),
              amqp10_declare_queue(Sess, Dest, #{<<"x-queue-type">> => {utf8, Type},
                                                 <<"x-overflow">> => {utf8, <<"reject-publish">>},
                                                 <<"x-max-length">> => {ulong, 5}
                                                }),
              amqp10_publish(Sess, Src, <<"hello">>, 10),
              ExtraArgs = [{<<"ack-mode">>, AckMode},
                           {<<"src-delete-after">>, 10}],
              ShovelArgs = ?config(shovel_args, Config) ++ ExtraArgs,
              set_param_nowait(Config, Param, ShovelArgs),
              await_autodelete(Config, Param),
              %% With on-publish ack mode, the last source ack and the
              %% autodelete exit race: the AMQP 1.0 disposition is sent
              %% asynchronously and may not reach the source queue before
              %% the connection is closed.
              ?awaitMatch(N when N =< MaxExpSrc,
                          list_queue_messages(Config, Src), 45_000),
              ?awaitMatch(ExpDest, list_queue_messages(Config, Dest), 45_000),
              amqp10_expect_count(Sess, Dest, ExpDest)
      end).

autodelete_with_quorum_rejections(Config, AckMode, ExpSrcFun) ->
    Src = ?config(srcq, Config),
    Dest = ?config(destq, Config),
    Param = ?config(param, Config),
    Type = <<"quorum">>,
    with_amqp10_session(
      Config,
      fun (Sess) ->
              amqp10_declare_queue(Sess, Src, #{<<"x-queue-type">> => {utf8, Type}}),
              amqp10_declare_queue(Sess, Dest, #{<<"x-queue-type">> => {utf8, Type},
                                                 <<"x-overflow">> => {utf8, <<"reject-publish">>},
                                                 <<"x-max-length">> => {ulong, 5}
                                                }),
              amqp10_publish(Sess, Src, <<"hello">>, 100),
              ExtraArgs = [{<<"ack-mode">>, AckMode},
                           {<<"src-delete-after">>, 50}],
              ShovelArgs = ?config(shovel_args, Config) ++ ExtraArgs,
              set_param_nowait(Config, Param, ShovelArgs),
              await_autodelete(Config, Param),
              eventually(
                ?_assert(
                   list_queue_messages(Config, Dest) >= 5),
                1000, 45),
              ExpDest = list_queue_messages(Config, Dest),
              amqp10_expect_count(Sess, Src, ExpSrcFun(ExpDest)),
              amqp10_expect_count(Sess, Dest, ExpDest)
      end).

no_vhost_access(Config) ->
    AltVHost = ?config(alt_vhost, Config),
    Param = ?config(param, Config),
    ok = rabbit_ct_broker_helpers:add_vhost(Config, AltVHost),
    Uri = make_uri(Config, 0, AltVHost),
    ExtraArgs = [{<<"src-uri">>,  Uri}, {<<"dest-uri">>, [Uri]}],
    ShovelArgs = ?config(shovel_args, Config) ++ ExtraArgs,
    ok = rabbit_ct_broker_helpers:rpc(
           Config, 0, rabbit_runtime_parameters, set,
           [<<"/">>, <<"shovel">>, Param, ShovelArgs, none]),
    await_no_shovel(Config, Param).

no_user_access(Config) ->
    Param = ?config(param, Config),
    Uri = make_uri(
            Config, 0, <<"guest">>, <<"forgotmypassword">>, <<"%2F">>),
    ShovelArgs = [{<<"src-uri">>,  Uri},
                  {<<"dest-uri">>, [Uri]}] ++ ?config(shovel_args, Config),
    ok = rabbit_ct_broker_helpers:rpc(
           Config, 0, rabbit_runtime_parameters, set,
           [<<"/">>, <<"shovel">>, Param, ShovelArgs, none]),
    await_no_shovel(Config, Param).

application_properties(Config) ->
    Src = ?config(srcq, Config),
    Dest = ?config(destq, Config),
    with_amqp10_session(
      Config,
      fun (Sess) ->
              set_param(Config, ?config(param, Config), ?config(shovel_args, Config)),
              Tag = <<"tag1">>,
              Msg = amqp10_msg:set_application_properties(
                      #{<<"key">> => <<"value">>},
                      amqp10_msg:set_headers(
                        #{durable => true},
                        amqp10_msg:new(Tag, <<"hello">>, false))),
              amqp10_publish_msg(Sess, Src, Tag, Msg),
              MsgRcv = amqp10_expect_one(Sess, Dest),
              AppProps = amqp10_msg:application_properties(MsgRcv),
              ?assertMatch(#{<<"key">> := <<"value">>},
                           AppProps)
      end).

delete_src_queue(Config) ->
    Src = ?config(srcq, Config),
    Dest = ?config(destq, Config),
    Param = ?config(param, Config),
    with_amqp10_session(Config,
      fun (Sess) ->
              set_param(Config, Param, ?config(shovel_args, Config)),
              _ = amqp10_publish_expect(Sess, Src, Dest, <<"hello">>, 1),
              ?awaitMatch({_Name, dynamic, {running, _}, #{forwarded := 1}, _},
                          shovel_status(Config, {<<"/">>, Param}),
                          30000),
              rabbit_ct_broker_helpers:rpc(Config, 0, ?MODULE, delete_queue,
                                           [Src, <<"/">>]),
              ?awaitMatch(0, list_queue_messages(Config, Src), 45_000),
              ?awaitMatch({_Name, dynamic, {running, _}, #{forwarded := 0}, _},
                          shovel_status(Config, {<<"/">>, Param}),
                          30000),
              _ = amqp10_publish_expect(Sess, Src, Dest, <<"hello">>, 1)
      end).

shovel_status(Config) ->
    Src = ?config(srcq, Config),
    Dest = ?config(destq, Config),
    SrcProtocol = ?config(src_protocol, Config),
    DestProtocol = ?config(dest_protocol, Config),
    Param = ?config(param, Config),
    set_param(Config, Param, ?config(shovel_args, Config)),
    Status = shovel_status(Config, {<<"/">>, Param}),
    ?assertMatch({_, dynamic, {running, _}, _, _}, Status),
    {_, dynamic, {running, Info}, _, _} = Status,
    ?assertMatch(SrcProtocol, proplists:get_value(src_protocol, Info)),
    ?assertMatch(DestProtocol, proplists:get_value(dest_protocol, Info)),
    SrcAddress = binary_to_atom(binary:replace(?config(src_address, Config), <<"-">>, <<"_">>)),
    DestAddress = binary_to_atom(binary:replace(?config(dest_address, Config), <<"-">>, <<"_">>)),
    ?assertMatch(Src, proplists:get_value(SrcAddress, Info)),
    ?assertMatch(Dest, proplists:get_value(DestAddress, Info)),
    ok.

change_definition(Config) ->
    Src = ?config(srcq, Config),
    Dest = ?config(destq, Config),
    Param = ?config(param, Config),
    Dest2 = <<Dest/binary,<<"_2">>/binary>>,
    DestAddress = ?config(dest_address, Config),
    with_amqp10_session(Config,
      fun (Sess) ->
              ShovelArgs = ?config(shovel_args, Config),
              set_param(Config, Param, ShovelArgs),
              amqp10_publish_expect(Sess, Src, Dest, <<"hello">>, 1),
              ShovelArgs0 = proplists:delete(DestAddress, ShovelArgs),
              ShovelArgs2 = [{DestAddress, Dest2} | ShovelArgs0],
              set_param(Config, Param, ShovelArgs2),
              amqp10_publish_expect(Sess, Src, Dest2, <<"hello">>, 1),
              amqp10_expect_empty(Sess, Dest),
              clear_param(Config, Param),
              amqp10_publish_expect(Sess, Src, Src, <<"hello">>, 1),
              amqp10_expect_empty(Sess, Dest),
              amqp10_expect_empty(Sess, Dest2)
      end).

disk_alarm(Config) ->
    Src = ?config(srcq, Config),
    Dest = ?config(destq, Config),
    with_amqp10_session(Config,
      fun (Sess) ->
              ShovelArgs = ?config(shovel_args, Config),
              amqp10_publish(Sess, Src, <<"hello">>, 10),
              rabbit_ct_broker_helpers:set_alarm(Config, 0, disk),
              set_param(Config, ?config(param, Config), ShovelArgs),
              amqp10_expect_empty(Sess, Dest),
              rabbit_ct_broker_helpers:clear_alarm(Config, 0, disk),
              amqp10_expect_count(Sess, Dest, 10)
      end).

%%----------------------------------------------------------------------------
maybe_skip_local_protocol(Config) ->
    [Node] = rabbit_ct_broker_helpers:get_node_configs(Config, nodename),
    case rabbit_ct_broker_helpers:enable_feature_flag(
           Config, [Node], 'rabbitmq_4.0.0') of
        ok ->
            Config;
        _ ->
            {skip, "This group requires rabbitmq_4.0.0 feature flag"}
    end.

list_queue_messages(Config, QName) ->
    List = rabbit_ct_broker_helpers:rabbitmqctl_list(
             Config, 0,
             ["list_queues", "name", "messages", "--no-table-headers"]),
    [[_, Messages]] = lists:filter(fun([Q, _]) ->
                                           Q == QName
                                   end, List),
    binary_to_integer(Messages).

delete_queue(Name, VHost) ->
    QName = rabbit_misc:r(VHost, queue, Name),
    case rabbit_amqqueue:lookup(QName) of
        {ok, Q} ->
            {ok, _} = rabbit_amqqueue:delete(Q, false, false, <<"dummy">>);
        _ ->
            ok
    end.

shovel_status(Config, Name) ->
    lists:keyfind(
      Name, 1,
      rabbit_ct_broker_helpers:rpc(Config, 0,
                                   rabbit_shovel_status, status, [])).
