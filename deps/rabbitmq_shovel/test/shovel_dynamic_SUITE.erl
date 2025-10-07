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

-import(shovel_test_utils, [set_param/3,
                            with_amqp10_session/2,
                            amqp10_publish_expect/5]).

-define(PARAM, <<"test">>).

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
     {amqp091, [], tests()},
     {amqp10, [], tests()},
     {local, [], tests()},
     {amqp091_to_amqp10, [], tests()},
     {amqp091_to_local, [], tests()},
     {amqp10_to_amqp091, [], tests()},
     {amqp10_to_local, [], tests()},
     {local_to_amqp091, [], tests()},
     {local_to_amqp10, [], tests()}
    ].

tests() ->
    [
     simple
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
          "dest_queue_down"
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
    ShovelArgs = [{<<"src-protocol">>, ?config(src_protocol, Config0)},
                  {<<"dest-protocol">>, ?config(dest_protocol, Config0)},
                  {?config(src_address, Config0), SrcQ},
                  {?config(dest_address, Config0), DestQ}],
    Config = rabbit_ct_helpers:set_config(
               Config0,
               [{srcq, SrcQ}, {destq, DestQ}, {shovel_args, ShovelArgs}]),
    rabbit_ct_helpers:testcase_started(Config, Testcase).

end_per_testcase(Testcase, Config) ->
    shovel_test_utils:clear_param(Config, ?PARAM),
    rabbit_ct_broker_helpers:rpc(Config, 0, shovel_test_utils, delete_all_queues, []),
    rabbit_ct_helpers:testcase_finished(Config, Testcase).

%% -------------------------------------------------------------------
%% Testcases.
%% -------------------------------------------------------------------
simple(Config) ->
    Name = <<"test">>,
    Src = ?config(srcq, Config),
    Dest = ?config(destq, Config),
    with_amqp10_session(
      Config,
      fun (Sess) ->
              set_param(Config, Name, ?config(shovel_args, Config)),
              amqp10_publish_expect(Sess, Src, Dest, <<"hello">>, 1),
              Status = rabbit_ct_broker_helpers:rpc(Config, 0, rabbit_shovel_status, lookup, [{<<"/">>, Name}]),
              ?assertMatch([_|_], Status),
              ?assertMatch(#{metrics := #{forwarded := 1}}, maps:from_list(Status))
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
