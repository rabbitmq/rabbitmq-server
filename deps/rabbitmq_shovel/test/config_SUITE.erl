%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2022 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(config_SUITE).

-compile(export_all).

-include_lib("eunit/include/eunit.hrl").

-define(EXCHANGE,    <<"test_exchange">>).
-define(TO_SHOVEL,   <<"to_the_shovel">>).
-define(FROM_SHOVEL, <<"from_the_shovel">>).
-define(UNSHOVELLED, <<"unshovelled">>).
-define(SHOVELLED,   <<"shovelled">>).
-define(TIMEOUT,     1000).

all() ->
    [
      {group, tests}
    ].

groups() ->
    [
      {tests, [parallel], [
          parse_amqp091,
          parse_amqp10_mixed
        ]}
    ].

%% -------------------------------------------------------------------
%% Testsuite setup/teardown.
%% -------------------------------------------------------------------

init_per_suite(Config) ->
    Config.

end_per_suite(Config) ->
    Config.

init_per_group(_, Config) ->
    Config.

end_per_group(_, Config) ->
    Config.

init_per_testcase(_Testcase, Config) -> Config.

end_per_testcase(_Testcase, Config) -> Config.


%% -------------------------------------------------------------------
%% Testcases.
%% -------------------------------------------------------------------

parse_amqp091(_Config) ->
    Amqp091Src = {source, [{protocol, amqp091},
                           {uris, ["ampq://myhost:5672/vhost"]},
                           {declarations, []},
                           {queue, <<"the-queue">>},
                           {delete_after, never},
                           {prefetch_count, 10}]},
    Amqp091Dst = {destination, [{protocol, amqp091},
                                {uris, ["ampq://myhost:5672"]},
                                {declarations, []},
                                {publish_properties, [{delivery_mode, 1}]},
                                {publish_fields, []},
                                {add_forward_headers, true}]},
    In = [Amqp091Src,
          Amqp091Dst,
          {ack_mode, on_confirm},
          {reconnect_delay, 2}],

    ?assertMatch(
       {ok, #{name := my_shovel,
              ack_mode := on_confirm,
              reconnect_delay := 2,
              dest := #{module := rabbit_amqp091_shovel,
                        uris := ["ampq://myhost:5672"],
                        fields_fun := _PubFields,
                        props_fun := _PubProps,
                        resource_decl := _DDecl,
                        add_timestamp_header := false,
                        add_forward_headers := true},
              source := #{module := rabbit_amqp091_shovel,
                          uris := ["ampq://myhost:5672/vhost"],
                          queue := <<"the-queue">>,
                          prefetch_count := 10,
                          delete_after := never,
                          resource_decl := _SDecl}}},
        rabbit_shovel_config:parse(my_shovel, In)),
    ok.

parse_amqp10_mixed(_Config) ->
    Amqp10Src = {source, [{protocol, amqp10},
                          {uris, ["ampq://myotherhost:5672"]},
                          {source_address, <<"the-queue">>}
                         ]},
    Amqp10Dst = {destination, [{protocol, amqp10},
                               {uris, ["ampq://myhost:5672"]},
                               {target_address, <<"targe-queue">>},
                               {message_annotations, [{soma_ann, <<"some-info">>}]},
                               {properties, [{user_id, <<"some-user">>}]},
                               {application_properties, [{app_prop_key, <<"app_prop_value">>}]},
                               {add_forward_headers, true}
                              ]},
    In = [Amqp10Src,
          Amqp10Dst,
          {ack_mode, on_confirm},
          {reconnect_delay, 2}],

    ?assertMatch(
       {ok, #{name := my_shovel,
              ack_mode := on_confirm,
              source := #{module := rabbit_amqp10_shovel,
                          uris := ["ampq://myotherhost:5672"],
                          source_address := <<"the-queue">>
                          },
              dest := #{module := rabbit_amqp10_shovel,
                        uris := ["ampq://myhost:5672"],
                        target_address := <<"targe-queue">>,
                        properties := #{user_id := <<"some-user">>},
                        application_properties := #{app_prop_key := <<"app_prop_value">>},
                        message_annotations := #{soma_ann := <<"some-info">>},
                        add_forward_headers := true}}},
        rabbit_shovel_config:parse(my_shovel, In)),
    ok.
