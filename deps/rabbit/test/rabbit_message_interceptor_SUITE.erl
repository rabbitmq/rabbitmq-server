%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2018-2023 VMware, Inc. or its affiliates.  All rights reserved.

-module(rabbit_message_interceptor_SUITE).

-include_lib("eunit/include/eunit.hrl").
-include_lib("amqp_client/include/amqp_client.hrl").

-compile([nowarn_export_all, export_all]).

-import(rabbit_ct_helpers, [eventually/1]).

all() ->
    [
     {group, tests}
    ].

groups() ->
    [
     {tests, [shuffle], [headers_overwrite,
                         headers_no_overwrite
                        ]}
    ].

init_per_suite(Config) ->
    rabbit_ct_helpers:log_environment(),
    rabbit_ct_helpers:run_setup_steps(Config).

end_per_suite(Config) ->
    rabbit_ct_helpers:run_teardown_steps(Config).

init_per_testcase(Testcase, Config0) ->
    Config1 = rabbit_ct_helpers:set_config(
                Config0, [{rmq_nodename_suffix, Testcase}]),
    Overwrite = case Testcase of
                    headers_overwrite -> true;
                    headers_no_overwrite -> false
                end,
    Val = maps:to_list(
            maps:from_keys([set_header_timestamp,
                            set_header_routing_node],
                           Overwrite)),
    Config = rabbit_ct_helpers:merge_app_env(
               Config1, {rabbit, [{incoming_message_interceptors, Val}]}),
    rabbit_ct_helpers:run_steps(
      Config,
      rabbit_ct_broker_helpers:setup_steps() ++
      rabbit_ct_client_helpers:setup_steps()).

end_per_testcase(Testcase, Config0) ->
    Config = rabbit_ct_helpers:testcase_finished(Config0, Testcase),
    rabbit_ct_helpers:run_teardown_steps(
      Config,
      rabbit_ct_client_helpers:teardown_steps() ++
      rabbit_ct_broker_helpers:teardown_steps()).

headers_overwrite(Config) ->
    headers(true, Config).

headers_no_overwrite(Config) ->
    headers(false, Config).

headers(Overwrite, Config) ->
    Server = atom_to_binary(rabbit_ct_broker_helpers:get_node_config(Config, 0, nodename)),
    Payload = QName = atom_to_binary(?FUNCTION_NAME),
    NowSecs = os:system_time(second),
    NowMs = os:system_time(millisecond),
    Ch = rabbit_ct_client_helpers:open_channel(Config),
    #'queue.declare_ok'{} = amqp_channel:call(Ch, #'queue.declare'{queue = QName}),
    amqp_channel:call(Ch, #'basic.publish'{routing_key = QName},
                      #amqp_msg{payload = Payload}),
    AssertHeaders =
    fun() ->
            eventually(
              ?_assertMatch(
                 {#'basic.get_ok'{},
                  #amqp_msg{payload = Payload,
                            props = #'P_basic'{
                                       timestamp = Secs,
                                       headers = [{<<"timestamp_in_ms">>, long, Ms},
                                                  {<<"x-routed-by">>, longstr, Server}]
                                      }}}
                   when Ms < NowMs + 4000 andalso
                        Ms > NowMs - 4000 andalso
                        Secs < NowSecs + 4 andalso
                        Secs > NowSecs - 4,
                 amqp_channel:call(Ch, #'basic.get'{queue = QName})))
    end,
    AssertHeaders(),

    Msg = #amqp_msg{payload = Payload,
                    props = #'P_basic'{
                               timestamp = 1,
                               headers = [{<<"timestamp_in_ms">>, long, 1000},
                                          {<<"x-routed-by">>, longstr, <<"rabbit@my-node">>}]
                              }},
    amqp_channel:call(Ch, #'basic.publish'{routing_key = QName}, Msg),
    case Overwrite of
        true ->
            AssertHeaders();
        false ->
            eventually(
              ?_assertMatch(
                 {#'basic.get_ok'{}, Msg},
                 amqp_channel:call(Ch, #'basic.get'{queue = QName})))
    end,

    #'queue.delete_ok'{} = amqp_channel:call(Ch, #'queue.delete'{queue = QName}),
    ok.
