%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2021 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(rabbit_hash_exchange_SUITE).

-compile(export_all).

-include_lib("common_test/include/ct.hrl").
-include_lib("amqp_client/include/amqp_client.hrl").
-include_lib("eunit/include/eunit.hrl").

all() ->
    [
      {group, non_parallel_tests}
    ].

groups() ->
    [
      {non_parallel_tests, [], [
                                routed_to_zero_queue_test,
                                routed_to_one_queue_test,
                                routed_to_many_queue_test
                               ]}
    ].

%% -------------------------------------------------------------------
%% Test suite setup/teardown
%% -------------------------------------------------------------------

init_per_suite(Config) ->
    rabbit_ct_helpers:log_environment(),
    Config1 = rabbit_ct_helpers:set_config(Config, [
        {rmq_nodename_suffix, ?MODULE}
      ]),
    rabbit_ct_helpers:run_setup_steps(Config1,
      rabbit_ct_broker_helpers:setup_steps() ++
      rabbit_ct_client_helpers:setup_steps()).

end_per_suite(Config) ->
    rabbit_ct_helpers:run_teardown_steps(Config,
      rabbit_ct_client_helpers:teardown_steps() ++
      rabbit_ct_broker_helpers:teardown_steps()).

init_per_group(_, Config) ->
    Config.

end_per_group(_, Config) ->
    Config.

init_per_testcase(Testcase, Config) ->
    TestCaseName = rabbit_ct_helpers:config_to_testcase_name(Config, Testcase),
    Config1 = rabbit_ct_helpers:set_config(Config, {test_resource_name,
                                                    re:replace(TestCaseName, "/", "-", [global, {return, list}])}),
    rabbit_ct_helpers:testcase_started(Config1, Testcase).

end_per_testcase(Testcase, Config) ->
    rabbit_ct_helpers:testcase_finished(Config, Testcase).

%% -------------------------------------------------------------------
%% Test cases
%% -------------------------------------------------------------------

routed_to_zero_queue_test(Config) ->
    test0(Config, fun () ->
                  #'basic.publish'{exchange = make_exchange_name(Config, "0"), routing_key = rnd()}
          end,
          fun() ->
                  #amqp_msg{props = #'P_basic'{}, payload = <<>>}
          end, [], 5, 0),

    passed.

routed_to_one_queue_test(Config) ->
    test0(Config, fun () ->
                  #'basic.publish'{exchange = make_exchange_name(Config, "0"), routing_key = rnd()}
          end,
          fun() ->
                  #amqp_msg{props = #'P_basic'{}, payload = <<>>}
          end, [<<"q1">>, <<"q2">>, <<"q3">>], 1, 1),

    passed.

routed_to_many_queue_test(Config) ->
    test0(Config, fun () ->
                  #'basic.publish'{exchange = make_exchange_name(Config, "0"), routing_key = rnd()}
          end,
          fun() ->
                  #amqp_msg{props = #'P_basic'{}, payload = <<>>}
          end, [<<"q1">>, <<"q2">>, <<"q3">>], 5, 5),

    passed.

test0(Config, MakeMethod, MakeMsg, Queues, MsgCount, Count) ->
    {Conn, Chan} = rabbit_ct_client_helpers:open_connection_and_channel(Config, 0),
    E = make_exchange_name(Config, "0"),

    #'exchange.declare_ok'{} =
        amqp_channel:call(Chan,
                          #'exchange.declare' {
                            exchange = E,
                            type = <<"x-modulus-hash">>,
                            auto_delete = true
                           }),
    [#'queue.declare_ok'{} =
         amqp_channel:call(Chan, #'queue.declare' {
                             queue = Q, exclusive = true }) || Q <- Queues],
    [#'queue.bind_ok'{} =
         amqp_channel:call(Chan, #'queue.bind'{queue = Q,
                                               exchange = E,
                                               routing_key = <<"">>})
     || Q <- Queues],

    amqp_channel:call(Chan, #'confirm.select'{}),

    [amqp_channel:call(Chan,
                       MakeMethod(),
                       MakeMsg()) || _ <- lists:duplicate(MsgCount, const)],

    % ensure that the messages have been delivered to the queues before asking
    % for the message count
    amqp_channel:wait_for_confirms_or_die(Chan),

    Counts =
        [begin
             #'queue.declare_ok'{message_count = M} =
                 amqp_channel:call(Chan, #'queue.declare' {queue     = Q,
                                                           exclusive = true }),
             M
         end || Q <- Queues],

    ?assertEqual(Count, lists:sum(Counts)),

    amqp_channel:call(Chan, #'exchange.delete' { exchange = E }),
    [amqp_channel:call(Chan, #'queue.delete' { queue = Q }) || Q <- Queues],

    rabbit_ct_client_helpers:close_connection_and_channel(Conn, Chan),
    ok.

rnd() ->
    list_to_binary(integer_to_list(rand:uniform(1000000))).

make_exchange_name(Config, Suffix) ->
    B = rabbit_ct_helpers:get_config(Config, test_resource_name),
    erlang:list_to_binary("x-" ++ B ++ "-" ++ Suffix).
