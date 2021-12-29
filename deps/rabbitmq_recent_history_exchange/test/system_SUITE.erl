%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2021 VMware, Inc. or its affiliates. All rights reserved.

-module(system_SUITE).

-compile(export_all).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("amqp_client/include/amqp_client.hrl").
-include("rabbit_recent_history.hrl").

all() ->
    [
      {group, non_parallel_tests}
    ].

groups() ->
    [
      {non_parallel_tests, [], [
                                default_length_test,
                                length_argument_test,
                                wrong_argument_type_test,
                                no_store_test,
                                e2e_test,
                                multinode_test
                               ]}
    ].

%% -------------------------------------------------------------------
%% Test suite setup/teardown.
%% -------------------------------------------------------------------

init_per_suite(Config) ->
    inets:start(),
    rabbit_ct_helpers:log_environment(),
    Config1 = rabbit_ct_helpers:set_config(Config, [
        {rmq_nodename_suffix, ?MODULE},
        {rmq_nodes_count,     2}
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
    rabbit_ct_helpers:set_config(Config, {test_resource_name,
                                          re:replace(TestCaseName, "/", "-", [global, {return, list}])}).

end_per_testcase(_Testcase, Config) ->
    Config.

%% -------------------------------------------------------------------
%% Test cases.
%% -------------------------------------------------------------------

default_length_test(Config) ->
    Qs = qs(),
    test0(Config, fun () ->
                  #'basic.publish'{exchange = make_exchange_name(Config, "0")}
          end,
          fun() ->
                  #amqp_msg{props = #'P_basic'{}, payload = <<>>}
          end, [], Qs, 100, length(Qs) * ?KEEP_NB).

length_argument_test(Config) ->
    Qs = qs(),
    test0(Config, fun () ->
                  #'basic.publish'{exchange = make_exchange_name(Config, "0")}
          end,
          fun() ->
                  #amqp_msg{props = #'P_basic'{}, payload = <<>>}
          end, [{<<"x-recent-history-length">>, long, 30}], Qs, 100, length(Qs) * 30).

wrong_argument_type_test(Config) ->
    wrong_argument_type_test0(Config, -30),
    wrong_argument_type_test0(Config, 0).


no_store_test(Config) ->
    Qs = qs(),
    test0(Config, fun () ->
                  #'basic.publish'{exchange = make_exchange_name(Config, "0")}
          end,
          fun() ->
                  H = [{<<"x-recent-history-no-store">>, bool, true}],
                  #amqp_msg{props = #'P_basic'{headers = H}, payload = <<>>}
          end, [], Qs, 100, 0).

e2e_test(Config) ->
    MsgCount = 10,

    {Conn, Chan} = rabbit_ct_client_helpers:open_connection_and_channel(Config, 0),

    #'exchange.declare_ok'{} =
        amqp_channel:call(Chan,
                          #'exchange.declare' {
                            exchange = make_exchange_name(Config, "1"),
                            type = <<"x-recent-history">>,
                            auto_delete = true
                           }),

    #'exchange.declare_ok'{} =
        amqp_channel:call(Chan,
                          #'exchange.declare' {
                            exchange = make_exchange_name(Config, "2"),
                            type = <<"direct">>,
                            auto_delete = true
                           }),

    #'queue.declare_ok'{queue = Q} =
        amqp_channel:call(Chan, #'queue.declare' {
                                   queue     = <<"q">>
                                  }),

    #'queue.bind_ok'{} =
        amqp_channel:call(Chan, #'queue.bind' {
                                   queue = Q,
                                   exchange = make_exchange_name(Config, "2"),
                                   routing_key = <<"">>
                                  }),

    #'tx.select_ok'{} = amqp_channel:call(Chan, #'tx.select'{}),
    [amqp_channel:call(Chan,
                       #'basic.publish'{exchange = make_exchange_name(Config, "1")},
                       #amqp_msg{props = #'P_basic'{}, payload = <<>>}) ||
        _ <- lists:duplicate(MsgCount, const)],
    amqp_channel:call(Chan, #'tx.commit'{}),

    amqp_channel:call(Chan,
                      #'exchange.bind' {
                         source      = make_exchange_name(Config, "1"),
                         destination = make_exchange_name(Config, "2"),
                         routing_key = <<"">>
                        }),

    #'queue.declare_ok'{message_count = Count, queue = Q} =
        amqp_channel:call(Chan, #'queue.declare' {
                                   passive   = true,
                                   queue     = Q
                                  }),

    ?assertEqual(MsgCount, Count),

    amqp_channel:call(Chan, #'exchange.delete' { exchange = make_exchange_name(Config, "1") }),
    amqp_channel:call(Chan, #'exchange.delete' { exchange = make_exchange_name(Config, "2") }),
    amqp_channel:call(Chan, #'queue.delete' { queue = Q }),

    rabbit_ct_client_helpers:close_connection_and_channel(Conn, Chan),
    ok.

multinode_test(Config) ->
    {Conn, Chan} = rabbit_ct_client_helpers:open_connection_and_channel(Config, 1),

    #'exchange.declare_ok'{} =
        amqp_channel:call(Chan,
                          #'exchange.declare' {
                            exchange = make_exchange_name(Config, "1"),
                            type = <<"x-recent-history">>,
                            auto_delete = false
                           }),

    #'queue.declare_ok'{queue = Q} =
        amqp_channel:call(Chan, #'queue.declare' {
                                   queue     = <<"q">>
                                  }),

    #'queue.bind_ok'{} =
        amqp_channel:call(Chan, #'queue.bind' {
                                   queue = Q,
                                   exchange = make_exchange_name(Config, "1"),
                                   routing_key = <<"">>
                                  }),

    amqp_channel:call(Chan, #'queue.delete' { queue = Q }),
    rabbit_ct_client_helpers:close_connection_and_channel(Conn, Chan),

    rabbit_ct_broker_helpers:restart_broker(Config, 1),

    {Conn2, Chan2} = rabbit_ct_client_helpers:open_connection_and_channel(Config, 0),

    #'queue.declare_ok'{queue = Q2} =
        amqp_channel:call(Chan2, #'queue.declare' {
                                   queue     = <<"q2">>
                                  }),

    #'queue.bind_ok'{} =
        amqp_channel:call(Chan2, #'queue.bind' {
                                   queue = Q2,
                                   exchange = make_exchange_name(Config, "1"),
                                   routing_key = <<"">>
                                  }),

    amqp_channel:call(Chan2, #'exchange.delete' { exchange = make_exchange_name(Config, "2") }),
    amqp_channel:call(Chan2, #'queue.delete' { queue = Q2 }),

    rabbit_ct_client_helpers:close_connection_and_channel(Conn2, Chan2),
    ok.

test0(Config, MakeMethod, MakeMsg, DeclareArgs, Queues, MsgCount, ExpectedCount) ->
    Chan = rabbit_ct_client_helpers:open_channel(Config),
    #'exchange.declare_ok'{} =
        amqp_channel:call(Chan,
                          #'exchange.declare' {
                            exchange = make_exchange_name(Config, "0"),
                            type = <<"x-recent-history">>,
                            auto_delete = true,
                            arguments = DeclareArgs
                           }),

    #'tx.select_ok'{} = amqp_channel:call(Chan, #'tx.select'{}),
    [amqp_channel:call(Chan,
                       MakeMethod(),
                       MakeMsg()) || _ <- lists:duplicate(MsgCount, const)],
    amqp_channel:call(Chan, #'tx.commit'{}),

    [#'queue.declare_ok'{} =
         amqp_channel:call(Chan, #'queue.declare' {
                             queue = Q, exclusive = true }) || Q <- Queues],

    [#'queue.bind_ok'{} =
         amqp_channel:call(Chan, #'queue.bind' { queue = Q,
                                                 exchange = make_exchange_name(Config, "0"),
                                                 routing_key = <<"">>})
     || Q <- Queues],

    %% Wait a few seconds for all messages to be queued.
    timer:sleep(3000),

    Counts =
        [begin
            #'queue.declare_ok'{message_count = M} =
                 amqp_channel:call(Chan, #'queue.declare' {queue     = Q,
                                                           exclusive = true }),
             M
         end || Q <- Queues],

    ?assertEqual(ExpectedCount, lists:sum(Counts)),

    amqp_channel:call(Chan, #'exchange.delete' { exchange = make_exchange_name(Config, "0") }),
    [amqp_channel:call(Chan, #'queue.delete' { queue = Q }) || Q <- Queues],
    rabbit_ct_client_helpers:close_channel(Chan),

    ok.

wrong_argument_type_test0(Config, Length) ->
    Conn = rabbit_ct_client_helpers:open_unmanaged_connection(Config),
    Chan = amqp_connection:open_channel(Conn),
    DeclareArgs = [{<<"x-recent-history-length">>, long, Length}],
    process_flag(trap_exit, true),
    ?assertExit(_, amqp_channel:call(Chan,
                          #'exchange.declare' {
                            exchange = make_exchange_name(Config, "0"),
                            type = <<"x-recent-history">>,
                            auto_delete = true,
                            arguments = DeclareArgs
                            })),
    amqp_connection:close(Conn),
    ok.

qs() ->
    [<<"q0">>, <<"q1">>, <<"q2">>, <<"q3">>].

make_exchange_name(Config, Suffix) ->
    B = rabbit_ct_helpers:get_config(Config, test_resource_name),
    erlang:list_to_binary("x-" ++ B ++ "-" ++ Suffix).
