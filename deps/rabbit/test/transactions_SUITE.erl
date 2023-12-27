%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2023 VMware, Inc. or its affiliates.  All rights reserved.
%%
-module(transactions_SUITE).
-compile(export_all).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("amqp_client/include/amqp_client.hrl").


all() ->
    [
      {group, non_parallel_tests}
    ].

groups() ->
    [
      {non_parallel_tests, [], [
                                published_visible_after_commit,
                                return_after_commit 
                               ]}
    ].

%% -------------------------------------------------------------------
%% Testsuite setup/teardown.
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
    Group = proplists:get_value(name, ?config(tc_group_properties, Config)),
    QName = rabbit_data_coercion:to_binary(io_lib:format("~p_~tp", [Group, Testcase])),
    {Conn, Ch} = rabbit_ct_client_helpers:open_connection_and_channel(Config, 0),
    declare_queue(Ch, QName),
    rabbit_ct_client_helpers:close_connection_and_channel(Conn, Ch),
    Config1 = rabbit_ct_helpers:set_config(Config, [{queue_name, QName}]),
    rabbit_ct_helpers:testcase_started(Config1, Testcase).

end_per_testcase(Testcase, Config) ->
    QName = queue_name(Config),
    {Conn, Ch} = rabbit_ct_client_helpers:open_connection_and_channel(Config, 0),
    delete_queue(Ch, QName),
    rabbit_ct_client_helpers:close_connection_and_channel(Conn, Ch),
    rabbit_ct_helpers:testcase_finished(Config, Testcase).

%% -------------------------------------------------------------------
%% Testcases.
%% -------------------------------------------------------------------

published_visible_after_commit(Config) ->
    QName = queue_name(Config),
    {Conn, Ch} = rabbit_ct_client_helpers:open_connection_and_channel(Config, 0),
    #'tx.select_ok'{} = amqp_channel:call(Ch, #'tx.select'{}),
    publish(Ch, QName, <<"msg">>),
    ?assertMatch(#'basic.get_empty'{}, get(Ch, QName)),
    amqp_channel:call(Ch, #'tx.commit'{}),
    ?assertMatch({#'basic.get_ok'{}, _}, get(Ch, QName)),
    rabbit_ct_client_helpers:close_connection_and_channel(Conn, Ch),
    ok.

return_after_commit(Config) ->
    QName0 = queue_name(Config),
    QName = <<QName0/binary, "foo">>,
    {Conn, Ch} = rabbit_ct_client_helpers:open_connection_and_channel(Config, 0),
    amqp_channel:register_return_handler(Ch, self()),
    #'tx.select_ok'{} = amqp_channel:call(Ch, #'tx.select'{}),
    publish(Ch, QName, <<"msg">>, true),
    Result = receive
                 {#'basic.return'{}, _} ->
                     return_before_commit
             after 1000 ->
                       return_after_commit
             end,
    ?assertEqual(return_after_commit, Result),
    #'tx.commit_ok'{} = amqp_channel:call(Ch, #'tx.commit'{}),
    rabbit_ct_client_helpers:close_connection_and_channel(Conn, Ch),
    ok.

queue_name(Config) ->
    ?config(queue_name, Config).

delete_queue(Ch, QName) ->
    #'queue.delete_ok'{} = amqp_channel:call(Ch, #'queue.delete'{queue = QName}).

declare_queue(Ch, QName) ->
    #'queue.declare_ok'{} = amqp_channel:call(Ch, #'queue.declare'{queue = QName,
                                                                   durable = false,
                                                                   exclusive = false,
                                                                   auto_delete = false}).
publish(Ch, QName, Payload) ->
    publish(Ch, QName, Payload, false).

publish(Ch, QName, Payload, Mandatory) ->
    amqp_channel:call(Ch, #'basic.publish'{routing_key = QName, mandatory = Mandatory},
                      #amqp_msg{payload = Payload}).

get(Ch, QName) ->
    get(Ch, QName, 10).

get(_, _, 0) ->
    #'basic.get_empty'{};
get(Ch, QName, Attempt) ->
    case amqp_channel:call(Ch, #'basic.get'{queue = QName, no_ack = true}) of
        #'basic.get_empty'{} ->
            timer:sleep(100),
            get(Ch, QName, Attempt - 1);
        GetOk ->
            GetOk
    end.
