%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2013-2023 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(rjms_topic_selector_SUITE).

-compile(export_all).

-include_lib("common_test/include/ct.hrl").
-include_lib("amqp_client/include/amqp_client.hrl").
-include("rabbit_jms_topic_exchange.hrl").

-import(rabbit_ct_client_helpers, [open_connection_and_channel/1,
                                   close_connection_and_channel/2]).

%% Useful test constructors
-define(BSELECTARG(BinStr), {?RJMS_COMPILED_SELECTOR_ARG, longstr, BinStr}).
-define(BASICMSG(Payload, Hdrs), #'amqp_msg'{props=#'P_basic'{headers=Hdrs}, payload=Payload}).

all() ->
    [
      {group, mnesia_store},
      {group, khepri_store},
      {group, khepri_migration}
    ].

groups() ->
    [
     {mnesia_store, [], [
                         test_topic_selection,
                         restart_with_auto_delete_topic_exchange
                        ]},
     {khepri_store, [], [
                         test_topic_selection,
                         restart_with_auto_delete_topic_exchange
                        ]},
     {khepri_migration, [], [from_mnesia_to_khepri]}
    ].

%% -------------------------------------------------------------------
%% Test suite setup/teardown.
%% -------------------------------------------------------------------

init_per_suite(Config) ->
    rabbit_ct_helpers:log_environment(),
    rabbit_ct_helpers:run_setup_steps(Config).

end_per_suite(Config) ->
    rabbit_ct_helpers:run_teardown_steps(Config).

init_per_group(mnesia_store = Group, Config0) ->
    Config = rabbit_ct_helpers:set_config(Config0, [{metadata_store, mnesia}]),
    init_per_group_common(Group, Config);
init_per_group(khepri_store = Group, Config0) ->
    Config = rabbit_ct_helpers:set_config(
               Config0,
               [{metadata_store, {khepri, [khepri_db]}}]),
    init_per_group_common(Group, Config);
init_per_group(khepri_migration = Group, Config0) ->
    Config = rabbit_ct_helpers:set_config(Config0, [{metadata_store, mnesia}]),
    init_per_group_common(Group, Config).

init_per_group_common(Group, Config) ->
    Config1 = rabbit_ct_helpers:set_config(Config, [
                                                    {rmq_nodename_suffix, Group}
                                                   ]),
    rabbit_ct_helpers:run_setup_steps(Config1,
                                      rabbit_ct_broker_helpers:setup_steps() ++
                                          rabbit_ct_client_helpers:setup_steps()).

end_per_group(_, Config) ->
    rabbit_ct_helpers:run_teardown_steps(Config,
                                         rabbit_ct_client_helpers:teardown_steps() ++
                                             rabbit_ct_broker_helpers:teardown_steps()).

init_per_testcase(Testcase, Config) ->
    rabbit_ct_helpers:testcase_started(Config, Testcase).

end_per_testcase(Testcase, Config) ->
    rabbit_ct_helpers:testcase_finished(Config, Testcase).

%% -------------------------------------------------------------------
%% Test cases.
%% -------------------------------------------------------------------

test_topic_selection(Config) ->
    {Connection, Channel} = open_connection_and_channel(Config),
    #'confirm.select_ok'{} = amqp_channel:call(Channel, #'confirm.select'{}),

    Exchange = declare_rjms_exchange(Channel, "rjms_test_topic_selector_exchange", false, false, []),

    %% Declare a queue and bind it
    Q = declare_queue(Channel),
    bind_queue(Channel, Q, Exchange, <<"select-key">>, [?BSELECTARG(<<"{ident, <<\"boolVal\">>}.">>)]),

    publish_two_messages(Channel, Exchange, <<"select-key">>),
    amqp_channel:wait_for_confirms(Channel, 5),

    get_and_check(Channel, Q, 0, <<"true">>),

    amqp_channel:call(Channel, #'exchange.delete'{exchange = Exchange}),
    close_connection_and_channel(Connection, Channel),
    ok.

restart_with_auto_delete_topic_exchange(Config) ->
    [Server | _] = rabbit_ct_broker_helpers:get_node_configs(Config, nodename),

    {_Connection, Channel} = open_connection_and_channel(Config),
    #'confirm.select_ok'{} = amqp_channel:call(Channel, #'confirm.select'{}),

    Exchange = declare_rjms_exchange(Channel, "restart_with_auto_delete_topic_exchange", true, true, []),
    %% Declare a queue and bind it
    %% Q = declare_queue(Channel),
    #'queue.declare_ok'{queue = Q} = amqp_channel:call(Channel, #'queue.declare'{durable = true}),
    bind_queue(Channel, Q, Exchange, <<"select-key">>, [?BSELECTARG(<<"{ident, <<\"boolVal\">>}.">>)]),
    ok = rabbit_control_helper:command(stop_app, Server),
    ok = rabbit_control_helper:command(start_app, Server).

from_mnesia_to_khepri(Config) ->
    {Connection, Channel} = open_connection_and_channel(Config),
    #'confirm.select_ok'{} = amqp_channel:call(Channel, #'confirm.select'{}),

    Exchange = declare_rjms_exchange(Channel, "rjms_test_topic_selector_exchange", false, false, []),

    %% Declare a queue and bind it
    Q = declare_queue(Channel),
    bind_queue(Channel, Q, Exchange, <<"select-key">>, [?BSELECTARG(<<"{ident, <<\"boolVal\">>}.">>)]),

    case rabbit_ct_broker_helpers:enable_feature_flag(Config, khepri_db) of
        ok ->
            case rabbit_ct_broker_helpers:enable_feature_flag(Config, rabbit_jms_topic_exchange_raft_based_metadata_store) of
                ok ->
                    publish_two_messages(Channel, Exchange, <<"select-key">>),
                    amqp_channel:wait_for_confirms(Channel, 5),
                    get_and_check(Channel, Q, 0, <<"true">>),
                    close_connection_and_channel(Connection, Channel),
                    ok;
                Skip ->
                    Skip
            end;
        Skip ->
            Skip
    end.

%% -------------------------------------------------------------------
%% Helpers.
%% -------------------------------------------------------------------

%% Declare a rjms_topic_selector exchange, with args
declare_rjms_exchange(Ch, XNameStr, Durable, AutoDelete, XArgs) ->
    Exchange = list_to_binary(XNameStr),
    Decl = #'exchange.declare'{ exchange = Exchange
                              , type = <<"x-jms-topic">>
                              , durable = Durable
                              , auto_delete = AutoDelete
                              , arguments = XArgs },
    #'exchange.declare_ok'{} = amqp_channel:call(Ch, Decl),
    Exchange.

%% Bind a selector queue to an exchange
bind_queue(Ch, Q, Ex, RKey, Args) ->
    Binding = #'queue.bind'{ queue       = Q
                           , exchange    = Ex
                           , routing_key = RKey
                           , arguments   = Args
                           },
    #'queue.bind_ok'{} = amqp_channel:call(Ch, Binding),
    ok.

%% Declare a queue, return Q name (as binary)
declare_queue(Ch) ->
    #'queue.declare_ok'{queue = Q} = amqp_channel:call(Ch, #'queue.declare'{}),
    Q.

%% Get message from Q and check remaining and payload.
get_and_check(Channel, Queue, ExpectedRemaining, ExpectedPayload) ->
    Get = #'basic.get'{queue = Queue},
    {#'basic.get_ok'{delivery_tag = Tag, message_count = Remaining}, Content}
      = amqp_channel:call(Channel, Get),
    amqp_channel:cast(Channel, #'basic.ack'{delivery_tag = Tag}),

    ExpectedRemaining = Remaining,
    ExpectedPayload = Content#amqp_msg.payload,
    ok.

publish_two_messages(Chan, Exch, RoutingKey) ->
    Publish = #'basic.publish'{exchange = Exch, routing_key = RoutingKey},
    amqp_channel:cast(Chan, Publish, ?BASICMSG(<<"false">>, [{<<"boolVal">>, 'bool', false}])),
    amqp_channel:cast(Chan, Publish, ?BASICMSG(<<"true">>, [{<<"boolVal">>, 'bool', true}])),
    ok.
