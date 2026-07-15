%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2026 Broadcom. All Rights Reserved. The term "Broadcom" refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
%%

-module(rjms_topic_selector_like_SUITE).

-compile(export_all).

-include_lib("eunit/include/eunit.hrl").
-include_lib("amqp_client/include/amqp_client.hrl").
-include("rabbit_jms_topic_exchange.hrl").

-import(rabbit_ct_client_helpers, [open_connection_and_channel/1,
                                   close_connection_and_channel/2]).

-define(BSELECTARG(BinStr), {?RJMS_COMPILED_SELECTOR_ARG, longstr, BinStr}).
-define(BASICMSG(Payload, Hdrs), #'amqp_msg'{props=#'P_basic'{headers=Hdrs}, payload=Payload}).

-define(CONFIRM_TIME_LIMIT_MS, 5000).

all() ->
    [
      {group, tests}
    ].

groups() ->
    [
     {tests, [], [
                  wildcard_heavy_selector_does_not_hang_routing
                 ]}
    ].

%% -------------------------------------------------------------------
%% Test suite setup/teardown.
%% -------------------------------------------------------------------

init_per_suite(Config) ->
    rabbit_ct_helpers:log_environment(),
    rabbit_ct_helpers:run_setup_steps(Config).

end_per_suite(Config) ->
    rabbit_ct_helpers:run_teardown_steps(Config).

init_per_group(Group, Config) ->
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

wildcard_heavy_selector_does_not_hang_routing(Config) ->
    {Connection, Channel} = open_connection_and_channel(Config),
    #'confirm.select_ok'{} = amqp_channel:call(Channel, #'confirm.select'{}),

    Exchange = declare_rjms_exchange(Channel, "wildcard_heavy_selector_exchange"),
    Q = declare_queue(Channel),
    Selector = compiled_selector({'like', {ident, <<"p">>}, wildcard_heavy_pattern(), no_escape}),
    bind_queue(Channel, Q, Exchange, <<"select-key">>, [?BSELECTARG(Selector)]),

    Subject = binary:copy(<<"a">>, 3000),
    Publish = #'basic.publish'{exchange = Exchange, routing_key = <<"select-key">>},
    amqp_channel:cast(Channel, Publish, ?BASICMSG(<<"payload">>, [{<<"p">>, longstr, Subject}])),

    {ElapsedUs, Confirmed} = timer:tc(fun() -> amqp_channel:wait_for_confirms(Channel, 5) end),
    ?assertEqual(true, Confirmed),
    ?assert(ElapsedUs < ?CONFIRM_TIME_LIMIT_MS * 1000),

    #'basic.get_empty'{} = amqp_channel:call(Channel, #'basic.get'{queue = Q}),

    amqp_channel:call(Channel, #'exchange.delete'{exchange = Exchange}),
    close_connection_and_channel(Connection, Channel),
    ok.

%% -------------------------------------------------------------------
%% Helpers.
%% -------------------------------------------------------------------

wildcard_heavy_pattern() ->
    iolist_to_binary([lists:duplicate(300, "%_"), "X"]).

%% Renders an already-parsed selector term back into the concrete syntax
%% sjx_parser accepts, so a generated pattern doesn't need hand quoting.
compiled_selector(Term) ->
    list_to_binary(io_lib:format("~p.", [Term])).

declare_rjms_exchange(Ch, XNameStr) ->
    Exchange = list_to_binary(XNameStr),
    Decl = #'exchange.declare'{ exchange = Exchange
                              , type = <<"x-jms-topic">>
                              , durable = false
                              , auto_delete = false
                              , arguments = [] },
    #'exchange.declare_ok'{} = amqp_channel:call(Ch, Decl),
    Exchange.

bind_queue(Ch, Q, Ex, RKey, Args) ->
    Binding = #'queue.bind'{ queue       = Q
                           , exchange    = Ex
                           , routing_key = RKey
                           , arguments   = Args
                           },
    #'queue.bind_ok'{} = amqp_channel:call(Ch, Binding),
    ok.

declare_queue(Ch) ->
    #'queue.declare_ok'{queue = Q} = amqp_channel:call(Ch, #'queue.declare'{durable = true}),
    Q.
