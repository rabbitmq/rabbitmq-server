%% The contents of this file are subject to the Mozilla Public License
%% Version 1.1 (the "License"); you may not use this file except in
%% compliance with the License. You may obtain a copy of the License
%% at http://www.mozilla.org/MPL/
%%
%% Software distributed under the License is distributed on an "AS IS"
%% basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See
%% the License for the specific language governing rights and
%% limitations under the License.
%%
%% The Original Code is RabbitMQ.
%%
%% The Initial Developer of the Original Code is GoPivotal, Inc.
%% Copyright (c) 2013 GoPivotal, Inc.  All rights reserved.
%%

-module(rjms_topic_selector_tests).

-export([all_tests/0]).
-include_lib("amqp_client/include/amqp_client.hrl").
-include("rabbit_jms_topic_exchange.hrl").

%% Useful test constructors
-define(XPOLICYARG(Policy), {?RJMS_POLICY_ARG, longstr, Policy}).
-define(BSELECTARG(BinStr), {?RJMS_SELECTOR_ARG, longstr, BinStr}).
-define(BASICMSG(Payload, Hdrs), #'amqp_msg'{props=#'P_basic'{headers=Hdrs}, payload=Payload}).

all_tests() ->
    test_default_topic_selection(),
    test_topic_selection(),
    test_queue_selection(),
    test_multiple_queue_selection(),
    ok.

test_topic_selection() ->
    {Connection, Channel} = open_connection_and_channel(),

    Exchange = declare_rjms_exchange(Channel, "rjms_test_topic_selector_exchange", [?XPOLICYARG(<<"jms-topic">>)]),

    %% Declare a queue and bind it
    Q = declare_queue(Channel),
    bind_queue(Channel, Q, Exchange, <<"select-key">>, [?BSELECTARG(<<"boolVal">>)]),

    publish_two_messages(Channel, Exchange, <<"select-key">>),

    get_and_check(Channel, Q, 0, <<"true">>),

    close_channel_and_connection(Connection, Channel),
    ok.

test_default_topic_selection() ->
    {Connection, Channel} = open_connection_and_channel(),

    Exchange = declare_rjms_exchange(Channel, "rjms_test_default_selector_exchange", []),

    %% Declare a queue and bind it
    Q = declare_queue(Channel),
    bind_queue(Channel, Q, Exchange, <<"select-key">>, [?BSELECTARG(<<"boolVal">>)]),

    publish_two_messages(Channel, Exchange, <<"select-key">>),

    get_and_check(Channel, Q, 0, <<"true">>),

    close_channel_and_connection(Connection, Channel),
    ok.

test_queue_selection() ->
    {Connection, Channel} = open_connection_and_channel(),

    Exchange = declare_rjms_exchange(Channel, "rjms_test_queue_selector_exchange", [?XPOLICYARG(<<"jms-queue">>)]),

    %% Declare a base queue and bind it
    Q = declare_queue(Channel),
    bind_queue(Channel, Q, Exchange),

    publish_two_messages(Channel, Exchange, Q),

    get_and_check(Channel, Q, 1, <<"false">>),
    get_and_check(Channel, Q, 0, <<"true">>),

    %% Declare a selector queue and bind it with SQL
    SelQ = declare_queue(Channel),
    bind_queue(Channel, SelQ, Exchange, Q, [?BSELECTARG(<<"boolVal">>)]),

    publish_two_messages(Channel, Exchange, Q),

    get_and_check(Channel, SelQ, 0, <<"true">>),
    get_and_check(Channel, Q, 0, <<"false">>),

    close_channel_and_connection(Connection, Channel),
    ok.

test_multiple_queue_selection() ->
    {Connection, Channel} = open_connection_and_channel(),

    Exchange = declare_rjms_exchange(Channel, "rjms_test_queue_selector_exchange", [?XPOLICYARG(<<"jms-queue">>)]),

    %% Declare a base queue and selector queue
    Q1 = declare_queue(Channel),
    SelQ1 = declare_queue(Channel),
    bind_queue(Channel, Q1, Exchange),
    bind_queue(Channel, SelQ1, Exchange, Q1, [?BSELECTARG(<<"boolVal">>)]),

    %% Declare another base queue and selector queue
    Q2 = declare_queue(Channel),
    SelQ2 = declare_queue(Channel),
    bind_queue(Channel, Q2, Exchange),
    bind_queue(Channel, SelQ2, Exchange, Q2, [?BSELECTARG(<<"boolVal">>)]),

    publish_two_messages(Channel, Exchange, Q1),
    publish_two_messages(Channel, Exchange, Q2),

    get_and_check(Channel, SelQ2, 0, <<"true">>),
    get_and_check(Channel, SelQ1, 0, <<"true">>),

    get_and_check(Channel, Q1, 0, <<"false">>),
    get_and_check(Channel, Q2, 0, <<"false">>),

    close_channel_and_connection(Connection, Channel),
    ok.

%% Close the channel and connection
close_channel_and_connection(Connection, Channel) ->
    amqp_channel:close(Channel),
    amqp_connection:close(Connection),
    ok.

%% Start a network connection, and open a channel
open_connection_and_channel() ->
    {ok, Connection} = amqp_connection:start(#amqp_params_network{}),
    {ok, Channel} = amqp_connection:open_channel(Connection),
    {Connection, Channel}.

%% Declare a rjms_topic_selector exchange, with args
declare_rjms_exchange(Ch, XNameStr, XArgs) ->
    Exchange = list_to_binary(XNameStr),
    Decl = #'exchange.declare'{ exchange = Exchange
                              , type = <<"x-jms-topic">>
                              , arguments = XArgs },
    #'exchange.declare_ok'{} = amqp_channel:call(Ch, Decl),
    Exchange.

%% Bind a base queue to a 'jms-queue' policy exchange
bind_queue(Ch, Q, Ex) -> bind_queue(Ch, Q, Ex, Q, []).

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
