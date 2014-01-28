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
%% The Initial Developer of the Original Code is Pivotal Software, Inc.
%% Copyright (c) 2013 Pivotal Software, Inc.  All rights reserved.
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
