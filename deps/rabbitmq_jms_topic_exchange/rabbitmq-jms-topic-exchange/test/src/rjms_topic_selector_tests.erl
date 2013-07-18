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
%% Reference Type information
%%
%% -type(amqp_field_type() ::
%%       'longstr' | 'signedint' | 'decimal' | 'timestamp' |
%%       'table' | 'byte' | 'double' | 'float' | 'long' |
%%       'short' | 'bool' | 'binary' | 'void' | 'array').
%% -type(amqp_table() :: [{binary(), amqp_field_type(), amqp_value()}]).
%% -type(amqp_array() :: [{amqp_field_type(), amqp_value()}]).
%% -type(amqp_value() :: binary() |    % longstr
%%                       integer() |   % signedint
%%                       {non_neg_integer(), non_neg_integer()} | % decimal
%%                       amqp_table() |
%%                       amqp_array() |
%%                       byte() |      % byte
%%                       float() |     % double
%%                       integer() |   % long
%%                       integer() |   % short
%%                       boolean() |   % bool
%%                       binary() |    % binary
%%                       'undefined' | % void
%%                       non_neg_integer() % timestamp
%%      ).
-module(rjms_topic_selector_tests).
-export([all_tests/0]).
-include_lib("amqp_client/include/amqp_client.hrl").

-include("rabbit_jms_topic_exchange.hrl").

-define(XPOLICYARG(Policy), {?RJMS_POLICY_ARG, longstr, Policy}).
-define(BSELECTARG(BinStr), {?RJMS_SELECTOR_ARG, longstr, BinStr}).
-define(BASICMSG(Payload, Hdrs), #'amqp_msg'{props=#'P_basic'{headers=Hdrs}, payload=Payload}).

all_tests() ->
    test_topic_selection(),
    test_queue_selection(),
    ok.

test_topic_selection() ->
    %% Start a network connection
    {ok, Connection} = amqp_connection:start(#amqp_params_network{}),

    %% Open a channel on the connection
    {ok, Channel} = amqp_connection:open_channel(Connection),

    %% Declare a rjms_topic_selector exchange
    Exchange = <<"rjms_test_topic_selector_exchange">>,
    DeclareX = #'exchange.declare'{ exchange = Exchange
                                  , type = <<"x-jms-topic">> },
    #'exchange.declare_ok'{} = amqp_channel:call(Channel, DeclareX),

    %% Declare a queue and bind it
    Q = declare_queue(Channel),
    bind_queue(Channel, Q, Exchange, <<"select-key">>, [?BSELECTARG(<<"boolVal">>)]),

    publish_two_messages(Channel, Exchange, <<"select-key">>),

    get_and_check(Channel, Q, 0, <<"true">>),

    %% Close the channel
    amqp_channel:close(Channel),
    %% Close the connection
    amqp_connection:close(Connection),
    ok.

test_queue_selection() ->
    %% Start a network connection, and open a channel
    {ok, Connection} = amqp_connection:start(#amqp_params_network{}),
    {ok, Channel} = amqp_connection:open_channel(Connection),

    %% Declare a rjms_topic_selector exchange, policy 'jms_queue'
    Exchange = <<"rjms_test_queue_selector_exchange">>,
    DeclareX = #'exchange.declare'{ exchange = Exchange
                                  , type = <<"x-jms-topic">>
                                  , arguments = [?XPOLICYARG(<<"jms-queue">>)] },
    #'exchange.declare_ok'{} = amqp_channel:call(Channel, DeclareX),

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

    %% Close the channel
    amqp_channel:close(Channel),
    %% Close the connection
    amqp_connection:close(Connection),
    ok.

%% Bind a base queue to an exchange
bind_queue(Ch, Q, Ex) ->
    bind_queue(Ch, Q, Ex, Q, []).
%% Bind a selector queue to an exchange
bind_queue(Ch, Q, Ex, RKey, Args) ->
    Binding = #'queue.bind'{ queue       = Q
                           , exchange    = Ex
                           , routing_key = RKey
                           , arguments   = Args
                           },
    #'queue.bind_ok'{} = amqp_channel:call(Ch, Binding).

%% Declare a queue
declare_queue(Ch) ->
    #'queue.declare_ok'{queue = Q}
      = amqp_channel:call(Ch, #'queue.declare'{}),
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
