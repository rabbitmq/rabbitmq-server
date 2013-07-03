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

all_tests() ->
    test_messages_selected_by_exchange(),
    ok.

test_messages_selected_by_exchange() ->
    %% Start a network connection
    {ok, Connection} = amqp_connection:start(#amqp_params_network{}),

    %% Open a channel on the connection
    {ok, Channel} = amqp_connection:open_channel(Connection),

    %% Declare a rjms_topic_selector exchange
    Exchange = <<"rjms_test_exchange">>,
    DeclareX = #'exchange.declare'{ exchange = Exchange
                                  , type = <<"x-jms-topic">> },
    #'exchange.declare_ok'{} = amqp_channel:call(Channel, DeclareX),

    %% Declare a queue
    #'queue.declare_ok'{queue = Q}
      = amqp_channel:call(Channel, #'queue.declare'{}),

    %% Bind the queue to the exchange
    Binding = #'queue.bind'{ queue       = Q
                           , exchange    = Exchange
                           , routing_key = <<"select-key">>
                           , arguments   = sqlArgs()
                           },
    #'queue.bind_ok'{} = amqp_channel:call(Channel, Binding),

    %% Publish a message
    publish_two_messages(Channel, Exchange),

    %% Get a message back from the queue
    Get = #'basic.get'{queue = Q},
    {#'basic.get_ok'{delivery_tag = Tag, message_count = Remaining}, Content}
      = amqp_channel:call(Channel, Get),

    0 = Remaining,  % there should be none remaining
    <<"true">> = Content#amqp_msg.payload,  % the content should be "true"

    %% Ack the message
    amqp_channel:cast(Channel, #'basic.ack'{delivery_tag = Tag}),
    %% Close the channel

    amqp_channel:close(Channel),
    %% Close the connection

    amqp_connection:close(Connection),
    ok.

publish_two_messages(Chan, Exch) ->
    Publish = #'basic.publish'{exchange = Exch, routing_key = <<"select-key">>},
    amqp_channel:cast(Chan, Publish, createMsg(<<"false">>, [{<<"boolVal">>, 'bool', false}])),
    amqp_channel:cast(Chan, Publish, createMsg(<<"true">>, [{<<"boolVal">>, 'bool', true}])),
    ok.

sqlArgs() ->
    [{?RJMS_SELECTOR_ARG, 'longstr', <<"boolVal">>}].

createMsg(PL, Hs) ->
    #'amqp_msg'{props=#'P_basic'{headers=Hs}, payload=PL}.