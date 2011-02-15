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
%% The Original Code is RabbitMQ Federation.
%%
%% The Initial Developer of the Original Code is VMware, Inc.
%% Copyright (c) 2007-2011 VMware, Inc.  All rights reserved.
%%

-module(rabbit_federation_test).

-include_lib("eunit/include/eunit.hrl").
-include_lib("amqp_client/include/amqp_client.hrl").

simple_test() ->
    with_ch(
      fun (Ch) ->
              declare_exchange(Ch, <<"upstream">>, <<"direct">>),
              declare_fed_exchange(Ch, <<"downstream">>,
                                   [<<"amqp://localhost/%2f/upstream">>],
                                   <<"direct">>),
              Q = bind_queue(Ch, <<"downstream">>, <<"key">>),
              publish_expect(Ch, <<"upstream">>, <<"key">>, Q, <<"HELLO">>),
              delete_exchange(Ch, <<"downstream">>),
              delete_exchange(Ch, <<"upstream">>)
      end).

conf_test() ->
    with_ch(
      fun (Ch) ->
              declare_exchange(Ch, <<"upstream-conf">>, <<"topic">>),
              Q = bind_queue(Ch, <<"downstream-conf">>, <<"key">>),
              publish_expect(Ch, <<"upstream-conf">>, <<"key">>, Q, <<"HELLO">>),
              delete_exchange(Ch, <<"upstream-conf">>)
      end).

multiple_upstreams_test() ->
    with_ch(
      fun (Ch) ->
              declare_exchange(Ch, <<"upstream1">>, <<"direct">>),
              declare_exchange(Ch, <<"upstream2">>, <<"direct">>),
              declare_fed_exchange(Ch, <<"downstream">>,
                                   [<<"amqp://localhost/%2f/upstream1">>,
                                    <<"amqp://localhost/%2f/upstream2">>],
                                   <<"direct">>),
              Q = bind_queue(Ch, <<"downstream">>, <<"key">>),
              publish_expect(Ch, <<"upstream1">>, <<"key">>, Q, <<"HELLO1">>),
              publish_expect(Ch, <<"upstream2">>, <<"key">>, Q, <<"HELLO2">>),
              delete_exchange(Ch, <<"downstream">>),
              delete_exchange(Ch, <<"upstream1">>),
              delete_exchange(Ch, <<"upstream2">>)
      end).

multiple_downstreams_test() ->
    with_ch(
      fun (Ch) ->
              declare_exchange(Ch, <<"upstream1">>, <<"direct">>),
              declare_exchange(Ch, <<"upstream2">>, <<"direct">>),
              declare_fed_exchange(Ch, <<"downstream1">>,
                                   [<<"amqp://localhost/%2f/upstream1">>],
                                   <<"direct">>),
              declare_fed_exchange(Ch, <<"downstream12">>,
                                   [<<"amqp://localhost/%2f/upstream1">>,
                                    <<"amqp://localhost/%2f/upstream2">>],
                                   <<"direct">>),
              Q1 = bind_queue(Ch, <<"downstream1">>, <<"key">>),
              Q12 = bind_queue(Ch, <<"downstream12">>, <<"key">>),
              publish(Ch, <<"upstream1">>, <<"key">>, <<"HELLO1">>),
              publish(Ch, <<"upstream2">>, <<"key">>, <<"HELLO2">>),
              expect(Ch, Q1, [<<"HELLO1">>]),
              expect(Ch, Q12, [<<"HELLO1">>, <<"HELLO2">>]),
              delete_exchange(Ch, <<"downstream1">>),
              delete_exchange(Ch, <<"downstream12">>),
              delete_exchange(Ch, <<"upstream1">>),
              delete_exchange(Ch, <<"upstream2">>)
      end).

%%----------------------------------------------------------------------------

%%----------------------------------------------------------------------------

with_ch(Fun) ->
    {ok, Conn} = amqp_connection:start(network),
    {ok, Ch} = amqp_connection:open_channel(Conn),
    Fun(Ch),
    amqp_connection:close(Conn),
    ok.

declare_fed_exchange(Ch, X, Upstreams, Type) ->
    amqp_channel:call(
      Ch, #'exchange.declare'{
        exchange  = X,
        type      = <<"x-federation">>,
        arguments = [{<<"upstreams">>, array, [{longstr, U} || U <- Upstreams]},
                     {<<"type">>,      longstr, Type}]
       }).

declare_exchange(Ch, X, Type) ->
    amqp_channel:call(Ch, #'exchange.declare'{ exchange = X,
                                               type     = Type }).

bind_queue(Ch, X, Key) ->
    #'queue.declare_ok'{ queue = Q } =
        amqp_channel:call(Ch, #'queue.declare'{ exclusive = true }),
    amqp_channel:call(Ch, #'queue.bind'{ queue       = Q,
                                         exchange    = X,
                                         routing_key = Key }),
    Q.

delete_exchange(Ch, X) ->
    amqp_channel:call(Ch, #'exchange.delete'{ exchange = X }).

publish(Ch, X, Key, Payload) ->
    amqp_channel:call(Ch, #'basic.publish'{ exchange    = X,
                                            routing_key = Key },
                      #amqp_msg { payload = Payload }).

expect(Ch, Q, Payloads) ->
    amqp_channel:subscribe(Ch, #'basic.consume'{ queue = Q },
                           self()),
    receive
        #'basic.consume_ok'{ consumer_tag = CTag } -> ok
    end,
    [receive
         {#'basic.deliver'{}, #amqp_msg { payload = Payload }} -> ok
     end || Payload <- Payloads],
    amqp_channel:call(Ch, #'basic.cancel'{ consumer_tag = CTag }).

publish_expect(Ch, X, Key, Q, Payload) ->
    publish(Ch, X, Key, Payload),
    expect(Ch, Q, [Payload]).

%%----------------------------------------------------------------------------
