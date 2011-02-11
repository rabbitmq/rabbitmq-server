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
                                   <<"amqp://localhost/%2f/upstream">>,
                                   <<"direct">>),
              Q = bind_queue(Ch, <<"downstream">>, <<"key">>),
              publish(Ch, <<"upstream">>, <<"key">>, <<"HELLO">>),
              expect(Ch, Q, <<"HELLO">>),
              delete_exchange(Ch, <<"downstream">>),
              delete_exchange(Ch, <<"upstream">>)
      end).

conf_test() ->
    with_ch(
      fun (Ch) ->
              declare_exchange(Ch, <<"upstream">>, <<"topic">>),
              Q = bind_queue(Ch, <<"downstream-conf">>, <<"key">>),
              publish(Ch, <<"upstream">>, <<"key">>, <<"HELLO">>),
              expect(Ch, Q, <<"HELLO">>),
              delete_exchange(Ch, <<"upstream">>)
      end).


%%----------------------------------------------------------------------------

with_ch(Fun) ->
    {ok, Conn} = amqp_connection:start(network),
    {ok, Ch} = amqp_connection:open_channel(Conn),
    Fun(Ch),
    amqp_connection:close(Conn),
    ok.

args(Args) ->
    [{list_to_binary(atom_to_list(K)), longstr, V} || {K, V} <- Args].

declare_fed_exchange(Ch, X, Upstream, Type) ->
    amqp_channel:call(
      Ch, #'exchange.declare'{
        exchange  = X,
        type      = <<"x-federation">>,
        arguments = args([{upstream, Upstream},
                          {type,     Type}])
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

expect(Ch, Q, Payload) ->
    amqp_channel:subscribe(Ch, #'basic.consume'{ queue = Q },
                           self()),
    receive
        #'basic.consume_ok'{ consumer_tag = CTag } -> ok
    end,
    receive
        {#'basic.deliver'{}, #amqp_msg { payload = Payload }} -> ok
    end,
    amqp_channel:call(Ch, #'basic.cancel'{ consumer_tag = CTag }).

%%----------------------------------------------------------------------------
