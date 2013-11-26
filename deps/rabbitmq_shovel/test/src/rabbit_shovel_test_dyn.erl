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
%% Copyright (c) 2007-2013 VMware, Inc.  All rights reserved.
%%

-module(rabbit_shovel_test_dyn).

-include_lib("eunit/include/eunit.hrl").
-include_lib("amqp_client/include/amqp_client.hrl").

-import(rabbit_misc, [pget/2]).

simple_test() ->
    with_ch(
      fun (Ch) ->
              set_param("shovel", "test",
                        [{"src-uri",    "amqp://"},
                         {"src-queue",  "src"},
                         {"dest-uri",   "amqp://"},
                         {"dest-queue", "dest"}]),
              publish_expect(Ch, <<>>, <<"src">>, <<"dest">>, <<"hello">>),
              clear_param("shovel", "test"),
              publish_expect(Ch, <<>>, <<"src">>, <<"src">>, <<"hello">>),
              expect_empty(Ch, <<"dest">>)
      end).

exchange_test() ->
    with_ch(
      fun (Ch) ->
              amqp_channel:call(Ch, #'queue.declare'{queue   = <<"queue">>,
                                                     durable = true}),
              amqp_channel:call(
                Ch, #'queue.bind'{queue       = <<"queue">>,
                                  exchange    = <<"amq.topic">>,
                                  routing_key = <<"test-key">>}),
              set_param("shovel", "test",
                        [{"src-uri",          "amqp://"},
                         {"src-exchange",     "amq.direct"},
                         {"src-exchange-key", "test-key"},
                         {"dest-uri",         "amqp://"},
                         {"dest-exchange",    "amq.topic"}]),
              publish_expect(Ch, <<"amq.direct">>, <<"test-key">>,
                             <<"queue">>, <<"hello">>),
              set_param("shovel", "test",
                        [{"src-uri",           "amqp://"},
                         {"src-exchange",      "amq.direct"},
                         {"src-exchange-key",  "test-key"},
                         {"dest-uri",          "amqp://"},
                         {"dest-exchange",     "amq.topic"},
                         {"dest-exchange-key", "new-key"}]),
              publish(Ch, <<"amq.direct">>, <<"test-key">>, <<"hello">>),
              expect_empty(Ch, <<"queue">>),
              amqp_channel:call(
                Ch, #'queue.bind'{queue       = <<"queue">>,
                                  exchange    = <<"amq.topic">>,
                                  routing_key = <<"new-key">>}),
              publish_expect(Ch, <<"amq.direct">>, <<"test-key">>,
                             <<"queue">>, <<"hello">>)
      end).

%%----------------------------------------------------------------------------

with_ch(Fun) ->
    {ok, Conn} = amqp_connection:start(#amqp_params_network{}),
    {ok, Ch} = amqp_connection:open_channel(Conn),
    Fun(Ch),
    amqp_connection:close(Conn),
    cleanup(),
    ok.

publish(Ch, X, Key, Payload) when is_binary(Payload) ->
    publish(Ch, X, Key, #amqp_msg{payload = Payload});

publish(Ch, X, Key, Msg = #amqp_msg{}) ->
    amqp_channel:cast(Ch, #'basic.publish'{exchange    = X,
                                           routing_key = Key}, Msg).

publish_expect(Ch, X, Key, Q, Payload) ->
    publish(Ch, X, Key, Payload),
    expect(Ch, Q, Payload).

expect(Ch, Q, Payload) ->
    amqp_channel:subscribe(Ch, #'basic.consume'{queue  = Q,
                                                no_ack = true}, self()),
    receive
        #'basic.consume_ok'{consumer_tag = CTag} -> ok
    end,
    receive
        {#'basic.deliver'{}, #amqp_msg{payload = Payload}} ->
            ok
    after 1000 ->
            exit({not_received, Payload})
    end,
    amqp_channel:call(Ch, #'basic.cancel'{consumer_tag = CTag}).

expect_empty(Ch, Q) ->
    ?assertMatch(#'basic.get_empty'{},
                 amqp_channel:call(Ch, #'basic.get'{ queue = Q })).

set_param(Component, Name, Value) ->
    ok = rabbit_runtime_parameters:set(
           <<"/">>, list_to_binary(Component), list_to_binary(Name),
          [{list_to_binary(K), list_to_binary(V)} || {K, V} <- Value]),
    await_shovel(list_to_binary(Name)).

await_shovel(Name) ->
    S = rabbit_shovel_status:status(),
    case lists:member(Name, [N || {N, dynamic, {running, _, _}, _} <- S]) of
        true  -> ok;
        false -> timer:sleep(100),
                 await_shovel(Name)
    end.

clear_param(Component, Name) ->
    rabbit_runtime_parameters:clear(
      <<"/">>, list_to_binary(Component), list_to_binary(Name)).

cleanup() ->
    [rabbit_runtime_parameters:clear(pget(vhost, P),
                                     pget(component, P),
                                     pget(name, P)) ||
        P <- rabbit_runtime_parameters:list()],
    [rabbit_amqqueue:delete(Q, false, false) || Q <- rabbit_amqqueue:list()].

