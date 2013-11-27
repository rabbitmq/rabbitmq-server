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
              set_param(<<"test">>, [{<<"src-queue">>,  <<"src">>},
                                     {<<"dest-queue">>, <<"dest">>}]),
              publish_expect(Ch, <<>>, <<"src">>, <<"dest">>, <<"hello">>)
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
              set_param(<<"test">>, [{<<"src-exchange">>,    <<"amq.direct">>},
                                     {<<"src-exchange-key">>,<<"test-key">>},
                                     {<<"dest-exchange">>,   <<"amq.topic">>}]),
              publish_expect(Ch, <<"amq.direct">>, <<"test-key">>,
                             <<"queue">>, <<"hello">>),
              set_param(<<"test">>, [{<<"src-exchange">>,     <<"amq.direct">>},
                                     {<<"src-exchange-key">>, <<"test-key">>},
                                     {<<"dest-exchange">>,    <<"amq.topic">>},
                                     {<<"dest-exchange-key">>,<<"new-key">>}]),
              publish(Ch, <<"amq.direct">>, <<"test-key">>, <<"hello">>),
              expect_empty(Ch, <<"queue">>),
              amqp_channel:call(
                Ch, #'queue.bind'{queue       = <<"queue">>,
                                  exchange    = <<"amq.topic">>,
                                  routing_key = <<"new-key">>}),
              publish_expect(Ch, <<"amq.direct">>, <<"test-key">>,
                             <<"queue">>, <<"hello">>)
      end).

restart_test() ->
    with_ch(
      fun (Ch) ->
              set_param(<<"test">>, [{<<"src-queue">>,  <<"src">>},
                                     {<<"dest-queue">>, <<"dest">>}]),
              %% The catch is because connections link to the shovel,
              %% so one connection will die, kill the shovel, kill
              %% the other connection, then we can't close it
              [catch amqp_connection:close(C) || C <- rabbit_direct:list()],
              publish_expect(Ch, <<>>, <<"src">>, <<"dest">>, <<"hello">>)
      end).

change_definition_test() ->
    with_ch(
      fun (Ch) ->
              set_param(<<"test">>, [{<<"src-queue">>,  <<"src">>},
                                     {<<"dest-queue">>, <<"dest">>}]),
              publish_expect(Ch, <<>>, <<"src">>, <<"dest">>, <<"hello">>),
              set_param(<<"test">>, [{<<"src-queue">>,  <<"src">>},
                                     {<<"dest-queue">>, <<"dest2">>}]),
              publish_expect(Ch, <<>>, <<"src">>, <<"dest2">>, <<"hello">>),
              expect_empty(Ch, <<"dest">>),
              clear_param(<<"test">>),
              publish_expect(Ch, <<>>, <<"src">>, <<"src">>, <<"hello">>),
              expect_empty(Ch, <<"dest">>),
              expect_empty(Ch, <<"dest2">>)
      end).

validation_test() ->
    URIs = [{<<"src-uri">>,  <<"amqp://">>},
            {<<"dest-uri">>, <<"amqp://">>}],

    %% Need valid src and dest URIs
    invalid_param([]),
    invalid_param([{<<"src-queue">>, <<"test">>},
                   {<<"src-uri">>,   <<"derp">>},
                   {<<"dest-uri">>,  <<"amqp://">>}]),
    invalid_param([{<<"src-queue">>, <<"test">>},
                   {<<"src-uri">>,   [<<"derp">>]},
                   {<<"dest-uri">>,  <<"amqp://">>}]),
    invalid_param([{<<"src-queue">>, <<"test">>},
                   {<<"dest-uri">>,  <<"amqp://">>}]),

    %% Also need src exchange or queue
    invalid_param(URIs),
    valid_param([{<<"src-exchange">>, <<"test">>} | URIs]),
    QURIs =     [{<<"src-queue">>,    <<"test">>} | URIs],
    valid_param(QURIs),

    %% But not both
    invalid_param([{<<"src-exchange">>, <<"test">>} | QURIs]),

    %% Check these are of right type
    invalid_param([{<<"prefetch-count">>,  <<"three">>} | QURIs]),
    invalid_param([{<<"reconnect-delay">>, <<"three">>} | QURIs]),
    invalid_param([{<<"ack-mode">>,        <<"whenever">>} | QURIs]),
    ok.

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

set_param(Name, Value) ->
    ok = rabbit_runtime_parameters:set(
           <<"/">>, <<"shovel">>, Name, [{<<"src-uri">>,  <<"amqp://">>},
                                         {<<"dest-uri">>, [<<"amqp://">>]} |
                                         Value]),
    await_shovel(Name).

invalid_param(Value) ->
    {error_string, _} = rabbit_runtime_parameters:set(
                          <<"/">>, <<"shovel">>, <<"invalid">>, Value).

valid_param(Value) ->
    ok = rabbit_runtime_parameters:set(<<"/">>, <<"shovel">>, <<"a">>, Value),
    ok = rabbit_runtime_parameters:clear(<<"/">>, <<"shovel">>, <<"a">>).

await_shovel(Name) ->
    S = rabbit_shovel_status:status(),
    case lists:member(Name, [N || {N, dynamic, {running, _, _}, _} <- S]) of
        true  -> ok;
        false -> timer:sleep(100),
                 await_shovel(Name)
    end.

clear_param(Name) ->
    rabbit_runtime_parameters:clear(<<"/">>, <<"shovel">>, Name).

cleanup() ->
    [rabbit_runtime_parameters:clear(pget(vhost, P),
                                     pget(component, P),
                                     pget(name, P)) ||
        P <- rabbit_runtime_parameters:list()],
    [rabbit_amqqueue:delete(Q, false, false) || Q <- rabbit_amqqueue:list()].

