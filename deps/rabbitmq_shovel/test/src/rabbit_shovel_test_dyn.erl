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
%% The Initial Developer of the Original Code is GoPivotal, Inc.
%% Copyright (c) 2007-2014 GoPivotal, Inc.  All rights reserved.
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

set_properties_test() ->
    with_ch(
      fun (Ch) ->
              Ps = [{<<"src-queue">>,      <<"src">>},
                    {<<"dest-queue">>,     <<"dest">>},
                    {<<"publish-properties">>, [{<<"cluster_id">>, <<"x">>}]}],
              set_param(<<"test">>, Ps),
              #amqp_msg{props = #'P_basic'{cluster_id = Cluster}} =
                  publish_expect(Ch, <<>>, <<"src">>, <<"dest">>, <<"hi">>),
              ?assertEqual(<<"x">>, Cluster)
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

autodelete_test_() ->
    [autodelete_case({<<"on-confirm">>, <<"queue-length">>,  0, 100}),
     autodelete_case({<<"on-confirm">>, 50,                 50,  50}),
     autodelete_case({<<"on-publish">>, <<"queue-length">>,  0, 100}),
     autodelete_case({<<"on-publish">>, 50,                 50,  50}),
     %% no-ack is not compatible with explicit count
     autodelete_case({<<"no-ack">>,     <<"queue-length">>,  0, 100})].

autodelete_case(Args) ->
    fun () -> with_ch(autodelete_do(Args)) end.

autodelete_do({AckMode, After, ExpSrc, ExpDest}) ->
    fun (Ch) ->
            amqp_channel:call(Ch, #'confirm.select'{}),
            amqp_channel:call(Ch, #'queue.declare'{queue = <<"src">>}),
            publish_count(Ch, <<>>, <<"src">>, <<"hello">>, 100),
            amqp_channel:wait_for_confirms(Ch),
            set_param_nowait(<<"test">>, [{<<"src-queue">>,    <<"src">>},
                                          {<<"dest-queue">>,   <<"dest">>},
                                          {<<"ack-mode">>,     AckMode},
                                          {<<"delete-after">>, After}]),
            await_autodelete(<<"test">>),
            expect_count(Ch, <<"src">>, <<"hello">>, ExpSrc),
            expect_count(Ch, <<"dest">>, <<"hello">>, ExpDest)
    end.

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
    invalid_param([{<<"delete-after">>,    <<"whenever">>} | QURIs]),

    %% Check properties have to look property-ish
    invalid_param([{<<"publish-properties">>, [{<<"nonexistent">>, <<>>}]}]),
    invalid_param([{<<"publish-properties">>, [{<<"cluster_id">>, 2}]}]),
    invalid_param([{<<"publish-properties">>, <<"something">>}]),

    %% Can't use explicit message count and no-ack together
    invalid_param([{<<"delete-after">>,    1},
                   {<<"ack-mode">>,        <<"no-ack">>} | QURIs]),
    ok.

security_validation_test() ->
    [begin
         rabbit_vhost:add(U),
         rabbit_auth_backend_internal:add_user(U, <<>>),
         rabbit_auth_backend_internal:set_permissions(
           U, U, <<".*">>, <<".*">>, <<".*">>)
     end || U <- [<<"a">>, <<"b">>]],

    Qs = [{<<"src-queue">>, <<"test">>},
          {<<"dest-queue">>, <<"test2">>}],

    A = lookup_user(<<"a">>),
    valid_param([{<<"src-uri">>,  <<"amqp:///a">>},
                 {<<"dest-uri">>, <<"amqp:///a">>} | Qs], A),
    invalid_param([{<<"src-uri">>,  <<"amqp:///a">>},
                   {<<"dest-uri">>, <<"amqp:///b">>} | Qs], A),
    invalid_param([{<<"src-uri">>,  <<"amqp:///b">>},
                   {<<"dest-uri">>, <<"amqp:///a">>} | Qs], A),
    [begin
         rabbit_vhost:delete(U),
         rabbit_auth_backend_internal:delete_user(U)
     end || U <- [<<"a">>, <<"b">>]],
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
    Msg = receive
              {#'basic.deliver'{}, #amqp_msg{payload = Payload} = M} ->
                  M
          after 1000 ->
                  exit({not_received, Payload})
          end,
    amqp_channel:call(Ch, #'basic.cancel'{consumer_tag = CTag}),
    Msg.

expect_empty(Ch, Q) ->
    ?assertMatch(#'basic.get_empty'{},
                 amqp_channel:call(Ch, #'basic.get'{ queue = Q })).

publish_count(Ch, X, Key, M, Count) ->
    [publish(Ch, X, Key, M) || _ <- lists:seq(1, Count)].

expect_count(Ch, Q, M, Count) ->
    [expect(Ch, Q, M) || _ <- lists:seq(1, Count)],
    expect_empty(Ch, Q).

set_param(Name, Value) ->
    set_param_nowait(Name, Value),
    await_shovel(Name).

set_param_nowait(Name, Value) ->
    ok = rabbit_runtime_parameters:set(
           <<"/">>, <<"shovel">>, Name, [{<<"src-uri">>,  <<"amqp://">>},
                                         {<<"dest-uri">>, [<<"amqp://">>]} |
                                         Value], none).

invalid_param(Value, User) ->
    {error_string, _} = rabbit_runtime_parameters:set(
                          <<"/">>, <<"shovel">>, <<"invalid">>, Value, User).

valid_param(Value, User) ->
    ok = rabbit_runtime_parameters:set(
           <<"/">>, <<"shovel">>, <<"a">>, Value, User),
    ok = rabbit_runtime_parameters:clear(<<"/">>, <<"shovel">>, <<"a">>).

invalid_param(Value) -> invalid_param(Value, none).
valid_param(Value) -> valid_param(Value, none).

lookup_user(Name) ->
    {ok, User} = rabbit_access_control:check_user_login(Name, []),
    User.

clear_param(Name) ->
    rabbit_runtime_parameters:clear(<<"/">>, <<"shovel">>, Name).

cleanup() ->
    [rabbit_runtime_parameters:clear(pget(vhost, P),
                                     pget(component, P),
                                     pget(name, P)) ||
        P <- rabbit_runtime_parameters:list()],
    [rabbit_amqqueue:delete(Q, false, false) || Q <- rabbit_amqqueue:list()].

await_shovel(Name) ->
    await(fun () -> lists:member(Name, shovels_from_status()) end).

await_autodelete(Name) ->
    await(fun () -> not lists:member(Name, shovels_from_parameters()) end),
    await(fun () -> not lists:member(Name, shovels_from_status()) end).

await(Pred) ->
    case Pred() of
        true  -> ok;
        false -> timer:sleep(100),
                 await(Pred)
    end.

shovels_from_status() ->
    S = rabbit_shovel_status:status(),
    [N || {{<<"/">>, N}, dynamic, {running, _}, _} <- S].

shovels_from_parameters() ->
    L = rabbit_runtime_parameters:list(<<"/">>, <<"shovel">>),
    [pget(name, Shovel) || Shovel <- L].
