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
%% Copyright (c) 2007-2016 Pivotal Software, Inc.  All rights reserved.
%%

-module(dynamic_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("amqp_client/include/amqp_client.hrl").

-compile(export_all).

all() ->
    [
      {group, non_parallel_tests}
    ].

groups() ->
    [
      {non_parallel_tests, [], [
          simple,
          set_properties,
          exchange,
          restart,
          change_definition,
          autodelete,
          validation,
          security_validation
        ]}
    ].

%% -------------------------------------------------------------------
%% Testsuite setup/teardown.
%% -------------------------------------------------------------------

init_per_suite(Config) ->
    rabbit_ct_helpers:log_environment(),
    Config1 = rabbit_ct_helpers:set_config(Config, [
        {rmq_nodename_suffix, ?MODULE}
      ]),
    Config2 = rabbit_ct_helpers:run_setup_steps(Config1,
      rabbit_ct_broker_helpers:setup_steps() ++
      rabbit_ct_client_helpers:setup_steps()),
    Config2.

end_per_suite(Config) ->
    rabbit_ct_helpers:run_teardown_steps(Config,
      rabbit_ct_client_helpers:teardown_steps() ++
      rabbit_ct_broker_helpers:teardown_steps()).

init_per_group(_, Config) ->
    Config.

end_per_group(_, Config) ->
    Config.

init_per_testcase(Testcase, Config) ->
    rabbit_ct_helpers:testcase_started(Config, Testcase).

end_per_testcase(Testcase, Config) ->
    rabbit_ct_helpers:testcase_finished(Config, Testcase).

%% -------------------------------------------------------------------
%% Testcases.
%% -------------------------------------------------------------------

simple(Config) ->
    with_ch(Config,
      fun (Ch) ->
              set_param(Config,
                        <<"test">>, [{<<"src-queue">>,  <<"src">>},
                                     {<<"dest-queue">>, <<"dest">>}]),
              publish_expect(Ch, <<>>, <<"src">>, <<"dest">>, <<"hello">>)
      end).

set_properties(Config) ->
    with_ch(Config,
      fun (Ch) ->
              Ps = [{<<"src-queue">>,      <<"src">>},
                    {<<"dest-queue">>,     <<"dest">>},
                    {<<"publish-properties">>, [{<<"cluster_id">>, <<"x">>}]}],
              set_param(Config, <<"test">>, Ps),
              #amqp_msg{props = #'P_basic'{cluster_id = Cluster}} =
                  publish_expect(Ch, <<>>, <<"src">>, <<"dest">>, <<"hi">>),
              <<"x">> = Cluster
      end).

exchange(Config) ->
    with_ch(Config,
      fun (Ch) ->
              amqp_channel:call(Ch, #'queue.declare'{queue   = <<"queue">>,
                                                     durable = true}),
              amqp_channel:call(
                Ch, #'queue.bind'{queue       = <<"queue">>,
                                  exchange    = <<"amq.topic">>,
                                  routing_key = <<"test-key">>}),
              set_param(Config,
                        <<"test">>, [{<<"src-exchange">>,    <<"amq.direct">>},
                                     {<<"src-exchange-key">>,<<"test-key">>},
                                     {<<"dest-exchange">>,   <<"amq.topic">>}]),
              publish_expect(Ch, <<"amq.direct">>, <<"test-key">>,
                             <<"queue">>, <<"hello">>),
              set_param(Config,
                        <<"test">>, [{<<"src-exchange">>,     <<"amq.direct">>},
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

restart(Config) ->
    with_ch(Config,
      fun (Ch) ->
              set_param(Config,
                        <<"test">>, [{<<"src-queue">>,  <<"src">>},
                                     {<<"dest-queue">>, <<"dest">>}]),
              %% The catch is because connections link to the shovel,
              %% so one connection will die, kill the shovel, kill
              %% the other connection, then we can't close it
              Conns = rabbit_ct_broker_helpers:rpc(Config, 0,
                rabbit_direct, list, []),
              [catch amqp_connection:close(C) || C <- Conns],
              publish_expect(Ch, <<>>, <<"src">>, <<"dest">>, <<"hello">>)
      end).

change_definition(Config) ->
    with_ch(Config,
      fun (Ch) ->
              set_param(Config,
                        <<"test">>, [{<<"src-queue">>,  <<"src">>},
                                     {<<"dest-queue">>, <<"dest">>}]),
              publish_expect(Ch, <<>>, <<"src">>, <<"dest">>, <<"hello">>),
              set_param(Config,
                        <<"test">>, [{<<"src-queue">>,  <<"src">>},
                                     {<<"dest-queue">>, <<"dest2">>}]),
              publish_expect(Ch, <<>>, <<"src">>, <<"dest2">>, <<"hello">>),
              expect_empty(Ch, <<"dest">>),
              clear_param(Config, <<"test">>),
              publish_expect(Ch, <<>>, <<"src">>, <<"src">>, <<"hello">>),
              expect_empty(Ch, <<"dest">>),
              expect_empty(Ch, <<"dest2">>)
      end).

autodelete(Config) ->
    autodelete_case(Config, {<<"on-confirm">>, <<"queue-length">>,  0, 100}),
    autodelete_case(Config, {<<"on-confirm">>, 50,                 50,  50}),
    autodelete_case(Config, {<<"on-publish">>, <<"queue-length">>,  0, 100}),
    autodelete_case(Config, {<<"on-publish">>, 50,                 50,  50}),
    %% no-ack is not compatible with explicit count
    autodelete_case(Config, {<<"no-ack">>,     <<"queue-length">>,  0, 100}).

autodelete_case(Config, Args) ->
    with_ch(Config, autodelete_do(Config, Args)).

autodelete_do(Config, {AckMode, After, ExpSrc, ExpDest}) ->
    fun (Ch) ->
            amqp_channel:call(Ch, #'confirm.select'{}),
            amqp_channel:call(Ch, #'queue.declare'{queue = <<"src">>}),
            publish_count(Ch, <<>>, <<"src">>, <<"hello">>, 100),
            amqp_channel:wait_for_confirms(Ch),
            set_param_nowait(Config,
                             <<"test">>, [{<<"src-queue">>,    <<"src">>},
                                          {<<"dest-queue">>,   <<"dest">>},
                                          {<<"ack-mode">>,     AckMode},
                                          {<<"delete-after">>, After}]),
            await_autodelete(Config, <<"test">>),
            expect_count(Ch, <<"src">>, <<"hello">>, ExpSrc),
            expect_count(Ch, <<"dest">>, <<"hello">>, ExpDest)
    end.

validation(Config) ->
    URIs = [{<<"src-uri">>,  <<"amqp://">>},
            {<<"dest-uri">>, <<"amqp://">>}],

    %% Need valid src and dest URIs
    invalid_param(Config, []),
    invalid_param(Config,
                  [{<<"src-queue">>, <<"test">>},
                   {<<"src-uri">>,   <<"derp">>},
                   {<<"dest-uri">>,  <<"amqp://">>}]),
    invalid_param(Config,
                  [{<<"src-queue">>, <<"test">>},
                   {<<"src-uri">>,   [<<"derp">>]},
                   {<<"dest-uri">>,  <<"amqp://">>}]),
    invalid_param(Config,
                  [{<<"src-queue">>, <<"test">>},
                   {<<"dest-uri">>,  <<"amqp://">>}]),

    %% Also need src exchange or queue
    invalid_param(Config,
                  URIs),
    valid_param(Config,
                [{<<"src-exchange">>, <<"test">>} | URIs]),
    QURIs =     [{<<"src-queue">>,    <<"test">>} | URIs],
    valid_param(Config, QURIs),

    %% But not both
    invalid_param(Config,
                  [{<<"src-exchange">>, <<"test">>} | QURIs]),

    %% Check these are of right type
    invalid_param(Config,
                  [{<<"prefetch-count">>,  <<"three">>} | QURIs]),
    invalid_param(Config,
                  [{<<"reconnect-delay">>, <<"three">>} | QURIs]),
    invalid_param(Config,
                  [{<<"ack-mode">>,        <<"whenever">>} | QURIs]),
    invalid_param(Config,
                  [{<<"delete-after">>,    <<"whenever">>} | QURIs]),

    %% Check properties have to look property-ish
    invalid_param(Config,
                  [{<<"publish-properties">>, [{<<"nonexistent">>, <<>>}]}]),
    invalid_param(Config,
                  [{<<"publish-properties">>, [{<<"cluster_id">>, 2}]}]),
    invalid_param(Config,
                  [{<<"publish-properties">>, <<"something">>}]),

    %% Can't use explicit message count and no-ack together
    invalid_param(Config,
                  [{<<"delete-after">>,    1},
                   {<<"ack-mode">>,        <<"no-ack">>} | QURIs]),
    ok.

security_validation(Config) ->
    ok = rabbit_ct_broker_helpers:rpc(Config, 0,
      ?MODULE, security_validation_add_user, []),

    Qs = [{<<"src-queue">>, <<"test">>},
          {<<"dest-queue">>, <<"test2">>}],

    A = lookup_user(Config, <<"a">>),
    valid_param(Config, [{<<"src-uri">>,  <<"amqp:///a">>},
                 {<<"dest-uri">>, <<"amqp:///a">>} | Qs], A),
    invalid_param(Config,
                  [{<<"src-uri">>,  <<"amqp:///a">>},
                   {<<"dest-uri">>, <<"amqp:///b">>} | Qs], A),
    invalid_param(Config,
                  [{<<"src-uri">>,  <<"amqp:///b">>},
                   {<<"dest-uri">>, <<"amqp:///a">>} | Qs], A),

    ok = rabbit_ct_broker_helpers:rpc(Config, 0,
      ?MODULE, security_validation_remove_user, []),
    ok.

security_validation_add_user() ->
    [begin
         rabbit_vhost:add(U),
         rabbit_auth_backend_internal:add_user(U, <<>>),
         rabbit_auth_backend_internal:set_permissions(
           U, U, <<".*">>, <<".*">>, <<".*">>)
     end || U <- [<<"a">>, <<"b">>]],
    ok.

security_validation_remove_user() ->
    [begin
         rabbit_vhost:delete(U),
         rabbit_auth_backend_internal:delete_user(U)
     end || U <- [<<"a">>, <<"b">>]],
    ok.

%%----------------------------------------------------------------------------

with_ch(Config, Fun) ->
    Ch = rabbit_ct_client_helpers:open_channel(Config, 0),
    Fun(Ch),
    rabbit_ct_client_helpers:close_channel(Ch),
    cleanup(Config),
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
    CTag = receive
        #'basic.consume_ok'{consumer_tag = CT} -> CT
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
    #'basic.get_empty'{} = amqp_channel:call(Ch, #'basic.get'{ queue = Q }).

publish_count(Ch, X, Key, M, Count) ->
    [publish(Ch, X, Key, M) || _ <- lists:seq(1, Count)].

expect_count(Ch, Q, M, Count) ->
    [expect(Ch, Q, M) || _ <- lists:seq(1, Count)],
    expect_empty(Ch, Q).

set_param(Config, Name, Value) ->
    set_param_nowait(Config, Name, Value),
    await_shovel(Config, Name).

set_param_nowait(Config, Name, Value) ->
    ok = rabbit_ct_broker_helpers:rpc(Config, 0,
      rabbit_runtime_parameters, set, [
        <<"/">>, <<"shovel">>, Name, [{<<"src-uri">>,  <<"amqp://">>},
                                      {<<"dest-uri">>, [<<"amqp://">>]} |
                                      Value], none]).

invalid_param(Config, Value, User) ->
    {error_string, _} = rabbit_ct_broker_helpers:rpc(Config, 0,
      rabbit_runtime_parameters, set,
      [<<"/">>, <<"shovel">>, <<"invalid">>, Value, User]).

valid_param(Config, Value, User) ->
    rabbit_ct_broker_helpers:rpc(Config, 0,
      ?MODULE, valid_param1, [Config, Value, User]).

valid_param1(_Config, Value, User) ->
    ok = rabbit_runtime_parameters:set(
           <<"/">>, <<"shovel">>, <<"a">>, Value, User),
    ok = rabbit_runtime_parameters:clear(<<"/">>, <<"shovel">>, <<"a">>).

invalid_param(Config, Value) -> invalid_param(Config, Value, none).
valid_param(Config, Value) -> valid_param(Config, Value, none).

lookup_user(Config, Name) ->
    {ok, User} = rabbit_ct_broker_helpers:rpc(Config, 0,
      rabbit_access_control, check_user_login, [Name, []]),
    User.

clear_param(Config, Name) ->
    rabbit_ct_broker_helpers:rpc(Config, 0,
      rabbit_runtime_parameters, clear, [<<"/">>, <<"shovel">>, Name]).

cleanup(Config) ->
    rabbit_ct_broker_helpers:rpc(Config, 0,
      ?MODULE, cleanup1, [Config]).

cleanup1(_Config) ->
    [rabbit_runtime_parameters:clear(rabbit_misc:pget(vhost, P),
                                     rabbit_misc:pget(component, P),
                                     rabbit_misc:pget(name, P)) ||
        P <- rabbit_runtime_parameters:list()],
    [rabbit_amqqueue:delete(Q, false, false) || Q <- rabbit_amqqueue:list()].

await_shovel(Config, Name) ->
    rabbit_ct_broker_helpers:rpc(Config, 0,
      ?MODULE, await_shovel1, [Config, Name]).

await_shovel1(_Config, Name) ->
    await(fun () -> lists:member(Name, shovels_from_status()) end).

await_autodelete(Config, Name) ->
    rabbit_ct_broker_helpers:rpc(Config, 0,
      ?MODULE, await_autodelete1, [Config, Name]).

await_autodelete1(_Config, Name) ->
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
    [rabbit_misc:pget(name, Shovel) || Shovel <- L].
