%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2024 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
%%

-module(system_SUITE).

-compile(export_all).

-include_lib("common_test/include/ct.hrl").
-include_lib("amqp_client/include/amqp_client.hrl").
-include_lib("eunit/include/eunit.hrl").

-import(rabbit_ct_client_helpers, [close_connection/1, close_channel/1,
                                   open_unmanaged_connection/4, open_unmanaged_connection/5,
                                   close_connection_and_channel/2]).
-import(rabbit_mgmt_test_util, [amqp_port/1]).

all() ->
    [
     {group, basic_happy_path},
     {group, basic_unhappy_path},
     {group, token_refresh},
     {group, extra_scopes_source},
     {group, scope_aliases},
     {group, rich_authorization_requests}
    ].

groups() ->
    [
     {basic_happy_path, [], [
                       test_successful_connection_with_a_full_permission_token_and_all_defaults,
                       test_successful_connection_with_a_full_permission_token_and_explicitly_configured_vhost,
                       test_successful_connection_with_simple_strings_for_aud_and_scope,
                       test_successful_token_refresh,
                       test_successful_connection_without_verify_aud,
                       mqtt
                      ]},
     {basic_unhappy_path, [], [
                       test_failed_connection_with_expired_token,
                       test_failed_connection_with_a_non_token,
                       test_failed_connection_with_a_token_with_insufficient_vhost_permission,
                       test_failed_connection_with_a_token_with_insufficient_resource_permission,
                       more_than_one_resource_server_id_not_allowed_in_one_token,
                       mqtt_expired_token,
                       mqtt_expirable_token,
                       web_mqtt_expirable_token,
                       amqp_expirable_token
                      ]},

     {token_refresh, [], [
                       test_failed_token_refresh_case1,
                       test_failed_token_refresh_case2
     ]},

     {extra_scopes_source, [], [
                       test_successful_connection_with_complex_claim_as_a_map,
                       test_successful_connection_with_complex_claim_as_a_list,
                       test_successful_connection_with_complex_claim_as_a_binary,
                       test_successful_connection_with_keycloak_token
     ]},

     {scope_aliases, [], [
                       test_successful_connection_with_with_single_scope_alias_in_extra_scopes_source,
                       test_successful_connection_with_with_multiple_scope_aliases_in_extra_scopes_source,
                       test_successful_connection_with_scope_alias_in_scope_field_case1,
                       test_successful_connection_with_scope_alias_in_scope_field_case2,
                       test_successful_connection_with_scope_alias_in_scope_field_case3,
                       test_failed_connection_with_with_non_existent_scope_alias_in_extra_scopes_source,
                       test_failed_connection_with_non_existent_scope_alias_in_scope_field
                      ]},
     {rich_authorization_requests, [], [
         test_successful_connection_with_rich_authorization_request_token
     ]}
    ].

%%
%% Setup and Teardown
%%

-define(UTIL_MOD, rabbit_auth_backend_oauth2_test_util).
-define(RESOURCE_SERVER_ID, <<"rabbitmq">>).
-define(EXTRA_SCOPES_SOURCE, <<"additional_rabbitmq_scopes">>).
-define(CLAIMS_FIELD, <<"claims">>).

-define(SCOPE_ALIAS_NAME,   <<"role-1">>).
-define(SCOPE_ALIAS_NAME_2, <<"role-2">>).
-define(SCOPE_ALIAS_NAME_3, <<"role-3">>).

init_per_suite(Config) ->
    rabbit_ct_helpers:log_environment(),
    rabbit_ct_helpers:run_setup_steps(Config,
      rabbit_ct_broker_helpers:setup_steps() ++ [
        fun preconfigure_node/1,
        fun preconfigure_token/1
      ]).

end_per_suite(Config) ->
    rabbit_ct_helpers:run_teardown_steps(Config, rabbit_ct_broker_helpers:teardown_steps()).


init_per_group(_Group, Config) ->
    %% The broker is managed by {init,end}_per_testcase().
    lists:foreach(fun(Value) ->
                          rabbit_ct_broker_helpers:add_vhost(Config, Value)
                  end,
                  [<<"vhost1">>, <<"vhost2">>, <<"vhost3">>, <<"vhost4">>]),
    Config.

end_per_group(_Group, Config) ->
    %% The broker is managed by {init,end}_per_testcase().
    lists:foreach(fun(Value) ->
                          rabbit_ct_broker_helpers:delete_vhost(Config, Value)
                  end,
                  [<<"vhost1">>, <<"vhost2">>, <<"vhost3">>, <<"vhost4">>]),
    Config.

%%
%% Per-case setup
%%

init_per_testcase(Testcase, Config) when Testcase =:= test_successful_connection_with_a_full_permission_token_and_explicitly_configured_vhost orelse
                                         Testcase =:= test_successful_token_refresh ->
    rabbit_ct_broker_helpers:add_vhost(Config, <<"vhost1">>),
    rabbit_ct_helpers:testcase_started(Config, Testcase),
    Config;

init_per_testcase(Testcase, Config) when Testcase =:= test_failed_token_refresh_case1 orelse
                                         Testcase =:= test_failed_token_refresh_case2 ->
    rabbit_ct_broker_helpers:add_vhost(Config, <<"vhost4">>),
    rabbit_ct_helpers:testcase_started(Config, Testcase),
    Config;

init_per_testcase(Testcase, Config) when Testcase =:= test_successful_connection_without_verify_aud ->
  ok = rabbit_ct_broker_helpers:rpc(Config, 0, application, set_env,
        [rabbitmq_auth_backend_oauth2, verify_aud, false]),
  rabbit_ct_helpers:testcase_started(Config, Testcase),
  Config;

init_per_testcase(Testcase, Config) when Testcase =:= test_successful_connection_with_complex_claim_as_a_map orelse
                                         Testcase =:= test_successful_connection_with_complex_claim_as_a_list orelse
                                         Testcase =:= test_successful_connection_with_complex_claim_as_a_binary orelse
                                         Testcase =:= mqtt ->
  ok = rabbit_ct_broker_helpers:rpc(Config, 0, application, set_env,
        [rabbitmq_auth_backend_oauth2, extra_scopes_source, ?EXTRA_SCOPES_SOURCE]),
  rabbit_ct_helpers:testcase_started(Config, Testcase),
  Config;

init_per_testcase(Testcase, Config) when Testcase =:= test_successful_connection_with_with_single_scope_alias_in_extra_scopes_source ->
    rabbit_ct_broker_helpers:add_vhost(Config, <<"vhost1">>),
    ok = rabbit_ct_broker_helpers:rpc(Config, 0, application, set_env,
        [rabbitmq_auth_backend_oauth2, extra_scopes_source, ?CLAIMS_FIELD]),
    ok = rabbit_ct_broker_helpers:rpc(Config, 0, application, set_env,
        [rabbitmq_auth_backend_oauth2, scope_aliases, #{
            ?SCOPE_ALIAS_NAME => [
                <<"rabbitmq.configure:vhost1/*">>,
                <<"rabbitmq.write:vhost1/*">>,
                <<"rabbitmq.read:vhost1/*">>
            ]}
        ]),
    rabbit_ct_helpers:testcase_started(Config, Testcase),
    Config;

init_per_testcase(Testcase, Config) when Testcase =:= test_successful_connection_with_with_multiple_scope_aliases_in_extra_scopes_source ->
    rabbit_ct_broker_helpers:add_vhost(Config, <<"vhost4">>),
    ok = rabbit_ct_broker_helpers:rpc(Config, 0, application, set_env,
        [rabbitmq_auth_backend_oauth2, extra_scopes_source, ?CLAIMS_FIELD]),
    ok = rabbit_ct_broker_helpers:rpc(Config, 0, application, set_env,
        [rabbitmq_auth_backend_oauth2, scope_aliases, #{
            ?SCOPE_ALIAS_NAME => [
                <<"rabbitmq.configure:vhost4/*">>
            ],
            ?SCOPE_ALIAS_NAME_2 => [
                <<"rabbitmq.write:vhost4/*">>
            ],
            ?SCOPE_ALIAS_NAME_3 => [
                <<"rabbitmq.read:vhost4/*">>
            ]
        }]),
    rabbit_ct_helpers:testcase_started(Config, Testcase),
    Config;

init_per_testcase(Testcase, Config) when Testcase =:= test_successful_connection_with_scope_alias_in_scope_field_case1 orelse
                                         Testcase =:= test_successful_connection_with_scope_alias_in_scope_field_case2 ->
    rabbit_ct_broker_helpers:add_vhost(Config, <<"vhost2">>),
    ok = rabbit_ct_broker_helpers:rpc(Config, 0, application, set_env,
        [rabbitmq_auth_backend_oauth2, scope_aliases, #{
            ?SCOPE_ALIAS_NAME => [
                <<"rabbitmq.configure:vhost2/*">>,
                <<"rabbitmq.write:vhost2/*">>,
                <<"rabbitmq.read:vhost2/*">>
            ]}
        ]),
    rabbit_ct_helpers:testcase_started(Config, Testcase),
    Config;
init_per_testcase(Testcase, Config) when Testcase =:= test_successful_connection_with_scope_alias_in_scope_field_case3 ->
    rabbit_ct_broker_helpers:add_vhost(Config, <<"vhost3">>),
    ok = rabbit_ct_broker_helpers:rpc(Config, 0, application, set_env,
        [rabbitmq_auth_backend_oauth2, scope_aliases, #{
            ?SCOPE_ALIAS_NAME => [
                <<"rabbitmq.configure:vhost3/*">>
            ],
            ?SCOPE_ALIAS_NAME_2 => [
                <<"rabbitmq.write:vhost3/*">>
            ],
            ?SCOPE_ALIAS_NAME_3 => [
                <<"rabbitmq.read:vhost3/*">>
            ]
        }]),
    rabbit_ct_helpers:testcase_started(Config, Testcase),
    Config;

init_per_testcase(Testcase, Config) when Testcase =:= test_successful_connection_with_rich_authorization_request_token ->
    ok = rabbit_ct_broker_helpers:rpc(Config, 0, application, set_env,
        [rabbitmq_auth_backend_oauth2, resource_server_type, <<"rabbitmq-type">> ]),
    rabbit_ct_helpers:testcase_started(Config, Testcase),
    Config;

init_per_testcase(multiple_resource_server_ids, Config) ->
  ok = rabbit_ct_broker_helpers:rpc(Config, 0, application, set_env,
      [rabbitmq_auth_backend_oauth2, scope_prefix, <<"rmq.">> ]),
  ok = rabbit_ct_broker_helpers:rpc(Config, 0, application, set_env,
      [rabbitmq_auth_backend_oauth2, resource_servers, #{
          <<"prod">> => [ ],
          <<"dev">> => [ ]
      }]),
  rabbit_ct_helpers:testcase_started(Config, multiple_resource_server_ids),
  Config;

init_per_testcase(Testcase, Config) ->
    rabbit_ct_helpers:testcase_started(Config, Testcase),
    Config.



%%
%% Per-case Teardown
%%

end_per_testcase(Testcase, Config) when Testcase =:= test_failed_token_refresh_case1 orelse
                                        Testcase =:= test_failed_token_refresh_case2 ->
    rabbit_ct_broker_helpers:delete_vhost(Config, <<"vhost4">>),
    rabbit_ct_helpers:testcase_finished(Config, Testcase),
    Config;

end_per_testcase(Testcase, Config) when Testcase =:= test_successful_connection_with_complex_claim_as_a_map orelse
                                        Testcase =:= test_successful_connection_with_complex_claim_as_a_list orelse
                                        Testcase =:= test_successful_connection_with_complex_claim_as_a_binary ->
  rabbit_ct_broker_helpers:delete_vhost(Config, <<"vhost1">>),
  ok = rabbit_ct_broker_helpers:rpc(Config, 0, application, unset_env,
        [rabbitmq_auth_backend_oauth2, extra_scopes_source]),
  rabbit_ct_helpers:testcase_finished(Config, Testcase),
  Config;

end_per_testcase(Testcase, Config) when Testcase =:= test_successful_connection_with_with_single_scope_alias_in_extra_scopes_source ->
  rabbit_ct_broker_helpers:delete_vhost(Config, <<"vhost1">>),
  ok = rabbit_ct_broker_helpers:rpc(Config, 0, application, unset_env,
        [rabbitmq_auth_backend_oauth2, scope_aliases]),
  ok = rabbit_ct_broker_helpers:rpc(Config, 0, application, unset_env,
        [rabbitmq_auth_backend_oauth2, extra_scopes_source]),
  rabbit_ct_helpers:testcase_finished(Config, Testcase),
  Config;

end_per_testcase(Testcase, Config) when Testcase =:= test_successful_connection_with_with_multiple_scope_aliases_in_extra_scopes_source ->
  rabbit_ct_broker_helpers:delete_vhost(Config, <<"vhost4">>),
  ok = rabbit_ct_broker_helpers:rpc(Config, 0, application, unset_env,
        [rabbitmq_auth_backend_oauth2, scope_aliases]),
  ok = rabbit_ct_broker_helpers:rpc(Config, 0, application, unset_env,
        [rabbitmq_auth_backend_oauth2, extra_scopes_source]),
  rabbit_ct_helpers:testcase_finished(Config, Testcase),
  Config;

end_per_testcase(Testcase, Config) when Testcase =:= test_successful_connection_with_scope_alias_in_scope_field_case1 orelse
                                        Testcase =:= test_successful_connection_with_scope_alias_in_scope_field_case2 ->
  rabbit_ct_broker_helpers:delete_vhost(Config, <<"vhost2">>),
  ok = rabbit_ct_broker_helpers:rpc(Config, 0, application, unset_env,
        [rabbitmq_auth_backend_oauth2, scope_aliases]),
  rabbit_ct_helpers:testcase_finished(Config, Testcase),
  Config;

end_per_testcase(Testcase, Config) when Testcase =:= test_successful_connection_with_scope_alias_in_scope_field_case3 ->
  rabbit_ct_broker_helpers:delete_vhost(Config, <<"vhost3">>),
  ok = rabbit_ct_broker_helpers:rpc(Config, 0, application, unset_env,
        [rabbitmq_auth_backend_oauth2, scope_aliases]),
  rabbit_ct_helpers:testcase_finished(Config, Testcase),
  Config;

end_per_testcase(multiple_resource_server_ids, Config) ->
  rabbit_ct_broker_helpers:rpc(Config, 0, application, unset_env,
      [rabbitmq_auth_backend_oauth2, scope_prefix ]),
  rabbit_ct_broker_helpers:rpc(Config, 0, application, unset_env,
      [rabbitmq_auth_backend_oauth2, resource_servers ]),
  rabbit_ct_helpers:testcase_started(Config, multiple_resource_server_ids),
  Config;

end_per_testcase(Testcase, Config) ->
    rabbit_ct_broker_helpers:delete_vhost(Config, <<"vhost1">>),
    rabbit_ct_helpers:testcase_finished(Config, Testcase),
    Config.

preconfigure_node(Config) ->
    ok = rabbit_ct_broker_helpers:rpc(Config, 0, application, set_env,
                                      [rabbit, auth_backends, [rabbit_auth_backend_oauth2]]),
    Jwk   = ?UTIL_MOD:fixture_jwk(),
    KeyConfig = [{signing_keys, #{<<"token-key">> => {map, Jwk}}}],
    ok = rabbit_ct_broker_helpers:rpc(Config, 0, application, set_env,
                                      [rabbitmq_auth_backend_oauth2, key_config, KeyConfig]),
    ok = rabbit_ct_broker_helpers:rpc(Config, 0, application, set_env,
                                      [rabbitmq_auth_backend_oauth2, resource_server_id, ?RESOURCE_SERVER_ID]),

    rabbit_ct_helpers:set_config(Config, {fixture_jwk, Jwk}).

generate_valid_token(Config) ->
    generate_valid_token(Config, ?UTIL_MOD:full_permission_scopes()).

generate_valid_token(Config, Scopes) ->
    generate_valid_token(Config, Scopes, undefined).

generate_valid_token(Config, Scopes, Audience) ->
    Jwk = case rabbit_ct_helpers:get_config(Config, fixture_jwk) of
              undefined -> ?UTIL_MOD:fixture_jwk();
              Value     -> Value
          end,
    Token = case Audience of
        undefined -> ?UTIL_MOD:fixture_token_with_scopes(Scopes);
        DefinedAudience -> maps:put(<<"aud">>, DefinedAudience, ?UTIL_MOD:fixture_token_with_scopes(Scopes))
    end,
    ?UTIL_MOD:sign_token_hs(Token, Jwk).

generate_valid_token_with_extra_fields(Config, ExtraFields) ->
    Jwk = case rabbit_ct_helpers:get_config(Config, fixture_jwk) of
              undefined -> ?UTIL_MOD:fixture_jwk();
              Value     -> Value
          end,
    Token = maps:merge(?UTIL_MOD:fixture_token_with_scopes([]), ExtraFields),
    ?UTIL_MOD:sign_token_hs(Token, Jwk).

generate_expired_token(Config) ->
    generate_expired_token(Config, ?UTIL_MOD:full_permission_scopes()).

generate_expired_token(Config, Scopes) ->
    Jwk = case rabbit_ct_helpers:get_config(Config, fixture_jwk) of
              undefined -> ?UTIL_MOD:fixture_jwk();
              Value     -> Value
          end,
    ?UTIL_MOD:sign_token_hs(?UTIL_MOD:expired_token_with_scopes(Scopes), Jwk).

generate_expirable_token(Config, Seconds) ->
    generate_expirable_token(Config, ?UTIL_MOD:full_permission_scopes(), Seconds).

generate_expirable_token(Config, Scopes, Seconds) ->
    Jwk = case rabbit_ct_helpers:get_config(Config, fixture_jwk) of
              undefined -> ?UTIL_MOD:fixture_jwk();
              Value     -> Value
          end,
    Expiration = os:system_time(seconds) + Seconds,
    ?UTIL_MOD:sign_token_hs(?UTIL_MOD:token_with_scopes_and_expiration(Scopes, Expiration), Jwk).

preconfigure_token(Config) ->
    Token = generate_valid_token(Config),
    rabbit_ct_helpers:set_config(Config, {fixture_jwt, Token}).

%%
%% Test Cases
%%

test_successful_connection_with_a_full_permission_token_and_all_defaults(Config) ->
    {_Algo, Token} = rabbit_ct_helpers:get_config(Config, fixture_jwt),
    Conn     = open_unmanaged_connection(Config, 0, <<"username">>, Token),
    {ok, Ch} = amqp_connection:open_channel(Conn),
    #'queue.declare_ok'{queue = _} =
        amqp_channel:call(Ch, #'queue.declare'{exclusive = true}),
    close_connection_and_channel(Conn, Ch).

test_successful_connection_with_a_full_permission_token_and_explicitly_configured_vhost(Config) ->
    {_Algo, Token} = generate_valid_token(Config, [<<"rabbitmq.configure:vhost1/*">>,
                                                   <<"rabbitmq.write:vhost1/*">>,
                                                   <<"rabbitmq.read:vhost1/*">>]),
    Conn     = open_unmanaged_connection(Config, 0, <<"vhost1">>, <<"username">>, Token),
    {ok, Ch} = amqp_connection:open_channel(Conn),
    #'queue.declare_ok'{queue = _} =
        amqp_channel:call(Ch, #'queue.declare'{exclusive = true}),
    close_connection_and_channel(Conn, Ch).

test_successful_connection_with_simple_strings_for_aud_and_scope(Config) ->
    {_Algo, Token} = generate_valid_token(
        Config,
        <<"rabbitmq.configure:*/* rabbitmq.write:*/* rabbitmq.read:*/*">>,
        [<<"hare">>, <<"rabbitmq">>]
    ),
    Conn     = open_unmanaged_connection(Config, 0, <<"username">>, Token),
    {ok, Ch} = amqp_connection:open_channel(Conn),
    #'queue.declare_ok'{queue = _} =
        amqp_channel:call(Ch, #'queue.declare'{exclusive = true}),
    close_connection_and_channel(Conn, Ch).

test_successful_connection_without_verify_aud(Config) ->
    {_Algo, Token} = generate_valid_token(
        Config,
        <<"rabbitmq.configure:*/* rabbitmq.write:*/* rabbitmq.read:*/*">>,
        <<>>
    ),
    Conn     = open_unmanaged_connection(Config, 0, <<"username">>, Token),
    {ok, Ch} = amqp_connection:open_channel(Conn),
    #'queue.declare_ok'{queue = _} =
        amqp_channel:call(Ch, #'queue.declare'{exclusive = true}),
    close_connection_and_channel(Conn, Ch).

mqtt(Config) ->
    Topic = <<"test/topic">>,
    Payload = <<"mqtt-test-message">>,
    {_Algo, Token} = generate_valid_token_with_extra_fields(
                       Config,
                       #{<<"additional_rabbitmq_scopes">> =>
                         <<"rabbitmq.configure:*/*/* rabbitmq.read:*/*/* rabbitmq.write:*/*/*">>}
                      ),
    Opts = [{port, rabbit_ct_broker_helpers:get_node_config(Config, 0, tcp_port_mqtt)},
            {proto_ver, v4},
            {username, <<"">>},
            {password, Token}],
    {ok, Sub} = emqtt:start_link([{clientid, <<"mqtt-subscriber">>} | Opts]),
    {ok, _} = emqtt:connect(Sub),
    {ok, _, [1]} = emqtt:subscribe(Sub, Topic, at_least_once),
    {ok, Pub} = emqtt:start_link([{clientid, <<"mqtt-publisher">>} | Opts]),
    {ok, _} = emqtt:connect(Pub),
    {ok, _} = emqtt:publish(Pub, Topic, Payload, at_least_once),
    receive {publish, #{client_pid := Sub,
                        topic := Topic,
                        payload := Payload}} -> ok
    after 1000 -> ct:fail("no publish received")
    end,
    ok = emqtt:disconnect(Sub),
    ok = emqtt:disconnect(Pub).

mqtt_expired_token(Config) ->
    {_Algo, Token} = generate_expired_token(Config),
    Opts = [{port, rabbit_ct_broker_helpers:get_node_config(Config, 0, tcp_port_mqtt)},
            {proto_ver, v5},
            {username, <<"">>},
            {password, Token}],
    ClientId = atom_to_binary(?FUNCTION_NAME),
    {ok, C} = emqtt:start_link([{clientid, ClientId} | Opts]),
    true = unlink(C),
    ?assertMatch({error, {bad_username_or_password, _}},
                 emqtt:connect(C)).

mqtt_expirable_token(Config) ->
    mqtt_expirable_token0(tcp_port_mqtt,
                          [],
                          fun emqtt:connect/1,
                          Config).

web_mqtt_expirable_token(Config) ->
    mqtt_expirable_token0(tcp_port_web_mqtt,
                          [{ws_path, "/ws"}],
                          fun emqtt:ws_connect/1,
                          Config).

mqtt_expirable_token0(Port, AdditionalOpts, Connect, Config) ->
    Topic = <<"test/topic">>,
    Payload = <<"mqtt-test-message">>,

    Seconds = 4,
    Millis = Seconds * 1000,
    {_Algo, Token} = generate_expirable_token(Config,
                                              [<<"rabbitmq.configure:*/*/*">>,
                                               <<"rabbitmq.write:*/*/*">>,
                                               <<"rabbitmq.read:*/*/*">>],
                                              Seconds),

    Opts = [{port, rabbit_ct_broker_helpers:get_node_config(Config, 0, Port)},
            {proto_ver, v5},
            {username, <<"">>},
            {password, Token}] ++ AdditionalOpts,
    {ok, Sub} = emqtt:start_link([{clientid, <<"my subscriber">>} | Opts]),
    {ok, _} = Connect(Sub),
    {ok, _, [1]} = emqtt:subscribe(Sub, Topic, at_least_once),
    {ok, Pub} = emqtt:start_link([{clientid, <<"my publisher">>} | Opts]),
    {ok, _} = Connect(Pub),
    {ok, _} = emqtt:publish(Pub, Topic, Payload, at_least_once),
    receive {publish, #{client_pid := Sub,
                        topic := Topic,
                        payload := Payload}} -> ok
    after 1000 -> ct:fail("no publish received")
    end,

    %% reason code "Maximum connect time" defined in
    %% https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html#_Toc3901208
    ReasonCode = 16#A0,
    true = unlink(Sub),
    true = unlink(Pub),

    %% In 4 seconds from now, we expect that RabbitMQ disconnects us because our token expired.
    receive {disconnected, ReasonCode, _} -> ok
    after Millis * 2 -> ct:fail("missing DISCONNECT packet from server")
    end,
    receive {disconnected, ReasonCode, _} -> ok
    after Millis * 2 -> ct:fail("missing DISCONNECT packet from server")
    end.

amqp_expirable_token(Config) ->
    {ok, _} = application:ensure_all_started(rabbitmq_amqp_client),

    Seconds = 4,
    Millis = Seconds * 1000,
    {_Algo, Token} = generate_expirable_token(Config,
                                              [<<"rabbitmq.configure:*/*">>,
                                               <<"rabbitmq.write:*/*">>,
                                               <<"rabbitmq.read:*/*">>],
                                              Seconds),

    %% Send and receive a message via AMQP 1.0.
    QName = atom_to_binary(?FUNCTION_NAME),
    Address = rabbitmq_amqp_address:queue(QName),
    Host = ?config(rmq_hostname, Config),
    Port = rabbit_ct_broker_helpers:get_node_config(Config, 0, tcp_port_amqp),
    OpnConf = #{address => Host,
                port => Port,
                container_id => <<"my container">>,
                sasl => {plain, <<"">>, Token}},
    {ok, Connection} = amqp10_client:open_connection(OpnConf),
    {ok, Session} = amqp10_client:begin_session_sync(Connection),
    {ok, LinkPair} = rabbitmq_amqp_client:attach_management_link_pair_sync(Session, <<"my link pair">>),
    {ok, _} = rabbitmq_amqp_client:declare_queue(LinkPair, QName, #{}),
    {ok, Sender} = amqp10_client:attach_sender_link(Session, <<"my sender">>, Address),
    receive {amqp10_event, {link, Sender, credited}} -> ok
    after 5000 -> ct:fail({missing_event, ?LINE})
    end,
    Body = <<"hey">>,
    Msg0 = amqp10_msg:new(<<"tag">>, Body),
    ok = amqp10_client:send_msg(Sender, Msg0),
    {ok, Receiver} = amqp10_client:attach_receiver_link(Session, <<"my receiver">>, Address),
    {ok, Msg} = amqp10_client:get_msg(Receiver),
    ?assertEqual([Body], amqp10_msg:body(Msg)),

    %% In 4 seconds from now, we expect that RabbitMQ disconnects us because our token expired.
    receive {amqp10_event,
             {connection, Connection,
              {closed, {unauthorized_access, <<"credential expired">>}}}} ->
                ok
    after Millis * 2 ->
              ct:fail("server did not close our connection")
    end.

test_successful_connection_with_complex_claim_as_a_map(Config) ->
    {_Algo, Token} = generate_valid_token_with_extra_fields(
        Config,
        #{<<"additional_rabbitmq_scopes">> => #{<<"rabbitmq">> => [<<"configure:*/*">>, <<"read:*/*">>, <<"write:*/*">>]}}
    ),
    Conn     = open_unmanaged_connection(Config, 0, <<"username">>, Token),
    {ok, Ch} = amqp_connection:open_channel(Conn),
    #'queue.declare_ok'{queue = _} =
        amqp_channel:call(Ch, #'queue.declare'{exclusive = true}),
    close_connection_and_channel(Conn, Ch).

test_successful_connection_with_complex_claim_as_a_list(Config) ->
    {_Algo, Token} = generate_valid_token_with_extra_fields(
        Config,
        #{<<"additional_rabbitmq_scopes">> => [<<"rabbitmq.configure:*/*">>, <<"rabbitmq.read:*/*">>, <<"rabbitmq.write:*/*">>]}
    ),
    Conn     = open_unmanaged_connection(Config, 0, <<"username">>, Token),
    {ok, Ch} = amqp_connection:open_channel(Conn),
    #'queue.declare_ok'{queue = _} =
        amqp_channel:call(Ch, #'queue.declare'{exclusive = true}),
    close_connection_and_channel(Conn, Ch).

test_successful_connection_with_complex_claim_as_a_binary(Config) ->
    {_Algo, Token} = generate_valid_token_with_extra_fields(
        Config,
        #{<<"additional_rabbitmq_scopes">> => <<"rabbitmq.configure:*/* rabbitmq.read:*/* rabbitmq.write:*/*">>}
    ),
    Conn     = open_unmanaged_connection(Config, 0, <<"username">>, Token),
    {ok, Ch} = amqp_connection:open_channel(Conn),
    #'queue.declare_ok'{queue = _} =
        amqp_channel:call(Ch, #'queue.declare'{exclusive = true}),
    close_connection_and_channel(Conn, Ch).

test_successful_connection_with_keycloak_token(Config) ->
    {_Algo, Token} = generate_valid_token_with_extra_fields(
        Config,
        #{<<"authorization">> => #{<<"permissions">> =>
                [#{<<"rsid">> => <<"2c390fe4-02ad-41c7-98a2-cebb8c60ccf1">>,
                   <<"rsname">> => <<"allvhost">>,
                   <<"scopes">> => [<<"rabbitmq.configure:*/*">>]},
                 #{<<"rsid">> => <<"e7f12e94-4c34-43d8-b2b1-c516af644cee">>,
                   <<"rsname">> => <<"vhost1">>,
                   <<"scopes">> => [<<"rabbitmq.write:*/*">>]},
                 #{<<"rsid">> => <<"12ac3d1c-28c2-4521-8e33-0952eff10bd9">>,
                   <<"rsname">> => <<"Default Resource">>,
                   <<"scopes">> => [<<"rabbitmq.read:*/*">>]},
                   %% this one won't be used because of the resource id
                 #{<<"rsid">> => <<"bee8fac6-c3ec-11e9-aa8c-2a2ae2dbcce4">>,
                   <<"rsname">> => <<"Default Resource">>,
                   <<"scopes">> => [<<"rabbitmq-resource-read">>]}]}}
    ),
    Conn     = open_unmanaged_connection(Config, 0, <<"username">>, Token),
    {ok, Ch} = amqp_connection:open_channel(Conn),
    #'queue.declare_ok'{queue = _} =
        amqp_channel:call(Ch, #'queue.declare'{exclusive = true}),
    close_connection_and_channel(Conn, Ch).

test_successful_connection_with_rich_authorization_request_token(Config) ->
    {_Algo, Token} = generate_valid_token_with_extra_fields(
        Config,
        #{<<"authorization_details">> =>
                [#{<<"type">> => <<"rabbitmq-type">>,
                  <<"locations">> => [<<"cluster:rabbitmq">> ],
                  <<"actions">> => [<<"read">>,<<"configure">>, <<"write">>]
                  }
                ]
           }
    ),
    Conn     = open_unmanaged_connection(Config, 0, <<"username">>, Token),
    {ok, Ch} = amqp_connection:open_channel(Conn),
    #'queue.declare_ok'{queue = _} =
        amqp_channel:call(Ch, #'queue.declare'{exclusive = true}),
    close_connection_and_channel(Conn, Ch).

test_successful_token_refresh(Config) ->
    Duration = 5,
    {_, Token} = generate_expirable_token(Config, [<<"rabbitmq.configure:vhost1/*">>,
                                                   <<"rabbitmq.write:vhost1/*">>,
                                                   <<"rabbitmq.read:vhost1/*">>],
                                                   Duration),
    Conn     = open_unmanaged_connection(Config, 0, <<"vhost1">>, <<"username">>, Token),
    {ok, Ch} = amqp_connection:open_channel(Conn),

    {_, Token2} = generate_valid_token(Config, [<<"rabbitmq.configure:vhost1/*">>,
                                                <<"rabbitmq.write:vhost1/*">>,
                                                <<"rabbitmq.read:vhost1/*">>]),
    ?UTIL_MOD:wait_for_token_to_expire(timer:seconds(Duration)),
    ?assertEqual(ok, amqp_connection:update_secret(Conn, Token2, <<"token refresh">>)),

    {ok, Ch2} = amqp_connection:open_channel(Conn),

    #'queue.declare_ok'{queue = _} =
        amqp_channel:call(Ch, #'queue.declare'{exclusive = true}),
    #'queue.declare_ok'{queue = _} =
        amqp_channel:call(Ch2, #'queue.declare'{exclusive = true}),

    amqp_channel:close(Ch2),
    close_connection_and_channel(Conn, Ch).


test_failed_connection_with_expired_token(Config) ->
    {_Algo, Token} = generate_expired_token(Config, [<<"rabbitmq.configure:vhost1/*">>,
                                                     <<"rabbitmq.write:vhost1/*">>,
                                                     <<"rabbitmq.read:vhost1/*">>]),
    ?assertMatch({error, {auth_failure, _}},
                 open_unmanaged_connection(Config, 0, <<"vhost1">>, <<"username">>, Token)).

test_failed_connection_with_a_non_token(Config) ->
    ?assertMatch({error, {auth_failure, _}},
                 open_unmanaged_connection(Config, 0, <<"vhost1">>, <<"username">>, <<"a-non-token-value">>)).

test_failed_connection_with_a_token_with_insufficient_vhost_permission(Config) ->
    {_Algo, Token} = generate_valid_token(Config, [<<"rabbitmq.configure:alt-vhost/*">>,
                                                   <<"rabbitmq.write:alt-vhost/*">>,
                                                   <<"rabbitmq.read:alt-vhost/*">>]),
    ?assertEqual({error, not_allowed},
                 open_unmanaged_connection(Config, 0, <<"off-limits-vhost">>, <<"username">>, Token)).

test_failed_connection_with_a_token_with_insufficient_resource_permission(Config) ->
    {_Algo, Token} = generate_valid_token(Config, [<<"rabbitmq.configure:vhost2/jwt*">>,
                                                   <<"rabbitmq.write:vhost2/jwt*">>,
                                                   <<"rabbitmq.read:vhost2/jwt*">>]),
    Conn     = open_unmanaged_connection(Config, 0, <<"vhost2">>, <<"username">>, Token),
    {ok, Ch} = amqp_connection:open_channel(Conn),
    ?assertExit({{shutdown, {server_initiated_close, 403, _}}, _},
       amqp_channel:call(Ch, #'queue.declare'{queue = <<"alt-prefix.eq.1">>, exclusive = true})),
    close_connection(Conn).

test_failed_token_refresh_case1(Config) ->
    {_, Token} = generate_valid_token(Config, [<<"rabbitmq.configure:vhost4/*">>,
                                               <<"rabbitmq.write:vhost4/*">>,
                                               <<"rabbitmq.read:vhost4/*">>]),
    Conn     = open_unmanaged_connection(Config, 0, <<"vhost4">>, <<"username">>, Token),
    {ok, Ch} = amqp_connection:open_channel(Conn),
    #'queue.declare_ok'{queue = _} =
        amqp_channel:call(Ch, #'queue.declare'{exclusive = true}),

    {_, Token2} = generate_expired_token(Config, [<<"rabbitmq.configure:vhost4/*">>,
                                                  <<"rabbitmq.write:vhost4/*">>,
                                                  <<"rabbitmq.read:vhost4/*">>]),
    %% the error is communicated asynchronously via a connection-level error
    ?assertEqual(ok, amqp_connection:update_secret(Conn, Token2, <<"token refresh">>)),

    {ok, Ch2} = amqp_connection:open_channel(Conn),
    ?assertExit({{shutdown, {server_initiated_close, 403, _}}, _},
       amqp_channel:call(Ch2, #'queue.declare'{queue = <<"a.q">>, exclusive = true})),

    close_connection(Conn).

test_failed_token_refresh_case2(Config) ->
    {_Algo, Token} = generate_valid_token(Config, [<<"rabbitmq.configure:vhost4/*">>,
                                                   <<"rabbitmq.write:vhost4/*">>,
                                                   <<"rabbitmq.read:vhost4/*">>]),
    Conn     = open_unmanaged_connection(Config, 0, <<"vhost4">>, <<"username">>, Token),
    {ok, Ch} = amqp_connection:open_channel(Conn),
    #'queue.declare_ok'{queue = _} =
        amqp_channel:call(Ch, #'queue.declare'{exclusive = true}),

    %% the error is communicated asynchronously via a connection-level error
    ?assertEqual(ok, amqp_connection:update_secret(Conn, <<"not-a-token-^^^^5%">>, <<"token refresh">>)),

    ?assertExit({{shutdown, {connection_closing, {server_initiated_close, 530, _}}}, _},
       amqp_connection:open_channel(Conn)),

    close_connection(Conn).


test_successful_connection_with_with_single_scope_alias_in_extra_scopes_source(Config) ->
    test_successful_connection_with_with_scope_aliases_in_extra_scopes_source(Config, ?SCOPE_ALIAS_NAME, <<"vhost1">>).

test_successful_connection_with_with_multiple_scope_aliases_in_extra_scopes_source(Config) ->
    Claims = [?SCOPE_ALIAS_NAME, ?SCOPE_ALIAS_NAME_2, ?SCOPE_ALIAS_NAME_3],
    test_successful_connection_with_with_scope_aliases_in_extra_scopes_source(Config, Claims, <<"vhost4">>).

test_successful_connection_with_with_scope_aliases_in_extra_scopes_source(Config, Claims, VHost) ->
    {_Algo, Token} = generate_valid_token_with_extra_fields(
        Config,
        #{<<"claims">> => Claims}
    ),
    Conn     = open_unmanaged_connection(Config, 0, VHost, <<"username">>, Token),
    {ok, Ch} = amqp_connection:open_channel(Conn),
    #'queue.declare_ok'{} =
        amqp_channel:call(Ch, #'queue.declare'{queue = <<"one">>, exclusive = true}),
    close_connection_and_channel(Conn, Ch).


test_successful_connection_with_scope_alias_in_scope_field_case1(Config) ->
    test_successful_connection_with_scope_alias_in_scope_field_case(Config, ?SCOPE_ALIAS_NAME, <<"vhost2">>).

test_successful_connection_with_scope_alias_in_scope_field_case2(Config) ->
    test_successful_connection_with_scope_alias_in_scope_field_case(Config, [?SCOPE_ALIAS_NAME], <<"vhost2">>).

test_successful_connection_with_scope_alias_in_scope_field_case3(Config) ->
    Scopes = [?SCOPE_ALIAS_NAME, ?SCOPE_ALIAS_NAME_2, ?SCOPE_ALIAS_NAME_3],
    test_successful_connection_with_scope_alias_in_scope_field_case(Config, Scopes, <<"vhost3">>).

test_successful_connection_with_scope_alias_in_scope_field_case(Config, Scopes, VHost) ->
    {_Algo, Token} = generate_valid_token(Config, Scopes),
    Conn     = open_unmanaged_connection(Config, 0, VHost, <<"username">>, Token),
    {ok, Ch} = amqp_connection:open_channel(Conn),
    #'queue.declare_ok'{} =
        amqp_channel:call(Ch, #'queue.declare'{queue = <<"one">>, exclusive = true}),
    close_connection_and_channel(Conn, Ch).

test_failed_connection_with_with_non_existent_scope_alias_in_extra_scopes_source(Config) ->
    {_Algo, Token} = generate_valid_token_with_extra_fields(
        Config,
        #{<<"claims">> => <<"non-existent alias 24823478374">>}
    ),
    ?assertMatch({error, not_allowed},
                 open_unmanaged_connection(Config, 0, <<"vhost1">>, <<"username">>, Token)).

test_failed_connection_with_non_existent_scope_alias_in_scope_field(Config) ->
    {_Algo, Token} = generate_valid_token(Config, <<"non-existent alias a8798s7doaisd79">>),
    ?assertMatch({error, not_allowed},
                 open_unmanaged_connection(Config, 0, <<"vhost2">>, <<"username">>, Token)).


more_than_one_resource_server_id_not_allowed_in_one_token(Config) ->
    {_Algo, Token} = generate_valid_token(Config, <<"rmq.configure:*/*">>, [<<"prod">>, <<"dev">>]),
    {error, _} = open_unmanaged_connection(Config, 0, <<"username">>, Token).
