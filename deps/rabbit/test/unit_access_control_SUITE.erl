% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2011-2023 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(unit_access_control_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("kernel/include/file.hrl").
-include_lib("amqp_client/include/amqp_client.hrl").
-include_lib("eunit/include/eunit.hrl").

-compile(export_all).

all() ->
    [
      {group, sequential_tests},
      {group, parallel_tests}
    ].

groups() ->
    [
      {parallel_tests, [parallel], [
          password_hashing,
          unsupported_connection_refusal
      ]},
      {sequential_tests, [], [
          login_with_credentials_but_no_password,
          login_of_passwordless_user,
          set_tags_for_passwordless_user,
          change_password,
          auth_backend_internal_expand_topic_permission,
          rabbit_direct_extract_extra_auth_props
      ]}
    ].

%% -------------------------------------------------------------------
%% Testsuite setup/teardown
%% -------------------------------------------------------------------

init_per_suite(Config) ->
    rabbit_ct_helpers:log_environment(),
    rabbit_ct_helpers:run_setup_steps(Config).

end_per_suite(Config) ->
    rabbit_ct_helpers:run_teardown_steps(Config).

init_per_group(Group, Config) ->
    Config1 = rabbit_ct_helpers:set_config(Config, [
        {rmq_nodename_suffix, Group},
        {rmq_nodes_count, 1}
      ]),
    rabbit_ct_helpers:run_steps(Config1,
      rabbit_ct_broker_helpers:setup_steps() ++
      rabbit_ct_client_helpers:setup_steps()).

end_per_group(_Group, Config) ->
    rabbit_ct_helpers:run_steps(Config,
      rabbit_ct_client_helpers:teardown_steps() ++
      rabbit_ct_broker_helpers:teardown_steps()).

init_per_testcase(Testcase, Config) ->
    rabbit_ct_helpers:testcase_started(Config, Testcase).

end_per_testcase(Testcase, Config) ->
    rabbit_ct_helpers:testcase_finished(Config, Testcase).

%% ---------------------------------------------------------------------------
%% Test Cases
%% ---------------------------------------------------------------------------

password_hashing(Config) ->
    passed = rabbit_ct_broker_helpers:rpc(Config, 0,
      ?MODULE, password_hashing1, [Config]).

password_hashing1(_Config) ->
    rabbit_password_hashing_sha256 = rabbit_password:hashing_mod(),
    application:set_env(rabbit, password_hashing_module,
                        rabbit_password_hashing_md5),
    rabbit_password_hashing_md5    = rabbit_password:hashing_mod(),
    application:set_env(rabbit, password_hashing_module,
                        rabbit_password_hashing_sha256),
    rabbit_password_hashing_sha256 = rabbit_password:hashing_mod(),

    rabbit_password_hashing_sha256 =
        rabbit_password:hashing_mod(rabbit_password_hashing_sha256),
    rabbit_password_hashing_md5    =
        rabbit_password:hashing_mod(rabbit_password_hashing_md5),
    rabbit_password_hashing_md5    =
        rabbit_password:hashing_mod(undefined),

    rabbit_password_hashing_md5    =
        rabbit_auth_backend_internal:hashing_module_for_user(
          internal_user:new()),
    rabbit_password_hashing_md5    =
        rabbit_auth_backend_internal:hashing_module_for_user(
          internal_user:new({hashing_algorithm, undefined})),
    rabbit_password_hashing_md5    =
        rabbit_auth_backend_internal:hashing_module_for_user(
          internal_user:new({hashing_algorithm, rabbit_password_hashing_md5})),

    rabbit_password_hashing_sha256 =
        rabbit_auth_backend_internal:hashing_module_for_user(
          internal_user:new({hashing_algorithm, rabbit_password_hashing_sha256})),

    passed.

change_password(Config) ->
    passed = rabbit_ct_broker_helpers:rpc(Config, 0,
      ?MODULE, change_password1, [Config]).

change_password1(_Config) ->
    UserName = <<"test_user">>,
    Password = <<"test_password">>,
    case rabbit_auth_backend_internal:lookup_user(UserName) of
        {ok, _} -> rabbit_auth_backend_internal:delete_user(UserName, <<"acting-user">>);
        _       -> ok
    end,
    ok = application:set_env(rabbit, password_hashing_module,
                             rabbit_password_hashing_md5),
    ok = rabbit_auth_backend_internal:add_user(UserName, Password, <<"acting-user">>),
    {ok, #auth_user{username = UserName}} =
        rabbit_auth_backend_internal:user_login_authentication(
            UserName, [{password, Password}]),
    ok = application:set_env(rabbit, password_hashing_module,
                             rabbit_password_hashing_sha256),
    {ok, #auth_user{username = UserName}} =
        rabbit_auth_backend_internal:user_login_authentication(
            UserName, [{password, Password}]),

    NewPassword = <<"test_password1">>,
    ok = rabbit_auth_backend_internal:change_password(UserName, NewPassword,
                                                      <<"acting-user">>),
    {ok, #auth_user{username = UserName}} =
        rabbit_auth_backend_internal:user_login_authentication(
            UserName, [{password, NewPassword}]),

    {refused, _, [UserName]} =
        rabbit_auth_backend_internal:user_login_authentication(
            UserName, [{password, Password}]),
    passed.


login_with_credentials_but_no_password(Config) ->
    passed = rabbit_ct_broker_helpers:rpc(Config, 0,
      ?MODULE, login_with_credentials_but_no_password1, [Config]).

login_with_credentials_but_no_password1(_Config) ->
    Username = <<"login_with_credentials_but_no_password-user">>,
    Password = <<"login_with_credentials_but_no_password-password">>,
    ok = rabbit_auth_backend_internal:add_user(Username, Password, <<"acting-user">>),

    ?assertMatch(
       {refused, _Message, [Username]},
        rabbit_auth_backend_internal:user_login_authentication(Username,
                                                              [{key, <<"value">>}])),

    ok = rabbit_auth_backend_internal:delete_user(Username, <<"acting-user">>),

    passed.

%% passwordless users are not supposed to be used with
%% this backend (and PLAIN authentication mechanism in general)
login_of_passwordless_user(Config) ->
    passed = rabbit_ct_broker_helpers:rpc(Config, 0,
      ?MODULE, login_of_passwordless_user1, [Config]).

login_of_passwordless_user1(_Config) ->
    Username = <<"login_of_passwordless_user-user">>,
    Password = <<"">>,
    ok = rabbit_auth_backend_internal:add_user(Username, Password, <<"acting-user">>),

    ?assertMatch(
       {refused, _Message, [Username]},
       rabbit_auth_backend_internal:user_login_authentication(Username,
                                                              [{password, <<"">>}])),

    ?assertMatch(
       {refused, _Format, [Username]},
       rabbit_auth_backend_internal:user_login_authentication(Username,
                                                              [{password, ""}])),

    ok = rabbit_auth_backend_internal:delete_user(Username, <<"acting-user">>),

    passed.


set_tags_for_passwordless_user(Config) ->
    passed = rabbit_ct_broker_helpers:rpc(Config, 0,
      ?MODULE, set_tags_for_passwordless_user1, [Config]).

set_tags_for_passwordless_user1(_Config) ->
    Username = <<"set_tags_for_passwordless_user">>,
    Password = <<"set_tags_for_passwordless_user">>,
    ok = rabbit_auth_backend_internal:add_user(Username, Password,
                                               <<"acting-user">>),
    ok = rabbit_auth_backend_internal:clear_password(Username,
                                                     <<"acting-user">>),
    ok = rabbit_auth_backend_internal:set_tags(Username, [management],
                                               <<"acting-user">>),

    {ok, User1} = rabbit_auth_backend_internal:lookup_user(Username),
    ?assertEqual([management], internal_user:get_tags(User1)),

    ok = rabbit_auth_backend_internal:set_tags(Username, [management, policymaker],
                                               <<"acting-user">>),

    {ok, User2} = rabbit_auth_backend_internal:lookup_user(Username),
    ?assertEqual([management, policymaker], internal_user:get_tags(User2)),

    ok = rabbit_auth_backend_internal:set_tags(Username, [],
                                               <<"acting-user">>),

    {ok, User3} = rabbit_auth_backend_internal:lookup_user(Username),
    ?assertEqual([], internal_user:get_tags(User3)),

    ok = rabbit_auth_backend_internal:delete_user(Username,
                                                  <<"acting-user">>),

    passed.


rabbit_direct_extract_extra_auth_props(_Config) ->
    {ok, CSC} = code_server_cache:start_link(),
    % no protocol to extract
    [] = rabbit_direct:extract_extra_auth_props(
        {<<"guest">>, <<"guest">>}, <<"/">>, 1,
        [{name,<<"127.0.0.1:52366 -> 127.0.0.1:1883">>}]),
    % protocol to extract, but no module to call
    [] = rabbit_direct:extract_extra_auth_props(
        {<<"guest">>, <<"guest">>}, <<"/">>, 1,
        [{protocol, {'PROTOCOL_WITHOUT_MODULE', "1.0"}}]),
    % see rabbit_dummy_protocol_connection_info module
    % protocol to extract, module that returns a client ID
    [{client_id, <<"DummyClientId">>}] = rabbit_direct:extract_extra_auth_props(
        {<<"guest">>, <<"guest">>}, <<"/">>, 1,
        [{protocol, {'DUMMY_PROTOCOL', "1.0"}}]),
    % protocol to extract, but error thrown in module
    [] = rabbit_direct:extract_extra_auth_props(
        {<<"guest">>, <<"guest">>}, <<"/">>, -1,
        [{protocol, {'DUMMY_PROTOCOL', "1.0"}}]),
    gen_server:stop(CSC),
    ok.

auth_backend_internal_expand_topic_permission(_Config) ->
    ExpandMap = #{<<"username">> => <<"guest">>, <<"vhost">> => <<"default">>},
    %% simple case
    <<"services/default/accounts/guest/notifications">> =
        rabbit_auth_backend_internal:expand_topic_permission(
            <<"services/{vhost}/accounts/{username}/notifications">>,
            ExpandMap
        ),
    %% replace variable twice
    <<"services/default/accounts/default/guest/notifications">> =
        rabbit_auth_backend_internal:expand_topic_permission(
            <<"services/{vhost}/accounts/{vhost}/{username}/notifications">>,
            ExpandMap
        ),
    %% nothing to replace
    <<"services/accounts/notifications">> =
        rabbit_auth_backend_internal:expand_topic_permission(
            <<"services/accounts/notifications">>,
            ExpandMap
        ),
    %% the expand map isn't defined
    <<"services/{vhost}/accounts/{username}/notifications">> =
        rabbit_auth_backend_internal:expand_topic_permission(
            <<"services/{vhost}/accounts/{username}/notifications">>,
            undefined
        ),
    %% the expand map is empty
    <<"services/{vhost}/accounts/{username}/notifications">> =
        rabbit_auth_backend_internal:expand_topic_permission(
            <<"services/{vhost}/accounts/{username}/notifications">>,
            #{}
        ),
    ok.

unsupported_connection_refusal(Config) ->
    passed = rabbit_ct_broker_helpers:rpc(Config, 0,
      ?MODULE, unsupported_connection_refusal1, [Config]).

unsupported_connection_refusal1(Config) ->
    H = ?config(rmq_hostname, Config),
    P = rabbit_ct_broker_helpers:get_node_config(Config, 0, tcp_port_amqp),
    [passed = test_unsupported_connection_refusal(H, P, V) ||
        V <- [<<"AMQP",9,9,9,9>>, <<"AMQP",0,1,0,0>>, <<"XXXX",0,0,9,1>>]],
    passed.

test_unsupported_connection_refusal(H, P, Header) ->
    {ok, C} = gen_tcp:connect(H, P, [binary, {active, false}]),
    ok = gen_tcp:send(C, Header),
    {ok, <<"AMQP",0,0,9,1>>} = gen_tcp:recv(C, 8, 100),
    ok = gen_tcp:close(C),
    passed.
