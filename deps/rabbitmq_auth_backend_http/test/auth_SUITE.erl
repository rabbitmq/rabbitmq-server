%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2024 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.

-module(auth_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("rabbit_common/include/rabbit.hrl").

-compile(export_all).

-define(AUTH_PORT, 8000).
-define(USER_PATH, "/auth/user").
-define(BACKEND_CONFIG,
	[{http_method, get},
     {user_path, "http://localhost:" ++ integer_to_list(?AUTH_PORT) ++ ?USER_PATH},
	 {vhost_path, "http://localhost:" ++ integer_to_list(?AUTH_PORT) ++ "/auth/vhost"},
     {resource_path, "http://localhost:" ++ integer_to_list(?AUTH_PORT) ++ "/auth/resource"},
	 {topic_path, "http://localhost:" ++ integer_to_list(?AUTH_PORT) ++ "/auth/topic"}]).
-define(ALLOWED_USER, #{username => <<"Ala1">>,
                        password => <<"Kocur">>,
												expected_credentials => [username, password],
                        tags => [policymaker, monitoring]}).
-define(ALLOWED_USER_WITH_EXTRA_CREDENTIALS, #{username => <<"Ala2">>,
                        password => <<"Kocur">>,
												client_id => <<"some_id">>,
												expected_credentials => [username, password, client_id],
                        tags => [policymaker, monitoring]}).
-define(DENIED_USER, #{username => <<"Alice">>,
											 password => <<"Cat">>
											 }).

all() -> [grants_access_to_user,
					denies_access_to_user,
					grants_access_to_user_passing_additional_required_authprops,
					grants_access_to_user_skipping_internal_authprops,
					grants_access_to_user_with_credentials_in_rabbit_auth_backend_http,
					grants_access_to_user_with_credentials_in_rabbit_auth_backend_cache].

init_per_suite(Config) ->
    configure_http_auth_backend(),
    {User1, Tuple1} = extractUserTuple(?ALLOWED_USER),
		{User2, Tuple2} = extractUserTuple(?ALLOWED_USER_WITH_EXTRA_CREDENTIALS),
    start_http_auth_server(?AUTH_PORT, ?USER_PATH, #{User1 => Tuple1, User2 => Tuple2}),
    [{allowed_user, ?ALLOWED_USER},
			{allowed_user_with_extra_credentials, ?ALLOWED_USER_WITH_EXTRA_CREDENTIALS},
			{denied_user, ?DENIED_USER} | Config].
extractUserTuple(User) ->
	#{username := Username, password := Password, tags := Tags, expected_credentials := ExpectedCredentials} = User,
	{Username, {Password, Tags, ExpectedCredentials}}.

end_per_suite(_Config) ->
    stop_http_auth_server().

grants_access_to_user(Config) ->
    #{username := U, password := P, tags := T} = ?config(allowed_user, Config),
		AuthProps = [{password, P}],
    {ok, User} = rabbit_auth_backend_http:user_login_authentication(U, AuthProps),

    ?assertMatch({U, T, AuthProps},
                 {User#auth_user.username, User#auth_user.tags, (User#auth_user.impl)()}).

denies_access_to_user(Config) ->
    #{username := U, password := P} = ?config(denied_user, Config),
    ?assertMatch({refused, "Denied by the backing HTTP service", []},
                  rabbit_auth_backend_http:user_login_authentication(U, [{password, P}])).


grants_access_to_user_passing_additional_required_authprops(Config) ->
    #{username := U, password := P, tags := T, client_id := ClientId} = ?config(allowed_user_with_extra_credentials, Config),
		AuthProps = [{password, P}, {client_id, ClientId}],
    {ok, User} = rabbit_auth_backend_http:user_login_authentication(U, AuthProps),
		?assertMatch({U, T, AuthProps},
                 {User#auth_user.username, User#auth_user.tags, (User#auth_user.impl)()}).

grants_access_to_user_skipping_internal_authprops(Config) ->
    #{username := U, password := P, tags := T, client_id := ClientId} = ?config(allowed_user_with_extra_credentials, Config),
		AuthProps = [{password, P}, {client_id, ClientId}, {rabbit_any_internal_property, <<"some value">>}],
    {ok, User} = rabbit_auth_backend_http:user_login_authentication(U, AuthProps),

		?assertMatch({U, T, AuthProps},
                 {User#auth_user.username, User#auth_user.tags, (User#auth_user.impl)()}).

grants_access_to_user_with_credentials_in_rabbit_auth_backend_http(Config) ->
    #{username := U, password := P, tags := T, client_id := ClientId} = ?config(allowed_user_with_extra_credentials, Config),
		AuthProps = [{rabbit_auth_backend_http, fun() -> [{password, P}, {client_id, ClientId}] end}],
    {ok, User} = rabbit_auth_backend_http:user_login_authentication(U, AuthProps),

    ?assertMatch({U, T, AuthProps},
                 {User#auth_user.username, User#auth_user.tags, (User#auth_user.impl)()}).

grants_access_to_user_with_credentials_in_rabbit_auth_backend_cache(Config) ->
    #{username := U, password := P, tags := T, client_id := ClientId} = ?config(allowed_user_with_extra_credentials, Config),
		AuthProps = [{rabbit_auth_backend_cache, fun() -> [{password, P}, {client_id, ClientId}] end}],
    {ok, User} = rabbit_auth_backend_http:user_login_authentication(U, AuthProps),

    ?assertMatch({U, T, AuthProps},
                 {User#auth_user.username, User#auth_user.tags, (User#auth_user.impl)()}).

%%% HELPERS

configure_http_auth_backend() ->
    {ok, _} = application:ensure_all_started(inets),
    [application:set_env(rabbitmq_auth_backend_http, K, V) || {K, V} <- ?BACKEND_CONFIG].

start_http_auth_server(Port, Path, Users) ->
    application:ensure_all_started(cowboy),
    Dispatch = cowboy_router:compile([{'_', [{Path, auth_http_mock, Users}]}]),
    {ok, _} = cowboy:start_clear(
        mock_http_auth_listener, [{port, Port}], #{env => #{dispatch => Dispatch}}).

stop_http_auth_server() ->
    cowboy:stop_listener(mock_http_auth_listener).
