%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2017-2021 VMware, Inc. or its affiliates.  All rights reserved.

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
-define(ALLOWED_USER, #{username => <<"Ala">>,
                        password => <<"Kocur">>,
                        tags => [policymaker, monitoring]}).
-define(DENIED_USER, #{username => <<"Alice">>, password => <<"Cat">>}).

all() -> [grants_access_to_user, denies_access_to_user].

init_per_suite(Config) ->
    configure_http_auth_backend(),
    #{username := Username, password := Password, tags := Tags} = ?ALLOWED_USER,
    start_http_auth_server(?AUTH_PORT, ?USER_PATH, #{Username => {Password, Tags}}),
    [{allowed_user, ?ALLOWED_USER}, {denied_user, ?DENIED_USER} | Config].

end_per_suite(_Config) ->
    stop_http_auth_server().

grants_access_to_user(Config) ->
    #{username := U, password := P, tags := T} = ?config(allowed_user, Config),
    ?assertMatch({ok, #auth_user{username = U, tags = T}},
		         rabbit_auth_backend_http:user_login_authentication(U, [{password, P}])).

denies_access_to_user(Config) ->
    #{username := U, password := P} = ?config(denied_user, Config),
    ?assertMatch({refused, "Denied by the backing HTTP service", []},
                  rabbit_auth_backend_http:user_login_authentication(U, [{password, P}])).

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
