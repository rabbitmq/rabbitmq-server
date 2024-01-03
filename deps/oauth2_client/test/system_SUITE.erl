%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2017-2023 VMware, Inc. or its affiliates.  All rights reserved.

-module(system_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("rabbit_common/include/rabbit.hrl").

-include_lib("oauth2_client.hrl").

-compile(export_all).

-define(MOCK_TOKEN_ENDPOINT, <<"/token">>).
-define(AUTH_PORT, 8000).
-define(GRANT_ACCESS_TOKEN,  #{request => #{
																method => <<"POST">>,
																path => ?MOCK_TOKEN_ENDPOINT,
																parameters => [
																	{?REQUEST_CLIENT_ID, <<"guest">>},
	                        		  	{?REQUEST_CLIENT_SECRET, <<"password">>}
																]},
															response => [
																{code, 200},
																{content_type, ?CONTENT_JSON},
																{payload, [
																	{access_token, <<"some access token">>},
																	{token_type, <<"Bearer">>}
																]}
															]
														}).
-define(DENIES_ACCESS_TOKEN, #{request => #{
																method => <<"POST">>,
																path => ?MOCK_TOKEN_ENDPOINT,
																parameters => [
																	{?REQUEST_CLIENT_ID, <<"invalid_client">>},
	                        		  	{?REQUEST_CLIENT_SECRET, <<"password">>}
																]},
															response => [
																{code, 400},
																{content_type, ?CONTENT_JSON},
																{payload, [
																	{error, <<"invalid_client">>},
																	{error_description, <<"invalid client found">>}
																]}
															]
														}).

-define(AUTH_SERVER_ERROR,   #{request => #{
																method => <<"POST">>,
																path => ?MOCK_TOKEN_ENDPOINT,
																parameters => [
																	{?REQUEST_CLIENT_ID, <<"guest">>},
	                        		  	{?REQUEST_CLIENT_SECRET, <<"password">>}
																]},
															response => [
																{code, 500}
															]
														}).

-define(NON_JSON_PAYLOAD,   #{request => #{
																method => <<"POST">>,
																path => ?MOCK_TOKEN_ENDPOINT,
																parameters => [
																	{?REQUEST_CLIENT_ID, <<"guest">>},
	                        		  	{?REQUEST_CLIENT_SECRET, <<"password">>}
																]},
															response => [
																{code, 400},
																{content_type, ?CONTENT_JSON},
																{payload, <<"{ some illegal json}">>}
															]
														}).

-define(GET_OPENID_CONFIGURATION,
														#{request => #{
																method => <<"GET">>,
																path => ?DEFAULT_OPENID_CONFIGURATION_PATH
												 			},
															response => [
																{code, 200},
																{content_type, ?CONTENT_JSON},
																{payload, [
																	{issuer, build_issuer("http") },
																	{authorization_endpoint, <<"http://localhost:8000/authorize">>},
																	{token_endpoint, build_token_endpoint_uri("http")},
																	{jwks_uri, build_jwks_uri("http")}
																]}
															]
														}).
-define(GET_OPENID_CONFIGURATION_WITH_SSL,
														#{request => #{
																method => <<"GET">>,
																path => ?DEFAULT_OPENID_CONFIGURATION_PATH
												 			},
															response => [
																{code, 200},
																{content_type, ?CONTENT_JSON},
																{payload, [
																	{issuer, build_issuer("https") },
																	{authorization_endpoint, <<"https://localhost:8000/authorize">>},
																	{token_endpoint, build_token_endpoint_uri("https")},
																	{jwks_uri, build_jwks_uri("https")}
																]}
															]
														}).
-define(GRANTS_REFRESH_TOKEN,
														#{request => #{
																method => <<"POST">>,
																path => ?MOCK_TOKEN_ENDPOINT,
																parameters => [
																	{?REQUEST_CLIENT_ID, <<"guest">>},
	                        		  	{?REQUEST_CLIENT_SECRET, <<"password">>},
																	{?REQUEST_REFRESH_TOKEN, <<"some refresh token">>}
																]
															},
												 			response => [
																{code, 200},
																{content_type, ?CONTENT_JSON},
																{payload, [
																	{access_token, <<"some refreshed access token">>},
																	{token_type, <<"Bearer">>}
																]}
															]
														}).

all() ->
    [
      {group, http_up}
			,{group, http_down}
      ,{group, https}
    ].

groups() ->
    [
     {http_up, [], [
		 							{group, verify_access_token},
									{group, with_all_oauth_provider_settings},
									{group, without_all_oauth_providers_settings}
     ]},
		 {with_all_oauth_provider_settings, [], [
		 						{group, verify_get_oauth_provider}
			]},
		 {without_all_oauth_providers_settings, [], [
		 						{group, verify_get_oauth_provider}
		 ]},
		 {verify_access_token, [], [
								 grants_access_token,
								 denies_access_token,
								 auth_server_error,
								 non_json_payload,
							 	 grants_refresh_token,
 								 grants_access_token_using_oauth_provider_id
		 ]},
		 {verify_get_oauth_provider, [], [
                  get_oauth_provider,
 									get_oauth_provider_given_oauth_provider_id
		 ]},

		 {http_down, [], [
									connection_error
								 ]},
     {https, [], [
		 							grants_access_token,
									grants_refresh_token,
                  ssl_connection_error,
									{group, with_all_oauth_provider_settings},
									{group, without_all_oauth_providers_settings}
                 ]}
    ].

init_per_suite(Config) ->
  [
			{denies_access_token, [ {token_endpoint, ?DENIES_ACCESS_TOKEN} ]},
		 	{auth_server_error, [ {token_endpoint, ?AUTH_SERVER_ERROR} ]},
			{non_json_payload, [ {token_endpoint, ?NON_JSON_PAYLOAD} ]},
			{grants_refresh_token, [ {token_endpoint, ?GRANTS_REFRESH_TOKEN} ]}

			| Config].

end_per_suite(Config) ->
  Config.

init_per_group(https, Config) ->
	{ok, _} = application:ensure_all_started(ssl),
  application:ensure_all_started(cowboy),
	Config0 = rabbit_ct_helpers:run_setup_steps(Config),
	CertsDir = ?config(rmq_certsdir, Config0),
	CaCertFile = filename:join([CertsDir, "testca", "cacert.pem"]),
	WrongCaCertFile = filename:join([CertsDir, "server", "server.pem"]),
	[{group, https},
		{oauth_provider_id, <<"uaa">>},
		{oauth_provider, build_https_oauth_provider(CaCertFile)},
		{oauth_provider_with_issuer, keep_only_issuer_and_ssl_options(build_https_oauth_provider(CaCertFile))},
		{issuer, build_issuer("https")},
		{oauth_provider_with_wrong_ca, build_https_oauth_provider(WrongCaCertFile)} |
	 	Config0];

init_per_group(http_up, Config) ->
	{ok, _} = application:ensure_all_started(inets),
  application:ensure_all_started(cowboy),
	[{group, http_up},
		{oauth_provider_id, <<"uaa">>},
		{issuer, build_issuer("http")},
		{oauth_provider_with_issuer, keep_only_issuer_and_ssl_options(build_http_oauth_provider())},
		{oauth_provider, build_http_oauth_provider()} | Config];

init_per_group(http_down, Config) ->
	[
		{issuer, build_issuer("http")},
		{oauth_provider_id, <<"uaa">>},
		{oauth_provider, build_http_oauth_provider()} | Config];

init_per_group(with_all_oauth_provider_settings, Config) ->
	[
		{with_all_oauth_provider_settings, true} | Config];

init_per_group(without_all_oauth_providers_settings, Config) ->
	[
		{with_all_oauth_provider_settings, false} | Config];

init_per_group(_, Config) ->
	Config.


get_http_oauth_server_expectations(TestCase, Config) ->
	case ?config(TestCase, Config) of
		undefined -> case ?config(group, Config) of
			 					  https -> [
										{token_endpoint, ?GRANT_ACCESS_TOKEN},
										{get_openid_configuration, ?GET_OPENID_CONFIGURATION_WITH_SSL }
										];
									_ -> [
										{token_endpoint, ?GRANT_ACCESS_TOKEN},
										{get_openid_configuration, ?GET_OPENID_CONFIGURATION }
										]
								end;
		Expectations -> Expectations
	end.

lookup_expectation(Endpoint, Config) ->
	proplists:get_value(Endpoint, ?config(oauth_server_expectations, Config)).

configure_all_oauth_provider_settings(Config) ->
	OAuthProvider = ?config(oauth_provider, Config),
	OAuthProviders = #{ ?config(oauth_provider_id, Config) => oauth_provider_to_proplist(OAuthProvider) },

	application:set_env(rabbitmq_auth_backend_oauth2, issuer, OAuthProvider#oauth_provider.issuer),
	application:set_env(rabbitmq_auth_backend_oauth2, oauth_providers, OAuthProviders),
	application:set_env(rabbitmq_auth_backend_oauth2, token_endpoint, OAuthProvider#oauth_provider.token_endpoint),
	KeyConfig = [ { jwks_url, OAuthProvider#oauth_provider.jwks_uri } ] ++
		case OAuthProvider#oauth_provider.ssl_options of
			undefined -> [];
			_ ->  [ {peer_verification, proplists:get_value(verify, OAuthProvider#oauth_provider.ssl_options) },
							{cacertfile, proplists:get_value(cacertfile, OAuthProvider#oauth_provider.ssl_options) } ]
	 end,
	application:set_env(rabbitmq_auth_backend_oauth2, key_config, KeyConfig).

configure_minimum_oauth_provider_settings(Config) ->
	OAuthProvider = ?config(oauth_provider_with_issuer, Config),
	OAuthProviders = #{ ?config(oauth_provider_id, Config) => oauth_provider_to_proplist(OAuthProvider) },
	application:set_env(rabbitmq_auth_backend_oauth2, oauth_providers, OAuthProviders),
	application:set_env(rabbitmq_auth_backend_oauth2, issuer, OAuthProvider#oauth_provider.issuer),
	KeyConfig =
		case OAuthProvider#oauth_provider.ssl_options of
			undefined -> [];
			_ ->  [ {peer_verification, proplists:get_value(verify, OAuthProvider#oauth_provider.ssl_options) },
							{cacertfile, proplists:get_value(cacertfile, OAuthProvider#oauth_provider.ssl_options) } ]
	 end,
	application:set_env(rabbitmq_auth_backend_oauth2, key_config, KeyConfig).

init_per_testcase(TestCase, Config) ->
	application:set_env(rabbitmq_auth_backend_oauth2, use_global_locks, false),

	case ?config(with_all_oauth_provider_settings, Config) of
		false -> configure_minimum_oauth_provider_settings(Config);
		true -> configure_all_oauth_provider_settings(Config);
		undefined -> configure_all_oauth_provider_settings(Config)
	end,

	HttpOauthServerExpectations = get_http_oauth_server_expectations(TestCase, Config),
	ListOfExpectations = maps:values(proplists:to_map(HttpOauthServerExpectations)),

	case ?config(group, Config) of
		http_up ->
			start_http_oauth_server(?AUTH_PORT, ListOfExpectations);
		https ->
			start_https_oauth_server(?AUTH_PORT, ?config(rmq_certsdir, Config), ListOfExpectations);
		_ -> ok
	end,
	[{oauth_server_expectations, HttpOauthServerExpectations} | Config ].

end_per_testcase(_, Config) ->
	application:unset_env(rabbitmq_auth_backend_oauth2, oauth_providers),
	application:unset_env(rabbitmq_auth_backend_oauth2, issuer),
	application:unset_env(rabbitmq_auth_backend_oauth2, token_endpoint),
	application:unset_env(rabbitmq_auth_backend_oauth2, key_config),
	case ?config(group, Config) of
		http_up ->
  		stop_http_auth_server();
		https ->
			stop_http_auth_server();
		_ -> ok
	end,
	Config.

end_per_group(https_and_rabbitmq_node, Config) ->
  rabbit_ct_helpers:run_steps(Config, rabbit_ct_broker_helpers:teardown_steps());

end_per_group(_, Config) ->
	Config.

grants_access_token_dynamically_resolving_oauth_provider(Config) ->
	#{request := #{parameters := Parameters},
	  response := [ {code, 200}, {content_type, _CT}, {payload, JsonPayload}] } = lookup_expectation(token_endpoint, Config),

	{ok, #successful_access_token_response{access_token = AccessToken, token_type = TokenType} } =
		oauth2_client:get_access_token(?config(oauth_provider_id, Config), build_access_token_request(Parameters)),

	?assertEqual(proplists:get_value(token_type, JsonPayload), TokenType),
	?assertEqual(proplists:get_value(access_token, JsonPayload), AccessToken).

grants_access_token_using_oauth_provider_id(Config) ->
	#{request := #{parameters := Parameters},
	  response := [ {code, 200}, {content_type, _CT}, {payload, JsonPayload}] } = lookup_expectation(token_endpoint, Config),

	{ok, #successful_access_token_response{access_token = AccessToken, token_type = TokenType} } =
		oauth2_client:get_access_token(?config(oauth_provider_id, Config), build_access_token_request(Parameters)),
	?assertEqual(proplists:get_value(token_type, JsonPayload), TokenType),
	?assertEqual(proplists:get_value(access_token, JsonPayload), AccessToken).

grants_access_token(Config) ->
  #{request := #{parameters := Parameters},
	  response := [ {code, 200}, {content_type, _CT}, {payload, JsonPayload}] }
		= lookup_expectation(token_endpoint, Config),

	{ok, #successful_access_token_response{access_token = AccessToken, token_type = TokenType} } =
		oauth2_client:get_access_token(?config(oauth_provider, Config), build_access_token_request(Parameters)),
	?assertEqual(proplists:get_value(token_type, JsonPayload), TokenType),
	?assertEqual(proplists:get_value(access_token, JsonPayload), AccessToken).

grants_refresh_token(Config) ->
  #{request := #{parameters := Parameters},
	  response := [ {code, 200}, {content_type, _CT}, {payload, JsonPayload}] }
		= lookup_expectation(token_endpoint, Config),

	{ok, #successful_access_token_response{access_token = AccessToken, token_type = TokenType} } =
		oauth2_client:refresh_access_token(?config(oauth_provider, Config), build_refresh_token_request(Parameters)),
	?assertEqual(proplists:get_value(token_type, JsonPayload), TokenType),
	?assertEqual(proplists:get_value(access_token, JsonPayload), AccessToken).

denies_access_token(Config) ->
  #{request := #{parameters := Parameters},
		response := [ {code, 400}, {content_type, _CT}, {payload, JsonPayload}] }
		= lookup_expectation(token_endpoint, Config),
	{error, #unsuccessful_access_token_response{error = Error, error_description = ErrorDescription} } =
		oauth2_client:get_access_token(?config(oauth_provider, Config),build_access_token_request(Parameters)),
	?assertEqual(proplists:get_value(error, JsonPayload), Error),
	?assertEqual(proplists:get_value(error_description, JsonPayload), ErrorDescription).

auth_server_error(Config) ->
  #{request := #{parameters := Parameters},
		response := [ {code, 500} ] } = lookup_expectation(token_endpoint, Config),
	{error, "Internal Server Error"} =
		oauth2_client:get_access_token(?config(oauth_provider, Config), build_access_token_request(Parameters)).

non_json_payload(Config) ->
  #{request := #{parameters := Parameters}} = lookup_expectation(token_endpoint, Config),
	{error, {failed_to_decode_json, _ErrorArgs}} =
		oauth2_client:get_access_token(?config(oauth_provider, Config), build_access_token_request(Parameters)).

connection_error(Config) ->
  #{request := #{parameters := Parameters}} = lookup_expectation(token_endpoint, Config),
	{error, {failed_connect, _ErrorArgs} } = oauth2_client:get_access_token(
		?config(oauth_provider, Config), build_access_token_request(Parameters)).


ssl_connection_error(Config) ->
	#{request := #{parameters := Parameters}} = lookup_expectation(token_endpoint, Config),

	{error, {failed_connect, _} } = oauth2_client:get_access_token(
		?config(oauth_provider_with_wrong_ca, Config), build_access_token_request(Parameters)).

get_oauth_provider(Config) ->
	#{response := [ {code, 200}, {content_type, _CT}, {payload, JsonPayload}] }
		= lookup_expectation(get_openid_configuration, Config),

 {ok, #oauth_provider{issuer = Issuer, token_endpoint = TokenEndPoint, jwks_uri = Jwks_uri}} =
		oauth2_client:get_oauth_provider([issuer, token_endpoint, jwks_uri]),

	?assertEqual(proplists:get_value(issuer, JsonPayload), Issuer),
	?assertEqual(proplists:get_value(token_endpoint, JsonPayload), TokenEndPoint),
	?assertEqual(proplists:get_value(jwks_uri, JsonPayload), Jwks_uri).

get_oauth_provider_given_oauth_provider_id(Config) ->
	#{response := [ {code, 200}, {content_type, _CT}, {payload, JsonPayload}] }
		= lookup_expectation(get_openid_configuration, Config),

	ct:log("get_oauth_provider ~p", [?config(oauth_provider_id, Config)]),
  {ok, #oauth_provider{issuer = Issuer, token_endpoint = TokenEndPoint, jwks_uri = Jwks_uri}} =
		oauth2_client:get_oauth_provider(?config(oauth_provider_id, Config), [issuer, token_endpoint, jwks_uri]),

	?assertEqual(proplists:get_value(issuer, JsonPayload), Issuer),
	?assertEqual(proplists:get_value(token_endpoint, JsonPayload), TokenEndPoint),
	?assertEqual(proplists:get_value(jwks_uri, JsonPayload), Jwks_uri).


%%% HELPERS
build_issuer(Scheme) ->
	uri_string:recompose(#{scheme => Scheme,
												 host => "localhost",
												 port => rabbit_data_coercion:to_integer(?AUTH_PORT),
												 path => ""}).

build_token_endpoint_uri(Scheme) ->
	uri_string:recompose(#{scheme => Scheme,
												 host => "localhost",
												 port => rabbit_data_coercion:to_integer(?AUTH_PORT),
												 path => "/token"}).

build_jwks_uri(Scheme) ->
	uri_string:recompose(#{scheme => Scheme,
												 host => "localhost",
												 port => rabbit_data_coercion:to_integer(?AUTH_PORT),
												 path => "/certs"}).

build_access_token_request(Request) ->
	#access_token_request {
	  client_id = proplists:get_value(?REQUEST_CLIENT_ID, Request),
	  client_secret = proplists:get_value(?REQUEST_CLIENT_SECRET, Request)
	}.
build_refresh_token_request(Request) ->
  #refresh_token_request{
    client_id = proplists:get_value(?REQUEST_CLIENT_ID, Request),
	  client_secret = proplists:get_value(?REQUEST_CLIENT_SECRET, Request),
	  refresh_token = proplists:get_value(?REQUEST_REFRESH_TOKEN, Request)
	}.
build_http_oauth_provider() ->
	#oauth_provider {
		issuer = build_issuer("http"),
	  token_endpoint = build_token_endpoint_uri("http"),
		jwks_uri = build_jwks_uri("http")
	}.
keep_only_issuer_and_ssl_options(OauthProvider) ->
		#oauth_provider {
			issuer = OauthProvider#oauth_provider.issuer,
		  ssl_options = OauthProvider#oauth_provider.ssl_options
		}.
build_https_oauth_provider(CaCertFile) ->
	#oauth_provider {
		issuer = build_issuer("https"),
	  token_endpoint = build_token_endpoint_uri("https"),
		jwks_uri = build_jwks_uri("https"),
		ssl_options = ssl_options(verify_peer, false, CaCertFile)
	}.
oauth_provider_to_proplist(#oauth_provider{ issuer = Issuer, token_endpoint = TokenEndpoint,
	ssl_options = SslOptions, jwks_uri = Jwks_url}) ->
	[ { issuer, Issuer}, {token_endpoint, TokenEndpoint},
		{ https, SslOptions}, {jwks_url, Jwks_url} ].

start_http_oauth_server(Port, Expectations) when is_list(Expectations) ->
	Dispatch = cowboy_router:compile([
		{'_', [{Path, oauth_http_mock, Expected} || #{request := #{path := Path}} = Expected <- Expectations ]}
	]),
	ct:log("start_http_oauth_server with expectation list : ~p -> dispatch: ~p", [Expectations, Dispatch]),
	{ok, _} = cowboy:start_clear(mock_http_auth_listener,[ {port, Port} ],
			 #{env => #{dispatch => Dispatch}});

start_http_oauth_server(Port, #{request := #{path := Path}} = Expected) ->
	Dispatch = cowboy_router:compile([
		{'_', [{Path, oauth_http_mock, Expected}]}
	]),
	ct:log("start_http_oauth_server with expectation : ~p -> dispatch: ~p ", [Expected, Dispatch]),
	{ok, _} = cowboy:start_clear(
      mock_http_auth_listener,
			 [{port, Port}
			 ],
			 #{env => #{dispatch => Dispatch}}).


start_https_oauth_server(Port, CertsDir, Expectations) when is_list(Expectations) ->
	Dispatch = cowboy_router:compile([
		{'_', [{Path, oauth_http_mock, Expected} || #{request := #{path := Path}} = Expected <- Expectations ]}
	]),
	ct:log("start_https_oauth_server with expectation list : ~p -> dispatch: ~p", [Expectations, Expectations]),
	{ok, _} = cowboy:start_tls(
      mock_http_auth_listener,
				[{port, Port},
				 {certfile, filename:join([CertsDir, "server", "cert.pem"])},
				 {keyfile, filename:join([CertsDir, "server", "key.pem"])}
				],
				#{env => #{dispatch => Dispatch}});

start_https_oauth_server(Port, CertsDir, #{request := #{path := Path}} = Expected) ->
	Dispatch = cowboy_router:compile([{'_', [{Path, oauth_http_mock, Expected}]}]),
  ct:log("start_https_oauth_server with expectation : ~p  -> dispatch: ~p", [Expected, Dispatch]),
	{ok, _} = cowboy:start_tls(
      mock_http_auth_listener,
				[{port, Port},
				 {certfile, filename:join([CertsDir, "server", "cert.pem"])},
				 {keyfile, filename:join([CertsDir, "server", "key.pem"])}
				],
				#{env => #{dispatch => Dispatch}}).

stop_http_auth_server() ->
  cowboy:stop_listener(mock_http_auth_listener).

-spec ssl_options(ssl:verify_type(), boolean(), file:filename()) -> list().
ssl_options(PeerVerification, FailIfNoPeerCert, CaCertFile) ->
	[{verify, PeerVerification},
	  {depth, 10},
	  {fail_if_no_peer_cert, FailIfNoPeerCert},
	  {crl_check, false},
	  {crl_cache, {ssl_crl_cache, {internal, [{http, 10000}]}}},
		{cacertfile, CaCertFile}].
