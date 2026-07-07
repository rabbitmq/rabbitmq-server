%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2026 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
%%

%% Drives rabbit_mgmt_oauth_token_proxy over HTTP against a broker whose
%% resource server points at a mock OpenID provider.
-module(rabbit_mgmt_oauth_token_proxy_http_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").

-compile(export_all).

-define(SECRET, <<"test-client-secret">>).
-define(MOCK_LISTENER, oauth2_mock_http_listener).

all() ->
    [{group, top_level}, {group, per_resource}].

groups() ->
    %% The core behaviour is checked under both ways of defining a resource
    %% server: the top-level keys, and an entry in the oauth_resource_servers map.
    Core = [rewrites_only_the_token_endpoint, injects_client_secret_and_forwards],
    [{top_level, [], Core ++ [
        keeps_client_supplied_secret,
        relays_provider_error,
        unknown_resource_server_returns_404,
        token_endpoint_rejects_get,
        bootstrap_does_not_disclose_secret
     ]},
     {per_resource, [], Core ++ [
        resource_without_secret_returns_404
     ]}].

%%
%% Setup
%%

init_per_suite(Config0) ->
    rabbit_ct_helpers:log_environment(),
    inets:start(),
    {ok, _} = application:ensure_all_started(cowboy),
    Config1 = rabbit_ct_helpers:set_config(Config0,
        [{rmq_nodename_suffix, ?MODULE}]),
    Config2 = rabbit_ct_helpers:run_setup_steps(Config1,
        rabbit_ct_broker_helpers:setup_steps() ++
        rabbit_ct_client_helpers:setup_steps()),
    MockPort = start_mock_provider(),
    [ok = rabbit_ct_broker_helpers:enable_plugin(Config2, 0, Plugin)
     || Plugin <- [rabbitmq_management, rabbitmq_auth_backend_oauth2]],
    rabbit_ct_helpers:set_config(Config2, {mock_port, MockPort}).

end_per_suite(Config) ->
    _ = cowboy:stop_listener(?MOCK_LISTENER),
    rabbit_ct_helpers:run_teardown_steps(Config,
        rabbit_ct_client_helpers:teardown_steps() ++
        rabbit_ct_broker_helpers:teardown_steps()).

init_per_group(Group, Config) ->
    reset_oauth(Config),
    configure(Group, Config).

end_per_group(_, Config) ->
    reset_oauth(Config),
    Config.

init_per_testcase(_, Config) -> Config.
end_per_testcase(_, Config) -> Config.

%% The mock runs in the CT node; the broker reaches it over localhost.
start_mock_provider() ->
    {ok, _} = cowboy:start_clear(?MOCK_LISTENER, [{port, 0}],
        #{env => #{dispatch => cowboy_router:compile([{'_', []}])}}),
    Port = ranch:get_port(?MOCK_LISTENER),
    Issuer = mock_base(Port),
    Dispatch = cowboy_router:compile([{'_', [
        {"/.well-known/openid-configuration", oauth2_mock_http_handler,
            #{kind => discovery, issuer => Issuer,
              token_endpoint => <<Issuer/binary, "/token">>}},
        {"/token", oauth2_mock_http_handler, #{kind => token}}
    ]}]),
    cowboy:set_env(?MOCK_LISTENER, dispatch, Dispatch),
    Port.

configure(top_level, Config) ->
    ProviderURL = mock_base(?config(mock_port, Config)),
    set_env(Config, rabbitmq_auth_backend_oauth2, resource_server_id,
        <<"rabbitmq">>),
    set_env(Config, rabbitmq_management, oauth_enabled, true),
    set_env(Config, rabbitmq_management, oauth_client_id, <<"rabbitmq_mgt">>),
    set_env(Config, rabbitmq_management, oauth_client_secret, ?SECRET),
    set_env(Config, rabbitmq_management, oauth_provider_url, ProviderURL),
    set_env(Config, rabbitmq_management, oauth_scopes, <<"openid">>),
    rabbit_ct_helpers:set_config(Config, {resource, <<"rabbitmq">>});
configure(per_resource, Config) ->
    ProviderURL = mock_base(?config(mock_port, Config)),
    set_env(Config, rabbitmq_auth_backend_oauth2, resource_servers,
        #{<<"rabbit_prod">> => [{id, <<"rabbit_prod">>}],
          <<"rabbit_public">> => [{id, <<"rabbit_public">>}]}),
    set_env(Config, rabbitmq_management, oauth_enabled, true),
    set_env(Config, rabbitmq_management, oauth_resource_servers,
        #{<<"rabbit_prod">> =>
              [{id, <<"rabbit_prod">>},
               {oauth_client_id, <<"prod_client">>},
               {oauth_client_secret, ?SECRET},
               {oauth_provider_url, ProviderURL}],
          %% rabbit_public has no client secret, so the proxy must refuse it.
          <<"rabbit_public">> =>
              [{id, <<"rabbit_public">>},
               {oauth_client_id, <<"public_client">>},
               {oauth_provider_url, ProviderURL}]}),
    rabbit_ct_helpers:set_config(Config, {resource, <<"rabbit_prod">>}).

reset_oauth(Config) ->
    [unset_env(Config, rabbitmq_management, Key)
     || Key <- [oauth_enabled, oauth_client_id, oauth_client_secret,
                oauth_provider_url, oauth_scopes, oauth_resource_servers]],
    [unset_env(Config, rabbitmq_auth_backend_oauth2, Key)
     || Key <- [resource_server_id, resource_servers]],
    ok.

%%
%% Test cases
%%

rewrites_only_the_token_endpoint(Config) ->
    Id = ?config(resource, Config),
    {200, Map} = get_json(Config, metadata_path(Id)),
    Issuer = mock_base(?config(mock_port, Config)),
    ?assertEqual(proxy_token_endpoint(Config, Id),
        maps:get(<<"token_endpoint">>, Map)),
    ?assertEqual(Issuer, maps:get(<<"issuer">>, Map)),
    ?assertEqual(<<Issuer/binary, "/authorize">>,
        maps:get(<<"authorization_endpoint">>, Map)),
    ?assertEqual(<<Issuer/binary, "/keys">>, maps:get(<<"jwks_uri">>, Map)).

injects_client_secret_and_forwards(Config) ->
    Id = ?config(resource, Config),
    Body = <<"grant_type=authorization_code&code=the-code&code_verifier=v">>,
    {200, Map} = post_form(Config, token_path(Id), Body),
    ?assertEqual(<<"mock-access-token">>, maps:get(<<"access_token">>, Map)),
    %% The mock echoes what it received; it only sees client_secret if the proxy
    %% injected it server-side.
    Received = maps:get(<<"received">>, Map),
    ?assertEqual(?SECRET, maps:get(<<"client_secret">>, Received)),
    ?assertEqual(<<"the-code">>, maps:get(<<"code">>, Received)),
    ?assertEqual(<<"authorization_code">>, maps:get(<<"grant_type">>, Received)).

keeps_client_supplied_secret(Config) ->
    Id = ?config(resource, Config),
    Body = <<"grant_type=refresh_token&refresh_token=r&client_secret=explicit">>,
    {200, Map} = post_form(Config, token_path(Id), Body),
    Received = maps:get(<<"received">>, Map),
    ?assertEqual(<<"explicit">>, maps:get(<<"client_secret">>, Received)).

relays_provider_error(Config) ->
    Id = ?config(resource, Config),
    Body = <<"grant_type=authorization_code&code=bad-code">>,
    {400, Map} = post_form(Config, token_path(Id), Body),
    ?assertEqual(<<"invalid_grant">>, maps:get(<<"error">>, Map)).

unknown_resource_server_returns_404(Config) ->
    {404, _} = get_raw(Config, metadata_path(<<"does-not-exist">>)).

token_endpoint_rejects_get(Config) ->
    {405, _} = get_raw(Config, token_path(?config(resource, Config))).

bootstrap_does_not_disclose_secret(Config) ->
    {200, Body} = get_raw(Config, "/js/oidc-oauth/bootstrap.js"),
    ?assertEqual(nomatch, binary:match(Body, <<"oauth_client_secret">>)),
    ?assertEqual(nomatch, binary:match(Body, ?SECRET)),
    ?assertNotEqual(nomatch, binary:match(Body, <<"use_token_endpoint_proxy">>)).

resource_without_secret_returns_404(Config) ->
    {404, _} = get_raw(Config, metadata_path(<<"rabbit_public">>)).

%%
%% Helpers
%%

token_path(Id) ->
    "/js/oidc-oauth/token-endpoint/" ++ binary_to_list(Id).

metadata_path(Id) ->
    token_path(Id) ++ "/openid-configuration".

proxy_token_endpoint(Config, Id) ->
    iolist_to_binary([mgmt_base(Config), token_path(Id)]).

mock_base(Port) ->
    iolist_to_binary(["http://localhost:", integer_to_list(Port)]).

mgmt_base(Config) ->
    Port = rabbit_ct_broker_helpers:get_node_config(Config, 0, tcp_port_mgmt),
    "http://localhost:" ++ integer_to_list(Port).

set_env(Config, App, Key, Value) ->
    ok = rabbit_ct_broker_helpers:rpc(Config, 0, application, set_env,
        [App, Key, Value]).

unset_env(Config, App, Key) ->
    ok = rabbit_ct_broker_helpers:rpc(Config, 0, application, unset_env,
        [App, Key]).

get_raw(Config, Path) ->
    {ok, {{_, Code, _}, _Headers, Body}} =
        httpc:request(get, {mgmt_base(Config) ++ Path, []}, [],
                      [{body_format, binary}]),
    {Code, Body}.

get_json(Config, Path) ->
    {Code, Body} = get_raw(Config, Path),
    {Code, rabbit_json:decode(Body)}.

post_form(Config, Path, Body) ->
    {ok, {{_, Code, _}, _Headers, RespBody}} =
        httpc:request(post,
            {mgmt_base(Config) ++ Path, [],
             "application/x-www-form-urlencoded", Body},
            [], [{body_format, binary}]),
    {Code, rabbit_json:decode(RespBody)}.
