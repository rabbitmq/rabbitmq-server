%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2023 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries.  All rights reserved.
%%

-module(rabbit_mgmt_wm_auth_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").

-compile(export_all).

all() ->
    [
     {group, without_any_settings},
     {group, with_oauth_disabled},
     {group, with_oauth_enabled}
    ].

groups() ->
    [
      {without_any_settings, [], [
        should_return_disabled_auth_settings
      ]},
      {with_oauth_disabled, [], [
        should_return_disabled_auth_settings
      ]},
      {with_oauth_enabled, [], [
        should_return_disabled_auth_settings,
        {with_two_oauth_providers, [], [
          {with_resource_server_A_with_oauth_provider_Idp1, [], [
            should_return_disabled_auth_settings,
            {with_oauth_resource_server_A_with_client_id, [], [
              should_return_oauth_resource_server_A
            ]}
          ]},
          {with_root_issuer, [], [
            should_return_disabled_auth_settings,
            {with_oauth_resource_server_A_with_client_id, [], [
              should_return_oauth_resource_server_A
            ]}
          ]}
        ]},
        {with_resource_server_id, [], [
          should_return_disabled_auth_settings,
          {with_client_id, [], [
            should_return_disabled_auth_settings,
            {with_mgt_aouth_provider_url, [], [
              should_return_enabled_auth_settings_with_client_server_id_and_resource_server_id,
              should_return_enabled_auth_settings_sp_initiated_logon,
              should_return_configured_oauth_provider_url,
              should_return_oauth_disable_basic_auth,
              should_return_empty_scopes,
              {with_idp_initiated_logon, [], [
                should_return_enabled_auth_settings_idp_initiated_logon
              ]}
            ]},
            {with_root_issuer, [], [
              should_return_enabled_auth_settings_with_client_server_id_and_resource_server_id,
              should_return_root_issuer_as_oauth_provider_url
            ]},
            {with_two_oauth_providers, [], [
              should_return_disabled_auth_settings,
              {with_unknown_default_oauth_provider, [], [
                should_return_disabled_auth_settings
              ]},
              {with_default_oauth_provider, [], [
                should_return_oauth_provider_issuer_as_oauth_provider_url
              ]}
            ]}
          ]}
        ]}
      ]}
    ].

%% -------------------------------------------------------------------
%% Setup/teardown.
%% -------------------------------------------------------------------
init_per_suite(Config) ->
  [ {resource_server_id, <<"rabbitmq">>},
    {oauth_client_id, <<"rabbitmq_client">>},
    {oauth_scopes, <<>>},
    {resource_A, <<"resource-a">>} | Config].

end_per_suite(_Config) ->
    ok.

init_per_group(with_oauth_disabled, Config) ->
  application:set_env(rabbitmq_management, oauth_enabled, false),
  Config;
init_per_group(with_oauth_enabled, Config) ->
  application:set_env(rabbitmq_management, oauth_enabled, true),
  Config;

init_per_group(with_resource_server_id, Config) ->
  application:set_env(rabbitmq_auth_backend_oauth2, resource_server_id, ?config(resource_server_id, Config)),
  Config;
init_per_group(with_client_id, Config) ->
  application:set_env(rabbitmq_management, oauth_client_id, ?config(oauth_client_id, Config)),
  Config;
init_per_group(with_mgt_aouth_provider_url, Config) ->
  application:set_env(rabbitmq_management, oauth_provider_url, <<"http://oauth_provider_url">>),
  Config;
init_per_group(with_root_issuer, Config) ->
  application:set_env(rabbitmq_auth_backend_oauth2, issuer, <<"http://issuer">>),
  Config;
init_per_group(with_idp_initiated_logon, Config) ->
  application:set_env(rabbitmq_management, oauth_initiated_logon_type, idp_initiated),
  [ {oauth_initiated_logon_type, idp_initiated} | Config];

init_per_group(with_two_oauth_providers, Config) ->
  application:set_env(rabbitmq_auth_backend_oauth2, oauth_providers, #{
    <<"idp1">> => [ { issuer, <<"https://idp1-oauth-provider">>} ],
    <<"idp2">> => [ { issuer, <<"https://idp2-oauth-provider">>} ]
  }),
  Config;
init_per_group(with_resource_server_A_with_oauth_provider_Idp1, Config) ->
  ResourceA = ?config(resource_A, Config),
  ResourceServers = application:get_env(rabbitmq_auth_backend_oauth2, resource_servers, #{}),
  ResourceServers1 = maps:put(ResourceA, [ { oauth_provider_id, <<"idp1">>} | maps:get(ResourceA, ResourceServers, []) ], ResourceServers),
  ResourceServers2 = case ResourceServers1 of
    [] -> maps:delete(ResourceA, ResourceServers1);
    _ -> ResourceServers1
  end,
  application:set_env(rabbitmq_auth_backend_oauth2, resource_servers, ResourceServers2),
  Config;

init_per_group(with_oauth_resource_server_A_with_client_id, Config) ->
  ResourceA = ?config(resource_A, Config),
  ResourceServers = application:get_env(rabbitmq_management, oauth_resource_servers, #{}),
  OAuthResourceA = [ { oauth_client_id, ?config(oauth_client_id, Config)} | maps:get(ResourceA, ResourceServers, []) ],
  application:set_env(rabbitmq_management, oauth_resource_servers,
    maps:put(ResourceA, OAuthResourceA, ResourceServers)),
  Config;

init_per_group(with_default_oauth_provider, Config) ->
  application:set_env(rabbitmq_auth_backend_oauth2, default_oauth_provider, <<"a">>),
  Config;
init_per_group(with_unknown_default_oauth_provider, Config) ->
  application:set_env(rabbitmq_auth_backend_oauth2, default_oauth_provider, <<"unknown">>),
  Config;

init_per_group(_, Config) ->
  Config.

end_per_group(with_oauth_disabled, Config) ->
  application:unset_env(rabbitmq_management, oauth_enabled),
  Config;
end_per_group(with_oauth_enabled, Config) ->
  application:unset_env(rabbitmq_management, oauth_enabled),
  Config;
end_per_group(with_resource_server_id, Config) ->
  application:unset_env(rabbitmq_auth_backend_oauth2, resource_server_id),
  Config;
end_per_group(with_mgt_aouth_provider_url, Config) ->
  application:unset_env(rabbitmq_management, oauth_provider_url),
  Config;
end_per_group(with_root_issuer, Config) ->
  application:unset_env(rabbitmq_auth_backend_oauth2, issuer),
  Config;

end_per_group(with_client_id, Config) ->
  application:unset_env(rabbitmq_management, oauth_client_id),
  Config;
end_per_group(with_idp_initiated_logon, Config) ->
  application:unset_env(rabbitmq_management, oauth_initiated_logon_type),
  Config;
end_per_group(with_two_oauth_providers, Config) ->
  application:unset_env(rabbitmq_auth_backend_oauth2, oauth_providers),
  Config;
end_per_group(with_many_resource_servers, Config) ->
  application:unset_env(rabbitmq_auth_backend_oauth2, resource_servers),
  Config;
end_per_group(with_resource_server_A_with_oauth_provider_Idp1, Config) ->
  ResourceA = ?config(resource_A, Config),
  ResourceServers = application:get_env(rabbitmq_auth_backend_oauth2, resource_servers, #{}),
  OAuthResourceA = proplists:delete(oauth_provider_id, maps:get(ResourceA, ResourceServers, [])),
  application:set_env(rabbitmq_auth_backend_oauth2, resource_servers,
    delete_key_with_empty_proplist(ResourceA, maps:put(ResourceA, OAuthResourceA, ResourceServers))),
  Config;

end_per_group(with_oauth_resource_server_A_with_client_id, Config) ->
  ResourceA = ?config(resource_A, Config),
  ResourceServers = application:get_env(rabbitmq_management, oauth_resource_servers, #{}),
  OAuthResourceA = proplists:delete(oauth_client_id, maps:get(ResourceA, ResourceServers, [])),
  application:set_env(rabbitmq_management, oauth_resource_servers,
    delete_key_with_empty_proplist(ResourceA, maps:put(ResourceA, OAuthResourceA, ResourceServers))),
  Config;

end_per_group(_, Config) ->
  Config.


delete_key_with_empty_proplist(Key, Map) ->
  case maps:get(Key, Map) of
    [] -> maps:delete(Key, Map)
  end.

%% -------------------------------------------------------------------
%% Test cases.
%% -------------------------------------------------------------------

should_return_disabled_auth_settings(_Config) ->
  [{oauth_enabled, false}] = rabbit_mgmt_wm_auth:authSettings().

should_return_enabled_auth_settings_with_client_server_id_and_resource_server_id(Config) ->
  ct:log("rabbitmq_management: ~p ", [application:get_all_env(rabbitmq_management)]),
  ct:log("rabbitmq_auth_backend_oauth2: ~p ", [application:get_all_env(rabbitmq_auth_backend_oauth2)]),
  ClientId = ?config(oauth_client_id, Config),
  ResourceId = ?config(resource_server_id, Config),
  Actual = rabbit_mgmt_wm_auth:authSettings(),
  ct:log("Actual : ~p vs ~p", [Actual, ResourceId]),
  ?assertEqual(true, proplists:get_value(oauth_enabled, Actual)),
  ?assertEqual(ClientId, proplists:get_value(oauth_client_id, Actual)),
  ?assertEqual(ResourceId, proplists:get_value(oauth_resource_id, Actual)),
  ?assertEqual(<<>>, proplists:get_value(oauth_metadata_url, Actual)).

should_return_empty_scopes(_Config) ->
  Actual = rabbit_mgmt_wm_auth:authSettings(),
  ?assertEqual(false, proplists:is_defined(scopes, Actual)).

should_return_enabled_auth_settings_sp_initiated_logon(_Config) ->
  Actual = rabbit_mgmt_wm_auth:authSettings(),
  ?assertEqual(false, proplists:is_defined(oauth_initiated_logon_type, Actual)).

should_return_enabled_auth_settings_idp_initiated_logon(Config) ->
  ResourceId = ?config(resource_server_id, Config),
  Actual = rabbit_mgmt_wm_auth:authSettings(),
  ?assertNot(proplists:is_defined(oauth_client_id, Actual)),
  ?assertNot(proplists:is_defined(scopes, Actual)),
  ?assertNot(proplists:is_defined(oauth_metadata_url, Actual)),
  ?assertEqual(ResourceId, proplists:get_value(oauth_resource_id, Actual)),
  ?assertEqual( <<"idp_initiated">>, proplists:get_value(oauth_initiated_logon_type, Actual)).

should_return_root_issuer_as_oauth_provider_url(_Config) ->
  Actual = rabbit_mgmt_wm_auth:authSettings(),
  Issuer = application:get_env(rabbitmq_auth_backend_oauth2, issuer, ""),
  ?assertEqual(Issuer, proplists:get_value(oauth_provider_url, Actual)).

should_return_oauth_disable_basic_auth(_Config) ->
  Actual = rabbit_mgmt_wm_auth:authSettings(),
  ?assertEqual(true, proplists:get_value(oauth_disable_basic_auth, Actual)).

should_return_oauth_enabled_basic_auth(_Config) ->
  Actual = rabbit_mgmt_wm_auth:authSettings(),
  ?assertEqual(false, proplists:get_value(oauth_disable_basic_auth, Actual)).

should_return_configured_oauth_provider_url(_Config) ->
  Actual = rabbit_mgmt_wm_auth:authSettings(),
  Issuer = application:get_env(rabbitmq_management, oauth_provider_url, ""),
  ?assertEqual(Issuer, proplists:get_value(oauth_provider_url, Actual)).

should_return_oauth_provider_issuer_as_oauth_provider_url(_Config) ->
  Actual = rabbit_mgmt_wm_auth:authSettings(),
  {ok, DefaultOAuthProvider} = application:get_env(rabbitmq_auth_backend_oauth2, default_oauth_provider),
  OauthProvider = maps:get(DefaultOAuthProvider,
    application:get_env(rabbitmq_auth_backend_oauth2, oauth_providers, #{})),
  ?assertEqual(proplists:get_value(issuer, OauthProvider), proplists:get_value(oauth_provider_url, Actual)).

log(AuthSettings) ->
  ct:log("rabbitmq_management: ~p ", [application:get_all_env(rabbitmq_management)]),
  ct:log("rabbitmq_auth_backend_oauth2: ~p ", [application:get_all_env(rabbitmq_auth_backend_oauth2)]),
  ct:log("authSettings: ~p ", [AuthSettings]).

should_return_oauth_resource_server_A(Config) ->
  ClientId = ?config(oauth_client_id, Config),
  ResourceA = ?config(resource_A, Config),
  Actual = rabbit_mgmt_wm_auth:authSettings(),
  log(Actual),

  OAuthResourceServers = proplists:get_value(oauth_resource_servers, Actual),
  OAuthResourceA = maps:get(ResourceA, OAuthResourceServers),

  ?assertEqual(true, proplists:get_value(oauth_enabled, Actual)),
  ?assertEqual(<<>>, proplists:get_value(oauth_client_id, Actual)),
  ?assertEqual(ClientId, proplists:get_value(oauth_client_id, OAuthResourceA)).
