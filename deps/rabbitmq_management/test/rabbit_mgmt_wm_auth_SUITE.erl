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
     {group, auth_setting_tests}
    ].

groups() ->
    [
      {auth_setting_tests, [], [
          auth_settings_with_oauth_disabled,
          auth_settings_with_oauth_enabled,
          auth_settings_with_oauth_disabled_due_to_misconfig,
          auth_settings_with_idp_initiated,

          resolve_oauth_provider_with_mgt_oauth_provider_url,
          resolve_oauth_provider_with_issuer,
          resolve_oauth_provider_with_default_provider_issuer,

          auth_settings_with_unknown_oauth_resources,
          auth_settings_with_multi_oauth_resources,
          auth_settings_for_multi_oauth_resources_with_missing_client_id,
          auth_settings_for_multi_oauth_resources_with_partly_missing_client_id,
          auth_settings_for_multi_oauth_resources_with_partly_missing_oauth_provider_url
      ]}
    ].

%% -------------------------------------------------------------------
%% Setup/teardown.
%% -------------------------------------------------------------------

init_per_group(_, Config) ->
    Config.

end_per_group(_, Config) ->
    Config.

init_per_testcase(_, Config) ->
  case application:get_all_env(rabbitmq_management) of
    {error, _} = Error -> Error;
    Env ->
      lists:foreach(fun({Key,_Value})->
          application:unset_env(rabbitmq_management, Key) end, Env),
      case application:get_all_env(rabbitmq_auth_backend_oauth2) of
        {error, _} = Error -> Error;
        Env2 -> lists:foreach(fun({Key,_Value})->
            application:unset_env(rabbitmq_auth_backend_oauth2, Key) end, Env2)
      end
  end,
  Config.



%% -------------------------------------------------------------------
%% Test cases.
%% -------------------------------------------------------------------

auth_settings_with_oauth_disabled(_Config) ->
  [{oauth_enabled, false}] = rabbit_mgmt_wm_auth:authSettings().

auth_settings_with_oauth_enabled(_Config) ->
  application:set_env(rabbitmq_management, oauth_enabled, true),
  application:set_env(rabbitmq_auth_backend_oauth2, resource_server_id, "some_id"),
  application:set_env(rabbitmq_management, oauth_client_id, "some_client_id"),
  application:set_env(rabbitmq_management, oauth_provider_url, "http://localhost"),
  [ {oauth_enabled, true},
    {oauth_disable_basic_auth, true},
    {oauth_client_id, <<"some_client_id">>},
    {oauth_provider_url, <<"http://localhost">>},
    {oauth_scopes,<<>>},
    {oauth_metadata_url,<<>>},
    {oauth_resource_id,<<"some_id">>}
  ] = rabbit_mgmt_wm_auth:authSettings().

auth_settings_with_oauth_disabled_due_to_misconfig(_Config) ->
  application:set_env(rabbitmq_management, oauth_enabled, true),
  [{oauth_enabled, false}] = rabbit_mgmt_wm_auth:authSettings(),

  application:set_env(rabbitmq_management, oauth_client_id, "some_id"),
  application:set_env(rabbitmq_management, oauth_resource_id, "some_resource_id"),
  [{oauth_enabled, false}] = rabbit_mgmt_wm_auth:authSettings(),

  application:unset_env(rabbitmq_management, oauth_client_id),
  application:set_env(rabbitmq_management, oauth_provider_url, "http://localhost"),
  [{oauth_enabled, false}] = rabbit_mgmt_wm_auth:authSettings().

auth_settings_with_idp_initiated(_Config) ->
  application:set_env(rabbitmq_management, oauth_enabled, true),
  application:set_env(rabbitmq_management, oauth_initiated_logon_type, idp_initiated),
  application:set_env(rabbitmq_auth_backend_oauth2, resource_server_id, "some_id"),
  application:set_env(rabbitmq_management, oauth_provider_url, "http://localhost"),
  [ {oauth_enabled, true},
    {oauth_disable_basic_auth, true},
    {oauth_initiated_logon_type, <<"idp_initiated">>},
    {oauth_provider_url,  <<"http://localhost">>},
    {oauth_resource_id, <<"some_id">>}
  ] = rabbit_mgmt_wm_auth:authSettings().

resolve_oauth_provider_with_mgt_oauth_provider_url(_Config) ->
  application:set_env(rabbitmq_management, oauth_provider_url, "http://localhost"),
  application:set_env(rabbitmq_auth_backend_oauth2, issuer, "http://issuer"),
  ?assertEqual("http://localhost", rabbit_mgmt_wm_auth:resolve_oauth_provider_url("")).

resolve_oauth_provider_with_default_provider_issuer(_Config) ->
  application:set_env(rabbitmq_auth_backend_oauth2, issuer, "http://issuer"),
  application:set_env(rabbitmq_auth_backend_oauth2, default_oauth_provider, <<"default">>),
  OAuthProviders = #{
    <<"default">> => [ {issuer, "http://default-issuer"} ]
  },
  application:set_env(rabbitmq_auth_backend_oauth2, oauth_providers, OAuthProviders),
  ?assertEqual("http://default-issuer", rabbit_mgmt_wm_auth:resolve_oauth_provider_url("")).

resolve_oauth_provider_with_issuer(_Config) ->
  application:set_env(rabbitmq_auth_backend_oauth2, issuer, "http://issuer"),
  ?assertEqual("http://issuer", rabbit_mgmt_wm_auth:resolve_oauth_provider_url("")).

auth_settings_with_unknown_oauth_resources(_Config) ->
  application:set_env(rabbitmq_management, oauth_enabled, true),
  ResourceServers = #{
    <<"one">> => [ {oauth_client_id, "client-one"}, {oauth_provider_url, "http://one"} ],
    <<"two">> => [ {oauth_client_id, "client-two"}, {oauth_provider_url, "http://two"},  {oauth_client_secret, "client-two-secret"} ]
  },
  application:set_env(rabbitmq_management, oauth_resource_servers, ResourceServers),
  [{oauth_enabled, false}] = rabbit_mgmt_wm_auth:authSettings().

auth_settings_with_multi_oauth_resources(_Config) ->
  application:set_env(rabbitmq_management, oauth_enabled, true),
  MgtResourceServers = #{
    <<"one">> => [ {oauth_client_id, "client-one"}, {oauth_provider_url, "http://one"},
      {id, <<"one">>} ],
    <<"two">> => [ {oauth_client_id, "client-two"}, {oauth_provider_url, "http://two"},
      {id, <<"two">>}, {oauth_client_secret, "client-two-secret"} ]
  },
  application:set_env(rabbitmq_management, oauth_resource_servers, MgtResourceServers),
  ResourceServers = #{
    <<"one">> => [ {id, <<"one">> } ],
    <<"two">> => [ {id, <<"two">> } ]
  },
  application:set_env(rabbitmq_auth_backend_oauth2, resource_servers, ResourceServers),
  ExpectedResourceServers = #{
    <<"one">> => [
      {oauth_client_id, <<"client-one">>},
      {id, <<"one">>},
      {oauth_provider_url, <<"http://one">>}
    ],
    <<"two">> => [
      {oauth_client_id, <<"client-two">>},
      {id, <<"two">>},
      {oauth_client_secret, <<"client-two-secret">>},
      {oauth_provider_url, <<"http://two">>}
    ]
  },
  [ {oauth_enabled, true},
    {oauth_resource_servers, ExpectedResourceServers},
    {oauth_disable_basic_auth, true}
  ] = rabbit_mgmt_wm_auth:authSettings().
çauth_settings_with_multi_oauth_resources(_Config) ->
  application:set_env(rabbitmq_management, oauth_enabled, true),
  MgtResourceServers = #{
    <<"one">> => [
      {id, <<"one">>},
      {oauth_client_id, "client-one"},
      {oauth_provider_url, "http://one"},
      {oauth_scopes, <<"rabbit1 rabbit2">>} ],
    <<"two">> => [
      {id, <<"two">>},
      {oauth_client_id, "client-two"},
      {oauth_provider_url, "http://two"},
      {oauth_client_secret, "client-two-secret"} ]
  },
  application:set_env(rabbitmq_management, oauth_resource_servers, MgtResourceServers),
  ResourceServers = #{
    <<"one">> => [ {id, <<"one">> } ],
    <<"two">> => [ {id, <<"two">> } ]
  },
  application:set_env(rabbitmq_auth_backend_oauth2, resource_servers, ResourceServers),
  ExpectedResourceServers = #{
    <<"one">> => [
      {oauth_client_id, <<"client-one">>},
      {id, <<"one">>},
      {oauth_provider_url, <<"http://one">>},
      {oauth_scopes, <<"rabbit1 rabbit2">>}
    ],
    <<"two">> => [
      {oauth_client_id, <<"client-two">>},
      {id, <<"two">>},
      {oauth_client_secret, <<"client-two-secret">>},
      {oauth_provider_url, <<"http://two">>}
    ]
  },
  [ {oauth_enabled, true},
    {oauth_resource_servers, ExpectedResourceServers},
    {oauth_disable_basic_auth, true}
  ] = rabbit_mgmt_wm_auth:authSettings().

auth_settings_for_multi_oauth_resources_with_missing_client_id(_Config) ->
  application:set_env(rabbitmq_management, oauth_enabled, true),
  MgtResourceServers = #{
    <<"one">> => [ {oauth_provider_url, "http://one"},
      {id, <<"one">>} ]
  },
  application:set_env(rabbitmq_management, oauth_resource_servers, MgtResourceServers),
  ResourceServers = #{
    <<"one">> => [ {id, <<"one">> } ]
  },
  application:set_env(rabbitmq_auth_backend_oauth2, resource_servers, ResourceServers),
  [{oauth_enabled, false}] = rabbit_mgmt_wm_auth:authSettings().

auth_settings_for_multi_oauth_resources_with_partly_missing_client_id(_Config) ->
  application:set_env(rabbitmq_management, oauth_enabled, true),
  application:set_env(rabbitmq_management, oauth_client_id, "some_id"),
  MgtResourceServers = #{
    <<"one">> => [ {oauth_provider_url, "http://one"},
      {id, <<"one">>} ]
  },
  application:set_env(rabbitmq_management, oauth_resource_servers, MgtResourceServers),
  ResourceServers = #{
    <<"one">> => [ {id, <<"one">> } ]
  },
  application:set_env(rabbitmq_auth_backend_oauth2, resource_servers, ResourceServers),
  ExpectedResourceServers = #{
    <<"one">> => [
      {id, <<"one">>},
      {oauth_provider_url, <<"http://one">>}
    ]
  },
  [ {oauth_enabled, true},
    {oauth_resource_servers, ExpectedResourceServers},
    {oauth_disable_basic_auth, true},
    {oauth_client_id, <<"some_id">>}
  ] = rabbit_mgmt_wm_auth:authSettings().


auth_settings_for_multi_oauth_resources_with_partly_missing_oauth_provider_url(_Config) ->
  application:set_env(rabbitmq_management, oauth_enabled, true),
  application:set_env(rabbitmq_management, oauth_client_id, "some_id"),
  application:set_env(rabbitmq_management, oauth_provider_url, <<"http://someurl">>),
  MgtResourceServers = #{
    <<"one">> => [
      {id, <<"one">>},
      {oauth_scopes, <<"scope1 scope2">>} ]
  },
  application:set_env(rabbitmq_management, oauth_resource_servers, MgtResourceServers),
  ResourceServers = #{
    <<"one">> => [ {id, <<"one">> } ]
  },
  application:set_env(rabbitmq_auth_backend_oauth2, resource_servers, ResourceServers),
  ExpectedResourceServers = #{
    <<"one">> => [
      {id, <<"one">>},
      {oauth_scopes, <<"scope1 scope2">>}
    ]
  },
  [ {oauth_enabled, true},
    {oauth_resource_servers, ExpectedResourceServers},
    {oauth_disable_basic_auth, true},
    {oauth_client_id, <<"some_id">>},
    {oauth_provider_url, <<"http://someurl">>}
  ] = rabbit_mgmt_wm_auth:authSettings().
