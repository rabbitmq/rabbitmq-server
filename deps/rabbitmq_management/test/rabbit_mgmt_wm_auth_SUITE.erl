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
        {with_resource_server_id, [], [
          {with_client_id, [], [
            should_return_disabled_auth_settings,
            {with_root_issuer, [], [
              should_return_enabled_auth_settings_sp_initiated_logon,
              {with_mgt_aouth_provider_url, [], [
                should_return_enabled_auth_settings_sp_initiated_logon
              ]}
            ]},
            {with_mgt_aouth_provider_url, [], [
              should_return_enabled_auth_settings_sp_initiated_logon,
              {with_idp_initiated_logon, [], [
                should_return_enabled_auth_settings_idp_initiated_logon
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
    {oauth_disable_basic_auth, true} | Config].

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
  [  {oauth_provider_url, <<"http://oauth_provider_url">>}  | Config];
init_per_group(with_root_issuer, Config) ->
  application:set_env(rabbitmq_auth_backend_oauth2, issuer, <<"http://issuer">>),
  [  {oauth_provider_url, <<"http://issuer">>}  | Config];
init_per_group(with_idp_initiated_logon, Config) ->
  application:set_env(rabbitmq_management, oauth_initiated_logon_type, idp_initiated),
  [ {oauth_initiated_logon_type, idp_initiated} | Config];

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
  application:unset_env(rabbitmq_auth_backend_oauth2, issuer);

end_per_group(with_client_id, Config) ->
  application:unset_env(rabbitmq_management, oauth_client_id),
  Config;
end_per_group(with_idp_initiated_logon, Config) ->
  application:set_env(rabbitmq_management, oauth_initiated_logon_type, idp_initiated),
  proplists:delete(oauth_initiated_logon_type, Config);

end_per_group(_, Config) ->
  Config.


%% -------------------------------------------------------------------
%% Test cases.
%% -------------------------------------------------------------------

should_return_disabled_auth_settings(_Config) ->
  [{oauth_enabled, false}] = rabbit_mgmt_wm_auth:authSettings().

should_return_enabled_auth_settings_sp_initiated_logon(Config) ->
  ClientId = ?config(oauth_client_id, Config),
  ProviderUrl = ?config(oauth_provider_url, Config),
  ResourceId = ?config(resource_server_id, Config),
  Scopes = ?config(oauth_scopes, Config),
  ct:log("rabbitmq_management : ~p", [application:get_all_env(rabbitmq_management)]),
  ct:log("rabbitmq_auth_backend_oauth2 : ~p", [application:get_all_env(rabbitmq_auth_backend_oauth2)]),
  [
    {oauth_enabled, true},
    {oauth_disable_basic_auth, true},
    {oauth_client_id, ClientId},
    {oauth_provider_url, ProviderUrl},
    {oauth_scopes, Scopes},
    {oauth_metadata_url, <<>>},
    {oauth_resource_id, ResourceId}
  ] = rabbit_mgmt_wm_auth:authSettings().

should_return_enabled_auth_settings_idp_initiated_logon(Config) ->
  ProviderUrl = ?config(oauth_provider_url, Config),
  ResourceId = ?config(resource_server_id, Config),
  ct:log("rabbitmq_management : ~p", [application:get_all_env(rabbitmq_management)]),
  ct:log("rabbitmq_auth_backend_oauth2 : ~p", [application:get_all_env(rabbitmq_auth_backend_oauth2)]),
  [
    {oauth_enabled, true},
    {oauth_disable_basic_auth, true},
    {oauth_initiated_logon_type, <<"idp_initiated">>},
    {oauth_provider_url, ProviderUrl},
    {oauth_resource_id, ResourceId}
  ] = rabbit_mgmt_wm_auth:authSettings().

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

oauth_settings_with_multi_oauth_resources(_Config) ->
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
