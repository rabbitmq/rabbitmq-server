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
        {with_oauth_providers_idp1_idp2, [], [
          {with_mgt_resource_server_a_with_client_id_x, [], [
            should_return_disabled_auth_settings
          ]},
          {with_resource_server_a_with_oauth_provider_idp1, [], [
            {with_mgt_oauth_client_id_z, [], [
              should_return_oauth_resource_server_a_with_oauth_provider_url_idp1_url,
              should_return_oauth_client_id_z
            ]},
            {with_mgt_resource_server_a_with_client_id_x, [], [
              should_return_oauth_resource_server_a_with_client_id_x
            ]}
          ]},
          {with_root_issuer_url1, [], [
            should_return_disabled_auth_settings,
            {with_resource_server_a, [], [
              should_return_disabled_auth_settings,
              {with_mgt_oauth_client_id_z, [], [
                should_return_oauth_resource_server_a_with_oauth_provider_url_url1,
                should_return_oauth_client_id_z
              ]},
              {with_mgt_resource_server_a_with_client_id_x, [], [
                should_return_oauth_resource_server_a_with_oauth_provider_url_url1,
                should_return_oauth_resource_server_a_with_client_id_x
              ]}
            ]}
          ]}
        ]},
        {with_resource_server_id_rabbit, [], [
          should_return_disabled_auth_settings,
          {with_mgt_oauth_client_id_z, [], [
            should_return_disabled_auth_settings,
            {with_resource_server_a, [], [
              should_return_disabled_auth_settings,
              {with_oauth_providers_idp1_idp2, [], [
                {with_default_oauth_provider_idp1, [], [
                  should_return_oauth_resource_server_rabbit_with_oauth_provider_url_idp1_url,
                  should_return_oauth_resource_server_a_with_oauth_provider_url_idp1_url,
                  {with_mgt_aouth_provider_url_url0, [], [
                    should_return_oauth_resource_server_rabbit_with_oauth_provider_url_url0,
                    should_return_oauth_resource_server_a_with_oauth_provider_url_url0,
                    {with_mgt_oauth_resource_server_a_with_oauth_provider_url_url1, [], [
                      should_return_oauth_resource_server_rabbit_with_oauth_provider_url_url0,
                      should_return_oauth_resource_server_a_with_oauth_provider_url_url1
                    ]}
                  ]}
                ]}
              ]},
              {with_root_issuer_url1, [], [
                should_return_oauth_enabled,
                should_return_oauth_resource_server_rabbit_with_oauth_provider_url_url1,
                should_return_oauth_resource_server_a_with_oauth_provider_url_url1,
                should_return_oauth_client_id_z,
                should_return_oauth_disable_basic_auth_true,
                should_not_return_oauth_initiated_logon_type,
                should_not_return_scopes,
                {with_mgt_aouth_provider_url_url0, [], [
                  should_return_oauth_resource_server_rabbit_with_oauth_provider_url_url0,
                  should_return_oauth_resource_server_a_with_oauth_provider_url_url0,
                  {with_oauth_providers_idp1_idp2, [], [
                    {with_resource_server_a_with_oauth_provider_idp1, [], [
                      should_return_oauth_resource_server_a_with_oauth_provider_url_url0
                    ]}
                  ]}
                ]}
              ]}
            ]},
            {with_mgt_aouth_provider_url_url0, [], [
              should_return_oauth_enabled,
              should_return_oauth_client_id_z,
              should_return_oauth_resource_server_rabbit_with_oauth_provider_url_url0,
              should_not_return_oauth_initiated_logon_type,
              should_return_oauth_disable_basic_auth_true,
              should_not_return_scopes,
              {with_idp_initiated_logon, [], [
                should_return_oauth_idp_initiated_logon
              ]},
              {with_oauth_disable_basic_auth_false, [], [
                should_return_oauth_disable_basic_auth_false
              ]},
              {with_scopes_admin_mgt, [], [
                should_return_scopes_admin_mgt
              ]}
            ]},
            {with_root_issuer_url1, [], [
              should_return_oauth_resource_server_rabbit_with_oauth_provider_url_url1
            ]},
            {with_oauth_providers_idp1_idp2, [], [
              should_return_disabled_auth_settings,
              {with_default_oauth_provider_idp3, [], [
                should_return_disabled_auth_settings
              ]},
              {with_default_oauth_provider_idp1, [], [
                should_return_oauth_resource_server_rabbit_with_oauth_provider_url_idp1_url
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
  [ {rabbit, <<"rabbit">>},
    {idp1, <<"idp1">>},
    {idp2, <<"idp2">>},
    {idp3, <<"idp3">>},
    {idp1_url, <<"https://idp1">>},
    {idp2_url, <<"https://idp2">>},
    {idp3_url, <<"https://idp3">>},
    {url0, <<"https://url0">>},
    {url1, <<"https://url1">>},
    {a, <<"a">>},
    {b, <<"b">>},
    {z, <<"z">>},
    {x, <<"x">>},
    {admin_mgt, <<"admin mgt">>},
    {read_write, <<"read write">>} | Config].

end_per_suite(_Config) ->
    ok.

init_per_group(with_oauth_disabled, Config) ->
  application:set_env(rabbitmq_management, oauth_enabled, false),
  Config;
init_per_group(with_oauth_enabled, Config) ->
  application:set_env(rabbitmq_management, oauth_enabled, true),
  Config;

init_per_group(with_resource_server_id_rabbit, Config) ->
  application:set_env(rabbitmq_auth_backend_oauth2, resource_server_id, ?config(rabbit, Config)),
  Config;
init_per_group(with_mgt_oauth_client_id_z, Config) ->
  application:set_env(rabbitmq_management, oauth_client_id, ?config(z, Config)),
  Config;
init_per_group(with_mgt_aouth_provider_url_url0, Config) ->
  application:set_env(rabbitmq_management, oauth_provider_url, ?config(url0, Config)),
  Config;
init_per_group(with_root_issuer_url1, Config) ->
  application:set_env(rabbitmq_auth_backend_oauth2, issuer, ?config(url1, Config)),
  Config;
init_per_group(with_scopes_admin_mgt, Config) ->
  application:set_env(rabbitmq_management, oauth_scopes, ?config(admin_mgt, Config)),
  Config;
init_per_group(with_scopes_write_read, Config) ->
  application:set_env(rabbitmq_management, oauth_scopes, ?config(write_read, Config)),
  Config;

init_per_group(with_idp_initiated_logon, Config) ->
  application:set_env(rabbitmq_management, oauth_initiated_logon_type, idp_initiated),
  Config;
init_per_group(with_oauth_disable_basic_auth_false, Config) ->
  application:set_env(rabbitmq_management, oauth_disable_basic_auth, false),
  Config;

init_per_group(with_oauth_providers_idp1_idp2, Config) ->
  Idp1 = ?config(idp1, Config),
  Idp2 = ?config(idp2, Config),
  Idp1Url = ?config(idp1_url, Config),
  Idp2Url = ?config(idp2_url, Config),
  application:set_env(rabbitmq_auth_backend_oauth2, oauth_providers, #{
    Idp1 => [ { issuer, Idp1Url} ],
    Idp2 => [ { issuer, Idp2Url} ]
  }),
  Config;
init_per_group(with_resource_server_a, Config) ->
  ResourceA = ?config(a, Config),
  ResourceServers = application:get_env(rabbitmq_auth_backend_oauth2, resource_servers, #{}),
  ResourceServers1 = maps:put(ResourceA, [ { id, ResourceA} | maps:get(ResourceA, ResourceServers, []) ], ResourceServers),
  application:set_env(rabbitmq_auth_backend_oauth2, resource_servers, ResourceServers1),
  Config;

init_per_group(with_resource_server_a_with_oauth_provider_idp1, Config) ->
  ResourceA = ?config(a, Config),
  Idp1 = ?config(idp1, Config),
  ResourceServers = application:get_env(rabbitmq_auth_backend_oauth2, resource_servers, #{}),
  ResourceServers1 = maps:put(ResourceA, [ { oauth_provider_id, Idp1} | maps:get(ResourceA, ResourceServers, []) ], ResourceServers),
  application:set_env(rabbitmq_auth_backend_oauth2, resource_servers, ResourceServers1),
  Config;

init_per_group(with_mgt_oauth_resource_server_a_with_oauth_provider_url_url1, Config) ->
  ResourceA = ?config(a, Config),
  Url = ?config(url1, Config),
  ResourceServers = application:get_env(rabbitmq_management, resource_servers, #{}),
  ResourceServers1 = maps:put(ResourceA, [ { oauth_provider_url, Url} | maps:get(ResourceA, ResourceServers, []) ], ResourceServers),
  application:set_env(rabbitmq_management, resource_servers, ResourceServers1),
  Config;


init_per_group(with_mgt_resource_server_a_with_client_id_x, Config) ->
  ResourceA = ?config(a, Config),
  ClientId = ?config(x, Config),
  ResourceServers = application:get_env(rabbitmq_management, resource_servers, #{}),
  OAuthResourceA = [ { oauth_client_id, ClientId} | maps:get(ResourceA, ResourceServers, []) ],
  application:set_env(rabbitmq_management, resource_servers,
    maps:put(ResourceA, OAuthResourceA, ResourceServers)),
  logEnvVars(),
  Config;

init_per_group(with_default_oauth_provider_idp1, Config) ->
  Idp = ?config(idp1, Config),
  application:set_env(rabbitmq_auth_backend_oauth2, default_oauth_provider, Idp),
  Config;
init_per_group(with_default_oauth_provider_idp3, Config) ->
  Idp = ?config(idp3, Config),
  application:set_env(rabbitmq_auth_backend_oauth2, default_oauth_provider, Idp),
  Config;

init_per_group(_, Config) ->
  Config.

end_per_group(with_scopes_admin_mgt, Config) ->
  application:unset_env(rabbitmq_management, oauth_scopes),
  Config;
end_per_group(with_scopes_write_read, Config) ->
  application:unset_env(rabbitmq_management, oauth_scopes),
  Config;
end_per_group(with_oauth_disabled, Config) ->
  application:unset_env(rabbitmq_management, oauth_enabled),
  Config;
end_per_group(with_oauth_enabled, Config) ->
  application:unset_env(rabbitmq_management, oauth_enabled),
  Config;
end_per_group(with_oauth_disable_basic_auth_false, Config) ->
  application:unset_env(rabbitmq_management, oauth_disable_basic_auth),
  Config;
end_per_group(with_resource_server_id_rabbit, Config) ->
  application:unset_env(rabbitmq_auth_backend_oauth2, resource_server_id),
  Config;
end_per_group(with_mgt_aouth_provider_url_url0, Config) ->
  application:unset_env(rabbitmq_management, oauth_provider_url),
  Config;
end_per_group(with_root_issuer_url1, Config) ->
  application:unset_env(rabbitmq_auth_backend_oauth2, issuer),
  Config;
end_per_group(with_mgt_oauth_client_id_z, Config) ->
  application:unset_env(rabbitmq_management, oauth_client_id),
  Config;
end_per_group(with_idp_initiated_logon, Config) ->
  application:unset_env(rabbitmq_management, oauth_initiated_logon_type),
  Config;
end_per_group(with_resource_server_a, Config) ->
  ResourceA = ?config(a, Config),
  ResourceServers = application:get_env(rabbitmq_auth_backend_oauth2, resource_servers, #{}),
  NewMap = maps:remove(ResourceA, ResourceServers),
  case maps:size(NewMap) of
    0 -> application:unset_env(rabbitmq_auth_backend_oauth2, resource_servers);
    _ -> application:set_env(rabbitmq_auth_backend_oauth2, resource_servers, NewMap)
  end,
  Config;
end_per_group(with_resource_server_a_with_oauth_provider_idp1, Config) ->
  ResourceA = ?config(a, Config),
  ResourceServers = application:get_env(rabbitmq_auth_backend_oauth2, resource_servers, #{}),
  OAuthResourceA = proplists:delete(oauth_provider_id, maps:get(ResourceA, ResourceServers, [])),
  NewMap = delete_key_with_empty_proplist(ResourceA, maps:put(ResourceA, OAuthResourceA, ResourceServers)),
  case maps:size(NewMap) of
    0 -> application:unset_env(rabbitmq_auth_backend_oauth2, resource_servers);
    _ -> application:set_env(rabbitmq_auth_backend_oauth2, resource_servers, NewMap)
  end,
  Config;
end_per_group(with_mgt_oauth_resource_server_a_with_oauth_provider_url_url1, Config) ->
  ResourceA = ?config(a, Config),
  ResourceServers = application:get_env(rabbitmq_management, resource_servers, #{}),
  OAuthResourceA = proplists:delete(oauth_provider_url, maps:get(ResourceA, ResourceServers, [])),
  NewMap = delete_key_with_empty_proplist(ResourceA, maps:put(ResourceA, OAuthResourceA, ResourceServers)),
  case maps:size(NewMap) of
    0 -> application:unset_env(rabbitmq_management, resource_servers);
    _ -> application:set_env(rabbitmq_management, resource_servers, NewMap)
  end,
  Config;

end_per_group(with_mgt_resource_server_a_with_client_id_x, Config) ->
  ResourceA = ?config(a, Config),
  ResourceServers = application:get_env(rabbitmq_management, resource_servers, #{}),
  OAuthResourceA = proplists:delete(oauth_client_id, maps:get(ResourceA, ResourceServers, [])),
  NewMap = delete_key_with_empty_proplist(ResourceA, maps:put(ResourceA, OAuthResourceA, ResourceServers)),
  case maps:size(NewMap) of
    0 -> application:unset_env(rabbitmq_management, resource_servers);
    _ -> application:set_env(rabbitmq_management, resource_servers, NewMap)
  end,
  Config;
end_per_group(with_default_oauth_provider_idp1, Config) ->
  application:unset_env(rabbitmq_auth_backend_oauth2, default_oauth_provider),
  Config;
end_per_group(with_default_oauth_provider_idp3, Config) ->
  application:unset_env(rabbitmq_auth_backend_oauth2, default_oauth_provider),
  Config;

end_per_group(_, Config) ->
  Config.


delete_key_with_empty_proplist(Key, Map) ->
  case maps:get(Key, Map) of
    [] -> maps:remove(Key, Map);
    _ -> Map
  end.

%% -------------------------------------------------------------------
%% Test cases.
%% -------------------------------------------------------------------

should_return_oauth_provider_url_idp1_url(Config) ->
  Actual = rabbit_mgmt_wm_auth:authSettings(),
  ?assertEqual(?config(idp1_url, Config), proplists:get_value(oauth_provider_url, Actual)).

should_return_scopes_admin_mgt(Config) ->
  Actual = rabbit_mgmt_wm_auth:authSettings(),
  ?assertEqual(?config(admin_mgt, Config), proplists:get_value(oauth_scopes, Actual)).

should_return_disabled_auth_settings(_Config) ->
  [{oauth_enabled, false}] = rabbit_mgmt_wm_auth:authSettings().

should_return_mgt_resource_server_a_oauth_provider_url_url0(Config) ->
  Actual = rabbit_mgmt_wm_auth:authSettings(),
  ResourceServers = proplists:get_value(oauth_resource_servers, Actual),
  ResourceServer = maps:get(?config(a, Config), ResourceServers),
  ?assertEqual(?config(url0), proplists:get_value(oauth_provider_url, ResourceServer)).

should_return_oauth_resource_server_a_with_client_id_x(Config) ->
  Actual = rabbit_mgmt_wm_auth:authSettings(),
  log(Actual),
  OAuthResourceServers =  proplists:get_value(oauth_resource_servers, Actual),
  OauthResource = maps:get(?config(a, Config), OAuthResourceServers),
  ?assertEqual(?config(x, Config), proplists:get_value(oauth_client_id, OauthResource)).

should_return_oauth_resource_server_a_with_oauth_provider_url_idp1_url(Config) ->
  Actual = rabbit_mgmt_wm_auth:authSettings(),
  OAuthResourceServers =  proplists:get_value(oauth_resource_servers, Actual),
  OauthResource = maps:get(?config(a, Config), OAuthResourceServers),
  ?assertEqual(?config(idp1_url, Config), proplists:get_value(oauth_provider_url, OauthResource)).

should_return_oauth_resource_server_a_with_oauth_provider_url_url1(Config) ->
  Actual = rabbit_mgmt_wm_auth:authSettings(),
  OAuthResourceServers =  proplists:get_value(oauth_resource_servers, Actual),
  OauthResource = maps:get(?config(a, Config), OAuthResourceServers),
  ?assertEqual(?config(url1, Config), proplists:get_value(oauth_provider_url, OauthResource)).

should_return_oauth_resource_server_a_with_oauth_provider_url_url0(Config) ->
  Actual = rabbit_mgmt_wm_auth:authSettings(),
  OAuthResourceServers =  proplists:get_value(oauth_resource_servers, Actual),
  OauthResource = maps:get(?config(a, Config), OAuthResourceServers),
  ?assertEqual(?config(url0, Config), proplists:get_value(oauth_provider_url, OauthResource)).

should_return_oauth_resource_server_rabbit_with_oauth_provider_url_idp1_url(Config) ->
  Actual = rabbit_mgmt_wm_auth:authSettings(),
  OAuthResourceServers =  proplists:get_value(oauth_resource_servers, Actual),
  OauthResource = maps:get(?config(rabbit, Config), OAuthResourceServers),
  ?assertEqual(?config(idp1_url, Config), proplists:get_value(oauth_provider_url, OauthResource)).

should_return_oauth_resource_server_rabbit_with_oauth_provider_url_url1(Config) ->
  Actual = rabbit_mgmt_wm_auth:authSettings(),
  OAuthResourceServers =  proplists:get_value(oauth_resource_servers, Actual),
  OauthResource = maps:get(?config(rabbit, Config), OAuthResourceServers),
  ?assertEqual(?config(url1, Config), proplists:get_value(oauth_provider_url, OauthResource)).

should_return_oauth_resource_server_rabbit_with_oauth_provider_url_url0(Config) ->
  Actual = rabbit_mgmt_wm_auth:authSettings(),
  OAuthResourceServers =  proplists:get_value(oauth_resource_servers, Actual),
  OauthResource = maps:get(?config(rabbit, Config), OAuthResourceServers),
  ?assertEqual(?config(url0, Config), proplists:get_value(oauth_provider_url, OauthResource)).

should_not_return_oauth_initiated_logon_type(_Config) ->
  Actual = rabbit_mgmt_wm_auth:authSettings(),
  ?assertEqual(false, proplists:is_defined(oauth_initiated_logon_type, Actual)).

should_not_return_scopes(_Config) ->
  Actual = rabbit_mgmt_wm_auth:authSettings(),
  ?assertEqual(false, proplists:is_defined(scopes, Actual)).

should_return_oauth_enabled(_Config) ->
  Actual = rabbit_mgmt_wm_auth:authSettings(),
  log(Actual),
  ?assertEqual(true, proplists:get_value(oauth_enabled, Actual)).

should_return_oauth_idp_initiated_logon(_Config) ->
  Actual = rabbit_mgmt_wm_auth:authSettings(),
  ?assertEqual(<<"idp_initiated">>, proplists:get_value(oauth_initiated_logon_type, Actual)).

should_return_oauth_disable_basic_auth_true(_Config) ->
  Actual = rabbit_mgmt_wm_auth:authSettings(),
  ?assertEqual(true, proplists:get_value(oauth_disable_basic_auth, Actual)).

should_return_oauth_disable_basic_auth_false(_Config) ->
  Actual = rabbit_mgmt_wm_auth:authSettings(),
  ?assertEqual(false, proplists:get_value(oauth_disable_basic_auth, Actual)).

should_return_oauth_client_id_z(Config) ->
  Actual = rabbit_mgmt_wm_auth:authSettings(),
  ?assertEqual(?config(z, Config), proplists:get_value(oauth_client_id, Actual)).

should_return_mgt_oauth_client_id(_Config) ->
  Actual = rabbit_mgmt_wm_auth:authSettings(),
  {ok, Expected} = application:get_env(rabbitmq_management, oauth_client_id),
  ?assertEqual(Expected, proplists:get_value(oauth_client_id, Actual)).

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
  logEnvVars(),
  ct:log("authSettings: ~p ", [AuthSettings]).
logEnvVars() ->
  ct:log("rabbitmq_management: ~p ", [application:get_all_env(rabbitmq_management)]),
  ct:log("rabbitmq_auth_backend_oauth2: ~p ", [application:get_all_env(rabbitmq_auth_backend_oauth2)]).


should_return_oauth_resource_server_A(Config) ->
  ClientId = ?config(oauth_client_id, Config),
  ResourceA = ?config(resource_A, Config),
  Actual = rabbit_mgmt_wm_auth:authSettings(),
  log(Actual),

  OAuthResourceServers = proplists:get_value(oauth_resource_servers, Actual),
  OAuthResourceA = maps:get(ResourceA, OAuthResourceServers),

  ?assertEqual(true, proplists:get_value(oauth_enabled, Actual)),
  ?assertEqual(ClientId, proplists:get_value(oauth_client_id, OAuthResourceA)).
