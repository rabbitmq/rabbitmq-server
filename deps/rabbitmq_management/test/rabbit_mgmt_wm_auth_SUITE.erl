%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2023 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries.  All rights reserved.
%%

-module(rabbit_mgmt_wm_auth_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").
-import(application, [set_env/3, unset_env/2]).
-import(rabbit_mgmt_wm_auth, [authSettings/0]).
-compile(export_all).

all() ->
    [
     {group, without_any_settings},
     {group, with_oauth_disabled},
     {group, verify_client_id_and_secret},
     {group, verify_mgt_oauth_provider_url_with_single_resource},
     {group, verify_mgt_oauth_provider_url_with_single_resource_and_another_resource},
     {group, verify_end_session_endpoint_with_single_resource},
     {group, verify_end_session_endpoint_with_single_resource_and_another_resource},
     {group, verify_multi_resource_and_provider},
     {group, verify_oauth_initiated_logon_type_for_sp_initiated},
     {group, verify_oauth_initiated_logon_type_for_idp_initiated},
     {group, verify_oauth_disable_basic_auth},
     {group, verify_oauth_scopes},
     {group, verify_extra_endpoint_params}
    ].

groups() ->
    [

      {verify_multi_resource_and_provider, [], [
        {with_oauth_enabled, [], [
            {with_oauth_providers_idp1_idp2, [], [
                {with_default_oauth_provider_idp1, [], [
                    {with_resource_server_a, [], [
                        should_return_disabled_auth_settings,
                        {with_mgt_resource_server_a_with_client_id_x, [], [
                            should_return_oauth_enabled,
                            should_return_oauth_resource_server_a_with_client_id_x
                        ]}
                    ]}
                ]}
            ]}
        ]}
      ]},
      {without_any_settings, [], [
        should_return_disabled_auth_settings
      ]},
      {with_oauth_disabled, [], [
        should_return_disabled_auth_settings
      ]},
      {verify_client_id_and_secret, [], [
        {with_oauth_enabled, [], [
          should_return_disabled_auth_settings,
          {with_root_issuer_url1, [], [
            {with_resource_server_id_rabbit, [], [
              should_return_disabled_auth_settings,
              {with_mgt_oauth_client_id_z, [], [
                should_return_oauth_enabled,
                should_return_oauth_client_id_z,
                should_not_return_oauth_client_secret,
                {with_mgt_oauth_client_secret_q, [], [
                  should_return_oauth_enabled,
                  should_return_oauth_client_secret_q
                ]}
              ]}
            ]},
            {with_resource_server_a, [], [
              should_return_disabled_auth_settings,
              {with_mgt_oauth_client_id_z, [], [
                should_return_oauth_enabled,
                should_return_oauth_client_id_z,
                {with_mgt_resource_server_a_with_client_id_x, [], [
                  should_return_oauth_resource_server_a_with_client_id_x
                ]},
                {with_mgt_resource_server_a_with_client_secret_w, [], [
                  should_return_oauth_resource_server_a_with_client_secret_w
                ]}
              ]}
            ]}
          ]}
        ]}
      ]},
      {verify_mgt_oauth_provider_url_with_single_resource, [], [
        {with_resource_server_id_rabbit, [], [
          {with_root_issuer_url1, [], [
            {with_oauth_enabled, [], [
              should_return_disabled_auth_settings,
              {with_mgt_oauth_client_id_z, [], [
                should_return_mgt_oauth_provider_url_url1,
                should_return_mgt_oauth_metadata_url_url1,
                {with_mgt_oauth_provider_url_url0, [], [
                  should_return_mgt_oauth_provider_url_url0,
                  should_return_mgt_oauth_metadata_url_url1,
                  {with_mgt_oauth_resource_server_rabbit_with_oauth_metadata_url_url1, [], [
                    should_return_oauth_resource_server_rabbit_with_oauth_metadata_url_url1
                  ]}                 
                ]}
              ]}
            ]}
          ]},
          {with_oauth_providers_idp1_idp2, [], [
            {with_default_oauth_provider_idp1, [], [
              {with_oauth_enabled, [], [
                should_return_disabled_auth_settings,
                {with_mgt_oauth_client_id_z, [], [
                  should_return_mgt_oauth_provider_url_idp1_url,
                  should_return_mgt_oauth_matadata_url_idp1_url,
                  {with_root_issuer_url1, [], [
                    should_return_mgt_oauth_provider_url_idp1_url
                  ]},
                  {with_mgt_oauth_provider_url_url0, [], [
                    should_return_mgt_oauth_provider_url_url0
                  ]}
                ]}
              ]}
            ]}
          ]}
        ]}
      ]},
      {verify_end_session_endpoint_with_single_resource, [], [
        {with_resource_server_id_rabbit, [], [
          {with_root_issuer_url1, [], [
            {with_oauth_enabled, [], [
              {with_mgt_oauth_client_id_z, [], [
                should_not_return_end_session_endpoint,
                {with_root_end_session_endpoint_0, [], [
                  should_return_end_session_endpoint_0
                ]}
              ]}
            ]}
          ]},
          {with_oauth_providers_idp1_idp2, [], [
            {with_default_oauth_provider_idp1, [], [
              {with_oauth_enabled, [], [
                {with_mgt_oauth_client_id_z, [], [
                  should_not_return_end_session_endpoint,
                  {with_end_session_endpoint_for_idp1_1, [], [
                    should_return_end_session_endpoint_1
                  ]},
                  {with_root_end_session_endpoint_0, [], [
                    should_not_return_end_session_endpoint,
                    {with_end_session_endpoint_for_idp1_1, [], [
                      should_return_end_session_endpoint_1
                    ]}
                  ]}
                ]}
              ]}
            ]}
          ]}
        ]}
      ]},
      {verify_end_session_endpoint_with_single_resource_and_another_resource, [], [
        {with_resource_server_id_rabbit, [], [
          {with_resource_server_a, [], [
            {with_root_issuer_url1, [], [
              {with_oauth_enabled, [], [
                should_return_disabled_auth_settings,
                {with_mgt_oauth_client_id_z, [], [
                  should_not_return_end_session_endpoint,
                  should_return_oauth_resource_server_a_without_end_session_endpoint,
                  {with_root_end_session_endpoint_0, [], [
                    should_return_end_session_endpoint_0,
                    should_return_oauth_resource_server_a_with_end_session_endpoint_0
                  ]},
                  {with_oauth_providers_idp1_idp2, [], [
                    {with_default_oauth_provider_idp1, [], [
                      {with_end_session_endpoint_for_idp1_1, [], [
                        should_return_end_session_endpoint_1,
                        should_return_oauth_resource_server_a_with_end_session_endpoint_1,
                        {with_oauth_provider_idp2_for_resource_server_a, [], [
                          {with_end_session_endpoint_for_idp2_2, [], [
                            should_return_oauth_resource_server_a_with_end_session_endpoint_2
                          ]}
                        ]}
                      ]}
                    ]}
                  ]}
                ]}
              ]}
            ]}
          ]}
        ]}
      ]},
      {verify_mgt_oauth_provider_url_with_single_resource_and_another_resource, [], [
        {with_resource_server_id_rabbit, [], [
          {with_resource_server_a, [], [
            {with_root_issuer_url1, [], [
              {with_oauth_enabled, [], [
                should_return_disabled_auth_settings,
                {with_mgt_oauth_client_id_z, [], [
                  should_return_oauth_resource_server_rabbit_with_oauth_provider_url_url1,
                  should_return_oauth_resource_server_rabbit_with_oauth_metadata_url_url1,
                  should_return_oauth_resource_server_a_with_oauth_provider_url_url1,
                  should_return_oauth_resource_server_a_with_oauth_metadata_url_url1,
                  {with_mgt_oauth_provider_url_url0, [], [
                    should_return_oauth_resource_server_rabbit_with_oauth_provider_url_url0,
                    should_return_oauth_resource_server_rabbit_with_oauth_metadata_url_url1,
                    should_return_oauth_resource_server_a_with_oauth_provider_url_url0,
                    should_return_oauth_resource_server_a_with_oauth_metadata_url_url1,
                    {with_mgt_oauth_resource_server_a_with_oauth_provider_url_url1, [], [
                      should_return_oauth_resource_server_rabbit_with_oauth_provider_url_url0,
                      should_return_oauth_resource_server_a_with_oauth_provider_url_url1                      
                    ]}
                  ]}
                ]}
              ]}
            ]},
            {with_oauth_providers_idp1_idp2, [], [
              {with_default_oauth_provider_idp1, [], [
                {with_oauth_enabled, [], [
                  should_return_disabled_auth_settings,
                  {with_mgt_oauth_client_id_z, [], [
                    should_return_oauth_resource_server_rabbit_with_oauth_provider_url_idp1_url,
                    should_return_oauth_resource_server_rabbit_with_oauth_metadata_url_idp1_url,
                    {with_mgt_oauth_resource_server_rabbit_with_oauth_metadata_url_url1, [], [
                      should_return_oauth_resource_server_rabbit_with_oauth_metadata_url_url1
                    ]},
                    {with_root_issuer_url1, [], [
                      should_return_oauth_resource_server_rabbit_with_oauth_provider_url_idp1_url
                    ]},
                    {with_mgt_oauth_provider_url_url0, [], [
                      should_return_oauth_resource_server_rabbit_with_oauth_provider_url_url0,
                      should_return_oauth_resource_server_rabbit_with_oauth_metadata_url_idp1_url,
                      {with_mgt_oauth_resource_server_a_with_oauth_provider_url_url1, [], [
                        should_return_oauth_resource_server_rabbit_with_oauth_provider_url_url0,
                        should_return_oauth_resource_server_a_with_oauth_provider_url_url1,
                        {with_mgt_oauth_resource_server_a_with_oauth_metadata_url_url1, [], [
                          should_return_oauth_resource_server_a_with_oauth_metadata_url_url1
                        ]}
                      ]}
                    ]}
                  ]}
                ]}
              ]}
            ]}
          ]}
        ]}
      ]},
      {verify_oauth_initiated_logon_type_for_sp_initiated, [], [
        should_return_disabled_auth_settings,
        {with_resource_server_id_rabbit, [], [
          {with_root_issuer_url1, [], [
            should_return_disabled_auth_settings,
            {with_oauth_enabled, [], [
              should_return_disabled_auth_settings,
              {with_mgt_oauth_client_id_z, [], [
                should_return_oauth_enabled,
                should_not_return_oauth_initiated_logon_type,
                {with_oauth_initiated_logon_type_sp_initiated, [], [
                  should_not_return_oauth_initiated_logon_type
                ]},
                {with_resource_server_a, [], [
                  {with_mgt_resource_server_a_with_oauth_initiated_logon_type_sp_initiated, [], [
                    should_return_oauth_resource_server_a_with_oauth_initiated_logon_type_sp_initiated
                  ]}
                ]}
              ]}
            ]}
          ]}
        ]}
      ]},
      {verify_oauth_initiated_logon_type_for_idp_initiated, [], [
        should_return_disabled_auth_settings,
        {with_root_issuer_url1, [], [
          should_return_disabled_auth_settings,
          {with_oauth_initiated_logon_type_idp_initiated, [], [
            should_return_disabled_auth_settings,
            {with_resource_server_id_rabbit, [], [
              should_return_disabled_auth_settings,
              {with_oauth_enabled, [], [
                should_return_oauth_enabled,
                should_return_oauth_initiated_logon_type_idp_initiated,
                {with_resource_server_a, [], [
                  {with_mgt_resource_server_a_with_oauth_initiated_logon_type_idp_initiated, [], [
                    should_return_oauth_resource_server_a_with_oauth_initiated_logon_type_idp_initiated
                  ]},
                  {with_mgt_resource_server_a_with_oauth_initiated_logon_type_sp_initiated, [], [
                    should_not_return_oauth_resource_server_a,
                    {with_mgt_resource_server_a_with_client_id_x, [], [
                      should_return_oauth_resource_server_a_with_client_id_x
                    ]}
                  ]}
                ]}
              ]}
            ]}
          ]}
        ]}
      ]},
      {verify_oauth_disable_basic_auth, [], [
        {with_resource_server_id_rabbit, [], [
          {with_root_issuer_url1, [], [
            {with_oauth_enabled, [], [
              {with_mgt_oauth_client_id_z, [], [
                should_return_oauth_disable_basic_auth_true,
                {with_oauth_disable_basic_auth_false, [], [
                  should_return_oauth_disable_basic_auth_false
                ]}
              ]}
            ]}
          ]}
        ]}
      ]},
      {verify_oauth_scopes, [], [
        {with_resource_server_id_rabbit, [], [
          {with_root_issuer_url1, [], [
            {with_oauth_enabled, [], [
              {with_mgt_oauth_client_id_z, [], [
                should_not_return_oauth_scopes,
                {with_oauth_scopes_admin_mgt, [], [
                  should_return_oauth_scopes_admin_mgt,
                  {with_resource_server_a, [], [
                    {with_mgt_resource_server_a_with_scopes_read_write, [], [
                      should_return_mgt_oauth_resource_server_a_with_scopes_read_write
                    ]}
                  ]}
                ]}
              ]}
            ]}
          ]}
        ]}
      ]},
      {verify_extra_endpoint_params, [], [
        {with_resource_server_id_rabbit, [], [
          {with_root_issuer_url1, [], [
            {with_oauth_enabled, [], [
              {with_mgt_oauth_client_id_z, [], [
                should_return_mgt_oauth_resource_rabbit_without_authorization_endpoint_params,
                should_return_mgt_oauth_resource_rabbit_without_token_endpoint_params,
                {with_authorization_endpoint_params_0, [], [
                  should_return_mgt_oauth_resource_rabbit_with_authorization_endpoint_params_0
                ]},
                {with_token_endpoint_params_0, [], [
                  should_return_mgt_oauth_resource_rabbit_with_token_endpoint_params_0
                ]},
                {with_resource_server_a, [], [
                  {with_mgt_resource_server_a_with_authorization_endpoint_params_1, [], [
                    should_return_mgt_oauth_resource_a_with_authorization_endpoint_params_1
                  ]},
                  {with_mgt_resource_server_a_with_token_endpoint_params_1, [], [
                    should_return_mgt_oauth_resource_a_with_token_endpoint_params_1
                  ]}
                ]}
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
    {meta_idp1_url, <<"https://idp1/.well-known/openid-configuration">>},
    {idp2_url, <<"https://idp2">>},
    {meta_idp2_url, <<"https://idp2/.well-known/openid-configuration">>},
    {idp3_url, <<"https://idp3">>},
    {meta_idp3_url, <<"https://idp3/.well-known/openid-configuration">>},
    {url0, <<"https://url0">>},
    {meta_url0, <<"https://url0/.well-known/openid-configuration">>},
    {url1, <<"https://url1">>},
    {meta_url1, <<"https://url1/.well-known/openid-configuration">>},
    {logout_url_0, <<"https://logout_0">>},
    {logout_url_1, <<"https://logout_1">>},
    {logout_url_2, <<"https://logout_2">>},
    {a, <<"a">>},
    {b, <<"b">>},
    {q, <<"q">>},
    {w, <<"w">>},
    {z, <<"z">>},
    {x, <<"x">>},
    {authorization_params_0, [{<<"a-param0">>, <<"value0">>}]},
    {authorization_params_1, [{<<"a-param1">>, <<"value1">>}]},
    {token_params_0, [{<<"t-param0">>, <<"value0">>}]},
    {token_params_1, [{<<"t-param1">>, <<"value1">>}]},
    {admin_mgt, <<"admin mgt">>},
    {read_write, <<"read write">>} | Config].

end_per_suite(_Config) ->
    ok.

init_per_group(with_oauth_disabled, Config) ->
  set_env(rabbitmq_management, oauth_enabled, false),
  Config;
init_per_group(with_oauth_enabled, Config) ->
  set_env(rabbitmq_management, oauth_enabled, true),
  Config;
init_per_group(with_resource_server_id_rabbit, Config) ->
  set_env(rabbitmq_auth_backend_oauth2, resource_server_id, ?config(rabbit, Config)),
  Config;
init_per_group(with_mgt_oauth_client_id_z, Config) ->
  set_env(rabbitmq_management, oauth_client_id, ?config(z, Config)),
  Config;
init_per_group(with_mgt_resource_server_a_with_client_secret_w, Config) ->
  set_attribute_in_entry_for_env_variable(rabbitmq_management, oauth_resource_servers,
    ?config(a, Config), oauth_client_secret, ?config(w, Config)),
  Config;
init_per_group(with_mgt_oauth_client_secret_q, Config) ->
  set_env(rabbitmq_management, oauth_client_secret, ?config(q, Config)),
  Config;
init_per_group(with_mgt_oauth_provider_url_url0, Config) ->
  set_env(rabbitmq_management, oauth_provider_url, ?config(url0, Config)),
  Config;
init_per_group(with_root_issuer_url1, Config) ->
  set_env(rabbitmq_auth_backend_oauth2, issuer, ?config(url1, Config)),
  Config;
init_per_group(with_oauth_scopes_admin_mgt, Config) ->
  set_env(rabbitmq_management, oauth_scopes, ?config(admin_mgt, Config)),
  Config;
init_per_group(with_oauth_scopes_write_read, Config) ->
  set_env(rabbitmq_management, oauth_scopes, ?config(write_read, Config)),
  Config;
init_per_group(with_oauth_initiated_logon_type_idp_initiated, Config) ->
  set_env(rabbitmq_management, oauth_initiated_logon_type, idp_initiated),
  Config;
init_per_group(with_oauth_initiated_logon_type_sp_initiated, Config) ->
  set_env(rabbitmq_management, oauth_initiated_logon_type, sp_initiated),
  Config;
init_per_group(with_mgt_resource_server_a_with_oauth_initiated_logon_type_sp_initiated, Config) ->
  set_attribute_in_entry_for_env_variable(rabbitmq_management, oauth_resource_servers,
    ?config(a, Config), oauth_initiated_logon_type, sp_initiated),
  Config;
init_per_group(with_mgt_resource_server_a_with_oauth_initiated_logon_type_idp_initiated, Config) ->
  set_attribute_in_entry_for_env_variable(rabbitmq_management, oauth_resource_servers,
    ?config(a, Config), oauth_initiated_logon_type, idp_initiated),
  Config;
init_per_group(with_oauth_disable_basic_auth_false, Config) ->
  set_env(rabbitmq_management, oauth_disable_basic_auth, false),
  Config;
init_per_group(with_oauth_providers_idp1_idp2, Config) ->
  set_env(rabbitmq_auth_backend_oauth2, oauth_providers, #{
    ?config(idp1, Config) => [ { issuer, ?config(idp1_url, Config)} ],
    ?config(idp2, Config) => [ { issuer, ?config(idp2_url, Config)} ]
  }),
  Config;
init_per_group(with_resource_server_a, Config) ->
  set_attribute_in_entry_for_env_variable(rabbitmq_auth_backend_oauth2, resource_servers,
    ?config(a, Config), id, ?config(a, Config)),
  Config;
init_per_group(with_resource_server_a_with_oauth_provider_idp1, Config) ->
  set_attribute_in_entry_for_env_variable(rabbitmq_auth_backend_oauth2, resource_servers,
    ?config(a, Config), oauth_provider_id, ?config(idp1, Config)),
  Config;
init_per_group(with_mgt_resource_server_a_with_scopes_read_write, Config) ->
  set_attribute_in_entry_for_env_variable(rabbitmq_management, oauth_resource_servers,
    ?config(a, Config), scopes, ?config(read_write, Config)),
  Config;
init_per_group(with_mgt_oauth_resource_server_a_with_oauth_provider_url_url1, Config) ->
  set_attribute_in_entry_for_env_variable(rabbitmq_management, oauth_resource_servers,
    ?config(a, Config), oauth_provider_url, ?config(url1, Config)),
  Config;
init_per_group(with_mgt_resource_server_a_with_client_id_x, Config) ->
  set_attribute_in_entry_for_env_variable(rabbitmq_management, oauth_resource_servers,
    ?config(a, Config), oauth_client_id, ?config(x, Config)),
  Config;

init_per_group(with_default_oauth_provider_idp1, Config) ->
  set_env(rabbitmq_auth_backend_oauth2, default_oauth_provider, ?config(idp1, Config)),
  Config;
init_per_group(with_mgt_oauth_resource_server_rabbit_with_oauth_metadata_url_url1, Config) ->
  set_env(rabbitmq_management, oauth_metadata_url, ?config(meta_url1, Config)),
  Config;
init_per_group(with_default_oauth_provider_idp3, Config) ->
  set_env(rabbitmq_auth_backend_oauth2, default_oauth_provider, ?config(idp3, Config)),
  Config;
init_per_group(with_root_end_session_endpoint_0, Config) ->
  set_env(rabbitmq_auth_backend_oauth2, end_session_endpoint, ?config(logout_url_0, Config)),
  Config;
init_per_group(with_end_session_endpoint_for_idp1_1, Config) ->
  set_attribute_in_entry_for_env_variable(rabbitmq_auth_backend_oauth2, oauth_providers,
    ?config(idp1, Config), end_session_endpoint, ?config(logout_url_1, Config)),
  Config;
init_per_group(with_end_session_endpoint_for_idp2_2, Config) ->
  set_attribute_in_entry_for_env_variable(rabbitmq_auth_backend_oauth2, oauth_providers,
    ?config(idp2, Config), end_session_endpoint, ?config(logout_url_2, Config)),
  Config;
init_per_group(with_oauth_provider_idp2_for_resource_server_a, Config) ->
  set_attribute_in_entry_for_env_variable(rabbitmq_auth_backend_oauth2, resource_servers,
    ?config(a, Config), oauth_provider_id, ?config(idp2, Config)),
  Config;
init_per_group(with_authorization_endpoint_params_0, Config) ->
  set_env(rabbitmq_management, oauth_authorization_endpoint_params,
    ?config(authorization_params_0, Config)),
  Config;
init_per_group(with_token_endpoint_params_0, Config) ->
  set_env(rabbitmq_management, oauth_token_endpoint_params,
    ?config(token_params_0, Config)),
  Config;
init_per_group(with_mgt_resource_server_a_with_authorization_endpoint_params_1, Config) ->
  set_attribute_in_entry_for_env_variable(rabbitmq_management, oauth_resource_servers,
    ?config(a, Config), oauth_authorization_endpoint_params, ?config(authorization_params_1, Config)),
  Config;
init_per_group(with_mgt_oauth_resource_server_a_with_oauth_metadata_url_url1, Config) ->
  set_attribute_in_entry_for_env_variable(rabbitmq_management, oauth_resource_servers,
    ?config(a, Config), oauth_metadata_url, ?config(meta_url1, Config)),
  Config;
init_per_group(with_mgt_resource_server_a_with_token_endpoint_params_1, Config) ->
  set_attribute_in_entry_for_env_variable(rabbitmq_management, oauth_resource_servers,
    ?config(a, Config), oauth_token_endpoint_params, ?config(token_params_1, Config)),
  Config;


init_per_group(_, Config) ->
  Config.

end_per_group(with_oauth_providers_idp1_idp2, Config) ->
  unset_env(rabbitmq_auth_backend_oauth2, oauth_providers),
  Config;
end_per_group(with_mgt_oauth_client_secret_q, Config) ->
  unset_env(rabbitmq_management, oauth_client_secret),
  Config;
end_per_group(with_oauth_scopes_admin_mgt, Config) ->
  unset_env(rabbitmq_management, oauth_scopes),
  Config;
end_per_group(with_oauth_scopes_write_read, Config) ->
  unset_env(rabbitmq_management, oauth_scopes),
  Config;
end_per_group(with_oauth_disabled, Config) ->
  unset_env(rabbitmq_management, oauth_enabled),
  Config;
end_per_group(with_oauth_enabled, Config) ->
  unset_env(rabbitmq_management, oauth_enabled),
  Config;
end_per_group(with_oauth_disable_basic_auth_false, Config) ->
  unset_env(rabbitmq_management, oauth_disable_basic_auth),
  Config;
end_per_group(with_resource_server_id_rabbit, Config) ->
  unset_env(rabbitmq_auth_backend_oauth2, resource_server_id),
  Config;
end_per_group(with_default_oauth_provider_idp1, Config) ->
  unset_env(rabbitmq_auth_backend_oauth2, default_oauth_provider),
  Config;
end_per_group(with_mgt_oauth_provider_url_url0, Config) ->
  unset_env(rabbitmq_management, oauth_provider_url),
  Config;
end_per_group(with_mgt_oauth_resource_server_rabbit_with_oauth_metadata_url_url1, Config) ->
  unset_env(rabbitmq_management, oauth_metadata_url),
  Config;
end_per_group(with_root_issuer_url1, Config) ->
  unset_env(rabbitmq_auth_backend_oauth2, issuer),
  unset_env(rabbitmq_auth_backend_oauth2, discovery_endpoint),
  Config;
end_per_group(with_mgt_oauth_client_id_z, Config) ->
  unset_env(rabbitmq_management, oauth_client_id),
  Config;
end_per_group(with_oauth_initiated_logon_type_idp_initiated, Config) ->
  unset_env(rabbitmq_management, oauth_initiated_logon_type),
  Config;
end_per_group(with_oauth_initiated_logon_type_sp_initiated, Config) ->
  unset_env(rabbitmq_management, oauth_initiated_logon_type),
  Config;
end_per_group(with_mgt_resource_server_a_with_client_secret_w, Config) ->
  remove_attribute_from_entry_from_env_variable(rabbitmq_management, oauth_resource_servers,
    ?config(a, Config), oauth_client_secret),
  Config;
end_per_group(with_resource_server_a, Config) ->
  remove_entry_from_env_variable(rabbitmq_auth_backend_oauth2, resource_servers,
    ?config(a, Config)),
  Config;
end_per_group(with_resource_server_a_with_oauth_provider_idp1, Config) ->
  remove_attribute_from_entry_from_env_variable(rabbitmq_auth_backend_oauth2, resource_servers,
    ?config(a, Config), oauth_provider_id),
  Config;
end_per_group(with_mgt_resource_server_a_with_scopes_read_write, Config) ->
  remove_attribute_from_entry_from_env_variable(rabbitmq_management, oauth_resource_servers,
    ?config(a, Config), scopes),
  Config;
end_per_group(with_mgt_oauth_resource_server_a_with_oauth_provider_url_url1, Config) ->
  remove_attribute_from_entry_from_env_variable(rabbitmq_management, oauth_resource_servers,
    ?config(a, Config), oauth_provider_url),
  Config;
end_per_group(with_mgt_oauth_resource_server_a_with_oauth_metadata_url_url1, Config) ->
  remove_attribute_from_entry_from_env_variable(rabbitmq_management, oauth_resource_servers,
    ?config(a, Config), oauth_metadata_url),
  Config;
end_per_group(with_mgt_resource_server_a_with_oauth_initiated_logon_type_sp_initiated, Config) ->
  remove_attribute_from_entry_from_env_variable(rabbitmq_management, oauth_resource_servers,
    ?config(a, Config), oauth_initiated_logon_type),
  Config;
end_per_group(with_mgt_resource_server_a_with_oauth_initiated_logon_type_idp_initiated, Config) ->
  remove_attribute_from_entry_from_env_variable(rabbitmq_management, oauth_resource_servers,
    ?config(a, Config), oauth_initiated_logon_type),
  Config;
end_per_group(with_mgt_resource_server_a_with_client_id_x, Config) ->
  remove_attribute_from_entry_from_env_variable(rabbitmq_management, oauth_resource_servers,
    ?config(a, Config), oauth_client_id),
  Config;
end_per_group(with_default_oauth_provider_idp1, Config) ->
  unset_env(rabbitmq_auth_backend_oauth2, default_oauth_provider),
  Config;
end_per_group(with_default_oauth_provider_idp3, Config) ->
  unset_env(rabbitmq_auth_backend_oauth2, default_oauth_provider),
  Config;
end_per_group(with_root_end_session_endpoint_0, Config) ->
  unset_env(rabbitmq_auth_backend_oauth2, end_session_endpoint),
  Config;
end_per_group(with_end_session_endpoint_for_idp1_1, Config) ->
  remove_attribute_from_entry_from_env_variable(rabbitmq_auth_backend_oauth2, oauth_providers,
    ?config(idp1, Config), end_session_endpoint),
  Config;
end_per_group(with_end_session_endpoint_for_idp2_2, Config) ->
  remove_attribute_from_entry_from_env_variable(rabbitmq_auth_backend_oauth2, oauth_providers,
    ?config(idp2, Config), end_session_endpoint),
  Config;
end_per_group(with_oauth_provider_idp2_for_resource_server_a, Config) ->
  remove_attribute_from_entry_from_env_variable(rabbitmq_auth_backend_oauth2, resource_servers,
    ?config(a, Config), oauth_provider_id),
  Config;
end_per_group(with_authorization_endpoint_params_0, Config) ->
  unset_env(rabbitmq_management, oauth_authorization_endpoint_params),
  Config;
end_per_group(with_token_endpoint_params_0, Config) ->
  unset_env(rabbitmq_management, oauth_token_endpoint_params),
  Config;
end_per_group(with_mgt_resource_server_a_with_authorization_endpoint_params_1, Config) ->
  remove_attribute_from_entry_from_env_variable(rabbitmq_management, oauth_resource_servers,
    ?config(a, Config), oauth_authorization_endpoint_params),
  Config;
end_per_group(with_mgt_resource_server_a_with_token_endpoint_params_1, Config) ->
  remove_attribute_from_entry_from_env_variable(rabbitmq_management, oauth_resource_servers,
    ?config(a, Config), oauth_token_endpoint_params),
  Config;


end_per_group(_, Config) ->
  Config.


%% -------------------------------------------------------------------
%% Test cases.
%% -------------------------------------------------------------------
should_not_return_oauth_client_secret(_Config) ->
  Actual = authSettings(),
  ?assertEqual(false, proplists:is_defined(oauth_client_secret, Actual)).
should_return_oauth_client_secret_q(Config) ->
  Actual = authSettings(),
  ?assertEqual(?config(q, Config), proplists:get_value(oauth_client_secret, Actual)).
should_return_oauth_resource_server_a_with_client_id_x(Config) ->
  assertEqual_on_attribute_for_oauth_resource_server(authSettings(),
    Config, a, oauth_client_id, x).
should_return_oauth_resource_server_a_with_client_secret_w(Config) ->
  assertEqual_on_attribute_for_oauth_resource_server(authSettings(),
    Config, a, oauth_client_secret, w).
should_not_return_oauth_resource_server_a_with_client_secret(Config) ->
  assert_attribute_not_defined_for_oauth_resource_server(authSettings(),
    Config, a, oauth_client_secret).

should_return_mgt_oauth_provider_url_idp1_url(Config) ->
  assertEqual_on_attribute_for_oauth_resource_server(authSettings(),
    Config, rabbit, oauth_provider_url, idp1_url).

should_return_mgt_oauth_matadata_url_idp1_url(Config) ->
  assertEqual_on_attribute_for_oauth_resource_server(authSettings(),
    Config, rabbit, oauth_metadata_url, meta_idp1_url).

should_return_mgt_oauth_provider_url_url1(Config) ->
  assertEqual_on_attribute_for_oauth_resource_server(authSettings(),
    Config, rabbit, oauth_provider_url, url1).

should_return_mgt_oauth_metadata_url_url1(Config) ->
  assertEqual_on_attribute_for_oauth_resource_server(authSettings(),
    Config, rabbit, oauth_metadata_url, meta_url1).

should_return_mgt_oauth_metadata_url_url0(Config) ->
  assertEqual_on_attribute_for_oauth_resource_server(authSettings(),
    Config, rabbit, oauth_metadata_url, meta_url0).

should_return_mgt_oauth_provider_url_url0(Config) ->
  assertEqual_on_attribute_for_oauth_resource_server(authSettings(),
    Config, rabbit, oauth_provider_url, url0).

should_return_oauth_scopes_admin_mgt(Config) ->
  Actual = authSettings(),
  ?assertEqual(?config(admin_mgt, Config), proplists:get_value(oauth_scopes, Actual)).

should_return_mgt_oauth_resource_server_a_with_scopes_read_write(Config) ->
  assertEqual_on_attribute_for_oauth_resource_server(authSettings(),
    Config, a, scopes, read_write).

should_return_disabled_auth_settings(_Config) ->
  [{oauth_enabled, false}] = authSettings().

should_return_mgt_resource_server_a_oauth_provider_url_url0(Config) ->
  assertEqual_on_attribute_for_oauth_resource_server(authSettings(),
    Config, a, oauth_provider_url, url0).

should_return_mgt_oauth_resource_server_a_with_client_id_x(Config) ->
  assertEqual_on_attribute_for_oauth_resource_server(authSettings(),
    Config, a, oauth_client_id, x).

should_return_oauth_resource_server_a_with_oauth_provider_url_idp1_url(Config) ->
  assertEqual_on_attribute_for_oauth_resource_server(authSettings(),
    Config, a, oauth_provider_url, idp1_url).

should_return_oauth_resource_server_a_with_oauth_provider_url_url1(Config) ->
  assertEqual_on_attribute_for_oauth_resource_server(authSettings(),
    Config, a, oauth_provider_url, url1).

should_return_oauth_resource_server_a_with_oauth_metadata_url_url1(Config) ->
  assertEqual_on_attribute_for_oauth_resource_server(authSettings(),
    Config, a, oauth_metadata_url, meta_url1).

should_return_oauth_resource_server_a_with_oauth_provider_url_url0(Config) ->
  assertEqual_on_attribute_for_oauth_resource_server(authSettings(),
    Config, a, oauth_provider_url, url0).

should_return_oauth_resource_server_rabbit_with_oauth_provider_url_idp1_url(Config) ->
  assertEqual_on_attribute_for_oauth_resource_server(authSettings(),
    Config, rabbit, oauth_provider_url, idp1_url).

should_return_oauth_resource_server_rabbit_with_oauth_metadata_url_idp1_url(Config) ->
  assertEqual_on_attribute_for_oauth_resource_server(authSettings(),
    Config, rabbit, oauth_metadata_url, meta_idp1_url).

should_return_oauth_resource_server_rabbit_with_oauth_provider_url_url1(Config) ->
  assertEqual_on_attribute_for_oauth_resource_server(authSettings(),
    Config, rabbit, oauth_provider_url, url1).

should_return_oauth_resource_server_rabbit_with_oauth_metadata_url_url1(Config) ->
  assertEqual_on_attribute_for_oauth_resource_server(authSettings(),
    Config, rabbit, oauth_metadata_url, meta_url1 ).

should_return_oauth_resource_server_rabbit_with_oauth_provider_url_url0(Config) ->
  assertEqual_on_attribute_for_oauth_resource_server(authSettings(),
    Config, rabbit, oauth_provider_url, url0).

should_return_oauth_resource_server_rabbit_with_oauth_metadata_url_url0(Config) ->
  assertEqual_on_attribute_for_oauth_resource_server(authSettings(),
    Config, rabbit, oauth_metadata_url, meta_url0).

should_not_return_oauth_initiated_logon_type(_Config) ->
  Actual = authSettings(),
  ?assertEqual(false, proplists:is_defined(oauth_initiated_logon_type, Actual)).
should_return_oauth_initiated_logon_type_idp_initiated(_Config) ->
  Actual = authSettings(),
  ?assertEqual(<<"idp_initiated">>, proplists:get_value(oauth_initiated_logon_type, Actual)).

should_not_return_oauth_resource_server_a(Config) ->
  Actual = authSettings(),
  assert_not_defined_oauth_resource_server(Actual, Config, a).

should_not_return_oauth_resource_server_a_with_oauth_initiated_logon_type(Config) ->
  assert_attribute_not_defined_for_oauth_resource_server(authSettings(),
    Config, a, oauth_initiated_logon_type).

should_return_oauth_resource_server_a_with_oauth_initiated_logon_type_idp_initiated(Config) ->
  assertEqual_on_attribute_for_oauth_resource_server(authSettings(),
    Config, a, oauth_initiated_logon_type, <<"idp_initiated">>).
should_return_oauth_resource_server_a_with_oauth_initiated_logon_type_sp_initiated(Config) ->
  assertEqual_on_attribute_for_oauth_resource_server(authSettings(),
    Config, a, oauth_initiated_logon_type, <<"sp_initiated">>).

should_not_return_oauth_scopes(_Config) ->
  Actual = authSettings(),
  ?assertEqual(false, proplists:is_defined(scopes, Actual)).

should_return_oauth_enabled(_Config) ->
  Actual = authSettings(),
  ?assertEqual(true, proplists:get_value(oauth_enabled, Actual)).


should_return_oauth_idp_initiated_logon(_Config) ->
  Actual = authSettings(),
  ?assertEqual(<<"idp_initiated">>, proplists:get_value(oauth_initiated_logon_type, Actual)).

should_return_oauth_disable_basic_auth_true(_Config) ->
  Actual = authSettings(),
  ?assertEqual(true, proplists:get_value(oauth_disable_basic_auth, Actual)).

should_return_oauth_disable_basic_auth_false(_Config) ->
  Actual = authSettings(),
  ?assertEqual(false, proplists:get_value(oauth_disable_basic_auth, Actual)).

should_return_oauth_client_id_z(Config) ->
  Actual = authSettings(),
  ?assertEqual(?config(z, Config), proplists:get_value(oauth_client_id, Actual)).

should_not_return_end_session_endpoint(Config) ->
  assert_attribute_not_defined_for_oauth_resource_server(authSettings(),
    Config, rabbit, end_session_endpoint).

should_return_end_session_endpoint_0(Config) ->
  assertEqual_on_attribute_for_oauth_resource_server(authSettings(),
    Config, rabbit, end_session_endpoint, ?config(logout_url_0, Config)).

should_return_end_session_endpoint_1(Config) ->
  assertEqual_on_attribute_for_oauth_resource_server(authSettings(),
    Config, rabbit, end_session_endpoint, ?config(logout_url_1, Config)).

should_return_oauth_resource_server_a_without_end_session_endpoint(Config) ->
  assert_attribute_not_defined_for_oauth_resource_server(authSettings(),
    Config, a, end_session_endpoint).

should_return_oauth_resource_server_a_with_end_session_endpoint_0(Config) ->
  assertEqual_on_attribute_for_oauth_resource_server(authSettings(),
    Config, a, end_session_endpoint, ?config(logout_url_0, Config)).

should_return_oauth_resource_server_a_with_end_session_endpoint_1(Config) ->
  assertEqual_on_attribute_for_oauth_resource_server(authSettings(),
    Config, a, end_session_endpoint, ?config(logout_url_1, Config)).

should_return_oauth_resource_server_a_with_end_session_endpoint_2(Config) ->
  assertEqual_on_attribute_for_oauth_resource_server(authSettings(),
    Config, a, end_session_endpoint, ?config(logout_url_2, Config)).

should_return_mgt_oauth_resource_rabbit_without_authorization_endpoint_params(Config) ->
  assert_attribute_not_defined_for_oauth_resource_server(authSettings(),
    Config, rabbit, oauth_authorization_endpoint_params).

should_return_mgt_oauth_resource_rabbit_without_token_endpoint_params(Config) ->
  assert_attribute_not_defined_for_oauth_resource_server(authSettings(),
    Config, rabbit, oauth_token_endpoint_params).

should_return_mgt_oauth_resource_rabbit_with_authorization_endpoint_params_0(Config) ->
  assertEqual_on_attribute_for_oauth_resource_server(authSettings(),
    Config, rabbit, oauth_authorization_endpoint_params, authorization_params_0).

should_return_mgt_oauth_resource_rabbit_with_token_endpoint_params_0(Config) ->
  assertEqual_on_attribute_for_oauth_resource_server(authSettings(),
    Config, rabbit, oauth_token_endpoint_params, token_params_0).

should_return_mgt_oauth_resource_a_with_authorization_endpoint_params_1(Config) ->
  assertEqual_on_attribute_for_oauth_resource_server(authSettings(),
    Config, a, oauth_authorization_endpoint_params, authorization_params_1).

should_return_mgt_oauth_resource_a_with_token_endpoint_params_1(Config) ->
  assertEqual_on_attribute_for_oauth_resource_server(authSettings(),
    Config, a, oauth_token_endpoint_params, token_params_1).

%% -------------------------------------------------------------------
%% Utility/helper functions
%% -------------------------------------------------------------------

delete_key_with_empty_proplist(Key, Map) ->
  case maps:get(Key, Map) of
    [] -> maps:remove(Key, Map);
    _ -> Map
  end.
remove_entry_from_env_variable(Application, EnvVar, Key) ->
  Map = application:get_env(Application, EnvVar, #{}),
  NewMap = maps:remove(Key, Map),
  case maps:size(NewMap) of
    0 -> unset_env(Application, EnvVar);
    _ -> set_env(Application, EnvVar, NewMap)
  end.
remove_attribute_from_entry_from_env_variable(Application, EnvVar, Key, Attribute) ->
  Map = application:get_env(Application, EnvVar, #{}),
  Proplist = proplists:delete(Attribute, maps:get(Key, Map, [])),
  NewMap = delete_key_with_empty_proplist(Key, maps:put(Key, Proplist, Map)),
  case maps:size(NewMap) of
    0 -> unset_env(Application, EnvVar);
    _ -> set_env(Application, EnvVar, NewMap)
  end.

assertEqual_on_attribute_for_oauth_resource_server(Actual, Config, ConfigKey, Attribute, ConfigValue) ->
  log(Actual),
  OAuthResourceServers =  proplists:get_value(oauth_resource_servers, Actual),
  OauthResource = maps:get(?config(ConfigKey, Config), OAuthResourceServers),
  Value = case ConfigValue of
    Binary when is_binary(Binary) -> Binary;
    _ -> ?config(ConfigValue, Config)
  end,
  ?assertEqual(Value, proplists:get_value(Attribute, OauthResource)).

assert_attribute_is_defined_for_oauth_resource_server(Actual, Config, ConfigKey, Attribute) ->
  log(Actual),
  OAuthResourceServers =  proplists:get_value(oauth_resource_servers, Actual),
  OauthResource = maps:get(?config(ConfigKey, Config), OAuthResourceServers),
  ?assertEqual(true, proplists:is_defined(Attribute, OauthResource)).

assert_attribute_not_defined_for_oauth_resource_server(Actual, Config, ConfigKey, Attribute) ->
  log(Actual),
  OAuthResourceServers =  proplists:get_value(oauth_resource_servers, Actual),
  OauthResource = maps:get(?config(ConfigKey, Config), OAuthResourceServers),
  ?assertEqual(false, proplists:is_defined(Attribute, OauthResource)).

assert_not_defined_oauth_resource_server(Actual, Config, ConfigKey) ->
  log(Actual),
  OAuthResourceServers =  proplists:get_value(oauth_resource_servers, Actual),
  ?assertEqual(false, maps:is_key(?config(ConfigKey, Config), OAuthResourceServers)).

set_attribute_in_entry_for_env_variable(Application, EnvVar, Key, Attribute, Value) ->
  Map = application:get_env(Application, EnvVar, #{}),
  ct:log("set_attribute_in_entry_for_env_variable before ~p", [Map]),
  Map1 = maps:put(Key, [ { Attribute, Value} | maps:get(Key, Map, []) ], Map),
  ct:log("set_attribute_in_entry_for_env_variable after ~p", [Map1]),
  set_env(Application, EnvVar, Map1).

log(AuthSettings) ->
  logEnvVars(),
  ct:log("authSettings: ~p ", [AuthSettings]).
logEnvVars() ->
  ct:log("rabbitmq_management: ~p ", [application:get_all_env(rabbitmq_management)]),
  ct:log("rabbitmq_auth_backend_oauth2: ~p ", [application:get_all_env(rabbitmq_auth_backend_oauth2)]).
