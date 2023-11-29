%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2023 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries.  All rights reserved.
%%

-module(rabbit_mgmt_wm_auth).

-export([init/2, to_json/2, content_types_provided/2, is_authorized/2]).
-export([variances/2]).
-export([authSettings/0]).

-include_lib("rabbitmq_management_agent/include/rabbit_mgmt_records.hrl").
-include_lib("rabbit_common/include/rabbit.hrl").

%%--------------------------------------------------------------------

init(Req, _State) ->
    {cowboy_rest, rabbit_mgmt_headers:set_common_permission_headers(Req, ?MODULE), #context{}}.

variances(Req, Context) ->
    {[<<"accept-encoding">>, <<"origin">>], Req, Context}.

content_types_provided(ReqData, Context) ->
   {rabbit_mgmt_util:responder_map(to_json), ReqData, Context}.

authSettings() ->
  EnableOAUTH = application:get_env(rabbitmq_management, oauth_enabled, false),
  case EnableOAUTH of
    true ->
      OAuthResourceServers = application:get_env(rabbitmq_management, oauth_resource_servers, #{}),
      case OAuthResourceServers of
        [] -> single_resource_auth_settings();
        _ -> multi_resource_auth_settings(OAuthResourceServers)
      end;
    false ->
      [{oauth_enabled, false}]
  end.
multi_resource_auth_settings(OAuthResourceServers) ->
  MapFun = fun(_K,V) ->
     [ {K1, rabbit_data_coercion:to_binary(V1)} || {K1,V1} <- V ]
  end,

  Settings = [ {oauth_enabled, true},
    {oauth_resource_servers, maps:map(MapFun, OAuthResourceServers)},
    oauth_tuple(oauth_initiated_logon_type, application:get_env(rabbitmq_management, oauth_initiated_logon_type, "")),
    oauth_tuple(oauth_disable_basic_auth, application:get_env(rabbitmq_management, oauth_disable_basic_auth, true)),
    oauth_tuple(oauth_client_id, application:get_env(rabbitmq_management, oauth_client_id, "")),
    oauth_tuple(oauth_client_secret, application:get_env(rabbitmq_management, oauth_client_secret, "")),
    oauth_tuple(oauth_metadata_url, application:get_env(rabbitmq_management, oauth_metadata_url, "")),
    oauth_tuple(oauth_scopes, application:get_env(rabbitmq_management, oauth_scopes, "")),
    oauth_tuple(oauth_provider_url, application:get_env(rabbitmq_management, oauth_provider_url, "")),
    oauth_tuple(oauth_resource_id, application:get_env(rabbitmq_auth_backend_oauth2, resource_server_id, ""))
  ],
  [ {K,V} || {K,V} <- Settings ].

single_resource_auth_settings() ->
  OAuthInitiatedLogonType = application:get_env(rabbitmq_management, oauth_initiated_logon_type, sp_initiated),
  OAuthDisableBasicAuth = application:get_env(rabbitmq_management, oauth_disable_basic_auth, true),
  OAuthProviderUrl = application:get_env(rabbitmq_management, oauth_provider_url, ""),
  case OAuthInitiatedLogonType of
    sp_initiated ->
      OAuthClientId = application:get_env(rabbitmq_management, oauth_client_id, ""),
      OAuthClientSecret = application:get_env(rabbitmq_management, oauth_client_secret, ""),
      OAuthMetadataUrl = application:get_env(rabbitmq_management, oauth_metadata_url, ""),
      OAuthScopes = application:get_env(rabbitmq_management, oauth_scopes, ""),
      OAuthResourceId = application:get_env(rabbitmq_auth_backend_oauth2, resource_server_id, ""),
      case is_invalid([OAuthResourceId]) of
        true ->
           [{oauth_enabled, false}];
        false ->
            case is_invalid([OAuthClientId, OAuthProviderUrl]) of
                true ->
                    [{oauth_enabled, false}, {oauth_disable_basic_auth, OAuthDisableBasicAuth}, {oauth_client_id, <<>>}, {oauth_provider_url, <<>>}];
                false ->
                    append_oauth_optional([
                     {oauth_enabled, true},
                     {oauth_disable_basic_auth, OAuthDisableBasicAuth},
                     {oauth_client_id, rabbit_data_coercion:to_binary(OAuthClientId)},
                     {oauth_provider_url, rabbit_data_coercion:to_binary(OAuthProviderUrl)},
                     {oauth_scopes, rabbit_data_coercion:to_binary(OAuthScopes)},
                     {oauth_metadata_url, rabbit_data_coercion:to_binary(OAuthMetadataUrl)},
                     {oauth_resource_id, rabbit_data_coercion:to_binary(OAuthResourceId)}
                     ], oauth_client_secret, OAuthClientSecret)
            end
      end;
    idp_initiated ->
       [{oauth_enabled, true},
        {oauth_disable_basic_auth, OAuthDisableBasicAuth},
        {oauth_initiated_logon_type, rabbit_data_coercion:to_binary(OAuthInitiatedLogonType)},
        {oauth_provider_url, rabbit_data_coercion:to_binary(OAuthProviderUrl)}
        ]
    end.

to_json(ReqData, Context) ->
   rabbit_mgmt_util:reply(authSettings(), ReqData, Context).

is_authorized(ReqData, Context) ->
    {true, ReqData, Context}.

is_invalid(List) ->
    lists:any(fun(V) -> V == "" end, List).

append_oauth_optional(List, _Attribute, Value) when Value == "" ->
    List;
append_oauth_optional(List, Attribute, Value) ->
    lists:append(List, [{Attribute, rabbit_data_coercion:to_binary(Value)}]).

oauth_tuple(_Attribute, Value) when Value == "" -> {};
oauth_tuple(Attribute, Value) when is_atom(Value) -> {Attribute, Value};
oauth_tuple(Attribute, Value) -> {Attribute, rabbit_data_coercion:to_binary(Value)}.
