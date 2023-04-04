%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2023 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(rabbit_mgmt_wm_auth).

-export([init/2, to_json/2, content_types_provided/2, is_authorized/2, authSettings/0]).
-export([variances/2]).

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
  EnableUAA = application:get_env(rabbitmq_management, enable_uaa, false),
  EnableOAUTH = application:get_env(rabbitmq_management, oauth_enabled, false),
  case EnableOAUTH of
    true ->
      OAuthInitiatedLogonType = application:get_env(rabbitmq_management, oauth_initiated_logon_type, sp_initiated),
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
                        [{oauth_enabled, false}, {oauth_client_id, <<>>}, {oauth_provider_url, <<>>}];
                    false ->
                        append_oauth_optional_secret([
                         {oauth_enabled, true},
                         {enable_uaa, rabbit_data_coercion:to_binary(EnableUAA)},
                         {oauth_client_id, rabbit_data_coercion:to_binary(OAuthClientId)},
                         {oauth_provider_url, rabbit_data_coercion:to_binary(OAuthProviderUrl)},
                         {oauth_scopes, rabbit_data_coercion:to_binary(OAuthScopes)},
                         {oauth_metadata_url, rabbit_data_coercion:to_binary(OAuthMetadataUrl)},
                         {oauth_resource_id, rabbit_data_coercion:to_binary(OAuthResourceId)}
                         ], OAuthClientSecret)
                end
          end;
        idp_initiated ->
           [{oauth_enabled, true},
            {oauth_initiated_logon_type, rabbit_data_coercion:to_binary(OAuthInitiatedLogonType)},
            {oauth_provider_url, rabbit_data_coercion:to_binary(OAuthProviderUrl)}
            ]
        end;
     false ->
        [{oauth_enabled, false}]
  end.

to_json(ReqData, Context) ->
   rabbit_mgmt_util:reply(authSettings(), ReqData, Context).

is_authorized(ReqData, Context) ->
    {true, ReqData, Context}.

is_invalid(List) ->
    lists:any(fun(V) -> V == "" end, List).

append_oauth_optional_secret(List, OAuthClientSecret) when OAuthClientSecret == "" ->
    List;
append_oauth_optional_secret(List, OAuthClientSecret) ->
    lists:append(List, [{oauth_client_secret, rabbit_data_coercion:to_binary(OAuthClientSecret)}]).
