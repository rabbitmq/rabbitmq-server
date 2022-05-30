%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2022 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(rabbit_mgmt_wm_auth).

-export([init/2, to_json/2, content_types_provided/2, is_authorized/2]).
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

to_json(ReqData, Context) ->
    EnableUAA = application:get_env(rabbitmq_management, enable_uaa, false),
    EnableOAUTH = application:get_env(rabbitmq_management, oauth_enable, false),
    Data = case EnableOAUTH of
               true ->
                   OAuthClientId = application:get_env(rabbitmq_management, oauth_client_id, ""),
                   OAuthClientSecret = application:get_env(rabbitmq_management, oauth_client_secret, ""),
                   OAuthProviderUrl = application:get_env(rabbitmq_management, oauth_provider_url, ""),
                   OAuthMetadataUrl = application:get_env(rabbitmq_management, oauth_metadata_url, ""),
                   OAuthScopes = application:get_env(rabbitmq_management, oauth_scopes, ""),
                   OAuthResourceId = application:get_env(rabbitmq_auth_backend_oauth2, resource_server_id, ""),
                   case is_invalid([OAuthClientId, OAuthClientSecret, OAuthResourceId, OAuthProviderUrl]) of
                       true ->
                           rabbit_log:warning("Disabling OAuth 2 authorization, relevant configuration settings are missing", []),
                           [{oauth_enable, false}, {oauth_client_id, <<>>}, {oauth_url, <<>>}];
                       false ->
                           [{oauth_enable, true},
                            {enable_uaa, rabbit_data_coercion:to_binary(EnableUAA)},
                            {oauth_client_id, rabbit_data_coercion:to_binary(OAuthClientId)},
                            {oauth_client_secret, rabbit_data_coercion:to_binary(OAuthClientSecret)},
                            {oauth_provider_url, rabbit_data_coercion:to_binary(OAuthProviderUrl)},
                            {oauth_scopes, rabbit_data_coercion:to_binary(OAuthScopes)},
                            {oauth_metadata_url, rabbit_data_coercion:to_binary(OAuthMetadataUrl)},
                            {oauth_resource_id, rabbit_data_coercion:to_binary(OAuthResourceId)}
                            ]
                   end;
               false ->
                   [{oauth_enable, false}]
           end,
    rabbit_mgmt_util:reply(Data, ReqData, Context).

is_authorized(ReqData, Context) ->
    {true, ReqData, Context}.

is_invalid(List) ->
    lists:any(fun(V) -> V == "" end, List).
