%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2024 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
%%

-module(rabbit_mgmt_wm_auth).

-export([init/2, to_json/2, content_types_provided/2, is_authorized/2]).
-export([variances/2]).
-export([authSettings/0,resolve_oauth_provider_url/1,resolve_oauth_provider_url/3]). %% for testing only

-include_lib("rabbitmq_management_agent/include/rabbit_mgmt_records.hrl").
-include_lib("rabbit_common/include/rabbit.hrl").
-include_lib("oauth2_client/include/oauth2_client.hrl").

%%--------------------------------------------------------------------

init(Req, _State) ->
    {cowboy_rest, rabbit_mgmt_headers:set_common_permission_headers(Req, ?MODULE), #context{}}.

variances(Req, Context) ->
    {[<<"accept-encoding">>, <<"origin">>], Req, Context}.

content_types_provided(ReqData, Context) ->
   {rabbit_mgmt_util:responder_map(to_json), ReqData, Context}.


%% Order of precedence to determine the provider url for the single oauth resource
%% 1. rabbitmq_management.oauth_provider_url
%% 2. if auth_oauth2.default_provider_id is set
%%  2.1. auth_oauth2.auth_providers.?.issuer where ? is the auth_provider whose id attribute = auth_oauth2.default_provider_id
%% 3. auth_oauth2.issuer
%%
%% If there is only one auth_provider and the url where to forward users to log in is the same as the issuer url
%% then users are encouraged to just set auth_oauth2.issuer
resolve_oauth_provider_url(DefaultValue) ->
  case application:get_env(rabbitmq_management, oauth_provider_url) of
    undefined ->
      case oauth2_client:get_oauth_provider([issuer]) of
        {ok, OAuthProvider} -> OAuthProvider#oauth_provider.issuer; %% 2.1 & 3
        {error, _} -> DefaultValue
      end;
    {ok, Value} -> Value %% 1
  end.

%% Order of precedence to determine the provider url for one specific oauth resource
%% 1. rabbitmq_management.resource_servers.?.oauth_provider_url same as rabbitmq_management.oauth_provider_url but
%%     for a given resource_server
%% 2. rabbitmq_management.oauth_provider_url
%% 3. if auth_oauth2.resource_servers.?.oauth_provider_id is set where ? is the resource whose id matches rabbitmq_management.resource_servers.?.id
%%    3.1 if auth_oauth2.auth_providers.?.issuer is set where ? is the auth_provider whose id matches the auth_oauth2.resource_servers.?.oauth_provider_id
%%        then use this issuer url to login
%% 4. if auth_oauth2.default_provider_id is set
%%  4.1. auth_oauth2.auth_providers.?.issuer where ? is the auth_provider whose id attribute = auth_oauth2.default_provider_id
%% 5. auth_oauth2.issuer
resolve_oauth_provider_url(ResourceServerId, ResourceServers, DefaultValue) ->
  case maps:get(ResourceServerId, ResourceServers, undefined) of
    undefined -> resolve_oauth_provider_url(DefaultValue);
    ResourceServer ->
      case proplists:get_value(oauth_provider_url, ResourceServer) of
        undefined ->
          case application:get_env(rabbitmq_management, oauth_provider_url) of
            undefined ->
              case proplists:get_value(oauth_provider_id, ResourceServer) of
                  undefined ->
                    case oauth2_client:get_oauth_provider([issuer]) of
                      {ok, OAuthProvider} -> OAuthProvider#oauth_provider.issuer; %%  5
                      {error, _} -> DefaultValue
                    end;
                  OauthProviderId ->
                    case oauth2_client:get_oauth_provider(OauthProviderId, [issuer]) of
                      {ok, OAuthProvider2} -> OAuthProvider2#oauth_provider.issuer; %%  3.1
                      {error, _} ->
                        case oauth2_client:get_oauth_provider([issuer]) of
                          {ok, OAuthProvider} -> OAuthProvider#oauth_provider.issuer; %%  5
                          {error, _} -> DefaultValue
                        end
                    end
              end;
            MgtOauthProviderUrl ->  MgtOauthProviderUrl %% 2
          end;
        ResourceMgtOAuthProviderUrl -> ResourceMgtOAuthProviderUrl  % 1
      end
  end.

skip_unknown_resource_servers(MgtOauthResources) ->
  DeclaredResourceServers = application:get_env(rabbitmq_auth_backend_oauth2, resource_servers, #{}),
  maps:filter(fun(Key, _Value) -> maps:is_key(Key, DeclaredResourceServers) end, MgtOauthResources).

authSettings() ->
  EnableOAUTH = application:get_env(rabbitmq_management, oauth_enabled, false),
  case EnableOAUTH of
    true ->
      OAuthResourceServers = application:get_env(rabbitmq_management, oauth_resource_servers, undefined),
      case OAuthResourceServers of
        undefined -> single_resource_auth_settings();
        _ -> case skip_unknown_resource_servers(OAuthResourceServers) of
              Map when map_size(Map) == 0 ->
                rabbit_log:error("Empty or unknown set of rabbitmq_management oauth_resource_servers"),
                [{oauth_enabled, false}];
              Map -> multi_resource_auth_settings(Map)
            end
      end;
    false ->
      [{oauth_enabled, false}]
  end.
%% Ensure each resource has a client_id or there is a top level some_client_id
%% and it has a provider_url
is_valid_value_orelse_its_default(Value, Default) ->
  not is_invalid([Value]) or not is_invalid([Default]).

validate_oauth_resource_servers_settings(Settings) ->
  FilteredMap = maps:filter(fun(_K,V) ->
    OAuthClientId = proplists:get_value(oauth_client_id, V, ""),
    OAuthProviderUrl = proplists:get_value(oauth_provider_url, V, ""),
    is_valid_value_orelse_its_default(OAuthClientId, proplists:get_value(oauth_client_id, Settings))
      and
      is_valid_value_orelse_its_default(OAuthProviderUrl, proplists:get_value(oauth_provider_url, Settings))
    end, proplists:get_value(oauth_resource_servers, Settings)),
  map_size(FilteredMap) > 0.

multi_resource_auth_settings(OAuthResourceServers) ->
  ConvertValuesToBinary = fun(K,V) ->
     [ {K1, rabbit_data_coercion:to_binary(V1)} || {K1,V1} <- V, K1 =/= oauth_provider_url ] ++
     [ {K1, rabbit_data_coercion:to_binary(resolve_oauth_provider_url(K, OAuthResourceServers, ""))} || {K1,_V1} <- V, K1 =:= oauth_provider_url ]
  end,

  Settings = [ {oauth_enabled, true},
    {oauth_resource_servers, maps:map(ConvertValuesToBinary, OAuthResourceServers)},
    oauth_tuple(oauth_disable_basic_auth, application:get_env(rabbitmq_management, oauth_disable_basic_auth, true)),
    oauth_tuple(oauth_client_id, application:get_env(rabbitmq_management, oauth_client_id, "")),
    oauth_tuple(oauth_client_secret, application:get_env(rabbitmq_management, oauth_client_secret, "")),
    oauth_tuple(oauth_scopes, application:get_env(rabbitmq_management, oauth_scopes, "")),
    oauth_tuple(oauth_provider_url, resolve_oauth_provider_url(""))
  ],
  case validate_oauth_resource_servers_settings(Settings) of
    true -> [ {K,V} || {K,V} <- Settings ];
    false ->
      rabbit_log:error("management.oauth_resource_servers is invalid (check oauth_client_id and/or oauth_provider_url)"),
      [{oauth_enabled, false}]
  end.

single_resource_auth_settings() ->
  OAuthInitiatedLogonType = application:get_env(rabbitmq_management, oauth_initiated_logon_type, sp_initiated),
  OAuthDisableBasicAuth = application:get_env(rabbitmq_management, oauth_disable_basic_auth, true),
  OAuthProviderUrl = resolve_oauth_provider_url(""),
  OAuthResourceId = application:get_env(rabbitmq_auth_backend_oauth2, resource_server_id, ""),
  case OAuthInitiatedLogonType of
    sp_initiated ->
      OAuthClientId = application:get_env(rabbitmq_management, oauth_client_id, ""),
      OAuthClientSecret = application:get_env(rabbitmq_management, oauth_client_secret, ""),
      OAuthMetadataUrl = application:get_env(rabbitmq_management, oauth_metadata_url, ""),
      OAuthScopes = application:get_env(rabbitmq_management, oauth_scopes, ""),
      case is_invalid([OAuthResourceId]) of
        true ->
            rabbit_log:error("Invalid rabbitmq_auth_backend_oauth2.resource_server_id ~p", [OAuthResourceId]),
            [{oauth_enabled, false}];
        false ->
            case is_invalid([OAuthClientId, OAuthProviderUrl]) of
                true ->
                    rabbit_log:error("Invalid rabbitmq_management oauth_client_id ~p or resolved oauth_provider_url ~p",
                      [OAuthResourceId, OAuthProviderUrl]),
                    [{oauth_enabled, false}];
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
      case is_invalid([OAuthResourceId]) of
        true ->
            rabbit_log:error("Invalid rabbitmq_auth_backend_oauth2.resource_server_id ~p", [OAuthResourceId]),
            [{oauth_enabled, false}];
        false ->
            case is_invalid([OAuthProviderUrl]) of
              true ->
                rabbit_log:error("Invalid rabbitmq_management resolved oauth_provider_url ~p", [OAuthProviderUrl]),
                [{oauth_enabled, false}];
              false ->
               [{oauth_enabled, true},
                {oauth_disable_basic_auth, OAuthDisableBasicAuth},
                {oauth_initiated_logon_type, rabbit_data_coercion:to_binary(OAuthInitiatedLogonType)},
                {oauth_provider_url, rabbit_data_coercion:to_binary(OAuthProviderUrl)},
                {oauth_resource_id, rabbit_data_coercion:to_binary(OAuthResourceId)}
                ]
              end
        end
    end.

to_json(ReqData, Context) ->
   rabbit_mgmt_util:reply(authSettings(), ReqData, Context).

is_authorized(ReqData, Context) ->
    {true, ReqData, Context}.

is_invalid(List) ->
    lists:any(fun(V) -> case V of
      "" -> true;
      undefined -> true;
      _ -> false
    end end, List).

append_oauth_optional(List, _Attribute, Value) when Value == "" ->
    List;
append_oauth_optional(List, Attribute, Value) ->
    lists:append(List, [{Attribute, rabbit_data_coercion:to_binary(Value)}]).

oauth_tuple(_Attribute, Value) when Value == "" -> {};
oauth_tuple(Attribute, Value) when is_atom(Value) -> {Attribute, Value};
oauth_tuple(Attribute, Value) -> {Attribute, rabbit_data_coercion:to_binary(Value)}.
