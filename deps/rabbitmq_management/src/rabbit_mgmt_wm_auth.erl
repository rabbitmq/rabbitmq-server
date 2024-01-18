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
resolve_oauth_provider_url(ManagementProps) ->
  case proplists:get_value(oauth_provider_url, ManagementProps) of
    undefined ->
      case oauth2_client:get_oauth_provider([issuer]) of
        {ok, OAuthProvider} -> OAuthProvider#oauth_provider.issuer; %% 2.1 & 3
        {error, _} = Error -> Error
      end;
    Value -> Value %% 1
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
resolve_oauth_provider_url(OAuthResourceServer, MgtResourceServer, ManagementProps) ->
  case proplists:get_value(oauth_provider_url, MgtResourceServer) of
    undefined ->
      case proplists:get_value(oauth_provider_url, ManagementProps) of
        undefined ->
          OAuthProviderResult = case proplists:get_value(oauth_provider_id, OAuthResourceServer) of
            undefined -> oauth2_client:get_oauth_provider([issuer]);
            OauthProviderId -> oauth2_client:get_oauth_provider(OauthProviderId, [issuer])
          end,
          case OAuthProviderResult of
            {ok, OAuthProvider} -> OAuthProvider#oauth_provider.issuer;
            {error, _} = Error -> Error
          end;
        MgtUrl -> MgtUrl
      end;
    ResourceMgtUrl -> ResourceMgtUrl
  end.


skip_unknown_resource_servers(MgtOauthResources, OAuth2Resources) ->
  maps:filter(fun(Key, _Value) -> maps:is_key(Key, OAuth2Resources) end, MgtOauthResources).
skip_disabled_mgt_resource_servers(MgtOauthResources) ->
  maps:filter(fun(_Key, Value) -> not proplists:get_value(disabled, Value, false) end, MgtOauthResources).

has_multi_resources(OAuth2BackendProps, ManagementProps) ->
  OAuth2Resources = getAllDeclaredOauth2Resources(OAuth2BackendProps),
  MgtResources0 = skip_unknown_resource_servers(proplists:get_value(resource_servers, ManagementProps, #{}), OAuth2Resources),
  MgtResources1 = maps:merge(MgtResources0, maps:filtermap(fun(K,_V) ->
      case maps:is_key(K, MgtResources0) of
        true -> false;
        false -> {true, [{id, K}]}
      end end, OAuth2Resources)),
  MgtResources = skip_disabled_mgt_resource_servers(MgtResources1),

  ct:log("OAuth2Resources: ~p MgtResources: ~p", [OAuth2Resources, MgtResources]),
  case maps:size(MgtResources) of
    0 ->
      case maps:size(OAuth2Resources) of
        0 -> false;
        _ -> {true, OAuth2Resources, MgtResources}
      end;
    _ -> {true, OAuth2Resources, MgtResources}
  end.
getAllDeclaredOauth2Resources(OAuth2BackendProps) ->
  OAuth2Resources = proplists:get_value(resource_servers, OAuth2BackendProps, #{}),
  case proplists:get_value(resource_server_id, OAuth2BackendProps) of
    undefined -> OAuth2Resources;
    Id -> maps:put(Id, [{id, Id}], OAuth2Resources)
  end.

authSettings() ->
  ManagementProps = application:get_all_env(rabbitmq_management),
  OAuth2BackendProps = application:get_all_env(rabbitmq_auth_backend_oauth2),
  EnableOAUTH = proplists:get_value(oauth_enabled, ManagementProps, false),
  case EnableOAUTH of
    false -> [{oauth_enabled, false}];
    true ->
      case has_multi_resources(OAuth2BackendProps, ManagementProps) of
        {true, OAuth2Resources, MgtResources} ->
          multi_resource_auth_settings(OAuth2Resources, MgtResources, ManagementProps);
        false -> single_resource_auth_settings(OAuth2BackendProps, ManagementProps)
      end
  end.

skip_resource_servers_without_oauth_client_id(MgtResourceServers) ->
  maps:filter(fun(_K,ResourceServer) ->
    not is_invalid([proplists:get_value(oauth_client_id, ResourceServer)]) end, MgtResourceServers).


filter_resource_servers_without_resolvable_oauth_client_id(MgtResourceServers, ManagementProps) ->
  case is_invalid([proplists:get_value(oauth_client_id, ManagementProps)]) of
    true -> skip_resource_servers_without_oauth_client_id(MgtResourceServers);
    false -> MgtResourceServers
  end.

filter_resource_servers_without_resolvable_oauth_provider_url(OAuthResourceServers, MgtResourceServers, ManagementProps) ->
  maps:filter(fun(_K1,V1) -> proplists:is_defined(oauth_provider_url, V1) end, maps:map(fun(K,V) ->
    case proplists:is_defined(oauth_provider_url, V)  of
      true -> V;
      false ->
        case maps:get(K, OAuthResourceServers) of
          {badkey, _} -> V;
          OAuthResourceServer ->
            case resolve_oauth_provider_url(OAuthResourceServer, V, ManagementProps) of
              {error, _} -> V;
              Url -> [ {oauth_provider_url, Url} | V ]
            end
        end
    end end , MgtResourceServers)).

multi_resource_auth_settings(OAuthResourceServers, MgtResourceServers, ManagementProps) ->
  ct:log("multi_resource_auth_settings: ~p ~p", [OAuthResourceServers, MgtResourceServers]),
  ConvertValuesToBinary = fun(_K,V) -> [ {K1, to_binary(V1)} || {K1,V1} <- V ] end,
  FilteredMgtResourceServers = filter_resource_servers_without_resolvable_oauth_provider_url(OAuthResourceServers,
    filter_resource_servers_without_resolvable_oauth_client_id(MgtResourceServers, ManagementProps), ManagementProps),

  case maps:size(FilteredMgtResourceServers) of
    0 -> [{oauth_enabled, false}];
    _ ->
       filter_empty_properties([
        {oauth_enabled, true},
        {oauth_resource_servers, maps:map(ConvertValuesToBinary, FilteredMgtResourceServers)},
        to_tuple(oauth_disable_basic_auth, ManagementProps, true),
        to_tuple(oauth_client_id, ManagementProps),
        to_tuple(oauth_client_secret, ManagementProps),
        to_tuple(oauth_scopes, ManagementProps),
        to_tuple(oauth_initiated_logon_type, ManagementProps)
        ])
  end.



single_resource_auth_settings(OAuth2BackendProps, ManagementProps) ->
  OAuthInitiatedLogonType = proplists:get_value(oauth_initiated_logon_type, ManagementProps, sp_initiated),
  OAuthDisableBasicAuth = proplists:get_value(oauth_disable_basic_auth, ManagementProps, true),
  OAuthProviderUrl = resolve_oauth_provider_url(ManagementProps),
  OAuthResourceId =  proplists:get_value(resource_server_id, OAuth2BackendProps),
  case OAuthInitiatedLogonType of
    sp_initiated ->
      OAuthClientId = proplists:get_value(oauth_client_id, ManagementProps),
      case is_invalid([OAuthResourceId]) of
        true ->
          rabbit_log:error("Invalid rabbitmq_auth_backend_oauth2.resource_server_id ~p", [OAuthResourceId]),
          [{oauth_enabled, false}];
        false ->
          case is_invalid([OAuthClientId, OAuthProviderUrl]) of
            true ->
              ct:log("Invalid rabbitmq_management oauth_client_id ~p or resolved oauth_provider_url ~p",
                [OAuthClientId, OAuthProviderUrl]),
              [{oauth_enabled, false}];
            false ->
              filter_empty_properties([
               {oauth_enabled, true},
               {oauth_disable_basic_auth, OAuthDisableBasicAuth},
               {oauth_client_id, to_binary(OAuthClientId)},
               {oauth_provider_url, to_binary(OAuthProviderUrl)},
               to_tuple(oauth_scopes, ManagementProps),
               to_tuple(oauth_metadata_url, ManagementProps),
               {oauth_resource_id, to_binary(OAuthResourceId)},
               to_tuple(oauth_client_secret, ManagementProps)
              ])
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
              {oauth_initiated_logon_type, to_binary(OAuthInitiatedLogonType)},
              {oauth_provider_url, to_binary(OAuthProviderUrl)},
              {oauth_resource_id, to_binary(OAuthResourceId)}
              ]
            end
        end
    end.

filter_empty_properties(ListOfProperties) ->
  lists:filter(fun(Prop) ->
      case Prop of
        {} -> false;
        _ -> true
      end
    end, ListOfProperties).

to_binary(Value) -> rabbit_data_coercion:to_binary(Value).

to_json(ReqData, Context) ->
   rabbit_mgmt_util:reply(authSettings(), ReqData, Context).

is_authorized(ReqData, Context) ->
    {true, ReqData, Context}.

is_invalid(Value) when Value == undefined -> true;
is_invalid(List) ->
    lists:any(fun(V) -> case V of
      "" -> true;
      undefined -> true;
      {error, _} -> true;
      _ -> false
    end end, List).

to_tuple(Key, Proplist) ->
  case proplists:is_defined(Key, Proplist) of
    true -> {Key, rabbit_data_coercion:to_binary(proplists:get_value(Key, Proplist))};
    false -> {}
  end.
to_tuple(Key, Proplist, DefaultValue) ->
  {Key, proplists:get_value(Key, Proplist, DefaultValue)}.
