%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2024 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
%%

-module(rabbit_mgmt_wm_auth).

-export([init/2, to_json/2, content_types_provided/2, is_authorized/2]).
-export([variances/2]).
-export([authSettings/0]). %% for testing only

-include_lib("rabbitmq_management_agent/include/rabbit_mgmt_records.hrl").
-include_lib("oauth2_client/include/oauth2_client.hrl").

%%--------------------------------------------------------------------

init(Req, _State) ->
    {cowboy_rest, rabbit_mgmt_headers:set_common_permission_headers(Req, ?MODULE), #context{}}.

variances(Req, Context) ->
    {[<<"accept-encoding">>, <<"origin">>], Req, Context}.

content_types_provided(ReqData, Context) ->
    {rabbit_mgmt_util:responder_map(to_json), ReqData, Context}.

merge_property(Key, List, MapIn) ->
    case proplists:get_value(Key, List) of
        undefined -> MapIn;
        V0 -> MapIn#{Key => V0}
    end.

extract_oauth_provider_info_props_as_map(ManagementProps) ->
    lists:foldl(fun(K, Acc) ->
        merge_property(K, ManagementProps, Acc) end, #{}, 
        [oauth_provider_url,
         oauth_metadata_url,
         oauth_authorization_endpoint_params,
         oauth_token_endpoint_params]).

merge_oauth_provider_info(OAuthResourceServer, MgtResourceServer, 
        ManagementProps) ->
    OAuthProviderResult = 
        case proplists:get_value(oauth_provider_id, OAuthResourceServer) of
            undefined -> 
                oauth2_client:get_oauth_provider([issuer]);
            OauthProviderId -> 
                oauth2_client:get_oauth_provider(OauthProviderId, [issuer])
        end,
    OAuthProviderInfo0 = 
        case OAuthProviderResult of
            {ok, OAuthProvider} -> oauth_provider_to_map(OAuthProvider);
            {error, _} -> #{}
        end,
    OAuthProviderInfo1 = maps:merge(OAuthProviderInfo0,
        extract_oauth_provider_info_props_as_map(ManagementProps)),
    maps:merge(OAuthProviderInfo1, proplists:to_map(MgtResourceServer)).

oauth_provider_to_map(OAuthProvider) ->
    % only include issuer and end_session_endpoint for now. 
    % The other endpoints are resolved by oidc-client library
    Map0 = case OAuthProvider#oauth_provider.issuer of
        undefined -> 
            #{};
        Issuer -> 
            #{  
                oauth_provider_url => Issuer,
                oauth_metadata_url => 
                    OAuthProvider#oauth_provider.discovery_endpoint
            }
    end,
    case OAuthProvider#oauth_provider.end_session_endpoint of
        undefined -> Map0;
        V -> maps:put(end_session_endpoint, V, Map0)
    end.

skip_unknown_mgt_resource_servers(ManagementProps, OAuth2Resources) ->
    maps:filter(fun(Key, _Value) -> maps:is_key(Key, OAuth2Resources) end, 
        proplists:get_value(oauth_resource_servers, ManagementProps, #{})).
skip_disabled_mgt_resource_servers(MgtOauthResources) ->
    maps:filter(fun(_Key, Value) -> 
        not proplists:get_value(disabled, Value, false) end, 
        MgtOauthResources).

extract_oauth2_and_mgt_resources(OAuth2BackendProps, ManagementProps) ->
    OAuth2Resources = getAllDeclaredOauth2Resources(OAuth2BackendProps),
    MgtResources0 = skip_unknown_mgt_resource_servers(ManagementProps,
        OAuth2Resources),
    MgtResources1 = maps:merge(maps:filtermap(fun(K,_V) ->
        case maps:is_key(K, MgtResources0) of
            true -> false;
            false -> {true, [{id, K}]}
        end end, OAuth2Resources), MgtResources0),
    MgtResources = maps:map(
        fun(K,V) -> merge_oauth_provider_info(
            maps:get(K, OAuth2Resources, #{}), V, ManagementProps) end,
        skip_disabled_mgt_resource_servers(MgtResources1)),
    case maps:size(MgtResources) of
        0 -> {};
        _ -> {MgtResources}
    end.

getAllDeclaredOauth2Resources(OAuth2BackendProps) ->
    OAuth2Resources = proplists:get_value(resource_servers, OAuth2BackendProps, 
        #{}),
    case proplists:get_value(resource_server_id, OAuth2BackendProps) of
        undefined -> 
            OAuth2Resources;
        Id -> 
            maps:put(Id, buildRootResourceServerIfAny(Id, OAuth2BackendProps),
                OAuth2Resources)
    end.
buildRootResourceServerIfAny(Id, Props) ->
    [ 
        {id, Id},
        {oauth_provider_id, proplists:get_value(oauth_provider_id, Props)}       
    ].

authSettings() ->
    ManagementProps = application:get_all_env(rabbitmq_management),
    OAuth2BackendProps = application:get_all_env(rabbitmq_auth_backend_oauth2),
    EnableOAUTH = proplists:get_value(oauth_enabled, ManagementProps, false),
    case EnableOAUTH of
        false -> [{oauth_enabled, false}];
        true ->
            case extract_oauth2_and_mgt_resources(OAuth2BackendProps,
                    ManagementProps) of
                {MgtResources} -> 
                    produce_auth_settings(MgtResources, ManagementProps);
                {} -> 
                    [{oauth_enabled, false}]
            end
  end.

% invalid -> those resources that dont have an oauth_client_id and 
%            their login_type is sp_initiated
skip_invalid_mgt_resource_servers(MgtResourceServers, ManagementProps) ->
    DefaultOauthInitiatedLogonType = proplists:get_value(
        oauth_initiated_logon_type, ManagementProps, sp_initiated),
    maps:filter(fun(_K,ResourceServer) ->
        SpInitiated = 
            case maps:get(oauth_initiated_logon_type, ResourceServer, 
                    DefaultOauthInitiatedLogonType) of
                sp_initiated -> true;
                _ -> false
            end,
        not SpInitiated or not is_invalid([maps:get(oauth_client_id, 
            ResourceServer, undefined)]) 
        end, MgtResourceServers).

% filter -> include only those resources with an oauth_client_id 
%           or those whose logon type is not sp_initiated 
filter_out_invalid_mgt_resource_servers(MgtResourceServers, ManagementProps) ->
    case is_invalid([proplists:get_value(oauth_client_id, ManagementProps)]) of
        true -> 
            skip_invalid_mgt_resource_servers(MgtResourceServers, 
                ManagementProps);
        false -> 
            MgtResourceServers
    end.

filter_mgt_resource_servers_without_oauth_provider_url(MgtResourceServers) ->
    maps:filter(fun(_K1,V1) -> maps:is_key(oauth_provider_url, V1) end, 
        MgtResourceServers).

ensure_oauth_resource_server_properties_are_binaries(Key, Value) ->
    case Key of
        oauth_authorization_endpoint_params -> Value;
        oauth_token_endpoint_params -> Value;
        _ -> to_binary(Value)
    end.

produce_auth_settings(MgtResourceServers, ManagementProps) ->
    ConvertValuesToBinary = fun(_K,V) -> 
        [
            {K1, ensure_oauth_resource_server_properties_are_binaries(K1, V1)} 
            || {K1,V1} <- maps:to_list(V) 
        ] end,
    FilteredMgtResourceServers = 
        filter_mgt_resource_servers_without_oauth_provider_url(
            filter_out_invalid_mgt_resource_servers(MgtResourceServers, 
                ManagementProps)),

    case maps:size(FilteredMgtResourceServers) of
        0 -> 
            [{oauth_enabled, false}];
        _ ->
            filter_empty_properties([
                {oauth_enabled, true},
                {oauth_resource_servers, 
                    maps:map(ConvertValuesToBinary, FilteredMgtResourceServers)},
                to_tuple(oauth_disable_basic_auth, ManagementProps, 
                    fun to_binary/1, true),
                to_tuple(oauth_client_id, ManagementProps),
                to_tuple(oauth_client_secret, ManagementProps),
                to_tuple(oauth_scopes, ManagementProps),
                case proplists:get_value(oauth_initiated_logon_type, 
                    ManagementProps, sp_initiated) of
                    sp_initiated -> 
                        {};
                    idp_initiated -> 
                        {oauth_initiated_logon_type, <<"idp_initiated">>}
                end,
                to_tuple(oauth_authorization_endpoint_params, ManagementProps, 
                    undefined, undefined),
                to_tuple(oauth_token_endpoint_params, ManagementProps, 
                    undefined, undefined)
            ])
  end.

filter_empty_properties(ListOfProperties) ->
    lists:filter(fun(Prop) ->
            case Prop of
                {} -> false;
                _ -> true
            end
        end, ListOfProperties).

to_binary(Value) when is_boolean(Value)-> Value;
to_binary(Value) -> rabbit_data_coercion:to_binary(Value).

to_json(ReqData, Context) ->
   rabbit_mgmt_util:reply(authSettings(), ReqData, Context).

is_authorized(ReqData, Context) ->
    {true, ReqData, Context}.

is_invalid(List) ->
    lists:any(fun(V) -> case V of
      "" -> true;
      undefined -> true;
      {error, _} -> true;
      _ -> false
    end end, List).

to_tuple(Key, Proplist) ->
    to_tuple(Key, Proplist, fun to_binary/1, undefined).

to_tuple(Key, Proplist, ConvertFun, DefaultValue) ->
    case proplists:is_defined(Key, Proplist) of
        true ->
            {Key, case ConvertFun of
                    undefined -> proplists:get_value(Key, Proplist);
                    _ -> ConvertFun(proplists:get_value(Key, Proplist))
                end
            };
        false ->
            case DefaultValue of
                undefined -> {};
                _ -> {Key, proplists:get_value(Key, Proplist, DefaultValue)}
            end
    end.
