%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2024 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
%%

-module(rabbit_oauth2_keycloak).

-include("oauth2.hrl").

-export([extract_scopes_from_keycloak_format/1, has_keycloak_scopes/1]).
-import(uaa_jwt, [get_scope/1, set_scope/2]).

-define(AUTHORIZATION_CLAIM, <<"authorization">>).
-define(PERMISSIONS_CLAIM, <<"permissions">>).
-define(SCOPES_CLAIM, <<"scopes">>).

-spec has_keycloak_scopes(Payload::map()) -> boolean().
has_keycloak_scopes(Payload) ->
    maps:is_key(?AUTHORIZATION_CLAIM, Payload).

-spec extract_scopes_from_keycloak_format(Payload :: map()) -> map().
%% keycloak token format: https://github.com/rabbitmq/rabbitmq-auth-backend-oauth2/issues/36
extract_scopes_from_keycloak_format(#{?AUTHORIZATION_CLAIM := Authorization} = Payload) ->
    AdditionalScopes = extract_scopes_from_keycloak_permissions([],
            maps:get(?PERMISSIONS_CLAIM, Authorization, [])),
    set_scope(AdditionalScopes ++ get_scope(Payload), Payload).

extract_scopes_from_keycloak_permissions(Acc, []) ->
    Acc;
extract_scopes_from_keycloak_permissions(Acc, [H | T]) when is_map(H) ->
    Scopes = case maps:get(?SCOPES_CLAIM, H, []) of
        ScopesAsList when is_list(ScopesAsList) ->
            ScopesAsList;
        ScopesAsBinary when is_binary(ScopesAsBinary) ->
            [ScopesAsBinary]
    end,
    extract_scopes_from_keycloak_permissions(Acc ++ Scopes, T);
extract_scopes_from_keycloak_permissions(Acc, [_ | T]) ->
    extract_scopes_from_keycloak_permissions(Acc, T).
