%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2021 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(rabbit_oauth2_scope).

-export([vhost_access/2, resource_access/3, topic_access/4, concat_scopes/2]).

-include_lib("rabbit_common/include/rabbit.hrl").

-type permission() :: read | write | configure.

%% API functions --------------------------------------------------------------
-spec vhost_access(binary(), [binary()]) -> boolean().
vhost_access(VHost, Scopes) ->
    PermissionScopes = get_scope_permissions(Scopes),
    lists:any(
        fun({VHostPattern, _, _, _}) ->
            wildcard:match(VHost, VHostPattern)
        end,
        PermissionScopes).

-spec resource_access(rabbit_types:r(atom()), permission(), [binary()]) -> boolean().
resource_access(#resource{virtual_host = VHost, name = Name},
                Permission, Scopes) ->
    lists:any(
        fun({VHostPattern, NamePattern, _, ScopeGrantedPermission}) ->
            wildcard:match(VHost, VHostPattern) andalso
            wildcard:match(Name, NamePattern) andalso
            Permission =:= ScopeGrantedPermission
        end,
        get_scope_permissions(Scopes)).

topic_access(#resource{virtual_host = VHost, name = ExchangeName},
             Permission,
             #{routing_key := RoutingKey},
             Scopes) ->
    lists:any(
        fun({VHostPattern, ExchangeNamePattern, RoutingKeyPattern, ScopeGrantedPermission}) ->
            is_binary(RoutingKeyPattern) andalso
            wildcard:match(VHost, VHostPattern) andalso
            wildcard:match(ExchangeName, ExchangeNamePattern) andalso
            wildcard:match(RoutingKey, RoutingKeyPattern) andalso
            Permission =:= ScopeGrantedPermission
        end,
        get_scope_permissions(Scopes)).

%% Internal -------------------------------------------------------------------

-spec get_scope_permissions([binary()]) -> [{rabbit_types:r(pattern), permission()}].
get_scope_permissions(Scopes) when is_list(Scopes) ->
    lists:filtermap(
        fun(ScopeEl) ->
            case parse_permission_pattern(ScopeEl) of
                ignore -> false;
                Perm   -> {true, Perm}
            end
        end,
        Scopes).

-spec concat_scopes([binary()], string()) -> string().
concat_scopes(Scopes, Separator) when is_list(Scopes) ->
    lists:concat(lists:join(Separator, lists:map(fun rabbit_data_coercion:to_list/1, Scopes))).

-spec parse_permission_pattern(binary()) -> {rabbit_types:r(pattern), permission()}.
parse_permission_pattern(<<"read:", ResourcePatternBin/binary>>) ->
    Permission = read,
    parse_resource_pattern(ResourcePatternBin, Permission);
parse_permission_pattern(<<"write:", ResourcePatternBin/binary>>) ->
    Permission = write,
    parse_resource_pattern(ResourcePatternBin, Permission);
parse_permission_pattern(<<"configure:", ResourcePatternBin/binary>>) ->
    Permission = configure,
    parse_resource_pattern(ResourcePatternBin, Permission);
parse_permission_pattern(_Other) ->
    ignore.

-spec parse_resource_pattern(binary(), permission()) ->
    {rabbit_types:vhost(), binary(), binary() | none, permission()}.
parse_resource_pattern(Pattern, Permission) ->
    case binary:split(Pattern, <<"/">>, [global]) of
        [VhostPattern, NamePattern] ->
            {VhostPattern, NamePattern, none, Permission};
        [VhostPattern, NamePattern, RoutingKeyPattern] ->
            {VhostPattern, NamePattern, RoutingKeyPattern, Permission};
        _Other -> ignore
    end.
