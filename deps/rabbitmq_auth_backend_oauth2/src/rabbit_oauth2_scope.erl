%% The contents of this file are subject to the Mozilla Public License
%% Version 1.1 (the "License"); you may not use this file except in
%% compliance with the License. You may obtain a copy of the License
%% at https://www.mozilla.org/MPL/
%%
%% Software distributed under the License is distributed on an "AS IS"
%% basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See
%% the License for the specific language governing rights and
%% limitations under the License.
%%
%% The Original Code is RabbitMQ HTTP authentication.
%%
%% The Initial Developer of the Original Code is VMware, Inc.
%% Copyright (c) 2007-2016 Pivotal Software, Inc.  All rights reserved.
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
