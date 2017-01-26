%% The contents of this file are subject to the Mozilla Public License
%% Version 1.1 (the "License"); you may not use this file except in
%% compliance with the License. You may obtain a copy of the License
%% at http://www.mozilla.org/MPL/
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

-export([vhost_access/2, resource_access/3, topic_access/4]).

-include_lib("rabbit_common/include/rabbit.hrl").

-type permission() :: read | write | configure.

%% API functions --------------------------------------------------------------
-spec vhost_access(binary(), [binary()]) -> boolean().
vhost_access(VHost, Scopes) ->
    lists:any(
        fun({ScopeVHost, _, _, _}) ->
            wildcard:match(VHost, ScopeVHost)
        end,
        get_scope_permissions(Scopes)).

-spec resource_access(rabbit_types:r(atom()), permission(), [binary()]) -> boolean().
resource_access(#resource{virtual_host = VHost, name = Name},
                Permission, Scopes) ->
    lists:any(
        fun({ScopeVHost, ScopeName, _, ScopePermission}) ->
            wildcard:match(VHost, ScopeVHost) andalso
            wildcard:match(Name, ScopeName) andalso
            Permission =:= ScopePermission
        end,
        get_scope_permissions(Scopes)).

topic_access(#resource{virtual_host = VHost, name = ExchangeName},
             Permission,
             #{routing_key := RoutingKey},
             Scopes) ->
    lists:any(
        fun({ScopeVHost, ScopeExchangeName, ScopeRoutingKey, ScopePermission}) ->
            is_binary(ScopeRoutingKey) andalso
            wildcard:match(VHost, ScopeVHost) andalso
            wildcard:match(ExchangeName, ScopeExchangeName) andalso
            wildcard:match(RoutingKey, ScopeRoutingKey) andalso
            Permission =:= ScopePermission
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


