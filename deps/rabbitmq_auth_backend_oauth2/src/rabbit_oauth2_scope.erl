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

-export([vhost_access/2, resource_access/3]).

-include_lib("rabbit_common/include/rabbit.hrl").

-type permission() :: read | write | configure.

%% API functions --------------------------------------------------------------
-spec vhost_access(binary(), [binary()]) -> boolean().
vhost_access(VHost, Scopes) ->
    lists:any(
        fun({#resource{virtual_host = ScopeVHost}, _}) ->
            wildcard:match(VHost, ScopeVHost)
        end,
        get_scope_permissions(Scopes)).

-spec resource_access(rabbit_types:r(atom()), permission(), [binary()]) -> boolean().
resource_access(#resource{virtual_host = VHost, name = Name},
                Permission, Scopes) ->
    lists:any(
        fun({#resource{virtual_host = ScopeVHost, name = ScopeName}, Perm}) ->
            wildcard:match(VHost, ScopeVHost) andalso
            wildcard:match(Name, ScopeName) andalso
            Permission =:= Perm
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

-spec parse_resource_pattern(binary(), permission()) -> {rabbit_types:r(pattern), permission()}.
parse_resource_pattern(Pattern, Permission) ->
    case binary:split(Pattern, <<"/">>) of
        [VhostPattern, NamePattern] ->
            {rabbit_misc:r(VhostPattern, pattern, NamePattern), Permission};
        _Other -> ignore
    end.


