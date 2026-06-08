%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2026 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
%%

-module(rabbit_oauth2_scope).

-include("oauth2.hrl").

-export([vhost_access/2,
        vhost_access/3,
        resource_access/3,
        resource_access/4,
        topic_access/4,
        topic_access/5,
        concat_scopes/2,
        filter_matching_scope_prefix/2,
        filter_matching_scope_prefix_and_drop_it/2]).

-include_lib("rabbit_common/include/rabbit.hrl").
-include_lib("kernel/include/logger.hrl").

-define(REGEX_MATCH_LIMIT, 10000).
-define(REGEX_MATCH_LIMIT_RECURSION, 1000).
-define(MAX_REGEX_PATTERN_BYTES, 2048).

-define(REJECTED_REGEX_CONSTRUCTS,
        [<<"(?i">>, <<"(?I">>,
         <<"(?m">>, <<"(?M">>,
         <<"(?s">>, <<"(?S">>,
         <<"(?x">>, <<"(?X">>,
         <<"(?J">>, <<"(?U">>, <<"(?n">>,
         <<"(?-">>, <<"(?C">>, <<"(?#">>,
         <<"(*">>]).

-type permission() :: read | write | configure.

%% API functions --------------------------------------------------------------
-spec vhost_access(binary(), [binary()]) -> boolean().
vhost_access(VHost, Scopes) ->
    vhost_access(VHost, Scopes, wildcard).

-spec vhost_access(binary(), [binary()], scope_pattern_syntax()) -> boolean().
vhost_access(VHost, Scopes, Syntax) ->
    PermissionScopes = get_scope_permissions(Scopes),
    lists:any(
        fun({VHostPattern, _, _, _}) ->
            pattern_match(Syntax, VHost, VHostPattern)
        end,
        PermissionScopes).

-spec resource_access(rabbit_types:r(atom()), permission(), [binary()]) -> boolean().
resource_access(Resource, Permission, Scopes) ->
    resource_access(Resource, Permission, Scopes, wildcard).

-spec resource_access(rabbit_types:r(atom()), permission(), [binary()],
        scope_pattern_syntax()) -> boolean().
resource_access(#resource{virtual_host = VHost, name = Name},
                Permission, Scopes, Syntax) ->
    lists:any(
        fun({VHostPattern, NamePattern, _, ScopeGrantedPermission}) ->
            pattern_match(Syntax, VHost, VHostPattern) andalso
            pattern_match(Syntax, Name, NamePattern) andalso
            Permission =:= ScopeGrantedPermission
        end,
        get_scope_permissions(Scopes)).

-spec topic_access(rabbit_types:r(atom()), permission(), map(), [binary()]) -> boolean().
topic_access(Resource, Permission, Context, Scopes) ->
    topic_access(Resource, Permission, Context, Scopes, wildcard).

-spec topic_access(rabbit_types:r(atom()), permission(), map(), [binary()],
        scope_pattern_syntax()) -> boolean().
topic_access(#resource{virtual_host = VHost, name = ExchangeName},
             Permission,
             #{routing_key := RoutingKey},
             Scopes,
             Syntax) ->
    lists:any(
        fun({VHostPattern, ExchangeNamePattern, RoutingKeyPattern, ScopeGrantedPermission}) ->
            is_binary(RoutingKeyPattern) andalso
            pattern_match(Syntax, VHost, VHostPattern) andalso
            pattern_match(Syntax, ExchangeName, ExchangeNamePattern) andalso
            pattern_match(Syntax, RoutingKey, RoutingKeyPattern) andalso
            Permission =:= ScopeGrantedPermission
        end,
        get_scope_permissions(Scopes)).

%% Internal -------------------------------------------------------------------

pattern_match(wildcard, Subject, Pattern) ->
    wildcard:match(scope_match_subject(Subject), Pattern);
pattern_match(regex, Subject, Pattern) ->
    regex_full_string_match(scope_match_subject(Subject), Pattern).

scope_match_subject(Subject) when is_binary(Subject) ->
    Subject;
scope_match_subject(Subject) ->
    rabbit_data_coercion:to_binary(Subject).

regex_full_string_match(Subject, Pattern)
        when is_binary(Subject), is_binary(Pattern) ->
    case byte_size(Pattern) > ?MAX_REGEX_PATTERN_BYTES of
        true ->
            ?LOG_WARNING(
                "OAuth 2 scope regex rejected: pattern exceeds the ~b-byte limit "
                "(actual size: ~b bytes)",
                [?MAX_REGEX_PATTERN_BYTES, byte_size(Pattern)]),
            false;
        false ->
            case has_rejected_construct(Pattern) of
                true ->
                    ?LOG_WARNING(
                        "OAuth 2 scope regex rejected: pattern contains an unsupported "
                        "construct (inline modifier, callout, comment or control verb): ~0tp",
                        [Pattern]),
                    false;
                false ->
                    do_regex_full_string_match(Subject, Pattern)
            end
    end;
regex_full_string_match(_, _) ->
    false.

do_regex_full_string_match(Subject, Pattern) ->
    Wrapped = <<"\\A(?:", Pattern/binary, ")\\z">>,
    try
        case re:compile(Wrapped, [unicode, anchored, ucp]) of
            {ok, MP} ->
                Opts = [{capture, none},
                        {match_limit, ?REGEX_MATCH_LIMIT},
                        {match_limit_recursion, ?REGEX_MATCH_LIMIT_RECURSION}],
                case re:run(Subject, MP, Opts) of
                    match ->
                        true;
                    nomatch ->
                        false;
                    {error, Reason} ->
                        ?LOG_WARNING(
                            "OAuth 2 scope regex match aborted "
                            "(reason: ~tp, pattern: ~0tp)",
                            [Reason, Pattern]),
                        false
                end;
            {error, CompileReason} ->
                ?LOG_WARNING(
                    "OAuth 2 scope regex failed to compile "
                    "(reason: ~tp, pattern: ~0tp)",
                    [CompileReason, Pattern]),
                false
        end
    catch _:_ ->
        false
    end.

has_rejected_construct(Pattern) ->
    binary:match(Pattern, ?REJECTED_REGEX_CONSTRUCTS) =/= nomatch.

%% -spec get_scope_permissions([binary()]) -> [{rabbit_types:r(pattern), permission()}].
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

-spec parse_permission_pattern(binary()) -> {rabbit_types:vhost(), binary(), binary() | none, permission()} | 'ignore'.
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
    {rabbit_types:vhost(), binary(), binary() | none, permission()} | 'ignore'.
parse_resource_pattern(Pattern, Permission) ->
    case binary:split(Pattern, <<"/">>, [global]) of
        [VhostPattern, NamePattern] ->
            {VhostPattern, NamePattern, none, Permission};
        [VhostPattern, NamePattern, RoutingKeyPattern] ->
            {VhostPattern, NamePattern, RoutingKeyPattern, Permission};
        _Other -> ignore
    end.

-spec filter_matching_scope_prefix(ResourceServer :: resource_server(),
        Payload :: map()) -> map().
filter_matching_scope_prefix(
    #resource_server{scope_prefix = ScopePrefix},
    #{?SCOPE_JWT_FIELD := Scopes} = Payload) -> 
        Payload#{?SCOPE_JWT_FIELD := 
            filter_matching_scope_prefix_and_drop_it(Scopes, ScopePrefix)};
filter_matching_scope_prefix(_, Payload) -> Payload.

-spec filter_matching_scope_prefix_and_drop_it(list(), binary()|list()) -> list().
filter_matching_scope_prefix_and_drop_it(Scopes, <<>>) -> Scopes;
filter_matching_scope_prefix_and_drop_it(Scopes, PrefixPattern)  ->
    PatternLength = byte_size(PrefixPattern),
    lists:filtermap(
        fun(ScopeEl) ->
            case binary:match(ScopeEl, PrefixPattern) of
                {0, PatternLength} ->
                    ElLength = byte_size(ScopeEl),
                    {true,
                     binary:part(ScopeEl,
                                 {PatternLength, ElLength - PatternLength})};
                _ -> false
            end
        end,
        Scopes).
