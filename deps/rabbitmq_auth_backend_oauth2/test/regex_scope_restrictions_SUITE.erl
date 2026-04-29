%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2026 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
%%

-module(regex_scope_restrictions_SUITE).

-compile(export_all).

-include_lib("rabbit_common/include/rabbit.hrl").
-include_lib("eunit/include/eunit.hrl").

-define(REDOS_TIME_BUDGET_MS, 500).

all() ->
    [
        catastrophic_nested_quantifier_returns_quickly,
        catastrophic_alternation_returns_quickly,
        oversized_pattern_denies_access,
        pattern_at_max_size_is_accepted,
        pattern_one_byte_over_max_is_rejected,
        invalid_pattern_denies_access,
        legitimate_patterns_still_match,
        end_anchor_rejects_trailing_newline,
        prefix_match_does_not_grant_extra_suffix,
        suffix_match_does_not_grant_extra_prefix,
        matching_is_case_sensitive_by_default,
        any_scope_in_list_grants_access,
        permission_is_required_to_match,
        inline_modifiers_are_rejected,
        scoped_modifier_groups_are_rejected,
        callouts_are_rejected,
        comments_are_rejected,
        control_verbs_are_rejected,
        topic_routing_key_rejects_inline_modifier,
        wildcard_syntax_unaffected_by_regex_restrictions,
        non_capturing_groups_are_allowed,
        lookarounds_are_allowed,
        atomic_groups_are_allowed,
        named_captures_are_allowed,
        unicode_character_classes_match
    ].

vhost_resource(VHost, Name) ->
    #resource{virtual_host = VHost, kind = queue, name = Name}.

time_match(Fun) ->
    T0 = erlang:monotonic_time(millisecond),
    Result = Fun(),
    {Result, erlang:monotonic_time(millisecond) - T0}.

deny(Pattern) ->
    ?assertEqual(false, rabbit_oauth2_scope:vhost_access(
        <<"foo">>, [<<"read:", Pattern/binary, "/x">>], regexpr)).

allow(Pattern, Subject) ->
    ?assertEqual(true, rabbit_oauth2_scope:vhost_access(
        Subject, [<<"read:", Pattern/binary, "/x">>], regexpr)).

catastrophic_nested_quantifier_returns_quickly(_Config) ->
    Subject = binary:copy(<<"a">>, 40),
    Scope = <<"read:vh/(a+)+b">>,
    {Result, ElapsedMs} = time_match(
        fun() ->
            rabbit_oauth2_scope:resource_access(
                vhost_resource(<<"vh">>, Subject), read, [Scope], regexpr)
        end),
    ?assertEqual(false, Result),
    ?assert(ElapsedMs < ?REDOS_TIME_BUDGET_MS).

catastrophic_alternation_returns_quickly(_Config) ->
    Subject = binary:copy(<<"a">>, 30),
    Scope = <<"read:vh/(a|aa)+c">>,
    {Result, ElapsedMs} = time_match(
        fun() ->
            rabbit_oauth2_scope:resource_access(
                vhost_resource(<<"vh">>, Subject), read, [Scope], regexpr)
        end),
    ?assertEqual(false, Result),
    ?assert(ElapsedMs < ?REDOS_TIME_BUDGET_MS).

oversized_pattern_denies_access(_Config) ->
    Big = binary:copy(<<"a">>, 4096),
    VHostScope = <<"read:", Big/binary, "/x">>,
    NameScope = <<"read:vh/", Big/binary>>,
    ?assertEqual(false, rabbit_oauth2_scope:vhost_access(
        <<"vh">>, [VHostScope], regexpr)),
    ?assertEqual(false, rabbit_oauth2_scope:resource_access(
        vhost_resource(<<"vh">>, <<"q">>), read, [NameScope], regexpr)).

pattern_at_max_size_is_accepted(_Config) ->
    Subject = binary:copy(<<"a">>, 2048),
    Scope = <<"read:", Subject/binary, "/x">>,
    ?assertEqual(true, rabbit_oauth2_scope:vhost_access(
        Subject, [Scope], regexpr)).

pattern_one_byte_over_max_is_rejected(_Config) ->
    Subject = binary:copy(<<"a">>, 2049),
    Scope = <<"read:", Subject/binary, "/x">>,
    ?assertEqual(false, rabbit_oauth2_scope:vhost_access(
        Subject, [Scope], regexpr)).

invalid_pattern_denies_access(_Config) ->
    ?assertEqual(false, rabbit_oauth2_scope:vhost_access(
        <<"vh">>, [<<"read:[unterminated/x">>], regexpr)),
    ?assertEqual(false, rabbit_oauth2_scope:resource_access(
        vhost_resource(<<"vh">>, <<"q">>),
        read, [<<"read:vh/[unterminated">>], regexpr)).

end_anchor_rejects_trailing_newline(_Config) ->
    ?assertEqual(false, rabbit_oauth2_scope:vhost_access(
        <<"foo\n">>, [<<"read:foo/x">>], regexpr)),
    ?assertEqual(false, rabbit_oauth2_scope:vhost_access(
        <<"foo\n">>, [<<"read:.+/x">>], regexpr)).

prefix_match_does_not_grant_extra_suffix(_Config) ->
    ?assertEqual(false, rabbit_oauth2_scope:vhost_access(
        <<"foobar">>, [<<"read:foo/x">>], regexpr)).

suffix_match_does_not_grant_extra_prefix(_Config) ->
    ?assertEqual(false, rabbit_oauth2_scope:vhost_access(
        <<"barfoo">>, [<<"read:foo/x">>], regexpr)).

matching_is_case_sensitive_by_default(_Config) ->
    ?assertEqual(false, rabbit_oauth2_scope:vhost_access(
        <<"FOO">>, [<<"read:foo/x">>], regexpr)),
    ?assertEqual(true, rabbit_oauth2_scope:vhost_access(
        <<"FOO">>, [<<"read:[Ff][Oo][Oo]/x">>], regexpr)).

any_scope_in_list_grants_access(_Config) ->
    Scopes = [<<"read:alpha/x">>, <<"read:beta/x">>, <<"read:[gG]amma/x">>],
    ?assertEqual(true, rabbit_oauth2_scope:vhost_access(
        <<"beta">>, Scopes, regexpr)),
    ?assertEqual(true, rabbit_oauth2_scope:vhost_access(
        <<"Gamma">>, Scopes, regexpr)),
    ?assertEqual(false, rabbit_oauth2_scope:vhost_access(
        <<"delta">>, Scopes, regexpr)).

permission_is_required_to_match(_Config) ->
    Resource = vhost_resource(<<"vh">>, <<"q">>),
    ?assertEqual(true, rabbit_oauth2_scope:resource_access(
        Resource, write, [<<"write:vh/q">>], regexpr)),
    ?assertEqual(false, rabbit_oauth2_scope:resource_access(
        Resource, write, [<<"read:vh/q">>], regexpr)),
    ?assertEqual(true, rabbit_oauth2_scope:resource_access(
        Resource, configure, [<<"configure:vh/q">>], regexpr)).

legitimate_patterns_still_match(_Config) ->
    ?assertEqual(true, rabbit_oauth2_scope:vhost_access(
        <<"abc">>, [<<"read:[a-z]+/x">>], regexpr)),
    ?assertEqual(true, rabbit_oauth2_scope:resource_access(
        vhost_resource(<<"vh">>, <<"queue-1">>),
        read, [<<"read:vh/queue-[0-9]+">>], regexpr)).

inline_modifiers_are_rejected(_Config) ->
    deny(<<"(?i)foo">>),
    deny(<<"(?m)foo">>),
    deny(<<"(?s)foo">>),
    deny(<<"(?x)foo">>),
    deny(<<"(?im)foo">>).

scoped_modifier_groups_are_rejected(_Config) ->
    deny(<<"(?i:foo)">>),
    deny(<<"(?-i:foo)">>),
    deny(<<"(?ms:foo)">>).

callouts_are_rejected(_Config) ->
    deny(<<"(?C1)foo">>).

comments_are_rejected(_Config) ->
    deny(<<"(?#a comment)foo">>).

control_verbs_are_rejected(_Config) ->
    deny(<<"(*LIMIT_MATCH=99999999)foo">>),
    deny(<<"(*LIMIT_RECURSION=99999999)foo">>),
    deny(<<"(*NO_START_OPT)foo">>),
    deny(<<"foo(*COMMIT)bar">>).

topic_routing_key_rejects_inline_modifier(_Config) ->
    Topic = #resource{virtual_host = <<"vh">>, kind = topic, name = <<"ex">>},
    Scope = <<"read:vh/ex/(?i)foo">>,
    ?assertEqual(false, rabbit_oauth2_scope:topic_access(
        Topic, read, #{routing_key => <<"foo">>}, [Scope], regexpr)).

wildcard_syntax_unaffected_by_regex_restrictions(_Config) ->
    ?assertEqual(true, rabbit_oauth2_scope:vhost_access(
        <<"(?i)foo">>, [<<"read:(?i)foo/x">>], wildcard)),
    ?assertEqual(true, rabbit_oauth2_scope:vhost_access(
        <<"(?i)foo">>, [<<"read:(?i)*/x">>], wildcard)).

non_capturing_groups_are_allowed(_Config) ->
    allow(<<"(?:fo)+o">>, <<"fofofoo">>).

lookarounds_are_allowed(_Config) ->
    allow(<<"foo(?=bar)bar">>, <<"foobar">>),
    allow(<<"foo(?!baz)bar">>, <<"foobar">>),
    allow(<<"f(?<=f)oobar">>, <<"foobar">>),
    allow(<<"f(?<!x)oobar">>, <<"foobar">>).

atomic_groups_are_allowed(_Config) ->
    allow(<<"(?>foo)bar">>, <<"foobar">>).

named_captures_are_allowed(_Config) ->
    allow(<<"(?<word>foo)">>, <<"foo">>),
    allow(<<"(?P<word>foo)">>, <<"foo">>).

unicode_character_classes_match(_Config) ->
    allow(<<"\\p{L}+">>, <<"café"/utf8>>).
