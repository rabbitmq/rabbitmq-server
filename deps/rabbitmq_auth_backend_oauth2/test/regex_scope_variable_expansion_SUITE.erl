%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2026 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
%%

-module(regex_scope_variable_expansion_SUITE).

-compile(export_all).

-include_lib("rabbit_common/include/rabbit.hrl").
-include_lib("eunit/include/eunit.hrl").

all() ->
    [
        wildcard_substitution_is_unchanged,
        regex_substitution_escapes_vhost_metacharacters,
        regex_substitution_escapes_claim_metacharacters,
        crafted_vhost_cannot_broaden_regex_scope,
        crafted_claim_cannot_broaden_regex_scope,
        backslash_in_substituted_value_is_escaped,
        multiple_variables_in_one_scope_are_each_escaped,
        literal_pattern_text_is_not_escaped,
        substituted_value_matches_self_under_regex,
        hyphen_in_substituted_value_is_escaped,
        crafted_value_cannot_form_range_inside_char_class
    ].

resource(VHost) ->
    #resource{virtual_host = VHost}.

expand(Token, VHost, Syntax) ->
    rabbit_auth_backend_oauth2:get_expanded_scopes(Token, resource(VHost), Syntax).

wildcard_substitution_is_unchanged(_Config) ->
    Token = #{<<"scope">> => [<<"read:{vhost}/q">>]},
    ?assertEqual([<<"read:foo/q">>], expand(Token, <<"foo">>, wildcard)),
    ?assertEqual([<<"read:foo.bar/q">>], expand(Token, <<"foo.bar">>, wildcard)).

regex_substitution_escapes_vhost_metacharacters(_Config) ->
    Token = #{<<"scope">> => [<<"read:{vhost}/q">>]},
    ?assertEqual([<<"read:foo\\.bar/q">>], expand(Token, <<"foo.bar">>, regexpr)),
    ?assertEqual([<<"read:\\(\\.\\*\\)/q">>], expand(Token, <<"(.*)">>, regexpr)).

regex_substitution_escapes_claim_metacharacters(_Config) ->
    Token = #{<<"scope">> => [<<"read:vh/{custom_claim}">>],
              <<"custom_claim">> => <<".*">>},
    ?assertEqual([<<"read:vh/\\.\\*">>], expand(Token, <<"vh">>, regexpr)).

crafted_vhost_cannot_broaden_regex_scope(_Config) ->
    Token = #{<<"scope">> => [<<"read:{vhost}/q">>]},
    [Expanded] = expand(Token, <<"(.*)">>, regexpr),
    ?assertEqual(true, rabbit_oauth2_scope:vhost_access(
        <<"(.*)">>, [Expanded], regexpr)),
    ?assertEqual(false, rabbit_oauth2_scope:vhost_access(
        <<"some-other-vhost">>, [Expanded], regexpr)).

crafted_claim_cannot_broaden_regex_scope(_Config) ->
    Token = #{<<"scope">> => [<<"read:vh/{custom_claim}">>],
              <<"custom_claim">> => <<".*">>},
    [Expanded] = expand(Token, <<"vh">>, regexpr),
    ?assertEqual(true, rabbit_oauth2_scope:resource_access(
        #resource{virtual_host = <<"vh">>, kind = queue, name = <<".*">>},
        read, [Expanded], regexpr)),
    ?assertEqual(false, rabbit_oauth2_scope:resource_access(
        #resource{virtual_host = <<"vh">>, kind = queue, name = <<"any-other-queue">>},
        read, [Expanded], regexpr)).

backslash_in_substituted_value_is_escaped(_Config) ->
    Token = #{<<"scope">> => [<<"read:{vhost}/q">>]},
    ?assertEqual([<<"read:foo\\\\bar/q">>], expand(Token, <<"foo\\bar">>, regexpr)).

multiple_variables_in_one_scope_are_each_escaped(_Config) ->
    Token = #{<<"scope">> => [<<"read:{vhost}/{custom_claim}">>],
              <<"custom_claim">> => <<"q.+">>},
    ?assertEqual([<<"read:foo\\.bar/q\\.\\+">>],
        expand(Token, <<"foo.bar">>, regexpr)).

literal_pattern_text_is_not_escaped(_Config) ->
    Token = #{<<"scope">> => [<<"read:[a-z]+/{vhost}-q">>]},
    ?assertEqual([<<"read:[a-z]+/foo\\.bar-q">>],
        expand(Token, <<"foo.bar">>, regexpr)).

substituted_value_matches_self_under_regex(_Config) ->
    Token = #{<<"scope">> => [<<"read:{vhost}/q">>]},
    Hostile = <<"a.b*c+d?e^f$g|h(i)j[k]l{m}n\\o-p">>,
    [Expanded] = expand(Token, Hostile, regexpr),
    ?assertEqual(true, rabbit_oauth2_scope:vhost_access(
        Hostile, [Expanded], regexpr)).

hyphen_in_substituted_value_is_escaped(_Config) ->
    Token = #{<<"scope">> => [<<"read:{vhost}/q">>]},
    ?assertEqual([<<"read:a\\-z/q">>], expand(Token, <<"a-z">>, regexpr)).

%% A template that wraps the substitution inside a character class
%% must not let the substituted value form a range (e.g. "a-z").
crafted_value_cannot_form_range_inside_char_class(_Config) ->
    Token = #{<<"scope">> => [<<"read:[{custom_claim}]/q">>],
              <<"custom_claim">> => <<"a-z">>},
    [Expanded] = expand(Token, <<"vh">>, regexpr),
    ?assertEqual(false, rabbit_oauth2_scope:vhost_access(
        <<"q">>, [Expanded], regexpr)),
    ?assertEqual(false, rabbit_oauth2_scope:vhost_access(
        <<"m">>, [Expanded], regexpr)).
