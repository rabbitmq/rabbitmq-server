%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2026 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
%%

-module(scope_segment_safety_SUITE).

-compile(export_all).

-include_lib("rabbit_common/include/rabbit.hrl").
-include_lib("eunit/include/eunit.hrl").

all() ->
    [
        wildcard_vhost_with_slash_drops_scope,
        regex_vhost_with_slash_drops_scope,
        wildcard_claim_with_slash_drops_scope,
        regex_claim_with_slash_drops_scope,
        vhost_with_multiple_slashes_drops_scope,
        slash_only_vhost_drops_scope,
        leading_slash_vhost_drops_scope,
        trailing_slash_vhost_drops_scope,
        legitimate_vhost_substitution_still_works_wildcard,
        legitimate_vhost_substitution_still_works_regex,
        slash_in_one_scope_does_not_affect_others,
        topic_template_with_slash_in_vhost_drops_scope,
        slash_in_one_variable_drops_scope_with_multiple_variables
    ].

resource(VHost) ->
    #resource{virtual_host = VHost}.

expand(Token, VHost, Syntax) ->
    rabbit_auth_backend_oauth2:get_expanded_scopes(Token, resource(VHost), Syntax).

wildcard_vhost_with_slash_drops_scope(_Config) ->
    Token = #{<<"scope">> => [<<"read:{vhost}/q">>]},
    ?assertEqual([<<>>], expand(Token, <<"vh/extra">>, wildcard)),
    ?assertEqual(false, rabbit_oauth2_scope:vhost_access(
        <<"vh">>, expand(Token, <<"vh/extra">>, wildcard), wildcard)),
    ?assertEqual(false, rabbit_oauth2_scope:vhost_access(
        <<"vh/extra">>, expand(Token, <<"vh/extra">>, wildcard), wildcard)).

regex_vhost_with_slash_drops_scope(_Config) ->
    Token = #{<<"scope">> => [<<"read:{vhost}/q">>]},
    ?assertEqual([<<>>], expand(Token, <<"vh/extra">>, regexpr)),
    ?assertEqual(false, rabbit_oauth2_scope:vhost_access(
        <<"vh">>, expand(Token, <<"vh/extra">>, regexpr), regexpr)),
    ?assertEqual(false, rabbit_oauth2_scope:vhost_access(
        <<"vh/extra">>, expand(Token, <<"vh/extra">>, regexpr), regexpr)).

wildcard_claim_with_slash_drops_scope(_Config) ->
    Token = #{<<"scope">> => [<<"read:vh/{custom_claim}">>],
              <<"custom_claim">> => <<"q/exchange/key">>},
    ?assertEqual([<<>>], expand(Token, <<"vh">>, wildcard)).

regex_claim_with_slash_drops_scope(_Config) ->
    Token = #{<<"scope">> => [<<"read:vh/{custom_claim}">>],
              <<"custom_claim">> => <<"q/exchange/key">>},
    ?assertEqual([<<>>], expand(Token, <<"vh">>, regexpr)).

vhost_with_multiple_slashes_drops_scope(_Config) ->
    Token = #{<<"scope">> => [<<"read:{vhost}/q">>]},
    ?assertEqual([<<>>], expand(Token, <<"a/b/c/d">>, wildcard)),
    ?assertEqual([<<>>], expand(Token, <<"a/b/c/d">>, regexpr)).

slash_only_vhost_drops_scope(_Config) ->
    Token = #{<<"scope">> => [<<"read:{vhost}/q">>]},
    ?assertEqual([<<>>], expand(Token, <<"/">>, wildcard)),
    ?assertEqual([<<>>], expand(Token, <<"/">>, regexpr)).

leading_slash_vhost_drops_scope(_Config) ->
    Token = #{<<"scope">> => [<<"read:{vhost}/q">>]},
    ?assertEqual([<<>>], expand(Token, <<"/leading">>, wildcard)),
    ?assertEqual([<<>>], expand(Token, <<"/leading">>, regexpr)).

trailing_slash_vhost_drops_scope(_Config) ->
    Token = #{<<"scope">> => [<<"read:{vhost}/q">>]},
    ?assertEqual([<<>>], expand(Token, <<"trailing/">>, wildcard)),
    ?assertEqual([<<>>], expand(Token, <<"trailing/">>, regexpr)).

legitimate_vhost_substitution_still_works_wildcard(_Config) ->
    Token = #{<<"scope">> => [<<"read:{vhost}/q">>]},
    ?assertEqual([<<"read:foo/q">>], expand(Token, <<"foo">>, wildcard)),
    ?assertEqual([<<"read:foo.bar/q">>], expand(Token, <<"foo.bar">>, wildcard)).

legitimate_vhost_substitution_still_works_regex(_Config) ->
    Token = #{<<"scope">> => [<<"read:{vhost}/q">>]},
    ?assertEqual([<<"read:foo/q">>], expand(Token, <<"foo">>, regexpr)),
    ?assertEqual([<<"read:foo\\.bar/q">>], expand(Token, <<"foo.bar">>, regexpr)).

slash_in_one_scope_does_not_affect_others(_Config) ->
    Token = #{<<"scope">> => [<<"read:{vhost}/q">>,
                              <<"write:static/q">>,
                              <<"configure:vh/q">>]},
    ?assertEqual(
        [<<>>, <<"write:static/q">>, <<"configure:vh/q">>],
        expand(Token, <<"vh/extra">>, regexpr)).

topic_template_with_slash_in_vhost_drops_scope(_Config) ->
    Token = #{<<"scope">> => [<<"read:{vhost}/ex/key">>]},
    ?assertEqual([<<>>], expand(Token, <<"vh/q">>, wildcard)),
    ?assertEqual([<<>>], expand(Token, <<"vh/q">>, regexpr)).

slash_in_one_variable_drops_scope_with_multiple_variables(_Config) ->
    Token = #{<<"scope">> => [<<"read:{vhost}/{custom_claim}">>],
              <<"custom_claim">> => <<"good-value">>},
    ?assertEqual([<<>>], expand(Token, <<"vh/extra">>, wildcard)),
    ?assertEqual([<<>>], expand(Token, <<"vh/extra">>, regexpr)),
    BadClaim = #{<<"scope">> => [<<"read:{vhost}/{custom_claim}">>],
                 <<"custom_claim">> => <<"a/b">>},
    ?assertEqual([<<>>], expand(BadClaim, <<"vh">>, wildcard)),
    ?assertEqual([<<>>], expand(BadClaim, <<"vh">>, regexpr)).
