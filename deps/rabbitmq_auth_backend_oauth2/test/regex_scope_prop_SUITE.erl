%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2026 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
%%

-module(regex_scope_prop_SUITE).

-compile(export_all).

-include_lib("common_test/include/ct.hrl").
-include_lib("rabbit_common/include/rabbit.hrl").
-include_lib("proper/include/proper.hrl").

-define(NUM_TESTS, 200).

all() ->
    [
        unrelated_vhosts_never_granted,
        catastrophic_patterns_always_bounded,
        rejected_construct_always_denies,
        substituted_vhost_grants_only_itself,
        substituted_claim_grants_only_itself,
        slash_in_substituted_vhost_drops_scope,
        slash_in_substituted_claim_drops_scope
    ].

init_per_suite(Config) ->
    rabbit_ct_helpers:log_environment(),
    rabbit_ct_helpers:run_setup_steps(Config).

end_per_suite(Config) ->
    rabbit_ct_helpers:run_teardown_steps(Config).

init_per_testcase(Testcase, Config) ->
    rabbit_ct_helpers:testcase_started(Config, Testcase).

end_per_testcase(Testcase, Config) ->
    rabbit_ct_helpers:testcase_finished(Config, Testcase).

%% A pattern matching one vhost name must never grant access to a different
%% vhost name with extra characters appended, since regex scopes are anchored
%% end-to-end.
unrelated_vhosts_never_granted(Config) ->
    Property = fun() -> prop_unrelated_vhosts_never_granted(Config) end,
    rabbit_ct_proper_helpers:run_proper(Property, [], ?NUM_TESTS).

prop_unrelated_vhosts_never_granted(_Config) ->
    ?FORALL(
        {VHost, Suffix},
        {non_empty_token(), non_empty_token()},
        begin
            Escaped = re_escape(VHost),
            Scope = iolist_to_binary([<<"read:">>, Escaped, <<"/x">>]),
            Other = <<VHost/binary, Suffix/binary>>,
            not rabbit_oauth2_scope:vhost_access(Other, [Scope], regex)
        end).

%% Catastrophic-backtracking patterns must return within a small time bound,
%% regardless of the subject.
catastrophic_patterns_always_bounded(Config) ->
    Property = fun() -> prop_catastrophic_patterns_always_bounded(Config) end,
    rabbit_ct_proper_helpers:run_proper(Property, [], 50).

prop_catastrophic_patterns_always_bounded(_Config) ->
    ?FORALL(
        N,
        range(20, 60),
        begin
            Subject = binary:copy(<<"a">>, N),
            Scope = <<"read:vh/(a+)+b">>,
            T0 = erlang:monotonic_time(millisecond),
            _ = rabbit_oauth2_scope:resource_access(
                #resource{virtual_host = <<"vh">>,
                          kind = queue,
                          name = Subject},
                read, [Scope], regex),
            (erlang:monotonic_time(millisecond) - T0) < 500
        end).

rejected_construct_always_denies(Config) ->
    Property = fun() -> prop_rejected_construct_always_denies(Config) end,
    rabbit_ct_proper_helpers:run_proper(Property, [], ?NUM_TESTS).

prop_rejected_construct_always_denies(_Config) ->
    ?FORALL(
        {Prefix, Tail, Subject},
        {rejected_prefix(), non_empty_token(), non_empty_token()},
        begin
            Pattern = <<Prefix/binary, Tail/binary>>,
            Scope = <<"read:", Pattern/binary, "/x">>,
            not rabbit_oauth2_scope:vhost_access(Subject, [Scope], regex)
        end).

substituted_vhost_grants_only_itself(Config) ->
    Property = fun() -> prop_substituted_vhost_grants_only_itself(Config) end,
    rabbit_ct_proper_helpers:run_proper(Property, [], ?NUM_TESTS).

prop_substituted_vhost_grants_only_itself(_Config) ->
    ?FORALL(
        {VHost, Other},
        {hostile_token(), hostile_token()},
        ?IMPLIES(
            VHost =/= Other,
            begin
                Token = #{<<"scope">> => [<<"read:{vhost}/q">>]},
                Resource = #resource{virtual_host = VHost},
                [Expanded] = rabbit_auth_backend_oauth2:get_expanded_scopes(
                    Token, Resource, regex),
                rabbit_oauth2_scope:vhost_access(VHost, [Expanded], regex)
                andalso
                not rabbit_oauth2_scope:vhost_access(Other, [Expanded], regex)
            end)).

substituted_claim_grants_only_itself(Config) ->
    Property = fun() -> prop_substituted_claim_grants_only_itself(Config) end,
    rabbit_ct_proper_helpers:run_proper(Property, [], ?NUM_TESTS).

prop_substituted_claim_grants_only_itself(_Config) ->
    ?FORALL(
        {ClaimValue, OtherName},
        {hostile_token(), hostile_token()},
        ?IMPLIES(
            ClaimValue =/= OtherName,
            begin
                Token = #{<<"scope">> => [<<"read:vh/{custom_claim}">>],
                          <<"custom_claim">> => ClaimValue},
                Resource = #resource{virtual_host = <<"vh">>},
                [Expanded] = rabbit_auth_backend_oauth2:get_expanded_scopes(
                    Token, Resource, regex),
                Self = #resource{virtual_host = <<"vh">>,
                                 kind = queue, name = ClaimValue},
                Other = #resource{virtual_host = <<"vh">>,
                                  kind = queue, name = OtherName},
                rabbit_oauth2_scope:resource_access(Self, read, [Expanded], regex)
                andalso
                not rabbit_oauth2_scope:resource_access(Other, read, [Expanded], regex)
            end)).

slash_in_substituted_vhost_drops_scope(Config) ->
    Property = fun() -> prop_slash_in_substituted_vhost_drops_scope(Config) end,
    rabbit_ct_proper_helpers:run_proper(Property, [], ?NUM_TESTS).

prop_slash_in_substituted_vhost_drops_scope(_Config) ->
    ?FORALL(
        {Prefix, Suffix, Syntax},
        {non_empty_token(), non_empty_token(), oneof([wildcard, regex])},
        begin
            VHost = <<Prefix/binary, "/", Suffix/binary>>,
            Token = #{<<"scope">> => [<<"read:{vhost}/q">>]},
            [Expanded] = rabbit_auth_backend_oauth2:get_expanded_scopes(
                Token, #resource{virtual_host = VHost}, Syntax),
            Expanded =:= <<>>
        end).

slash_in_substituted_claim_drops_scope(Config) ->
    Property = fun() -> prop_slash_in_substituted_claim_drops_scope(Config) end,
    rabbit_ct_proper_helpers:run_proper(Property, [], ?NUM_TESTS).

prop_slash_in_substituted_claim_drops_scope(_Config) ->
    ?FORALL(
        {Prefix, Suffix, Syntax},
        {non_empty_token(), non_empty_token(), oneof([wildcard, regex])},
        begin
            Claim = <<Prefix/binary, "/", Suffix/binary>>,
            Token = #{<<"scope">> => [<<"read:vh/{custom_claim}">>],
                      <<"custom_claim">> => Claim},
            [Expanded] = rabbit_auth_backend_oauth2:get_expanded_scopes(
                Token, #resource{virtual_host = <<"vh">>}, Syntax),
            Expanded =:= <<>>
        end).

%% Includes the regex metacharacters that escaping must neutralise.
hostile_token() ->
    ?LET(Chars,
         non_empty(list(oneof([range($a, $z), range($0, $9),
                               $-, $_,
                               $., $*, $+, $?, $^, $$, $|,
                               $(, $), $[, $], ${, $}, $\\]))),
         list_to_binary(Chars)).

rejected_prefix() ->
    oneof([<<"(?i)">>, <<"(?m)">>, <<"(?s)">>, <<"(?x)">>,
           <<"(?i:">>, <<"(?-i:">>, <<"(?C1)">>, <<"(?#c)">>,
           <<"(*LIMIT_MATCH=1000)">>, <<"(*NO_START_OPT)">>]).

non_empty_token() ->
    ?LET(Chars,
         non_empty(list(oneof([range($a, $z), range($0, $9),
                               $-, $., $_, $+]))),
         list_to_binary(Chars)).

re_escape(Bin) when is_binary(Bin) ->
    re:replace(Bin, <<"[.^$|()\\[\\]{}*+?\\\\]">>, <<"\\\\&">>,
               [global, {return, binary}]).
