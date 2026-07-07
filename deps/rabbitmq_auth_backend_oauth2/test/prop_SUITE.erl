%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2026 Broadcom. All Rights Reserved. The term "Broadcom" refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
%%

-module(prop_SUITE).

-compile(export_all).

-include_lib("common_test/include/ct.hrl").
-include_lib("proper/include/proper.hrl").
-include("oauth2.hrl").

-import(rabbit_auth_backend_oauth2, [normalize_token_scope/2]).
-import(rabbit_oauth2_resource_server, [new_resource_server/1]).
-import(uaa_jwt, [parse_jwks_response/2]).

-define(NUM_TESTS, 100).

all() ->
    [
        scopes_within_limit_accepted,
        scopes_over_limit_refused,
        duplicate_scopes_counted_after_dedup,
        scenario_a,
        scenario_b,
        scenario_c
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

%%
%% Generators
%%

scope(N) ->
    <<"rabbitmq.read:*/q_", (integer_to_binary(N))/binary>>.

scope_list(Len) ->
    [scope(N) || N <- lists:seq(1, Len)].

%%
%% Properties
%%

scopes_within_limit_accepted(Config) ->
    Property = fun() -> prop_scopes_within_limit_accepted(Config) end,
    rabbit_ct_proper_helpers:run_proper(Property, [], ?NUM_TESTS).

prop_scopes_within_limit_accepted(_Config) ->
    ?FORALL(Len, range(0, 2048),
        begin
            ResourceServer = new_resource_server(<<"rabbitmq">>),
            Token0 = #{?SCOPE_JWT_FIELD => scope_list(Len)},
            case normalize_token_scope(ResourceServer, Token0) of
                {ok, _} -> true;
                _       -> false
            end
        end).

scopes_over_limit_refused(Config) ->
    Property = fun() -> prop_scopes_over_limit_refused(Config) end,
    rabbit_ct_proper_helpers:run_proper(Property, [], ?NUM_TESTS).

prop_scopes_over_limit_refused(_Config) ->
    ?FORALL(Len, range(2049, 4096),
        begin
            ResourceServer = new_resource_server(<<"rabbitmq">>),
            Token0 = #{?SCOPE_JWT_FIELD => scope_list(Len)},
            normalize_token_scope(ResourceServer, Token0) =:= {error, too_many_scopes}
        end).

duplicate_scopes_counted_after_dedup(Config) ->
    Property = fun() -> prop_duplicate_scopes_counted_after_dedup(Config) end,
    rabbit_ct_proper_helpers:run_proper(Property, [], ?NUM_TESTS).

prop_duplicate_scopes_counted_after_dedup(_Config) ->
    ?FORALL(Len, range(1, 2048),
        begin
            ResourceServer = new_resource_server(<<"rabbitmq">>),
            Base = scope_list(Len),
            %% Double the list so total count > 2048 is possible,
            %% but unique count is always =< 2048
            Token0 = #{?SCOPE_JWT_FIELD => Base ++ Base},
            case normalize_token_scope(ResourceServer, Token0) of
                {ok, _} -> true;
                _       -> false
            end
        end).

%% Any non-200 response is rejected on status code alone, whatever the body.
scenario_a(Config) ->
    Property = fun() -> prop_scenario_a(Config) end,
    rabbit_ct_proper_helpers:run_proper(Property, [], ?NUM_TESTS).

prop_scenario_a(_Config) ->
    ?FORALL({StatusCode, Body}, {non_200_status_code(), jwks_like_body()},
        parse_jwks_response({"HTTP/1.1", StatusCode, "reason"}, Body) =:=
            {error, {unexpected_status_code, StatusCode}}).

%% A 200 response is accepted exactly when it carries at least one key, and
%% the parsed map holds one entry per key.
scenario_b(Config) ->
    Property = fun() -> prop_scenario_b(Config) end,
    rabbit_ct_proper_helpers:run_proper(Property, [], ?NUM_TESTS).

prop_scenario_b(_Config) ->
    ?FORALL(KeyCount, range(0, 20),
        begin
            Body = rabbit_json:encode(#{keys => key_list(KeyCount)}),
            case parse_jwks_response({"HTTP/1.1", 200, "OK"}, Body) of
                {error, empty_jwks_response} -> KeyCount =:= 0;
                {ok, Keys} -> KeyCount > 0 andalso maps:size(Keys) =:= KeyCount
            end
        end).

%% Parsing a 200 body is total: no arbitrary byte sequence ever throws.
scenario_c(Config) ->
    Property = fun() -> prop_scenario_c(Config) end,
    rabbit_ct_proper_helpers:run_proper(Property, [], ?NUM_TESTS).

prop_scenario_c(_Config) ->
    ?FORALL(Body, binary(),
        case parse_jwks_response({"HTTP/1.1", 200, "OK"}, Body) of
            {ok, Keys} when is_map(Keys) -> true;
            {error, empty_jwks_response} -> true;
            {error, invalid_jwks_response} -> true;
            _ -> false
        end).

%%
%% Generators
%%

non_200_status_code() ->
    ?SUCHTHAT(StatusCode, range(100, 599), StatusCode =/= 200).

jwks_like_body() ->
    oneof([
        <<"{\"error\":\"down\"}">>,
        <<"{}">>,
        <<"{\"keys\":[{\"kid\":\"irrelevant\"}]}">>,
        <<"not even json">>
    ]).

key_list(Len) ->
    [#{<<"kid">> => <<"key-", (integer_to_binary(N))/binary>>} ||
        N <- lists:seq(1, Len)].
