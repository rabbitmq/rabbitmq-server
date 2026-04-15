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

-define(NUM_TESTS, 100).

all() ->
    [
        scopes_within_limit_accepted,
        scopes_over_limit_refused,
        duplicate_scopes_counted_after_dedup
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
