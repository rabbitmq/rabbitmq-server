%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2026 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
%%

-module(rabbit_mgmt_http_sessions_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("rabbitmq_ct_helpers/include/rabbit_mgmt_test.hrl").
-include_lib("rabbitmq_ct_helpers/include/rabbit_assert.hrl").

-define(NOT_FOUND, 404).
-define(FORBIDDEN, 403).

-import(rabbit_ct_broker_helpers, [rpc/4, rpc/5]).
-import(rabbit_mgmt_test_util, [http_get/2, http_get/3, http_get/5,
                                http_post/4, http_post/6,
                                http_put/4, http_put/6,
                                http_delete/3, http_delete/4, http_delete/5,
                                req/6, decode_body/1]).

-compile([export_all, nowarn_export_all]).

all() ->
    [
        feature_disabled_test,
        authorization_and_metadata_test,
        concurrency_limits_test,
        distributed_conflict_resolution_test,
        distributed_session_counting_test,
        session_expiry_test
    ].

init_per_suite(Config) ->
    %% We need a 2-node cluster for the distributed tests
    rabbit_ct_helpers:log_environment(),
    Config1 = rabbit_ct_helpers:set_config(Config, [
        {rmq_nodename_suffix, ?MODULE},
        {rmq_nodes_count, 2}
    ]),
    rabbit_ct_helpers:run_setup_steps(Config1,
        rabbit_ct_broker_helpers:setup_steps() ++
        rabbit_ct_client_helpers:setup_steps()).

end_per_suite(Config) ->
    rabbit_ct_helpers:run_teardown_steps(Config, rabbit_ct_broker_helpers:teardown_steps()).

init_per_testcase(Testcase, Config) ->
    %% Ensure the management HTTP listener is up
    rabbit_ct_helpers:await_condition(fun() ->
        try
            rabbit_mgmt_test_util:http_get(Config, "/overview"),
            true
        catch _:_ ->
            false
        end
    end),
    %% Set default configs via rpc
    %% We set sessions_enabled = true for all tests EXCEPT feature_disabled_test
    Enabled = Testcase =/= feature_disabled_test,
    [N1, N2 | _] = ?config(rmq_nodes, Config),
    rpc(Config, N1, application, set_env, [rabbitmq_management, sessions_enabled, Enabled]),
    rpc(Config, N2, application, set_env, [rabbitmq_management, sessions_enabled, Enabled]),
    rpc(Config, N1, application, set_env, [rabbitmq_management, sessions_max_concurrent, 1]),
    rpc(Config, N2, application, set_env, [rabbitmq_management, sessions_max_concurrent, 1]),
    
    %% Clean up any existing sessions
    case Enabled of
        true ->
            %% wait a moment for app env to apply just in case
            timer:sleep(100),
            %% restart the gen_servers if needed? Actually they might be started via sup tree conditionally.
            %% If they aren't started, we might need to restart rabbitmq_management, 
            %% or start them manually.
            %% Let's just restart the management app to be safe and ensure the tree picks up the config.
            rpc(Config, N1, application, stop, [rabbitmq_management]),
            rpc(Config, N2, application, stop, [rabbitmq_management]),
            rpc(Config, N1, application, start, [rabbitmq_management]),
            rpc(Config, N2, application, start, [rabbitmq_management]);
        false ->
            %% Ensure stopped
            rpc(Config, N1, application, stop, [rabbitmq_management]),
            rpc(Config, N2, application, stop, [rabbitmq_management]),
            rpc(Config, N1, application, start, [rabbitmq_management]),
            rpc(Config, N2, application, start, [rabbitmq_management])
    end,

    %% Create some users AFTER the server is back up and running
    http_put(Config, "/users/test_admin", [{password, <<"test_admin">>}, {tags, <<"administrator">>}], {group, '2xx'}),
    http_put(Config, "/users/test_user_a", [{password, <<"test_user_a">>}, {tags, <<"management">>}], {group, '2xx'}),
    http_put(Config, "/users/test_user_b", [{password, <<"test_user_b">>}, {tags, <<"management">>}], {group, '2xx'}),
    rabbit_ct_helpers:testcase_started(Config, Testcase).

end_per_testcase(Testcase, Config) ->
    http_delete(Config, "/users/test_admin", {group, '2xx'}),
    http_delete(Config, "/users/test_user_a", {group, '2xx'}),
    http_delete(Config, "/users/test_user_b", {group, '2xx'}),
    rabbit_ct_helpers:testcase_finished(Config, Testcase).

feature_disabled_test(Config) ->
    %% POST
    http_post(Config, "/session", #{}, "test_user_a", "test_user_a", ?NOT_FOUND),
    %% PUT
    http_put(Config, "/session/123", #{}, "test_user_a", "test_user_a", ?NOT_FOUND),
    %% DELETE (user)
    http_delete(Config, "/session/123", "test_user_a", "test_user_a", ?NOT_FOUND),
    %% GET admin
    http_get(Config, "/sessions", "test_admin", "test_admin", ?NOT_FOUND),
    passed.

authorization_and_metadata_test(Config) ->
    Headers = [{"x-forwarded-for", "203.0.113.5, 10.0.0.1"},
               {"user-agent", "test-agent"}],
    {ok, {{_Http, 201, _}, _, BodyJSON}} = req(post, "/session", #{}, "test_user_a", "test_user_a", Headers),
    Body = decode_body(BodyJSON),
    SessionId = maps:get(<<"session_id">>, Body),
    
    %% Heartbeat self -> 200
    http_put(Config, "/session/" ++ binary_to_list(SessionId), #{}, "test_user_a", "test_user_a", ?OK),
    
    %% Heartbeat by another user -> 403
    http_put(Config, "/session/" ++ binary_to_list(SessionId), #{}, "test_user_b", "test_user_b", ?FORBIDDEN),
    
    %% Delete by another user -> 403
    http_delete(Config, "/session/" ++ binary_to_list(SessionId), "test_user_b", "test_user_b", ?FORBIDDEN),
    
    %% Admin GET
    SessionsRes = http_get(Config, "/sessions", "test_admin", "test_admin", ?OK),
    Items = maps:get(<<"items">>, SessionsRes),
    [Session] = [S || S <- Items, maps:get(<<"id">>, S) == SessionId],
    
    Metadata = maps:get(<<"metadata">>, Session),
    <<"203.0.113.5">> = maps:get(<<"ip">>, Metadata),
    <<"test-agent">> = maps:get(<<"user-agent">>, Metadata),
    
    %% Non-admin GET
    http_get(Config, "/sessions", "test_user_a", "test_user_a", ?FORBIDDEN),
    
    %% Admin DELETE
    http_delete(Config, "/sessions/" ++ binary_to_list(SessionId), "test_admin", "test_admin", ?NO_CONTENT),
    
    %% Verify deleted
    http_put(Config, "/session/" ++ binary_to_list(SessionId), #{}, "test_user_a", "test_user_a", ?NOT_AUTHORISED),
    passed.

concurrency_limits_test(Config) ->
    %% max=1 already set in init
    
    %% A logs in -> 201
    http_post(Config, "/session", #{}, "test_user_a", "test_user_a", ?CREATED),
    
    %% B logs in -> 201 (isolation)
    http_post(Config, "/session", #{}, "test_user_b", "test_user_b", ?CREATED),
    
    %% A logs in again on same node -> 403 (immediate limit)
    http_post(Config, "/session", #{}, "test_user_a", "test_user_a", ?FORBIDDEN),
    passed.

distributed_conflict_resolution_test(Config) ->
    [N1, N2 | _] = ?config(rmq_nodes, Config),
    
    %% A logs in on N1 -> 201
    {ok, {{_Http1, 201, _}, _, BodyJSON1}} = req(post, rabbit_mgmt_test_util:uri_base_from(N1, Config) ++ "/session", 
                                                 #{}, "test_user_a", "test_user_a", []),
    Body1 = decode_body(BodyJSON1),
    SessionId1 = maps:get(<<"session_id">>, Body1),
    
    %% A logs in on N2 -> 201
    {ok, {{_Http2, 201, _}, _, BodyJSON2}} = req(post, rabbit_mgmt_test_util:uri_base_from(N2, Config) ++ "/session", 
                                                 #{}, "test_user_a", "test_user_a", []),
    Body2 = decode_body(BodyJSON2),
    SessionId2 = maps:get(<<"session_id">>, Body2),
    
    %% Wait for gossip (5 seconds) + buffer
    timer:sleep(6000),
    
    %% Check. The NEWER session (SessionId2) should be terminated.
    %% SessionId1 should be kept.
    {ok, {{_, Status1, _}, _, _}} = req(put, rabbit_mgmt_test_util:uri_base_from(N1, Config) ++ "/session/" ++ binary_to_list(SessionId1), 
                                        #{}, "test_user_a", "test_user_a", []),
    {ok, {{_, Status2, _}, _, _}} = req(put, rabbit_mgmt_test_util:uri_base_from(N2, Config) ++ "/session/" ++ binary_to_list(SessionId2), 
                                        #{}, "test_user_a", "test_user_a", []),
    
    ?assertEqual(200, Status1),
    ?assertEqual(401, Status2),
    passed.

distributed_session_counting_test(Config) ->
    [N1, N2 | _] = ?config(rmq_nodes, Config),
    
    %% Set limit to 2
    rpc(Config, N1, application, set_env, [rabbitmq_management, sessions_max_concurrent, 2]),
    rpc(Config, N2, application, set_env, [rabbitmq_management, sessions_max_concurrent, 2]),
    
    %% A logs in on N1 -> 201
    req(post, rabbit_mgmt_test_util:uri_base_from(N1, Config) ++ "/session", #{}, "test_user_a", "test_user_a", []),
    
    %% A logs in on N2 -> 201
    req(post, rabbit_mgmt_test_util:uri_base_from(N2, Config) ++ "/session", #{}, "test_user_a", "test_user_a", []),
    
    %% Wait for gossip
    timer:sleep(6000),
    
    %% A logs in again -> 403
    {ok, {{_, Status3, _}, _, _}} = req(post, rabbit_mgmt_test_util:uri_base_from(N1, Config) ++ "/session", 
                                        #{}, "test_user_a", "test_user_a", []),
    
    ?assertEqual(403, Status3),
    passed.

session_expiry_test(Config) ->
    [N1 | _] = ?config(rmq_nodes, Config),
    
    %% Set very short TTL just for this test
    rpc(Config, N1, application, set_env, [rabbitmq_management, login_session_timeout, 1]), %% 1 minute
    rpc(Config, N1, meck, new, [rabbit_mgmt_sessions, [passthrough]]),
    rpc(Config, N1, meck, expect, [rabbit_mgmt_sessions, session_timeout_ms, fun() -> 1000 end]),
    
    {ok, {{_Http, 201, _}, _, BodyJSON}} = req(post, rabbit_mgmt_test_util:uri_base_from(N1, Config) ++ "/session", 
                                               #{}, "test_user_a", "test_user_a", []),
    Body = decode_body(BodyJSON),
    SessionId = maps:get(<<"session_id">>, Body),
    
    %% Wait for TTL + gossip cleanup
    timer:sleep(7000),
    
    %% Should be 401 now
    {ok, {{_, Status, _}, _, _}} = req(put, rabbit_mgmt_test_util:uri_base_from(N1, Config) ++ "/session/" ++ binary_to_list(SessionId), 
                                        #{}, "test_user_a", "test_user_a", []),
    ?assertEqual(401, Status),
    
    rpc(Config, N1, meck, unload, [rabbit_mgmt_sessions]),
    passed.
