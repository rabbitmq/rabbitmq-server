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
        session_expiry_test,
        auto_resume_orphaned_session_test,
        delete_user_sessions_test
    ].

init_per_suite(Config) ->
    %% We need a 2-node cluster for the distributed tests
    rabbit_ct_helpers:log_environment(),
    inets:start(),
    Config1 = rabbit_ct_helpers:set_config(Config, [
        {rmq_nodename_suffix, ?MODULE},
        {rmq_nodes_count, 2}
    ]),
    rabbit_ct_helpers:run_setup_steps(Config1,
        rabbit_ct_broker_helpers:setup_steps() ++
        rabbit_ct_client_helpers:setup_steps()).

end_per_suite(Config) ->
    inets:stop(),
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
    N1 = rabbit_ct_broker_helpers:get_node_config(Config, 0, nodename),
    N2 = rabbit_ct_broker_helpers:get_node_config(Config, 1, nodename),
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

%% req/6 targets a specific node and needs Basic auth headers and a JSON-encoded
%% body, none of which the Config-only http_* helpers provide.
req_node(Config, Node, Type, Path, User, Pass, Body) ->
    req_node(Config, Node, Type, Path, User, Pass, Body, []).

req_node(Config, Node, Type, Path, User, Pass, Body, ExtraHeaders) ->
    Headers = [rabbit_mgmt_test_util:auth_header(User, Pass) | ExtraHeaders],
    JsonBody = iolist_to_binary(rabbit_json:encode(Body)),
    rabbit_mgmt_test_util:req(Config, Node, Type, Path, Headers, JsonBody).

feature_disabled_test(Config) ->
    %% POST
    http_post(Config, "/session", #{}, "test_user_a", "test_user_a", 405),
    %% PUT
    http_put(Config, "/session/123", #{}, "test_user_a", "test_user_a", 405),
    %% DELETE (user)
    http_delete(Config, "/session/123", "test_user_a", "test_user_a", 405),
    %% GET admin
    http_get(Config, "/sessions", "test_admin", "test_admin", ?NOT_FOUND),
    passed.

authorization_and_metadata_test(Config) ->
    Headers = [{"x-forwarded-for", "203.0.113.5, 10.0.0.1"},
               {"user-agent", "test-agent"}],
    {ok, {{_Http, 201, _}, _, BodyJSON}} = req_node(Config, 0, post, "/session", "test_user_a", "test_user_a", #{}, Headers),
    Body = decode_body(BodyJSON),
    SessionId = maps:get('session_id', Body),
    
    %% Heartbeat self -> 204
    http_put(Config, "/session/" ++ binary_to_list(SessionId), #{}, "test_user_a", "test_user_a", ?NO_CONTENT),
    
    %% Heartbeat by another user -> 403
    http_put(Config, "/session/" ++ binary_to_list(SessionId), #{}, "test_user_b", "test_user_b", ?FORBIDDEN),
    
    %% Delete by another user -> 403
    http_delete(Config, "/session/" ++ binary_to_list(SessionId), "test_user_b", "test_user_b", ?FORBIDDEN),
    
    %% Admin GET
    SessionsRes = http_get(Config, "/sessions", "test_admin", "test_admin", ?OK),
    Items = maps:get('items', SessionsRes),
    [Session] = [S || S <- Items, maps:get('id', S) == SessionId],
    
    Metadata = maps:get('metadata', Session),
    <<"203.0.113.5">> = maps:get('ip', Metadata),
    <<"test-agent">> = maps:get('user-agent', Metadata),
    
    %% Non-admin GET
    http_get(Config, "/sessions", "test_user_a", "test_user_a", ?NOT_AUTHORISED),
    
    %% Admin DELETE
    http_delete(Config, "/sessions/" ++ binary_to_list(SessionId), "test_admin", "test_admin", ?NO_CONTENT),
    
    %% Verify deleted. To prevent auto-resume, fill the limit for test_user_a first.
    {ok, {{_, 201, _}, _, FillerBodyJSON}} = req_node(Config, 0, post, "/session", "test_user_a", "test_user_a", #{}),
    FillerBody = decode_body(FillerBodyJSON),
    FillerSessionId = maps:get('session_id', FillerBody),

    http_put(Config, "/session/" ++ binary_to_list(SessionId), #{}, "test_user_a", "test_user_a", ?NOT_AUTHORISED),
    
    %% Clean up filler session
    http_delete(Config, "/session/" ++ binary_to_list(FillerSessionId), "test_user_a", "test_user_a", ?NO_CONTENT),
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
    N1 = rabbit_ct_broker_helpers:get_node_config(Config, 0, nodename),
    N2 = rabbit_ct_broker_helpers:get_node_config(Config, 1, nodename),
    
    %% A logs in on N1 -> 201
    {ok, {{_Http1, 201, _}, _, BodyJSON1}} = req_node(Config, N1, post, "/session", "test_user_a", "test_user_a", #{}),
    Body1 = decode_body(BodyJSON1),
    SessionId1 = maps:get('session_id', Body1),

    %% A logs in on N2 -> 201
    {ok, {{_Http2, 201, _}, _, BodyJSON2}} = req_node(Config, N2, post, "/session", "test_user_a", "test_user_a", #{}),
    Body2 = decode_body(BodyJSON2),
    SessionId2 = maps:get('session_id', Body2),

    %% Wait for gossip (5 seconds) + buffer
    timer:sleep(6000),

    %% Check. The NEWER session (SessionId2) should be terminated.
    %% SessionId1 should be kept.
    {ok, {{_, Status1, _}, _, _}} = req_node(Config, N1, put, "/session/" ++ binary_to_list(SessionId1), "test_user_a", "test_user_a", #{}),
    {ok, {{_, Status2, _}, _, _}} = req_node(Config, N2, put, "/session/" ++ binary_to_list(SessionId2), "test_user_a", "test_user_a", #{}),

    ?assertEqual(204, Status1),
    ?assertEqual(401, Status2),
    
    %% Clean up
    http_delete(Config, "/session/" ++ binary_to_list(SessionId1), "test_user_a", "test_user_a", ?NO_CONTENT),
    passed.

distributed_session_counting_test(Config) ->
    N1 = rabbit_ct_broker_helpers:get_node_config(Config, 0, nodename),
    N2 = rabbit_ct_broker_helpers:get_node_config(Config, 1, nodename),
    
    %% Set limit to 2
    rpc(Config, N1, application, set_env, [rabbitmq_management, sessions_max_concurrent, 2]),
    rpc(Config, N2, application, set_env, [rabbitmq_management, sessions_max_concurrent, 2]),
    
    %% A logs in on N1 -> 201
    {ok, {{_, 201, _}, _, BodyJSON1}} = req_node(Config, N1, post, "/session", "test_user_a", "test_user_a", #{}),
    Body1 = decode_body(BodyJSON1),
    SessionId1 = maps:get('session_id', Body1),

    %% A logs in on N2 -> 201
    {ok, {{_, 201, _}, _, BodyJSON2}} = req_node(Config, N2, post, "/session", "test_user_a", "test_user_a", #{}),
    Body2 = decode_body(BodyJSON2),
    SessionId2 = maps:get('session_id', Body2),

    %% Wait for gossip
    timer:sleep(6000),

    %% A logs in again -> 403
    {ok, {{_, Status3, _}, _, _}} = req_node(Config, N1, post, "/session", "test_user_a", "test_user_a", #{}),
    
    ?assertEqual(403, Status3),
    
    %% Clean up
    http_delete(Config, "/session/" ++ binary_to_list(SessionId1), "test_user_a", "test_user_a", ?NO_CONTENT),
    http_delete(Config, "/session/" ++ binary_to_list(SessionId2), "test_user_a", "test_user_a", ?NO_CONTENT),
    passed.

session_expiry_test(Config) ->
    N1 = rabbit_ct_broker_helpers:get_node_config(Config, 0, nodename),
    
    %% Set very short TTL just for this test (0 minutes = 0 ms)
    rpc(Config, N1, application, set_env, [rabbitmq_management, login_session_timeout, 0]),
    rpc(Config, N1, application, set_env, [rabbitmq_management, sessions_heartbeat_interval, 0]),
    
    {ok, {{_Http, 201, _}, _, BodyJSON}} = req_node(Config, N1, post, "/session", "test_user_a", "test_user_a", #{}),
    Body = decode_body(BodyJSON),
    SessionId = maps:get('session_id', Body),
    
    %% Wait for TTL + gossip cleanup (broadcast interval is 5s)
    timer:sleep(7000),
    
    %% Should be 404 now (not found, because it expired and we don't adopt expired ones if limit is reached, 
    %% wait, if limit is NOT reached, it would adopt it! But since it's a completely new session id adoption, 
    %% it will adopt it if we just send a heartbeat. BUT wait, if we adopt it, it returns 200!
    %% Let's check: if we send a heartbeat for an expired session, it's not in local, not in remote.
    %% The auto-resume logic will see Count (0) < MaxConcurrent (1), and ADOPT it!
    %% So it will return 204 No Content. Is this what we want for expired sessions?
    %% Yes, if a user sends a heartbeat with a valid token, and they have free slots, we create a session for them.
    %% BUT wait, if the token is valid, they are authenticated. The session is just a UI construct.
    %% If we want it to fail, we need to exceed the limit.
    %% Let's just create another session to fill the limit, then the heartbeat will fail with 401.
    req_node(Config, N1, post, "/session", "test_user_a", "test_user_a", #{}),

    {ok, {{_, Status, _}, _, _}} = req_node(Config, N1, put, "/session/" ++ binary_to_list(SessionId), "test_user_a", "test_user_a", #{}),
    ?assertEqual(401, Status),
    
    passed.

auto_resume_orphaned_session_test(Config) ->
    N1 = rabbit_ct_broker_helpers:get_node_config(Config, 0, nodename),
    N2 = rabbit_ct_broker_helpers:get_node_config(Config, 1, nodename),
    
    %% A logs in on N1 -> 201
    {ok, {{_Http1, 201, _}, _, BodyJSON1}} = req_node(Config, N1, post, "/session", "test_user_a", "test_user_a", #{}),
    Body1 = decode_body(BodyJSON1),
    SessionId1 = maps:get('session_id', Body1),

    %% Simulate N1 crashing by deleting the session directly from N1's memory
    %% (This avoids actually stopping the node which would break the test framework's expectations)
    rpc(Config, N1, rabbit_mgmt_sessions, delete_session, [SessionId1]),

    %% Wait for gossip to propagate the deletion to N2
    timer:sleep(6000),

    %% Send heartbeat to N2. N2 does not have the session locally, nor remotely.
    %% But since test_user_a now has 0 sessions (well under max_concurrent=1),
    %% N2 should ADOPT the session and return 204 No Content.
    {ok, {{_, Status2, _}, _, _}} = req_node(Config, N2, put, "/session/" ++ binary_to_list(SessionId1), "test_user_a", "test_user_a", #{}),
    
    ?assertEqual(204, Status2),
    
    %% Verify the session is now owned by N2. Query N2 directly since gossip to N1 might not have happened yet.
    {ok, {{_Http, 200, _}, _, ResBody}} =
        rabbit_mgmt_test_util:req(Config, N2, get, "/sessions", [rabbit_mgmt_test_util:auth_header("test_admin", "test_admin")]),
    SessionsRes = decode_body(ResBody),
    Items = maps:get('items', SessionsRes),
    [Session] = [S || S <- Items, maps:get('id', S) == SessionId1],
    
    ExpectedNodeBin = atom_to_binary(N2, utf8),
    ?assertEqual(ExpectedNodeBin, maps:get('node', Session)),
    
    %% Clean up
    http_delete(Config, "/session/" ++ binary_to_list(SessionId1), "test_user_a", "test_user_a", ?NO_CONTENT),
    passed.

delete_user_sessions_test(Config) ->
    N1 = rabbit_ct_broker_helpers:get_node_config(Config, 0, nodename),
    N2 = rabbit_ct_broker_helpers:get_node_config(Config, 1, nodename),
    
    %% Increase limit so we can create multiple sessions
    rpc(Config, N1, application, set_env, [rabbitmq_management, sessions_max_concurrent, 5]),
    rpc(Config, N2, application, set_env, [rabbitmq_management, sessions_max_concurrent, 5]),
    
    %% Create 2 sessions for test_user_a (one on N1, one on N2)
    {ok, {{_, 201, _}, _, BodyJSON1}} = req_node(Config, N1, post, "/session", "test_user_a", "test_user_a", #{}),
    SessionId1 = maps:get('session_id', decode_body(BodyJSON1)),
    
    {ok, {{_, 201, _}, _, BodyJSON2}} = req_node(Config, N2, post, "/session", "test_user_a", "test_user_a", #{}),
    SessionId2 = maps:get('session_id', decode_body(BodyJSON2)),
    
    %% Create 1 session for test_user_b on N1
    {ok, {{_, 201, _}, _, BodyJSON3}} = req_node(Config, N1, post, "/session", "test_user_b", "test_user_b", #{}),
    SessionId3 = maps:get('session_id', decode_body(BodyJSON3)),
    
    %% Wait for gossip to propagate
    timer:sleep(6000),
    
    %% Delete all sessions for test_user_a (requires admin)
    http_delete(Config, "/sessions/user/test_user_a", "test_admin", "test_admin", ?NO_CONTENT),
    
    %% Wait for broadcast to propagate the deletion
    timer:sleep(1000),
    
    %% Verify test_user_a sessions are gone
    {ok, {{_, 401, _}, _, _}} = req_node(Config, N1, put, "/session/" ++ binary_to_list(SessionId1), "test_user_a", "test_user_a", #{}),
    {ok, {{_, 401, _}, _, _}} = req_node(Config, N2, put, "/session/" ++ binary_to_list(SessionId2), "test_user_a", "test_user_a", #{}),
    
    %% Verify test_user_b session is still alive
    {ok, {{_, 204, _}, _, _}} = req_node(Config, N1, put, "/session/" ++ binary_to_list(SessionId3), "test_user_b", "test_user_b", #{}),
    
    %% Verify GET /sessions/user/:username works
    {ok, {{_, 200, _}, _, ResBody1}} = rabbit_mgmt_test_util:req(Config, N1, get, "/sessions/user/test_user_b", [rabbit_mgmt_test_util:auth_header("test_admin", "test_admin")]),
    SessionsRes1 = decode_body(ResBody1),
    Items1 = maps:get('items', SessionsRes1),
    ?assertEqual(1, length(Items1)),
    [Session1] = Items1,
    ?assertEqual(SessionId3, maps:get('id', Session1)),
    ?assertEqual(<<"test_user_b">>, maps:get('username', Session1)),
    
    %% Verify GET /sessions/user/:username for user with no sessions
    {ok, {{_, 200, _}, _, ResBody2}} = rabbit_mgmt_test_util:req(Config, N1, get, "/sessions/user/test_user_a", [rabbit_mgmt_test_util:auth_header("test_admin", "test_admin")]),
    SessionsRes2 = decode_body(ResBody2),
    Items2 = maps:get('items', SessionsRes2),
    ?assertEqual(0, length(Items2)),
    
    %% Clean up test_user_b
    http_delete(Config, "/session/" ++ binary_to_list(SessionId3), "test_user_b", "test_user_b", ?NO_CONTENT),
    passed.
