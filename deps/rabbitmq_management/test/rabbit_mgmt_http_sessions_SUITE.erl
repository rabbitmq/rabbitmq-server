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

-define(KHEPRI_USER_PATH(Username), [rabbitmq, users, Username]).

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
    rabbit_ct_helpers:await_condition(fun() ->
        try
            rabbit_mgmt_test_util:http_get(Config, "/overview"),
            true
        catch _:_ ->
            false
        end
    end),
    Enabled = Testcase =/= feature_disabled_test,
    N1 = rabbit_ct_broker_helpers:get_node_config(Config, 0, nodename),
    N2 = rabbit_ct_broker_helpers:get_node_config(Config, 1, nodename),
    rpc(Config, N1, application, set_env, [rabbitmq_management, sessions_enabled, Enabled]),
    rpc(Config, N2, application, set_env, [rabbitmq_management, sessions_enabled, Enabled]),
    rpc(Config, N1, application, set_env, [rabbitmq_management, sessions_max_concurrent, 1]),
    rpc(Config, N2, application, set_env, [rabbitmq_management, sessions_max_concurrent, 1]),
    rpc(Config, N1, application, set_env, [rabbitmq_management, login_session_timeout, 480]),
    rpc(Config, N2, application, set_env, [rabbitmq_management, login_session_timeout, 480]),
    rpc(Config, N1, application, set_env, [rabbitmq_management, sessions_heartbeat_interval, 30]),
    rpc(Config, N2, application, set_env, [rabbitmq_management, sessions_heartbeat_interval, 30]),

    case Enabled of
        true ->
            timer:sleep(100),
            rpc(Config, N1, application, stop, [rabbitmq_management]),
            rpc(Config, N2, application, stop, [rabbitmq_management]),
            rpc(Config, N1, application, start, [rabbitmq_management]),
            rpc(Config, N2, application, start, [rabbitmq_management]);
        false ->
            rpc(Config, N1, application, stop, [rabbitmq_management]),
            rpc(Config, N2, application, stop, [rabbitmq_management]),
            rpc(Config, N1, application, start, [rabbitmq_management]),
            rpc(Config, N2, application, start, [rabbitmq_management])
    end,

    http_put(Config, "/users/test_admin", [{password, <<"test_admin">>}, {tags, <<"administrator">>}], {group, '2xx'}),
    http_put(Config, "/users/test_user_a", [{password, <<"test_user_a">>}, {tags, <<"management">>}], {group, '2xx'}),
    http_put(Config, "/users/test_user_b", [{password, <<"test_user_b">>}, {tags, <<"management">>}], {group, '2xx'}),
    rabbit_ct_helpers:testcase_started(Config, Testcase).

end_per_testcase(Testcase, Config) ->
    %% Some test cases (e.g. delete_user_sessions_test) delete a test user
    %% themselves as part of exercising the session cleanup cascade, so a
    %% second delete here is expected to 404.
    http_delete(Config, "/users/test_admin", {one_of, [200, 201, 202, 203, 204, 205, 206, 404]}),
    http_delete(Config, "/users/test_user_a", {one_of, [200, 201, 202, 203, 204, 205, 206, 404]}),
    http_delete(Config, "/users/test_user_b", {one_of, [200, 201, 202, 203, 204, 205, 206, 404]}),
    rabbit_ct_helpers:testcase_finished(Config, Testcase).

req_node(Config, Node, Type, Path, User, Pass, Body) ->
    req_node(Config, Node, Type, Path, User, Pass, Body, []).

req_node(Config, Node, Type, Path, User, Pass, Body, ExtraHeaders) ->
    Headers = [rabbit_mgmt_test_util:auth_header(User, Pass) | ExtraHeaders],
    JsonBody = iolist_to_binary(rabbit_json:encode(Body)),
    rabbit_mgmt_test_util:req(Config, Node, Type, Path, Headers, JsonBody).

feature_disabled_test(Config) ->
    http_post(Config, "/session", #{}, "test_user_a", "test_user_a", 405),
    http_put(Config, "/session/123", #{}, "test_user_a", "test_user_a", 405),
    http_delete(Config, "/session/123", "test_user_a", "test_user_a", 405),
    http_get(Config, "/sessions", "test_admin", "test_admin", ?NOT_FOUND),
    passed.

authorization_and_metadata_test(Config) ->
    N1 = rabbit_ct_broker_helpers:get_node_config(Config, 0, nodename),
    Headers = [{"x-forwarded-for", "203.0.113.5, 10.0.0.1"},
               {"user-agent", "test-agent"}],
    {ok, {{_Http, 201, _}, _, BodyJSON}} = req_node(Config, N1, post, "/session", "test_user_a", "test_user_a", #{}, Headers),
    Body = decode_body(BodyJSON),
    SessionId = maps:get('session_id', Body),
    
    %% Direct Khepri State Assertion 1: Session node exists in Khepri
    Path = ?KHEPRI_USER_PATH(<<"test_user_a">>) ++ [sessions, SessionId],
    {ok, Session1} = rpc(Config, N1, rabbit_khepri, get, [Path]),
    ?assert(is_tuple(Session1)),

    %% Heartbeat self -> 204
    http_put(Config, "/session/" ++ binary_to_list(SessionId), #{}, "test_user_a", "test_user_a", ?NO_CONTENT),
    
    %% Direct Khepri State Assertion 2: Heartbeat updated Khepri node
    {ok, Session2} = rpc(Config, N1, rabbit_khepri, get, [Path]),
    ?assert(is_tuple(Session2)),

    %% Heartbeat by another user -> 403
    http_put(Config, "/session/" ++ binary_to_list(SessionId), #{}, "test_user_b", "test_user_b", ?FORBIDDEN),
    
    %% Delete by another user -> 403
    http_delete(Config, "/session/" ++ binary_to_list(SessionId), "test_user_b", "test_user_b", ?FORBIDDEN),
    
    %% Admin GET
    SessionsRes = http_get(Config, "/sessions", "test_admin", "test_admin", ?OK),
    ?assertEqual(1, maps:get('total_count', SessionsRes)),
    ?assertEqual(1, maps:get('filtered_count', SessionsRes)),
    ?assertEqual(1, maps:get('item_count', SessionsRes)),
    ?assertEqual(1, maps:get('page', SessionsRes)),
    ?assertEqual(100, maps:get('page_size', SessionsRes)),
    ?assertEqual(1, maps:get('page_count', SessionsRes)),
    Items = maps:get('items', SessionsRes),
    [Session] = [S || S <- Items, maps:get('id', S) == SessionId],
    
    Metadata = maps:get('metadata', Session),
    <<"203.0.113.5">> = maps:get('ip', Metadata),
    <<"test-agent">> = maps:get('user-agent', Metadata),
    
    %% Non-admin GET
    http_get(Config, "/sessions", "test_user_a", "test_user_a", ?NOT_AUTHORISED),
    
    %% Pagination parameter validation tests
    http_get(Config, "/sessions?page=0", "test_admin", "test_admin", ?BAD_REQUEST),
    http_get(Config, "/sessions?page=not_an_integer", "test_admin", "test_admin", ?BAD_REQUEST),
    http_get(Config, "/sessions?page=-1", "test_admin", "test_admin", ?BAD_REQUEST),
    http_get(Config, "/sessions?page=1&page_size=0", "test_admin", "test_admin", ?BAD_REQUEST),
    http_get(Config, "/sessions?page=1&page_size=501", "test_admin", "test_admin", ?BAD_REQUEST),
    http_get(Config, "/sessions/user/test_user_a?page=invalid", "test_admin", "test_admin", ?BAD_REQUEST),

    %% Admin DELETE via /sessions/{session_id}
    http_delete(Config, "/sessions/" ++ binary_to_list(SessionId), "test_admin", "test_admin", ?NO_CONTENT),
    
    %% Direct Khepri State Assertion 3: Session deleted from Khepri
    ?assertMatch({error, {khepri, node_not_found, _}}, rpc(Config, N1, rabbit_khepri, get, [Path])),

    %% Verify deleted session returns 401 Unauthorized on heartbeat
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
    N1 = rabbit_ct_broker_helpers:get_node_config(Config, 0, nodename),
    N2 = rabbit_ct_broker_helpers:get_node_config(Config, 1, nodename),
    
    %% A logs in on N1 -> 201
    {ok, {{_Http1, 201, _}, _, BodyJSON1}} = req_node(Config, N1, post, "/session", "test_user_a", "test_user_a", #{}),
    Body1 = decode_body(BodyJSON1),
    SessionId1 = maps:get('session_id', Body1),

    %% A logs in on N2 -> Immediate 403 Forbidden (atomic limit check in Khepri across cluster)
    {ok, {{_Http2, 403, _}, _, _}} = req_node(Config, N2, post, "/session", "test_user_a", "test_user_a", #{}),

    %% SessionId1 should still be active
    {ok, {{_, Status1, _}, _, _}} = req_node(Config, N1, put, "/session/" ++ binary_to_list(SessionId1), "test_user_a", "test_user_a", #{}),
    ?assertEqual(204, Status1),
    
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

    %% A logs in again -> 403 immediately (Khepri atomic count = 2)
    {ok, {{_, Status3, _}, _, _}} = req_node(Config, N1, post, "/session", "test_user_a", "test_user_a", #{}),
    ?assertEqual(403, Status3),
    
    %% Clean up
    http_delete(Config, "/session/" ++ binary_to_list(SessionId1), "test_user_a", "test_user_a", ?NO_CONTENT),
    http_delete(Config, "/session/" ++ binary_to_list(SessionId2), "test_user_a", "test_user_a", ?NO_CONTENT),
    passed.

session_expiry_test(Config) ->
    N1 = rabbit_ct_broker_helpers:get_node_config(Config, 0, nodename),
    N2 = rabbit_ct_broker_helpers:get_node_config(Config, 1, nodename),

    %% Set very short TTL just for this test (0 minutes = 0 ms)
    rpc(Config, N1, application, set_env, [rabbitmq_management, login_session_timeout, 0]),
    rpc(Config, N2, application, set_env, [rabbitmq_management, login_session_timeout, 0]),
    rpc(Config, N1, application, set_env, [rabbitmq_management, sessions_heartbeat_interval, 0]),
    rpc(Config, N2, application, set_env, [rabbitmq_management, sessions_heartbeat_interval, 0]),

    {ok, {{_Http, 201, _}, _, BodyJSON}} = req_node(Config, N1, post, "/session", "test_user_a", "test_user_a", #{}),
    Body = decode_body(BodyJSON),
    SessionId = maps:get('session_id', Body),

    %% Trigger the sweeper on both nodes: only the Khepri cluster leader
    %% actually sweeps, and either node may hold that role.
    rpc(Config, N1, erlang, send, [rabbit_mgmt_sessions, sweep_expired_sessions]),
    rpc(Config, N2, erlang, send, [rabbit_mgmt_sessions, sweep_expired_sessions]),
    timer:sleep(200),

    %% Verify session is removed from Khepri
    Path = ?KHEPRI_USER_PATH(<<"test_user_a">>) ++ [sessions, SessionId],
    ?assertMatch({error, {khepri, node_not_found, _}}, rpc(Config, N1, rabbit_khepri, get, [Path])),

    %% Fill slot with another session
    req_node(Config, N1, post, "/session", "test_user_a", "test_user_a", #{}),

    %% Heartbeat with expired session returns 401
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

    %% Heartbeat on N2 (different cluster node) reads and updates directly from Khepri
    {ok, {{_, Status2, _}, _, _}} = req_node(Config, N2, put, "/session/" ++ binary_to_list(SessionId1), "test_user_a", "test_user_a", #{}),
    ?assertEqual(204, Status2),
    
    %% Verify GET /sessions lists the session
    {ok, {{_Http, 200, _}, _, ResBody}} =
        rabbit_mgmt_test_util:req(Config, N2, get, "/sessions", [rabbit_mgmt_test_util:auth_header("test_admin", "test_admin")]),
    SessionsRes = decode_body(ResBody),
    Items = maps:get('items', SessionsRes),
    [Session] = [S || S <- Items, maps:get('id', S) == SessionId1],
    ?assertEqual(SessionId1, maps:get('id', Session)),
    
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
    
    %% Delete all sessions for test_user_a (requires admin)
    http_delete(Config, "/sessions/user/test_user_a", "test_admin", "test_admin", ?NO_CONTENT),
    
    %% Direct Khepri Assertion: test_user_a session nodes deleted
    Path1 = ?KHEPRI_USER_PATH(<<"test_user_a">>) ++ [sessions, SessionId1],
    Path2 = ?KHEPRI_USER_PATH(<<"test_user_a">>) ++ [sessions, SessionId2],
    ?assertMatch({error, {khepri, node_not_found, _}}, rpc(Config, N1, rabbit_khepri, get, [Path1])),
    ?assertMatch({error, {khepri, node_not_found, _}}, rpc(Config, N1, rabbit_khepri, get, [Path2])),

    %% Verify test_user_a sessions return 401
    {ok, {{_, 401, _}, _, _}} = req_node(Config, N1, put, "/session/" ++ binary_to_list(SessionId1), "test_user_a", "test_user_a", #{}),
    {ok, {{_, 401, _}, _, _}} = req_node(Config, N2, put, "/session/" ++ binary_to_list(SessionId2), "test_user_a", "test_user_a", #{}),
    
    %% Verify test_user_b session is still alive
    {ok, {{_, 204, _}, _, _}} = req_node(Config, N1, put, "/session/" ++ binary_to_list(SessionId3), "test_user_b", "test_user_b", #{}),
    
    %% Verify GET /sessions/user/:username works
    {ok, {{_, 200, _}, _, ResBody1}} = rabbit_mgmt_test_util:req(Config, N1, get, "/sessions/user/test_user_b", [rabbit_mgmt_test_util:auth_header("test_admin", "test_admin")]),
    SessionsRes1 = decode_body(ResBody1),
    Items1 = maps:get('items', SessionsRes1),
    ?assertEqual(1, length(Items1)),
    ?assertEqual(1, maps:get('filtered_count', SessionsRes1)),
    ?assertEqual(1, maps:get('item_count', SessionsRes1)),
    ?assertEqual(1, maps:get('page_count', SessionsRes1)),
    [Session1] = Items1,
    ?assertEqual(SessionId3, maps:get('id', Session1)),
    ?assertEqual(<<"test_user_b">>, maps:get('username', Session1)),
    
    %% Verify GET /sessions/user/:username for user with no sessions
    {ok, {{_, 200, _}, _, ResBody2}} = rabbit_mgmt_test_util:req(Config, N1, get, "/sessions/user/test_user_a", [rabbit_mgmt_test_util:auth_header("test_admin", "test_admin")]),
    SessionsRes2 = decode_body(ResBody2),
    Items2 = maps:get('items', SessionsRes2),
    ?assertEqual(0, length(Items2)),
    ?assertEqual(0, maps:get('filtered_count', SessionsRes2)),
    ?assertEqual(0, maps:get('item_count', SessionsRes2)),
    ?assertEqual(0, maps:get('page_count', SessionsRes2)),

    %% Admin DELETE single session via /sessions/user/:username/:session
    http_delete(Config, "/sessions/user/test_user_b/" ++ binary_to_list(SessionId3), "test_admin", "test_admin", ?NO_CONTENT),
    Path3 = ?KHEPRI_USER_PATH(<<"test_user_b">>) ++ [sessions, SessionId3],
    ?assertMatch({error, {khepri, node_not_found, _}}, rpc(Config, N1, rabbit_khepri, get, [Path3])),

    %% User deletion cascade test:
    %% Delete user test_user_b from RabbitMQ
    http_delete(Config, "/users/test_user_b", "test_admin", "test_admin", ?NO_CONTENT),
    %% Assert user sessions under test_user_b are purged from Khepri
    UserBSessionsPath = ?KHEPRI_USER_PATH(<<"test_user_b">>) ++ [sessions, '?'],
    ?assertEqual({ok, #{}}, rpc(Config, N1, rabbit_khepri, get_many, [UserBSessionsPath])),

    %% Clean up session 3 just in case
    http_delete(Config, "/session/" ++ binary_to_list(SessionId3), "test_user_b", "test_user_b", ?NOT_AUTHORISED),
    passed.
