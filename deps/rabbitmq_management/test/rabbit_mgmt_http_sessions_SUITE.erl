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

-define(KHEPRI_USER_SESSIONS_PATH(Username), [rabbitmq, mgmt_sessions, Username]).

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
        session_metadata_without_proxy_test,
        session_metadata_behind_proxy_test,
        session_metadata_forwarded_for_is_truncated_test,
        concurrency_limits_test,
        distributed_conflict_resolution_test,
        distributed_session_counting_test,
        session_expiry_test,
        stale_sessions_are_deleted_on_create_test,
        session_expiry_is_capped_by_token_expiry_test,
        auto_resume_orphaned_session_test,
        delete_user_sessions_test,
        session_removed_with_internal_user_test,
        session_for_user_without_internal_record_test
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
    %% `delete_user_sessions_test` deletes a user itself, so a 404 is expected here.
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
    
    Path = ?KHEPRI_USER_SESSIONS_PATH(<<"test_user_a">>) ++ [SessionId],
    {ok, Session1} = rpc(Config, N1, rabbit_khepri, get, [Path]),
    ?assert(is_map(Session1)),

    http_put(Config, "/session/" ++ binary_to_list(SessionId), #{}, "test_user_a", "test_user_a", ?NO_CONTENT),
    
    {ok, Session2} = rpc(Config, N1, rabbit_khepri, get, [Path]),
    ?assert(is_map(Session2)),

    http_put(Config, "/session/" ++ binary_to_list(SessionId), #{}, "test_user_b", "test_user_b", ?FORBIDDEN),
    
    http_delete(Config, "/session/" ++ binary_to_list(SessionId), "test_user_b", "test_user_b", ?FORBIDDEN),
    
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
    ?assertNotEqual(<<"203.0.113.5">>, maps:get('ip', Metadata)),
    <<"203.0.113.5, 10.0.0.1">> = maps:get(forwarded_for, Metadata),
    <<"test-agent">> = maps:get('user-agent', Metadata),
    
    http_get(Config, "/sessions", "test_user_a", "test_user_a", ?NOT_AUTHORISED),
    
    http_get(Config, "/sessions?page=0", "test_admin", "test_admin", ?BAD_REQUEST),
    http_get(Config, "/sessions?page=not_an_integer", "test_admin", "test_admin", ?BAD_REQUEST),
    http_get(Config, "/sessions?page=-1", "test_admin", "test_admin", ?BAD_REQUEST),
    http_get(Config, "/sessions?page=1&page_size=0", "test_admin", "test_admin", ?BAD_REQUEST),
    http_get(Config, "/sessions?page=1&page_size=501", "test_admin", "test_admin", ?BAD_REQUEST),
    http_get(Config, "/sessions/user/test_user_a?page=invalid", "test_admin", "test_admin", ?BAD_REQUEST),

    http_delete(Config, "/sessions/" ++ binary_to_list(SessionId), "test_admin", "test_admin", ?NO_CONTENT),
    
    ?assertMatch({error, {khepri, node_not_found, _}}, rpc(Config, N1, rabbit_khepri, get, [Path])),

    http_put(Config, "/session/" ++ binary_to_list(SessionId), #{}, "test_user_a", "test_user_a", ?NOT_AUTHORISED),
    passed.

concurrency_limits_test(Config) ->
    
    http_post(Config, "/session", #{}, "test_user_a", "test_user_a", ?CREATED),
    
    http_post(Config, "/session", #{}, "test_user_b", "test_user_b", ?CREATED),
    
    http_post(Config, "/session", #{}, "test_user_a", "test_user_a", ?FORBIDDEN),
    passed.

distributed_conflict_resolution_test(Config) ->
    N1 = rabbit_ct_broker_helpers:get_node_config(Config, 0, nodename),
    N2 = rabbit_ct_broker_helpers:get_node_config(Config, 1, nodename),
    
    {ok, {{_Http1, 201, _}, _, BodyJSON1}} = req_node(Config, N1, post, "/session", "test_user_a", "test_user_a", #{}),
    Body1 = decode_body(BodyJSON1),
    SessionId1 = maps:get('session_id', Body1),

    {ok, {{_Http2, 403, _}, _, _}} = req_node(Config, N2, post, "/session", "test_user_a", "test_user_a", #{}),

    {ok, {{_, Status1, _}, _, _}} = req_node(Config, N1, put, "/session/" ++ binary_to_list(SessionId1), "test_user_a", "test_user_a", #{}),
    ?assertEqual(204, Status1),
    
    http_delete(Config, "/session/" ++ binary_to_list(SessionId1), "test_user_a", "test_user_a", ?NO_CONTENT),
    passed.

distributed_session_counting_test(Config) ->
    N1 = rabbit_ct_broker_helpers:get_node_config(Config, 0, nodename),
    N2 = rabbit_ct_broker_helpers:get_node_config(Config, 1, nodename),
    
    rpc(Config, N1, application, set_env, [rabbitmq_management, sessions_max_concurrent, 2]),
    rpc(Config, N2, application, set_env, [rabbitmq_management, sessions_max_concurrent, 2]),
    
    {ok, {{_, 201, _}, _, BodyJSON1}} = req_node(Config, N1, post, "/session", "test_user_a", "test_user_a", #{}),
    Body1 = decode_body(BodyJSON1),
    SessionId1 = maps:get('session_id', Body1),

    {ok, {{_, 201, _}, _, BodyJSON2}} = req_node(Config, N2, post, "/session", "test_user_a", "test_user_a", #{}),
    Body2 = decode_body(BodyJSON2),
    SessionId2 = maps:get('session_id', Body2),

    {ok, {{_, Status3, _}, _, _}} = req_node(Config, N1, post, "/session", "test_user_a", "test_user_a", #{}),
    ?assertEqual(403, Status3),
    
    http_delete(Config, "/session/" ++ binary_to_list(SessionId1), "test_user_a", "test_user_a", ?NO_CONTENT),
    http_delete(Config, "/session/" ++ binary_to_list(SessionId2), "test_user_a", "test_user_a", ?NO_CONTENT),
    passed.

session_expiry_test(Config) ->
    N1 = rabbit_ct_broker_helpers:get_node_config(Config, 0, nodename),
    N2 = rabbit_ct_broker_helpers:get_node_config(Config, 1, nodename),

    rpc(Config, N1, application, set_env, [rabbitmq_management, login_session_timeout, 0]),
    rpc(Config, N2, application, set_env, [rabbitmq_management, login_session_timeout, 0]),
    rpc(Config, N1, application, set_env, [rabbitmq_management, sessions_heartbeat_interval, 0]),
    rpc(Config, N2, application, set_env, [rabbitmq_management, sessions_heartbeat_interval, 0]),

    {ok, {{_Http, 201, _}, _, BodyJSON}} = req_node(Config, N1, post, "/session", "test_user_a", "test_user_a", #{}),
    Body = decode_body(BodyJSON),
    SessionId = maps:get('session_id', Body),

    %% Only the Khepri leader sweeps, and either node can be the leader.
    rpc(Config, N1, erlang, send, [rabbit_mgmt_sessions, sweep_expired_sessions]),
    rpc(Config, N2, erlang, send, [rabbit_mgmt_sessions, sweep_expired_sessions]),

    Path = ?KHEPRI_USER_SESSIONS_PATH(<<"test_user_a">>) ++ [SessionId],
    ?awaitMatch({error, {khepri, node_not_found, _}}, rpc(Config, N1, rabbit_khepri, get, [Path]), 30000),
    ?awaitMatch({error, {khepri, node_not_found, _}},
                rpc(Config, N1, rabbit_khepri, get, [?KHEPRI_USER_SESSIONS_PATH(<<"test_user_a">>)]),
                30000),

    req_node(Config, N1, post, "/session", "test_user_a", "test_user_a", #{}),

    {ok, {{_, Status, _}, _, _}} = req_node(Config, N1, put, "/session/" ++ binary_to_list(SessionId), "test_user_a", "test_user_a", #{}),
    ?assertEqual(401, Status),
    
    passed.

stale_sessions_are_deleted_on_create_test(Config) ->
    N1 = rabbit_ct_broker_helpers:get_node_config(Config, 0, nodename),
    Username = <<"stale_user">>,

    rpc(Config, N1, application, set_env, [rabbitmq_management, login_session_timeout, 0]),
    rpc(Config, N1, application, set_env, [rabbitmq_management, sessions_heartbeat_interval, 0]),
    {ok, StaleId} = rpc(Config, N1, rabbit_mgmt_sessions, create_session, [Username, #{}, never]),
    StalePath = ?KHEPRI_USER_SESSIONS_PATH(Username) ++ [StaleId],
    ?assertMatch({ok, _}, rpc(Config, N1, rabbit_khepri, get, [StalePath])),

    rpc(Config, N1, application, set_env, [rabbitmq_management, login_session_timeout, 480]),
    rpc(Config, N1, application, set_env, [rabbitmq_management, sessions_heartbeat_interval, 30]),
    {ok, SessionId} = rpc(Config, N1, rabbit_mgmt_sessions, create_session, [Username, #{}, never]),

    ?assertMatch({error, {khepri, node_not_found, _}}, rpc(Config, N1, rabbit_khepri, get, [StalePath])),
    ?assertMatch({ok, _}, rpc(Config, N1, rabbit_khepri, get, [?KHEPRI_USER_SESSIONS_PATH(Username) ++ [SessionId]])),

    ok = rpc(Config, N1, rabbit_mgmt_sessions, delete_session, [SessionId, Username]),
    passed.

session_expiry_is_capped_by_token_expiry_test(Config) ->
    N1 = rabbit_ct_broker_helpers:get_node_config(Config, 0, nodename),
    Username = <<"token_user">>,
    TokenExpiry = erlang:system_time(second) + 3,
    Path = fun(Id) -> ?KHEPRI_USER_SESSIONS_PATH(Username) ++ [Id] end,

    {ok, SessionId} = rpc(Config, N1, rabbit_mgmt_sessions, create_session, [Username, #{}, TokenExpiry]),
    {ok, #{expires_at := ExpiresAt1}} = rpc(Config, N1, rabbit_khepri, get, [Path(SessionId)]),
    ?assertEqual(TokenExpiry * 1000, ExpiresAt1),

    ?assertMatch(ok, rpc(Config, N1, rabbit_mgmt_sessions, touch, [SessionId, Username, TokenExpiry])),
    {ok, #{expires_at := ExpiresAt2}} = rpc(Config, N1, rabbit_khepri, get, [Path(SessionId)]),
    ?assertEqual(TokenExpiry * 1000, ExpiresAt2),

    timer:sleep(3500),
    ?assertEqual({error, not_found}, rpc(Config, N1, rabbit_mgmt_sessions, touch, [SessionId, Username, TokenExpiry])),

    {ok, SessionId2} = rpc(Config, N1, rabbit_mgmt_sessions, create_session, [Username, #{}, never]),
    ?assertMatch({error, {khepri, node_not_found, _}}, rpc(Config, N1, rabbit_khepri, get, [Path(SessionId)])),
    ok = rpc(Config, N1, rabbit_mgmt_sessions, delete_session, [SessionId2, Username]),
    passed.

auto_resume_orphaned_session_test(Config) ->
    N1 = rabbit_ct_broker_helpers:get_node_config(Config, 0, nodename),
    N2 = rabbit_ct_broker_helpers:get_node_config(Config, 1, nodename),
    
    {ok, {{_Http1, 201, _}, _, BodyJSON1}} = req_node(Config, N1, post, "/session", "test_user_a", "test_user_a", #{}),
    Body1 = decode_body(BodyJSON1),
    SessionId1 = maps:get('session_id', Body1),

    {ok, {{_, Status2, _}, _, _}} = req_node(Config, N2, put, "/session/" ++ binary_to_list(SessionId1), "test_user_a", "test_user_a", #{}),
    ?assertEqual(204, Status2),
    
    {ok, {{_Http, 200, _}, _, ResBody}} =
        rabbit_mgmt_test_util:req(Config, N2, get, "/sessions", [rabbit_mgmt_test_util:auth_header("test_admin", "test_admin")]),
    SessionsRes = decode_body(ResBody),
    Items = maps:get('items', SessionsRes),
    [Session] = [S || S <- Items, maps:get('id', S) == SessionId1],
    ?assertEqual(SessionId1, maps:get('id', Session)),
    
    http_delete(Config, "/session/" ++ binary_to_list(SessionId1), "test_user_a", "test_user_a", ?NO_CONTENT),
    passed.

session_metadata_without_proxy_test(Config) ->
    N1 = rabbit_ct_broker_helpers:get_node_config(Config, 0, nodename),
    {ok, {{_, 201, _}, _, BodyJSON}} = req_node(Config, N1, post, "/session", "test_user_a", "test_user_a", #{}),
    SessionId = maps:get('session_id', decode_body(BodyJSON)),

    Metadata = session_metadata(Config, SessionId),
    ?assert(lists:member(maps:get(ip, Metadata), [<<"127.0.0.1">>, <<"::1">>])),
    ?assertNot(maps:is_key(forwarded_for, Metadata)),
    passed.

session_metadata_behind_proxy_test(Config) ->
    N1 = rabbit_ct_broker_helpers:get_node_config(Config, 0, nodename),
    Headers = [{"x-forwarded-for", "198.51.100.7, 10.0.0.1"}],
    {ok, {{_, 201, _}, _, BodyJSON}} = req_node(Config, N1, post, "/session", "test_user_a", "test_user_a", #{}, Headers),
    SessionId = maps:get('session_id', decode_body(BodyJSON)),

    Metadata = session_metadata(Config, SessionId),
    ?assert(lists:member(maps:get(ip, Metadata), [<<"127.0.0.1">>, <<"::1">>])),
    ?assertEqual(<<"198.51.100.7, 10.0.0.1">>, maps:get(forwarded_for, Metadata)),
    passed.

session_metadata_forwarded_for_is_truncated_test(Config) ->
    N1 = rabbit_ct_broker_helpers:get_node_config(Config, 0, nodename),
    Headers = [{"x-forwarded-for", lists:duplicate(300, $1)}],
    {ok, {{_, 201, _}, _, BodyJSON}} = req_node(Config, N1, post, "/session", "test_user_a", "test_user_a", #{}, Headers),
    SessionId = maps:get('session_id', decode_body(BodyJSON)),

    Metadata = session_metadata(Config, SessionId),
    ?assertEqual(256, byte_size(maps:get(forwarded_for, Metadata))),
    passed.

session_metadata(Config, SessionId) ->
    SessionsRes = http_get(Config, "/sessions", "test_admin", "test_admin", ?OK),
    [Session] = [S || S <- maps:get('items', SessionsRes), maps:get('id', S) == SessionId],
    maps:get('metadata', Session).

session_removed_with_internal_user_test(Config) ->
    N1 = rabbit_ct_broker_helpers:get_node_config(Config, 0, nodename),
    N2 = rabbit_ct_broker_helpers:get_node_config(Config, 1, nodename),
    {ok, {{_, 201, _}, _, BodyJSON}} = req_node(Config, N1, post, "/session", "test_user_b", "test_user_b", #{}),
    SessionId = maps:get('session_id', decode_body(BodyJSON)),
    Path = ?KHEPRI_USER_SESSIONS_PATH(<<"test_user_b">>) ++ [SessionId],
    ?assertMatch({ok, _}, rpc(Config, N1, rabbit_khepri, get, [Path])),

    true = rpc(Config, N2, rabbit_db_user, delete, [<<"test_user_b">>]),

    ?awaitMatch({error, {khepri, node_not_found, _}}, rpc(Config, N1, rabbit_khepri, get, [Path]), 30000),
    passed.

session_for_user_without_internal_record_test(Config) ->
    N1 = rabbit_ct_broker_helpers:get_node_config(Config, 0, nodename),
    Username = <<"external_user">>,
    {ok, SessionId} = rpc(Config, N1, rabbit_mgmt_sessions, create_session, [Username, #{}, never]),
    Path = ?KHEPRI_USER_SESSIONS_PATH(Username) ++ [SessionId],
    ?assertMatch({ok, _}, rpc(Config, N1, rabbit_khepri, get, [Path])),
    ?assertMatch({error, {khepri, node_not_found, _}},
                 rpc(Config, N1, rabbit_khepri, get, [[rabbitmq, users, Username]])),

    %% the session does not prevent creating an internal user with the same name
    ok = rpc(Config, N1, rabbit_auth_backend_internal, add_user, [Username, <<"pw">>, <<"guest">>]),
    ok = rpc(Config, N1, rabbit_auth_backend_internal, delete_user, [Username, <<"guest">>]),

    ok = rpc(Config, N1, rabbit_mgmt_sessions, delete_session, [SessionId, Username]),
    ?awaitMatch({error, {khepri, node_not_found, _}},
                rpc(Config, N1, rabbit_khepri, get, [?KHEPRI_USER_SESSIONS_PATH(Username)]),
                30000),
    passed.

delete_user_sessions_test(Config) ->
    N1 = rabbit_ct_broker_helpers:get_node_config(Config, 0, nodename),
    N2 = rabbit_ct_broker_helpers:get_node_config(Config, 1, nodename),
    
    rpc(Config, N1, application, set_env, [rabbitmq_management, sessions_max_concurrent, 5]),
    rpc(Config, N2, application, set_env, [rabbitmq_management, sessions_max_concurrent, 5]),
    
    {ok, {{_, 201, _}, _, BodyJSON1}} = req_node(Config, N1, post, "/session", "test_user_a", "test_user_a", #{}),
    SessionId1 = maps:get('session_id', decode_body(BodyJSON1)),
    
    {ok, {{_, 201, _}, _, BodyJSON2}} = req_node(Config, N2, post, "/session", "test_user_a", "test_user_a", #{}),
    SessionId2 = maps:get('session_id', decode_body(BodyJSON2)),
    
    {ok, {{_, 201, _}, _, BodyJSON3}} = req_node(Config, N1, post, "/session", "test_user_b", "test_user_b", #{}),
    SessionId3 = maps:get('session_id', decode_body(BodyJSON3)),
    
    http_delete(Config, "/sessions/user/test_user_a", "test_admin", "test_admin", ?NO_CONTENT),
    
    Path1 = ?KHEPRI_USER_SESSIONS_PATH(<<"test_user_a">>) ++ [SessionId1],
    Path2 = ?KHEPRI_USER_SESSIONS_PATH(<<"test_user_a">>) ++ [SessionId2],
    ?assertMatch({error, {khepri, node_not_found, _}}, rpc(Config, N1, rabbit_khepri, get, [Path1])),
    ?assertMatch({error, {khepri, node_not_found, _}}, rpc(Config, N1, rabbit_khepri, get, [Path2])),

    {ok, {{_, 401, _}, _, _}} = req_node(Config, N1, put, "/session/" ++ binary_to_list(SessionId1), "test_user_a", "test_user_a", #{}),
    {ok, {{_, 401, _}, _, _}} = req_node(Config, N2, put, "/session/" ++ binary_to_list(SessionId2), "test_user_a", "test_user_a", #{}),
    
    {ok, {{_, 204, _}, _, _}} = req_node(Config, N1, put, "/session/" ++ binary_to_list(SessionId3), "test_user_b", "test_user_b", #{}),
    
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
    
    {ok, {{_, 200, _}, _, ResBody2}} = rabbit_mgmt_test_util:req(Config, N1, get, "/sessions/user/test_user_a", [rabbit_mgmt_test_util:auth_header("test_admin", "test_admin")]),
    SessionsRes2 = decode_body(ResBody2),
    Items2 = maps:get('items', SessionsRes2),
    ?assertEqual(0, length(Items2)),
    ?assertEqual(0, maps:get('filtered_count', SessionsRes2)),
    ?assertEqual(0, maps:get('item_count', SessionsRes2)),
    ?assertEqual(0, maps:get('page_count', SessionsRes2)),

    http_delete(Config, "/sessions/user/test_user_b/" ++ binary_to_list(SessionId3), "test_admin", "test_admin", ?NO_CONTENT),
    Path3 = ?KHEPRI_USER_SESSIONS_PATH(<<"test_user_b">>) ++ [SessionId3],
    ?assertMatch({error, {khepri, node_not_found, _}}, rpc(Config, N1, rabbit_khepri, get, [Path3])),

    ?assertMatch({error, {khepri, node_not_found, _}},
                 rpc(Config, N1, rabbit_khepri, get, [?KHEPRI_USER_SESSIONS_PATH(<<"test_user_b">>)])),

    ?assertMatch({error, {khepri, node_not_found, _}},
                 rpc(Config, N1, rabbit_khepri, get, [[rabbitmq, users, <<"test_user_a">>, sessions]])),

    http_delete(Config, "/session/" ++ binary_to_list(SessionId3), "test_user_b", "test_user_b", ?NOT_FOUND),
    http_delete(Config, "/sessions/" ++ binary_to_list(SessionId3), "test_admin", "test_admin", ?NOT_FOUND),
    http_delete(Config, "/sessions/user/test_user_b/" ++ binary_to_list(SessionId3), "test_admin", "test_admin", ?NOT_FOUND),

    {ok, {{_, 201, _}, _, BodyJSON4}} = req_node(Config, N1, post, "/session", "test_user_b", "test_user_b", #{}),
    SessionId4 = maps:get('session_id', decode_body(BodyJSON4)),
    ?assertMatch({ok, _}, rpc(Config, N1, rabbit_khepri, get,
                              [?KHEPRI_USER_SESSIONS_PATH(<<"test_user_b">>) ++ [SessionId4]])),
    ok = rpc(Config, N2, rabbit_auth_backend_internal, delete_user, [<<"test_user_b">>, <<"guest">>]),
    ?awaitMatch({error, {khepri, node_not_found, _}},
                rpc(Config, N1, rabbit_khepri, get,
                    [?KHEPRI_USER_SESSIONS_PATH(<<"test_user_b">>) ++ [SessionId4]]),
                30000),
    passed.
