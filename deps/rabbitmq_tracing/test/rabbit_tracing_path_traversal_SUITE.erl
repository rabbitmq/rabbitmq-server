%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2026 Broadcom. All Rights Reserved. The term "Broadcom" refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
%%

-module(rabbit_tracing_path_traversal_SUITE).
-compile([export_all]).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("rabbitmq_ct_helpers/include/rabbit_mgmt_test.hrl").

-define(LOG_DIR, "/var/tmp/rabbitmq-tracing/").

suite() ->
    [{timetrap, {minutes, 2}}].

all() ->
    [path_traversal_rejected,
     normal_trace_name_accepted].

init_per_suite(Config) ->
    inets:start(),
    rabbit_ct_helpers:log_environment(),
    Config1 = rabbit_ct_helpers:set_config(Config,
                                           [{rmq_nodename_suffix, ?MODULE}]),
    rabbit_ct_helpers:run_setup_steps(
      Config1,
      rabbit_ct_broker_helpers:setup_steps() ++
      rabbit_ct_client_helpers:setup_steps()).

end_per_suite(Config) ->
    rabbit_ct_helpers:run_teardown_steps(
      Config,
      rabbit_ct_client_helpers:teardown_steps() ++
      rabbit_ct_broker_helpers:teardown_steps()).

init_per_testcase(Testcase, Config) ->
    rabbit_ct_helpers:testcase_started(Config, Testcase).

end_per_testcase(Testcase, Config) ->
    rabbit_ct_helpers:testcase_finished(Config, Testcase).

%% Trace names containing path traversal sequences must be rejected.
path_traversal_rejected(Config) ->
    Args = #{format => <<"json">>, pattern => <<"#">>},
    http_put(Config, "/traces/%2f/..%2f..%2fetc%2fcron.d%2fpwned",
             Args, ?BAD_REQUEST),
    http_put(Config, "/traces/%2f/..%2f..%2ftmp%2fevil",
             Args, ?BAD_REQUEST),
    http_put(Config, "/traces/%2f/foo%2f..%2f..%2fbar",
             Args, ?BAD_REQUEST),
    [] = http_get(Config, "/traces/%2f/"),
    ok.

%% A simple trace name without path traversal must work as before.
normal_trace_name_accepted(Config) ->
    Args = #{format => <<"json">>, pattern => <<"#">>},
    http_put(Config, "/traces/%2f/safe-trace", Args, ?CREATED),
    assert_item(#{name => <<"safe-trace">>},
                http_get(Config, "/traces/%2f/safe-trace")),
    http_delete(Config, "/traces/%2f/safe-trace", ?NO_CONTENT),
    ok.

%%--------------------------------------------------------------------
%% HTTP helpers (same pattern as rabbit_tracing_SUITE)
%%--------------------------------------------------------------------

http_get(Config, Path) ->
    http_get(Config, Path, ?OK).

http_get(Config, Path, CodeExp) ->
    http_get(Config, Path, "guest", "guest", CodeExp).

http_get(Config, Path, User, Pass, CodeExp) ->
    {ok, {{_HTTP, CodeAct, _}, Headers, ResBody}} =
        req(Config, get, Path, [auth_header(User, Pass)]),
    assert_code(CodeExp, CodeAct, "GET", Path, ResBody),
    decode(CodeExp, Headers, ResBody).

http_put(Config, Path, Map, CodeExp) ->
    http_put_raw(Config, Path, format_for_upload(Map), CodeExp).

format_for_upload(Map0) ->
    Map = maps:fold(fun(K, V, Acc) ->
        Acc#{atom_to_binary(K, latin1) => V}
    end, #{}, Map0),
    iolist_to_binary(rabbit_json:encode(Map)).

http_put_raw(Config, Path, Body, CodeExp) ->
    http_upload_raw(Config, put, Path, Body, "guest", "guest", CodeExp).

http_upload_raw(Config, Type, Path, Body, User, Pass, CodeExp) ->
    {ok, {{_HTTP, CodeAct, _}, Headers, ResBody}} =
        req(Config, Type, Path, [auth_header(User, Pass)], Body),
    assert_code(CodeExp, CodeAct, Type, Path, ResBody),
    decode(CodeExp, Headers, ResBody).

http_delete(Config, Path, CodeExp) ->
    {ok, {{_HTTP, CodeAct, _}, Headers, ResBody}} =
        req(Config, delete, Path, [auth_header("guest", "guest")]),
    assert_code(CodeExp, CodeAct, "DELETE", Path, ResBody),
    decode(CodeExp, Headers, ResBody).

assert_code(CodeExp, CodeAct, Type, Path, Body) ->
    case CodeExp of
        CodeAct -> ok;
        _       -> error({expected, CodeExp, got, CodeAct, type, Type,
                          path, Path, body, Body})
    end.

req(Config, Type, Path, Headers) ->
    httpc:request(Type, {uri_base_from(Config) ++ Path, Headers},
                  ?HTTPC_OPTS, []).

req(Config, Type, Path, Headers, Body) ->
    httpc:request(Type, {uri_base_from(Config) ++ Path, Headers,
                         "application/json", Body},
                  ?HTTPC_OPTS, []).

uri_base_from(Config) ->
    Port = rabbit_ct_broker_helpers:get_node_config(Config, 0, tcp_port_mgmt),
    binary_to_list(
      rabbit_mgmt_format:print("http://localhost:~w/api", [Port])).

decode(?OK, _Headers, ResBody) ->
    cleanup(rabbit_json:decode(rabbit_data_coercion:to_binary(ResBody)));
decode(_, Headers, _ResBody) ->
    Headers.

cleanup(L) when is_list(L) -> [cleanup(I) || I <- L];
cleanup(M) when is_map(M) ->
    maps:fold(fun(K, V, Acc) ->
        Acc#{binary_to_atom(K, latin1) => cleanup(V)}
    end, #{}, M);
cleanup(I) -> I.

auth_header(Username, Password) ->
    {"Authorization",
     "Basic " ++ binary_to_list(base64:encode(Username ++ ":" ++ Password))}.

assert_item(ExpI, ActI) ->
    ExpI = maps:with(maps:keys(ExpI), ActI),
    ok.
