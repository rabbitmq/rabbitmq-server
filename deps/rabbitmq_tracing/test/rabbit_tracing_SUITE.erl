%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2021 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(rabbit_tracing_SUITE).

-compile(export_all).

-define(LOG_DIR, "/var/tmp/rabbitmq-tracing/").

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("amqp_client/include/amqp_client.hrl").
-include_lib("rabbitmq_ct_helpers/include/rabbit_mgmt_test.hrl").

all() ->
    [
      {group, non_parallel_tests}
    ].

groups() ->
    [
      {non_parallel_tests, [], [
                                tracing_test,
                                tracing_validation_test
                               ]}
    ].

%% -------------------------------------------------------------------
%% Testsuite setup/teardown.
%% -------------------------------------------------------------------

init_per_suite(Config) ->
    inets:start(),
    rabbit_ct_helpers:log_environment(),
    %% initializes httpc
    inets:start(),
    Config1 = rabbit_ct_helpers:set_config(Config, [
        {rmq_nodename_suffix, ?MODULE}
      ]),
    rabbit_ct_helpers:run_setup_steps(Config1,
      rabbit_ct_broker_helpers:setup_steps() ++
      rabbit_ct_client_helpers:setup_steps()).

end_per_suite(Config) ->
    rabbit_ct_helpers:run_teardown_steps(Config,
      rabbit_ct_client_helpers:teardown_steps() ++
      rabbit_ct_broker_helpers:teardown_steps()).

init_per_group(_, Config) ->
    Config.

end_per_group(_, Config) ->
    Config.

init_per_testcase(Testcase, Config) ->
    rabbit_ct_helpers:testcase_started(Config, Testcase).

end_per_testcase(Testcase, Config) ->
    rabbit_ct_helpers:testcase_finished(Config, Testcase).

%% -------------------------------------------------------------------
%% Testcases.
%% -------------------------------------------------------------------


tracing_test(Config) ->
    case filelib:is_dir(?LOG_DIR) of
        true -> {ok, Files} = file:list_dir(?LOG_DIR),
                [ok = file:delete(?LOG_DIR ++ F) || F <- Files];
        _    -> ok
    end,

    [] = http_get(Config, "/traces/%2f/"),
    [] = http_get(Config, "/trace-files/"),

    Args = #{format  => <<"json">>,
             pattern => <<"#">>},
    http_put(Config, "/traces/%2f/test", Args, ?CREATED),
    assert_list([#{name    => <<"test">>,
                   format  => <<"json">>,
                   pattern => <<"#">>}], http_get(Config, "/traces/%2f/")),
    assert_item(#{name    => <<"test">>,
                  format  => <<"json">>,
                  pattern => <<"#">>}, http_get(Config, "/traces/%2f/test")),

    Ch = rabbit_ct_client_helpers:open_channel(Config),
    amqp_channel:cast(Ch, #'basic.publish'{ exchange    = <<"amq.topic">>,
                                            routing_key = <<"key">> },
                      #amqp_msg{props   = #'P_basic'{},
                                payload = <<"Hello world">>}),

    rabbit_ct_client_helpers:close_channel(Ch),

    timer:sleep(100),

    http_delete(Config, "/traces/%2f/test", ?NO_CONTENT),
    [] = http_get(Config, "/traces/%2f/"),
    assert_list([#{name => <<"test.log">>}], http_get(Config, "/trace-files/")),
    %% This is a bit cheeky as the log is actually one JSON doc per
    %% line and only check the first line
    RawLog = http_get_raw(Config, "/trace-files/test.log"),
    FirstLogLine = hd(string:tokens(RawLog, "\n")),
    FirstLogItem = decode(?OK, [], FirstLogLine),
    assert_item(#{type         => <<"published">>,
                  exchange     => <<"amq.topic">>,
                  routing_keys => [<<"key">>],
                  payload      => base64:encode(<<"Hello world">>)},
                FirstLogItem),
    http_delete(Config, "/trace-files/test.log", ?NO_CONTENT),

    passed.

tracing_validation_test(Config) ->
    Path = "/traces/%2f/test",
    http_put(Config, Path, #{pattern           => <<"#">>},    ?BAD_REQUEST),
    http_put(Config, Path, #{format            => <<"json">>}, ?BAD_REQUEST),
    http_put(Config, Path, #{format            => <<"ebcdic">>,
                             pattern           => <<"#">>},    ?BAD_REQUEST),
    http_put(Config, Path, #{format            => <<"text">>,
                             pattern           => <<"#">>,
                             max_payload_bytes => <<"abc">>},  ?BAD_REQUEST),
    http_put(Config, Path, #{format            => <<"json">>,
                             pattern           => <<"#">>,
                             max_payload_bytes => 1000},       ?CREATED),
    http_delete(Config, Path, ?NO_CONTENT),
    ok.

%%---------------------------------------------------------------------------
%% TODO: Below is copied from rabbit_mgmt_test_http,
%%       should be moved to use rabbit_mgmt_test_util once rabbitmq_management
%%       is moved to Common Test

http_get(Config, Path) ->
    http_get(Config, Path, ?OK).

http_get(Config, Path, CodeExp) ->
    http_get(Config, Path, "guest", "guest", CodeExp).

http_get(Config, Path, User, Pass, CodeExp) ->
    {ok, {{_HTTP, CodeAct, _}, Headers, ResBody}} =
        req(Config, get, Path, [auth_header(User, Pass)]),
    assert_code(CodeExp, CodeAct, "GET", Path, ResBody),
    decode(CodeExp, Headers, ResBody).

http_get_raw(Config, Path) ->
    http_get_raw(Config, Path, "guest", "guest", ?OK).

http_get_raw(Config, Path, User, Pass, CodeExp) ->
    {ok, {{_HTTP, CodeAct, _}, _Headers, ResBody}} =
        req(Config, get, Path, [auth_header(User, Pass)]),
    assert_code(CodeExp, CodeAct, "GET", Path, ResBody),
    ResBody.

http_put(Config, Path, List, CodeExp) ->
    http_put_raw(Config, Path, format_for_upload(List), CodeExp).

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
    http_delete(Config, Path, "guest", "guest", CodeExp).

http_delete(Config, Path, User, Pass, CodeExp) ->
    {ok, {{_HTTP, CodeAct, _}, Headers, ResBody}} =
        req(Config, delete, Path, [auth_header(User, Pass)]),
    assert_code(CodeExp, CodeAct, "DELETE", Path, ResBody),
    decode(CodeExp, Headers, ResBody).

assert_code(CodeExp, CodeAct, Type, Path, Body) ->
    case CodeExp of
        CodeAct -> ok;
        _       -> error({expected, CodeExp, got, CodeAct, type, Type,
                          path, Path, body, Body})
    end.

mgmt_port(Config) ->
    config_port(Config, tcp_port_mgmt).

config_port(Config, PortKey) ->
    rabbit_ct_broker_helpers:get_node_config(Config, 0, PortKey).

uri_base_from(Config) ->
    binary_to_list(
      rabbit_mgmt_format:print(
        "http://localhost:~w/api",
        [mgmt_port(Config)])).

req(Config, Type, Path, Headers) ->
    httpc:request(Type, {uri_base_from(Config) ++ Path, Headers}, ?HTTPC_OPTS, []).

req(Config, Type, Path, Headers, Body) ->
    httpc:request(Type, {uri_base_from(Config) ++ Path, Headers, "application/json", Body},
                  ?HTTPC_OPTS, []).

decode(?OK, _Headers,  ResBody) -> cleanup(rabbit_json:decode(rabbit_data_coercion:to_binary(ResBody)));
decode(_,    Headers, _ResBody) -> Headers.

cleanup(L) when is_list(L) ->
    [cleanup(I) || I <- L];
cleanup(M) when is_map(M) ->
    maps:fold(fun(K, V, Acc) ->
        Acc#{binary_to_atom(K, latin1) => cleanup(V)}
    end, #{}, M);
cleanup(I) ->
    I.

auth_header(Username, Password) ->
    {"Authorization",
     "Basic " ++ binary_to_list(base64:encode(Username ++ ":" ++ Password))}.

%%---------------------------------------------------------------------------

assert_list(Exp, Act) ->
    _ = [assert_item(ExpI, ActI) || {ExpI, ActI} <- lists:zip(Exp, Act)],
    ok.

assert_item(ExpI, ActI) ->
    ExpI = maps:with(maps:keys(ExpI), ActI),
    ok.
