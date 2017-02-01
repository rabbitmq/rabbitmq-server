%%   The contents of this file are subject to the Mozilla Public License
%%   Version 1.1 (the "License"); you may not use this file except in
%%   compliance with the License. You may obtain a copy of the License at
%%   http://www.mozilla.org/MPL/
%%
%%   Software distributed under the License is distributed on an "AS IS"
%%   basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See the
%%   License for the specific language governing rights and limitations
%%   under the License.
%%
%%   The Original Code is RabbitMQ Management Console.
%%
%%   The Initial Developer of the Original Code is GoPivotal, Inc.
%%   Copyright (c) 2010-2012 GoPivotal, Inc.  All rights reserved.
%%

-module(rabbit_mgmt_test_util).

-include("rabbit_mgmt_test.hrl").

-compile(export_all).

reset_management_settings(Config) ->
    rabbit_ct_broker_helpers:rpc(Config, 0, application, set_env,
                                 [rabbit, collect_statistics_interval, 5000]),
    Config.

merge_stats_app_env(Config, Interval, SampleInterval) ->
    Config1 = rabbit_ct_helpers:merge_app_env(
        Config, {rabbit, [{collect_statistics_interval, Interval}]}),
    rabbit_ct_helpers:merge_app_env(
      Config1, {rabbitmq_management_agent, [{sample_retention_policies,
                       [{global,   [{605, SampleInterval}]},
                        {basic,    [{605, SampleInterval}]},
                        {detailed, [{10, SampleInterval}]}] }]}).
http_get_from_node(Config, Node, Path) ->
    {ok, {{_HTTP, CodeAct, _}, Headers, ResBody}} =
        req(Config, Node, get, Path, [auth_header("guest", "guest")]),
    assert_code(?OK, CodeAct, "GET", Path, ResBody),
    decode(?OK, Headers, ResBody).


http_get(Config, Path) ->
    http_get(Config, Path, ?OK).

http_get(Config, Path, CodeExp) ->
    http_get(Config, Path, "guest", "guest", CodeExp).

http_get(Config, Path, User, Pass, CodeExp) ->
    {ok, {{_HTTP, CodeAct, _}, Headers, ResBody}} =
        req(Config, 0, get, Path, [auth_header(User, Pass)]),
    assert_code(CodeExp, CodeAct, "GET", Path, ResBody),
    decode(CodeExp, Headers, ResBody).

http_get_no_map(Config, Path) ->
    {ok, {{_HTTP, CodeAct, _}, _Headers, ResBody}} =
        req(Config, get, Path, [auth_header("guest", "guest")]),
    assert_code(?OK, CodeAct, "GET", Path, ResBody),
    cleanup(rabbit_json:decode(rabbit_data_coercion:to_binary(ResBody))).

http_put(Config, Path, List, CodeExp) ->
    http_put_raw(Config, Path, format_for_upload(List), CodeExp).

http_put(Config, Path, List, User, Pass, CodeExp) ->
    http_put_raw(Config, Path, format_for_upload(List), User, Pass, CodeExp).

http_post(Config, Path, List, CodeExp) ->
    http_post_raw(Config, Path, format_for_upload(List), CodeExp).

http_post(Config, Path, List, User, Pass, CodeExp) ->
    http_post_raw(Config, Path, format_for_upload(List), User, Pass, CodeExp).

http_post_accept_json(Config, Path, List, CodeExp) ->
    http_post_accept_json(Config, Path, List, "guest", "guest", CodeExp).

http_post_accept_json(Config, Path, List, User, Pass, CodeExp) ->
    http_post_raw(Config, Path, format_for_upload(List), User, Pass, CodeExp,
          [{"Accept", "application/json"}]).

req(Config, Type, Path, Headers) ->
    req(Config, 0, Type, Path, Headers).

req(Config, Node, Type, Path, Headers) ->
    httpc:request(Type, {uri_base_from(Config, Node) ++ Path, Headers}, ?HTTPC_OPTS, []).

req(Config, Node, Type, Path, Headers, Body) ->
    httpc:request(Type, {uri_base_from(Config, Node) ++ Path, Headers, "application/json", Body},
                  ?HTTPC_OPTS, []).

uri_base_from(Config, Node) ->
    binary_to_list(
      rabbit_mgmt_format:print(
        "http://localhost:~w/api",
        [mgmt_port(Config, Node)])).

auth_header(Username, Password) when is_binary(Username) ->
    auth_header(binary_to_list(Username), Password);
auth_header(Username, Password) when is_binary(Password) ->
    auth_header(Username, binary_to_list(Password));
auth_header(Username, Password) ->
    {"Authorization",
     "Basic " ++ binary_to_list(base64:encode(Username ++ ":" ++ Password))}.

amqp_port(Config) ->
    config_port(Config, tcp_port_amqp).

mgmt_port(Config, Node) ->
    config_port(Config, Node, tcp_port_mgmt).

config_port(Config, PortKey) ->
    config_port(Config, 0, PortKey).

config_port(Config, Node, PortKey) ->
    rabbit_ct_broker_helpers:get_node_config(Config, Node, PortKey).

http_put_raw(Config, Path, Body, CodeExp) ->
    http_upload_raw(Config, put, Path, Body, "guest", "guest", CodeExp, []).

http_put_raw(Config, Path, Body, User, Pass, CodeExp) ->
    http_upload_raw(Config, put, Path, Body, User, Pass, CodeExp, []).


http_post_raw(Config, Path, Body, CodeExp) ->
    http_upload_raw(Config, post, Path, Body, "guest", "guest", CodeExp, []).

http_post_raw(Config, Path, Body, User, Pass, CodeExp) ->
    http_upload_raw(Config, post, Path, Body, User, Pass, CodeExp, []).

http_post_raw(Config, Path, Body, User, Pass, CodeExp, MoreHeaders) ->
    http_upload_raw(Config, post, Path, Body, User, Pass, CodeExp, MoreHeaders).


http_upload_raw(Config, Type, Path, Body, User, Pass, CodeExp, MoreHeaders) ->
    {ok, {{_HTTP, CodeAct, _}, Headers, ResBody}} =
    req(Config, 0, Type, Path, [auth_header(User, Pass)] ++ MoreHeaders, Body),
    assert_code(CodeExp, CodeAct, Type, Path, ResBody),
    decode(CodeExp, Headers, ResBody).

http_delete(Config, Path, CodeExp) ->
    http_delete(Config, Path, "guest", "guest", CodeExp).

http_delete(Config, Path, User, Pass, CodeExp) ->
    {ok, {{_HTTP, CodeAct, _}, Headers, ResBody}} =
        req(Config, 0, delete, Path, [auth_header(User, Pass)]),
    assert_code(CodeExp, CodeAct, "DELETE", Path, ResBody),
    decode(CodeExp, Headers, ResBody).

format_for_upload(none) ->
    <<"">>;
format_for_upload(List) ->
    iolist_to_binary(rabbit_json:encode(List)).

assert_code({one_of, CodesExpected}, CodeAct, Type, Path, Body) when is_list(CodesExpected) ->
    case lists:member(CodeAct, CodesExpected) of
        true ->
            ok;
        false ->
            error({expected, CodesExpected, got, CodeAct, type, Type,
                   path, Path, body, Body})
    end;
assert_code({group, '2xx'} = CodeExp, CodeAct, Type, Path, Body) ->
    case CodeAct of
        200 -> ok;
        201 -> ok;
        202 -> ok;
        203 -> ok;
        204 -> ok;
        205 -> ok;
        206 -> ok;
        _   -> error({expected, CodeExp, got, CodeAct, type, Type,
                          path, Path, body, Body})
    end;
assert_code({group, '3xx'} = CodeExp, CodeAct, Type, Path, Body) ->
    case CodeAct of
        300 -> ok;
        301 -> ok;
        302 -> ok;
        303 -> ok;
        304 -> ok;
        305 -> ok;
        306 -> ok;
        307 -> ok;
        _   -> error({expected, CodeExp, got, CodeAct, type, Type,
                          path, Path, body, Body})
    end;
assert_code({group, '4xx'} = CodeExp, CodeAct, Type, Path, Body) ->
    case CodeAct of
        400 -> ok;
        401 -> ok;
        402 -> ok;
        403 -> ok;
        404 -> ok;
        405 -> ok;
        406 -> ok;
        407 -> ok;
        408 -> ok;
        409 -> ok;
        410 -> ok;
        411 -> ok;
        412 -> ok;
        413 -> ok;
        414 -> ok;
        415 -> ok;
        416 -> ok;
        417 -> ok;
        _   -> error({expected, CodeExp, got, CodeAct, type, Type,
                          path, Path, body, Body})
    end;
assert_code(CodeExp, CodeAct, Type, Path, Body) ->
    case CodeExp of
        CodeAct -> ok;
        _       -> error({expected, CodeExp, got, CodeAct, type, Type,
                          path, Path, body, Body})
    end.

decode(?OK, _Headers,  ResBody) -> 
    cleanup(rabbit_json:decode(rabbit_data_coercion:to_binary(ResBody)));
decode(_,    Headers, _ResBody) -> Headers.

cleanup(L) when is_list(L) ->
    [cleanup(I) || I <- L];
cleanup(M) when is_map(M) ->
    maps:fold(fun(K, V, Acc) ->
        Acc#{binary_to_atom(K, latin1) => cleanup(V)}
    end, #{}, M);
cleanup(I) ->
    I.

%% @todo There wasn't a specific order before; now there is; maybe we shouldn't have one?
assert_list(Exp, Act) ->
    case length(Exp) == length(Act) of
        true  -> ok;
        false -> throw({expected, Exp, actual, Act})
    end,
    [case length(lists:filter(fun(ActI) -> test_item(ExpI, ActI) end, Act)) of
         1 -> ok;
         N -> throw({found, N, ExpI, in, Act})
     end || ExpI <- Exp].
    %_ = [assert_item(ExpI, ActI) || {ExpI, ActI} <- lists:zip(Exp, Act)],

assert_item(ExpI, [H | _] = ActI) when is_list(ActI) ->
    %% just check first item of the list
    assert_item(ExpI, H),
    ok;
assert_item(ExpI, ActI) ->
    ExpI = maps:with(maps:keys(ExpI), ActI),
    ok.

assert_item_kv(Exp, Act) when is_list(Exp) ->
    case test_item0_kv(Exp, Act) of
        [] -> ok;
        Or -> throw(Or)
    end.

test_item(Exp, Act) ->
    case test_item0(Exp, Act) of
        [] -> true;
        _  -> false
    end.

test_item0(Exp, Act) ->
    [{did_not_find, KeyExpI, in, Act} || KeyExpI <- maps:keys(Exp),
        maps:get(KeyExpI, Exp) =/= maps:get(KeyExpI, Act, null)].

test_item0_kv(Exp, Act) ->
    [{did_not_find, ExpI, in, Act} || ExpI <- Exp,
                                      not lists:member(ExpI, Act)].

assert_keys(Exp, Act) ->
    case test_key0(Exp, Act) of
        [] -> ok;
        Or -> throw(Or)
    end.

test_key0(Exp, Act) ->
    [{did_not_find, ExpI, in, Act} || ExpI <- Exp,
                                      not maps:is_key(ExpI, Act)].
assert_no_keys(NotExp, Act) ->
    case test_no_key0(NotExp, Act) of
        [] -> ok;
        Or -> throw(Or)
    end.

test_no_key0(Exp, Act) ->
    [{invalid_key, ExpI, in, Act} || ExpI <- Exp,
                                      maps:is_key(ExpI, Act)].
