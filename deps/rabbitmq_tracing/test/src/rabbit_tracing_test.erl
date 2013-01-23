%% The contents of this file are subject to the Mozilla Public License
%% Version 1.1 (the "License"); you may not use this file except in
%% compliance with the License. You may obtain a copy of the License
%% at http://www.mozilla.org/MPL/
%%
%% Software distributed under the License is distributed on an "AS IS"
%% basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See
%% the License for the specific language governing rights and
%% limitations under the License.
%%
%% The Original Code is RabbitMQ.
%%
%% The Initial Developer of the Original Code is VMware, Inc.
%% Copyright (c) 2007-2013 VMware, Inc.  All rights reserved.
%%

-module(rabbit_tracing_test).

-define(LOG_DIR, "/var/tmp/rabbitmq-tracing/").

-include_lib("eunit/include/eunit.hrl").
-include_lib("amqp_client/include/amqp_client.hrl").
-include_lib("rabbitmq_management/include/rabbit_mgmt_test.hrl").

-import(rabbit_misc, [pget/2]).

tracing_test() ->
    case filelib:is_dir(?LOG_DIR) of
        true -> {ok, Files} = file:list_dir(?LOG_DIR),
                [ok = file:delete(?LOG_DIR ++ F) || F <- Files];
        _    -> ok
    end,

    [] = http_get("/traces/%2f/"),
    [] = http_get("/trace-files/"),

    Args = [{format,  <<"json">>},
            {pattern, <<"#">>}],
    http_put("/traces/%2f/test", Args, ?NO_CONTENT),
    assert_list([[{name,    <<"test">>},
                  {format,  <<"json">>},
                  {pattern, <<"#">>}]], http_get("/traces/%2f/")),
    assert_item([{name,    <<"test">>},
                 {format,  <<"json">>},
                 {pattern, <<"#">>}], http_get("/traces/%2f/test")),

    {ok, Conn} = amqp_connection:start(#amqp_params_network{}),
    {ok, Ch} = amqp_connection:open_channel(Conn),
    amqp_channel:cast(Ch, #'basic.publish'{ exchange    = <<"amq.topic">>,
                                            routing_key = <<"key">> },
                      #amqp_msg{props   = #'P_basic'{},
                                payload = <<"Hello world">>}),

    amqp_channel:close(Ch),
    amqp_connection:close(Conn),

    timer:sleep(100),

    http_delete("/traces/%2f/test", ?NO_CONTENT),
    [] = http_get("/traces/%2f/"),
    assert_list([[{name, <<"test.log">>}]], http_get("/trace-files/")),
    %% This is a bit cheeky as the log is actually one JSON doc per
    %% line and we assume here it's only one line
    assert_item([{type,         <<"published">>},
                 {exchange,     <<"amq.topic">>},
                 {routing_keys, [<<"key">>]},
                 {payload,      base64:encode(<<"Hello world">>)}],
                http_get("/trace-files/test.log")),
    http_delete("/trace-files/test.log", ?NO_CONTENT),
    ok.

%%---------------------------------------------------------------------------
%% Below is copypasta from rabbit_mgmt_test_http, it's not obvious how
%% to share that given the build system.

http_get(Path) ->
    http_get(Path, ?OK).

http_get(Path, CodeExp) ->
    http_get(Path, "guest", "guest", CodeExp).

http_get(Path, User, Pass, CodeExp) ->
    {ok, {{_HTTP, CodeAct, _}, Headers, ResBody}} =
        req(get, Path, [auth_header(User, Pass)]),
    assert_code(CodeExp, CodeAct, "GET", Path, ResBody),
    decode(CodeExp, Headers, ResBody).

http_put(Path, List, CodeExp) ->
    http_put_raw(Path, format_for_upload(List), CodeExp).

http_put(Path, List, User, Pass, CodeExp) ->
    http_put_raw(Path, format_for_upload(List), User, Pass, CodeExp).

http_post(Path, List, CodeExp) ->
    http_post_raw(Path, format_for_upload(List), CodeExp).

http_post(Path, List, User, Pass, CodeExp) ->
    http_post_raw(Path, format_for_upload(List), User, Pass, CodeExp).

format_for_upload(List) ->
    iolist_to_binary(mochijson2:encode({struct, List})).

http_put_raw(Path, Body, CodeExp) ->
    http_upload_raw(put, Path, Body, "guest", "guest", CodeExp).

http_put_raw(Path, Body, User, Pass, CodeExp) ->
    http_upload_raw(put, Path, Body, User, Pass, CodeExp).

http_post_raw(Path, Body, CodeExp) ->
    http_upload_raw(post, Path, Body, "guest", "guest", CodeExp).

http_post_raw(Path, Body, User, Pass, CodeExp) ->
    http_upload_raw(post, Path, Body, User, Pass, CodeExp).

http_upload_raw(Type, Path, Body, User, Pass, CodeExp) ->
    {ok, {{_HTTP, CodeAct, _}, Headers, ResBody}} =
        req(Type, Path, [auth_header(User, Pass)], Body),
    assert_code(CodeExp, CodeAct, Type, Path, ResBody),
    decode(CodeExp, Headers, ResBody).

http_delete(Path, CodeExp) ->
    http_delete(Path, "guest", "guest", CodeExp).

http_delete(Path, User, Pass, CodeExp) ->
    {ok, {{_HTTP, CodeAct, _}, Headers, ResBody}} =
        req(delete, Path, [auth_header(User, Pass)]),
    assert_code(CodeExp, CodeAct, "DELETE", Path, ResBody),
    decode(CodeExp, Headers, ResBody).

assert_code(CodeExp, CodeAct, Type, Path, Body) ->
    case CodeExp of
        CodeAct -> ok;
        _       -> throw({expected, CodeExp, got, CodeAct, type, Type,
                          path, Path, body, Body})
    end.

req(Type, Path, Headers) ->
    httpc:request(Type, {?PREFIX ++ Path, Headers}, ?HTTPC_OPTS, []).

req(Type, Path, Headers, Body) ->
    httpc:request(Type, {?PREFIX ++ Path, Headers, "application/json", Body},
                  ?HTTPC_OPTS, []).

decode(?OK, _Headers,  ResBody) -> cleanup(mochijson2:decode(ResBody));
decode(_,    Headers, _ResBody) -> Headers.

cleanup(L) when is_list(L) ->
    [cleanup(I) || I <- L];
cleanup({struct, I}) ->
    cleanup(I);
cleanup({K, V}) when is_binary(K) ->
    {list_to_atom(binary_to_list(K)), cleanup(V)};
cleanup(I) ->
    I.

auth_header(Username, Password) ->
    {"Authorization",
     "Basic " ++ binary_to_list(base64:encode(Username ++ ":" ++ Password))}.

%%---------------------------------------------------------------------------

assert_list(Exp, Act) ->
    case length(Exp) == length(Act) of
        true  -> ok;
        false -> throw({expected, Exp, actual, Act})
    end,
    [case length(lists:filter(fun(ActI) -> test_item(ExpI, ActI) end, Act)) of
         1 -> ok;
         N -> throw({found, N, ExpI, in, Act})
     end || ExpI <- Exp].

assert_item(Exp, Act) ->
    case test_item0(Exp, Act) of
        [] -> ok;
        Or -> throw(Or)
    end.

test_item(Exp, Act) ->
    case test_item0(Exp, Act) of
        [] -> true;
        _  -> false
    end.

test_item0(Exp, Act) ->
    [{did_not_find, ExpI, in, Act} || ExpI <- Exp,
                                      not lists:member(ExpI, Act)].
