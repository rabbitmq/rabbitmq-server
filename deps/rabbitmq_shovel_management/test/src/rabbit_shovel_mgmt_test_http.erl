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
%%   Copyright (c) 2010-2014 GoPivotal, Inc.  All rights reserved.
%%

-module(rabbit_shovel_mgmt_test_http).

-include_lib("rabbitmq_management/include/rabbit_mgmt_test.hrl").

-import(rabbit_misc, [pget/2]).

shovels_test() ->
    http_put("/users/admin",  [{password, <<"admin">>},
                               {tags, <<"administrator">>}], ?NO_CONTENT),
    http_put("/users/mon",    [{password, <<"mon">>},
                               {tags, <<"monitoring">>}], ?NO_CONTENT),
    http_put("/vhosts/v", none, ?NO_CONTENT),
    Perms = [{configure, <<".*">>},
             {write,     <<".*">>},
             {read,      <<".*">>}],
    http_put("/permissions/v/guest",  Perms, ?NO_CONTENT),
    http_put("/permissions/v/admin",  Perms, ?NO_CONTENT),
    http_put("/permissions/v/mon",    Perms, ?NO_CONTENT),

    [http_put("/parameters/shovel/" ++ V ++ "/my-dynamic",
              [{value, [{'src-uri', <<"amqp://">>},
                        {'dest-uri', <<"amqp://">>},
                        {'src-queue', <<"test">>},
                        {'dest-queue', <<"test2">>}]}], ?NO_CONTENT)
     || V <- ["%2f", "v"]],
    Static = [{name,  <<"my-static">>},
              {type,  <<"static">>}],
    Dynamic1 = [{name,  <<"my-dynamic">>},
                {vhost, <<"/">>},
                {type,  <<"dynamic">>}],
    Dynamic2 = [{name,  <<"my-dynamic">>},
                {vhost, <<"v">>},
                {type,  <<"dynamic">>}],
    Assert = fun (Req, User, Res) ->
                     assert_list(Res, http_get(Req, User, User, ?OK))
             end,
    Assert("/shovels",     "guest", [Static, Dynamic1, Dynamic2]),
    Assert("/shovels/%2f", "guest", [Dynamic1]),
    Assert("/shovels/v",   "guest", [Dynamic2]),
    Assert("/shovels",     "admin", [Static, Dynamic2]),
    Assert("/shovels/%2f", "admin", []),
    Assert("/shovels/v",   "admin", [Dynamic2]),
    Assert("/shovels",     "mon", [Dynamic2]),
    Assert("/shovels/%2f", "mon", []),
    Assert("/shovels/v",   "mon", [Dynamic2]),

    http_delete("/vhosts/v", ?NO_CONTENT),
    http_delete("/users/admin", ?NO_CONTENT),
    http_delete("/users/mon", ?NO_CONTENT),
    ok.

%%---------------------------------------------------------------------------
%% TODO this is all copypasta from the mgmt tests

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

format_for_upload(none) ->
    <<"">>;
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
