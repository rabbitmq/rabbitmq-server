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
%%   The Initial Developers of the Original Code are Rabbit Technologies Ltd.
%%
%%   Copyright (C) 2010 Rabbit Technologies Ltd.
%%
%%   All Rights Reserved.
%%
%%   Contributor(s): ______________________________________.
%%
-module(rabbit_mgmt_test_unit).

-include_lib("eunit/include/eunit.hrl").
-include_lib("amqp_client/include/amqp_client.hrl").

-define(OK, 200).
-define(CREATED, 201).
-define(NO_CONTENT, 204).
-define(BAD_REQUEST, 400).
-define(NOT_AUTHORISED, 401).
%%-define(NOT_FOUND, 404). Defined for AMQP by amqp_client.hrl (as 404)
-define(PREFIX, "http://localhost:55672/api").

rates_test() ->
    Previous = [{foo, 1}, {bar, 100}, {baz, 3}],
    PreviousTS = {0, 0, 0},
    New = [{foo, 2}, {bar, 200}, {bash, 100}, {baz, 3}],
    NewTS = {0, 10, 0},
    WithRates = rabbit_mgmt_db:rates(New, NewTS, Previous, PreviousTS,
                                     [foo, bar, bash]),
    equals(0.1, pget(rate, pget(foo_details, WithRates))),
    equals(10, pget(rate, pget(bar_details, WithRates))),
    undefined = pget(bash_details, WithRates),
    undefined = pget(baz_details, WithRates).

http_overview_test() ->
    %% Rather crude, but this req doesn't say much and at least this means it
    %% didn't blow up.
    Overview = http_get("/overview"),
    [<<"0.0.0.0:5672">>] = pget(bound_to, Overview).

http_auth_test() ->
    test_auth(?NOT_AUTHORISED, []),
    test_auth(?NOT_AUTHORISED, [auth_header("guest", "gust")]),
    test_auth(?OK, [auth_header("guest", "guest")]).

%% This test is rather over-verbose as we're trying to test understanding of
%% Webmachine
http_vhosts_test() ->
    [<<"/">>] = http_get("/vhosts"),
    %% Create a new one
    http_put("/vhosts/myvhost", [], ?NO_CONTENT),
    %% PUT should be idempotent
    http_put("/vhosts/myvhost", [], ?NO_CONTENT),
    %% Check it's there
    [<<"/">>, <<"myvhost">>] = http_get("/vhosts"),
    %% Check individually
    <<"/">> = http_get("/vhosts/%2f", ?OK),
    <<"myvhost">> = http_get("/vhosts/myvhost"),
    %% Delete it
    http_delete("/vhosts/myvhost", ?NO_CONTENT),
    %% It's not there
    http_get("/vhosts/myvhost", ?NOT_FOUND),
    http_delete("/vhosts/myvhost", ?NOT_FOUND).

http_users_test() ->
    http_get("/users/myuser", ?NOT_FOUND),
    http_put_raw("/users/myuser", "Something not JSON", ?BAD_REQUEST),
    http_put("/users/myuser", [{flim, "flam"}], ?BAD_REQUEST),
    http_put("/users/myuser", [{password, "myuser"},
                               {administrator, false}], ?NO_CONTENT),
    http_put("/users/myuser", [{password, "password"},
                               {administrator, true}], ?NO_CONTENT),
    [{name,          <<"myuser">>},
     {password,      <<"password">>},
     {administrator, true}] =
        http_get("/users/myuser"),
    [[{name,<<"guest">>},
      {password,<<"guest">>},
      {administrator, true}],
     [{name,          <<"myuser">>},
      {password,      <<"password">>},
      {administrator, true}]] =
        http_get("/users"),
    test_auth(?OK, [auth_header("myuser", "password")]),
    http_delete("/users/myuser", ?NO_CONTENT),
    test_auth(?NOT_AUTHORISED, [auth_header("myuser", "password")]),
    http_get("/users/myuser", ?NOT_FOUND),
    ok.

http_permissions_validation_test() ->
    Good = [{configure, ".*"}, {write, ".*"},
            {read,      ".*"}, {scope, "client"}],
    http_put("/permissions/wrong/guest", Good, ?BAD_REQUEST),
    http_put("/permissions/%2f/wrong", Good, ?BAD_REQUEST),
    http_put("/permissions/%2f/guest",
             [{configure, ".*"}, {write, ".*"},
              {read,      ".*"}], ?BAD_REQUEST),
    http_put("/permissions/%2f/guest",
             [{configure, ".*"}, {write, ".*"},
              {read,      ".*"}, {scope, "wrong"}], ?BAD_REQUEST),
    http_put("/permissions/%2f/guest",
             [{configure, "["}, {write, ".*"},
              {read,      ".*"}, {scope, "client"}], ?BAD_REQUEST),
    http_put("/permissions/%2f/guest", Good, ?NO_CONTENT),
    ok.

http_permissions_list_test() ->
    [[{vhost,<<"/">>},
      {user,<<"guest">>},
      {configure,<<".*">>},
      {write,<<".*">>},
      {read,<<".*">>},
      {scope,<<"client">>}]] =
        http_get("/permissions"),

    http_put("/users/myuser1", [{password, ""}, {administrator, true}],
             ?NO_CONTENT),
    http_put("/users/myuser2", [{password, ""}, {administrator, true}],
             ?NO_CONTENT),
    http_put("/vhosts/myvhost1", [], ?NO_CONTENT),
    http_put("/vhosts/myvhost2", [], ?NO_CONTENT),

    Perms = [{configure, "foo"}, {write, "foo"},
             {read,      "foo"}, {scope, "client"}],
    http_put("/permissions/myvhost1/myuser1", Perms, ?NO_CONTENT),
    http_put("/permissions/myvhost2/myuser1", Perms, ?NO_CONTENT),
    http_put("/permissions/myvhost1/myuser2", Perms, ?NO_CONTENT),

    4 = length(http_get("/permissions")),
    2 = length(http_get("/users/myuser1/permissions")),
    1 = length(http_get("/users/myuser2/permissions")),

    http_delete("/users/myuser1", ?NO_CONTENT),
    http_delete("/users/myuser2", ?NO_CONTENT),
    http_delete("/vhosts/myvhost1", ?NO_CONTENT),
    http_delete("/vhosts/myvhost2", ?NO_CONTENT),
    ok.

http_permissions_test() ->
    http_put("/users/myuser", [{password, "myuser"}, {administrator, true}],
             ?NO_CONTENT),
    http_put("/vhosts/myvhost", [], ?NO_CONTENT),

    http_put("/permissions/myvhost/myuser",
             [{configure, "foo"}, {write, "foo"},
              {read,      "foo"}, {scope, "client"}], ?NO_CONTENT),

    [{vhost,<<"myvhost">>},
     {configure,<<"foo">>},
     {write,<<"foo">>},
     {read,<<"foo">>},
     {scope,<<"client">>}] =
        http_get("/permissions/myvhost/myuser"),
    http_delete("/permissions/myvhost/myuser", ?NO_CONTENT),
    http_get("/permissions/myvhost/myuser", ?NOT_FOUND),

    http_delete("/users/myuser", ?NO_CONTENT),
    http_delete("/vhosts/myvhost", ?NO_CONTENT),
    ok.

http_connections_test() ->
    {ok, Conn} = amqp_connection:start(network),
    LocalPort = rabbit_mgmt_test_db:local_port(Conn),
    Path = binary_to_list(
             rabbit_mgmt_format:print(
               "/connections/127.0.0.1%3A~w", [LocalPort])),
    http_get(Path, ?OK),
    http_delete(Path, ?NO_CONTENT),
    http_get(Path, ?NOT_FOUND).

test_auth(Code, Headers) ->
    {ok, {{_, Code, _}, _, _}} = req(get, "/overview", Headers).

http_exchanges_test() ->
    %% Can pass booleans or strings
    Good = [{type, "direct"}, {durable, "true"}, {auto_delete, false},
            {arguments, ""}],
    http_put("/vhosts/myvhost", [], ?NO_CONTENT),
    http_get("/exchanges/myvhost/foo", ?NOT_AUTHORISED),
    http_put("/exchanges/myvhost/foo", Good, ?NOT_AUTHORISED),
    http_put("/permissions/myvhost/guest",
             [{configure, ".*"}, {write, ".*"},
              {read,      ".*"}, {scope, "client"}], ?NO_CONTENT),
    http_get("/exchanges/myvhost/foo", ?NOT_FOUND),
    http_put("/exchanges/myvhost/foo", Good, ?NO_CONTENT),
    http_put("/exchanges/myvhost/foo", Good, ?NO_CONTENT),
    http_get("/exchanges/%2f/foo", ?NOT_FOUND),
    [{name,<<"foo">>},
     {vhost,<<"myvhost">>},
     {type,<<"direct">>},
     {durable,true},
     {auto_delete,false},
     {arguments,[]}] =
        http_get("/exchanges/myvhost/foo"),

    http_put("/exchanges/badvhost/bar", Good, ?NOT_FOUND),
    http_put("/exchanges/myvhost/bar",
             [{type, "bad_exchange_type"},
              {durable, true}, {auto_delete, false}, {arguments, ""}],
             ?BAD_REQUEST),
    http_put("/exchanges/myvhost/bar",
             [{type, "direct"},
              {durable, "troo"}, {auto_delete, false}, {arguments, ""}],
             ?BAD_REQUEST),
    http_put("/exchanges/myvhost/foo",
             [{type, "direct"},
              {durable, false}, {auto_delete, false}, {arguments, ""}],
             ?BAD_REQUEST),

    http_delete("/exchanges/myvhost/foo", ?NO_CONTENT),
    http_delete("/exchanges/myvhost/foo", ?NOT_FOUND),

    http_delete("/vhosts/myvhost", ?NO_CONTENT),
    ok.

http_queues_test() ->
    Good = [{durable, "true"}, {auto_delete, false}, {arguments, ""}],
    http_get("/queues/%2f/foo", ?NOT_FOUND),
    http_put("/queues/%2f/foo", Good, ?NO_CONTENT),
    http_put("/queues/%2f/foo", Good, ?NO_CONTENT),
    http_get("/queues/%2f/foo", ?OK),

    http_put("/queues/badvhost/bar", Good, ?NOT_FOUND),
    http_put("/queues/%2f/bar",
             [{durable, "troo"}, {auto_delete, false}, {arguments, ""}],
             ?BAD_REQUEST),
    http_put("/queues/%2f/foo",
             [{durable, false}, {auto_delete, false}, {arguments, ""}],
             ?BAD_REQUEST),

    http_put("/queues/%2f/baz", Good, ?NO_CONTENT),

    Queues = http_get("/queues/%2f"),
    Queue = http_get("/queues/%2f/foo"),
    assert_list([[{name,        <<"foo">>},
                  {vhost,       <<"/">>},
                  {durable,     true},
                  {auto_delete, false},
                  {arguments,   []}],
                 [{name,        <<"baz">>},
                  {vhost,       <<"/">>},
                  {durable,     true},
                  {auto_delete, false},
                  {arguments,   []}]], Queues),
    assert_item([{name,        <<"foo">>},
                 {vhost,       <<"/">>},
                 {durable,     true},
                 {auto_delete, false},
                 {arguments,   []}], Queue),

    http_delete("/queues/%2f/foo", ?NO_CONTENT),
    http_delete("/queues/%2f/baz", ?NO_CONTENT),
    http_delete("/queues/%2f/foo", ?NOT_FOUND),
    ok.

http_bindings_test() ->
    XArgs = [{type, "direct"}, {durable, false}, {auto_delete, false},
             {arguments, ""}],
    QArgs = [{durable, false}, {auto_delete, false}, {arguments, ""}],
    http_put("/exchanges/%2f/myexchange", XArgs, ?NO_CONTENT),
    http_put("/queues/%2f/myqueue", QArgs, ?NO_CONTENT),
    http_put("/bindings/%2f/badqueue/myexchange/key_routing", [], ?NOT_FOUND),
    http_put("/bindings/%2f/myqueue/badexchange/key_routing", [], ?NOT_FOUND),
    http_put("/bindings/%2f/myqueue/myexchange/bad_routing", [], ?BAD_REQUEST),
    http_put("/bindings/%2f/myqueue/myexchange/key_routing", [], ?NO_CONTENT),
    [{exchange,<<"myexchange">>},
     {vhost,<<"/">>},
     {queue,<<"myqueue">>},
     {routing_key,<<"routing">>},
     {arguments,[]},
     {properties_key,<<"key_routing">>}] =
        http_get("/bindings/%2f/myqueue/myexchange/key_routing", ?OK),
    http_delete("/bindings/%2f/myqueue/myexchange/key_routing", ?NO_CONTENT),
    http_delete("/bindings/%2f/myqueue/myexchange/key_routing", ?NOT_FOUND),
    http_delete("/exchanges/%2f/myexchange", ?NO_CONTENT),
    http_delete("/queues/%2f/myqueue", ?NO_CONTENT),
    ok.

http_bindings_post_test() ->
    XArgs = [{type, "direct"}, {durable, false}, {auto_delete, false},
             {arguments, ""}],
    QArgs = [{durable, false}, {auto_delete, false}, {arguments, ""}],
    BArgs = [{routing_key, "routing"}, {arguments, ""}],
    http_put("/exchanges/%2f/myexchange", XArgs, ?NO_CONTENT),
    http_put("/queues/%2f/myqueue", QArgs, ?NO_CONTENT),
    http_post("/bindings/%2f/badqueue/myexchange", BArgs, ?NOT_FOUND),
    http_post("/bindings/%2f/myqueue/badexchange", BArgs, ?NOT_FOUND),
    http_post("/bindings/%2f/myqueue/myexchange", [{a, "b"}], ?BAD_REQUEST),
    Headers = http_post("/bindings/%2f/myqueue/myexchange", BArgs, ?CREATED),
    "/api/bindings/%2F/myqueue/myexchange/key_routing" =
        pget("location", Headers),
    [{exchange,<<"myexchange">>},
     {vhost,<<"/">>},
     {queue,<<"myqueue">>},
     {routing_key,<<"routing">>},
     {arguments,[]},
     {properties_key,<<"key_routing">>}] =
        http_get("/bindings/%2f/myqueue/myexchange/key_routing", ?OK),
    http_delete("/bindings/%2f/myqueue/myexchange/key_routing", ?NO_CONTENT),
    http_delete("/exchanges/%2f/myexchange", ?NO_CONTENT),
    http_delete("/queues/%2f/myqueue", ?NO_CONTENT),
    ok.

http_permissions_administrator_test() ->
    http_put("/users/notadmin", [{password, "notadmin"},
                                 {administrator, false}], ?NO_CONTENT),
    Test =
        fun(Path) ->
                http_get(Path, "notadmin", "notadmin", ?NOT_AUTHORISED),
                http_get(Path, "guest", "guest", ?OK)
        end,
    Test("/vhosts"),
    Test("/vhosts/%2f"),
    Test("/users"),
    Test("/users/guest"),
    Test("/users/guest/permissions"),
    Test("/permissions"),
    Test("/permissions/%2f/guest"),
    http_delete("/users/notadmin", ?NO_CONTENT),
    ok.

http_permissions_vhost_test() ->
    QArgs = [{durable, false}, {auto_delete, false}, {arguments, ""}],
    PermArgs = [{configure, ".*"}, {write, ".*"},
                {read,      ".*"}, {scope, "client"}],
    http_put("/users/myuser", [{password, "myuser"},
                               {administrator, false}], ?NO_CONTENT),
    http_put("/vhosts/myvhost1", [], ?NO_CONTENT),
    http_put("/vhosts/myvhost2", [], ?NO_CONTENT),
    http_put("/permissions/myvhost1/myuser", PermArgs, ?NO_CONTENT),
    http_put("/permissions/myvhost1/guest", PermArgs, ?NO_CONTENT),
    http_put("/permissions/myvhost2/guest", PermArgs, ?NO_CONTENT),
    http_put("/queues/myvhost1/myqueue", QArgs, ?NO_CONTENT),
    http_put("/queues/myvhost2/myqueue", QArgs, ?NO_CONTENT),
    Test1 =
        fun(Path) ->
                Results = http_get(Path, "myuser", "myuser", ?OK),
                [case pget(vhost, Result) of
                     <<"myvhost2">> ->
                         throw({got_result_from_vhost2_in, Path, Result});
                     _ ->
                         ok
                 end || Result <- Results]
        end,
    Test2 =
        fun(Path1, Path2) ->
                http_get(Path1 ++ "/myvhost1/" ++ Path2, "myuser", "myuser",
                         ?OK),
                http_get(Path1 ++ "/myvhost2/" ++ Path2, "myuser", "myuser",
                         ?NOT_AUTHORISED)
        end,
    Test1("/exchanges"),
    Test2("/exchanges", ""),
    Test2("/exchanges", "amq.direct"),
    Test1("/queues"),
    Test2("/queues", ""),
    Test2("/queues", "myqueue"),
    Test1("/bindings"),
    Test2("/bindings", ""),
    Test2("/queues", "myqueue/bindings"),
    Test2("/exchanges", "amq.default/bindings"),
    Test2("/bindings", "myqueue/amq.default"),
    Test2("/bindings", "myqueue/amq.default/key_test"),
    http_delete("/vhosts/myvhost1", ?NO_CONTENT),
    http_delete("/vhosts/myvhost2", ?NO_CONTENT),
    http_delete("/users/myuser", ?NO_CONTENT),
    ok.

get_conn(Username, Password) ->
    {ok, Conn} = amqp_connection:start(network, #amqp_params{
                                        username = Username,
                                        password = Password}),
    LocalPort = rabbit_mgmt_test_db:local_port(Conn),
    ConnPath = binary_to_list(
                 rabbit_mgmt_format:print(
                   "/connections/127.0.0.1%3A~w", [LocalPort])),
    ChPath = binary_to_list(
               rabbit_mgmt_format:print(
                 "/channels/127.0.0.1%3A~w%3A1", [LocalPort])),
    {Conn, ConnPath, ChPath}.

http_permissions_connection_channel_test() ->
    PermArgs = [{configure, ".*"}, {write, ".*"},
                {read,      ".*"}, {scope, "client"}],
    http_put("/users/user", [{password, "user"},
                             {administrator, false}], ?NO_CONTENT),
    http_put("/permissions/%2f/user", PermArgs, ?NO_CONTENT),
    {Conn1, ConnPath1, ChPath1} = get_conn("user", "user"),
    {Conn2, ConnPath2, ChPath2} = get_conn("guest", "guest"),
    {ok, _Ch1} = amqp_connection:open_channel(Conn1),
    {ok, _Ch2} = amqp_connection:open_channel(Conn2),

    2 = length(http_get("/connections", ?OK)),
    1 = length(http_get("/connections", "user", "user", ?OK)),
    http_get(ConnPath1, ?OK),
    http_get(ConnPath2, ?OK),
    http_get(ConnPath1, "user", "user", ?OK),
    http_get(ConnPath2, "user", "user", ?NOT_AUTHORISED),
    2 = length(http_get("/channels", ?OK)),
    1 = length(http_get("/channels", "user", "user", ?OK)),
    http_get(ChPath1, ?OK),
    http_get(ChPath2, ?OK),
    http_get(ChPath1, "user", "user", ?OK),
    http_get(ChPath2, "user", "user", ?NOT_AUTHORISED),

    amqp_connection:close(Conn1),
    amqp_connection:close(Conn2),
    ok.

http_unicode_test() ->
    QArgs = [{durable, false}, {auto_delete, false}, {arguments, ""}],
    http_put("/queues/%2f/♫♪♫♪", QArgs, ?NO_CONTENT),
    http_get("/queues/%2f/♫♪♫♪", ?OK),
    http_delete("/queues/%2f/♫♪♫♪", ?NO_CONTENT),
    ok.


%%---------------------------------------------------------------------------
http_get(Path) ->
    http_get(Path, ?OK).

http_get(Path, CodeExp) ->
    http_get(Path, "guest", "guest", CodeExp).

http_get(Path, User, Pass, CodeExp) ->
    {ok, {{_HTTP, CodeExp, _}, Headers, ResBody}} =
        req(get, Path, [auth_header(User, Pass)]),
    decode(CodeExp, Headers, ResBody).

http_put(Path, List, CodeExp) ->
    http_put_raw(Path, format_for_upload(List), CodeExp).

http_post(Path, List, CodeExp) ->
    http_post_raw(Path, format_for_upload(List), CodeExp).

format_for_upload(List) ->
    L2 = [{K, format_for_upload_item(V)} || {K, V} <- List],
    iolist_to_binary(mochijson2:encode({struct, L2})).

format_for_upload_item(V) when is_list(V) ->
    list_to_binary(V);
format_for_upload_item(V) ->
    V.

http_put_raw(Path, Body, CodeExp) ->
    http_upload_raw(put, Path, Body, CodeExp).

http_post_raw(Path, Body, CodeExp) ->
    http_upload_raw(post, Path, Body, CodeExp).

%% TODO Lose the sleep below. What is happening async?
http_upload_raw(Type, Path, Body, CodeExp) ->
    {ok, {{_HTTP, CodeExp, _}, Headers, ResBody}} =
        req(Type, Path, [auth_header()], Body),
    timer:sleep(100),
    decode(CodeExp, Headers, ResBody).

http_delete(Path, CodeExp) ->
    {ok, {{_HTTP, CodeExp, _}, Headers, ResBody}} =
        req(delete, Path, [auth_header()]),
    decode(CodeExp, Headers, ResBody).

req(Type, Path, Headers) ->
    httpc:request(Type, {?PREFIX ++ Path, Headers}, [], []).

req(Type, Path, Headers, Body) ->
    httpc:request(Type, {?PREFIX ++ Path, Headers, "application/json", Body},
                  [], []).

decode(Code, Headers, ResBody) ->
    case Code of
        ?OK -> cleanup(mochijson2:decode(ResBody));
        _   -> Headers
    end.

cleanup(L) when is_list(L) ->
    [cleanup(I) || I <- L];
cleanup({struct, I}) ->
    cleanup(I);
cleanup({K, V}) when is_binary(K) ->
    {list_to_atom(binary_to_list(K)), cleanup(V)};
cleanup(I) ->
    I.

auth_header() ->
    auth_header("guest", "guest").

auth_header(Username, Password) ->
    {"Authorization",
     "Basic " ++ binary_to_list(base64:encode(Username ++ ":" ++ Password))}.

%%---------------------------------------------------------------------------

assert_list(Exp, Act) ->
    Len = length(Exp),
    Len = length(Act),
    [case length(lists:filter(fun(ActI) -> test_item(ExpI, ActI) end, Act)) of
         1 -> ok;
         N -> throw({found, N, ExpI, in, Act})
     end
     || ExpI <- Exp].

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
    lists:filter(fun (I) -> I =/= ok end,
                 [case lists:member(ExpI, Act) of
                      true  -> ok;
                      false -> {did_not_find, ExpI, in, Act}
                  end|| ExpI <- Exp]).

%%---------------------------------------------------------------------------

pget(K, L) ->
     proplists:get_value(K, L).

equals(F1, F2) ->
    true = (abs(F1 - F2) < 0.001).
