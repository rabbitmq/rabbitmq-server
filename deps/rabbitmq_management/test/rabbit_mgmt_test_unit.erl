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
    equals(0.1, pget(foo_rate, WithRates)),
    equals(10, pget(bar_rate, WithRates)),
    undefined = pget(bash_rate, WithRates),
    undefined = pget(baz_rate, WithRates).

http_overview_test() ->
    %% Rather crude, but this req doesn't say much and at least this means it
    %% didn't blow up.
    [<<"0.0.0.0:5672">>] = pget(<<"bound_to">>, http_get("/overview", ?OK)).

http_auth_test() ->
    test_auth(?NOT_AUTHORISED, []),
    test_auth(?NOT_AUTHORISED, [auth_header("guest", "gust")]),
    test_auth(?OK, [auth_header("guest", "guest")]).

%% This test is rather over-verbose as we're trying to test understanding of
%% Webmachine
http_vhosts_test() ->
    [<<"/">>] = http_get_check("/vhosts", "vhosts"),
    %% Create a new one
    http_put("/vhosts/myvhost", [], ?NO_CONTENT),
    %% PUT should be idempotent
    http_put("/vhosts/myvhost", [], ?NO_CONTENT),
    %% Check it's there
    [<<"/">>, <<"myvhost">>] = http_get_check("/vhosts", "vhosts"),
    %% Check individually
    <<"/">> = http_get_check("/vhosts/%2f", "vhost"),
    <<"myvhost">> = http_get_check("/vhosts/myvhost", "vhost"),
    %% Delete it
    http_delete("/vhosts/myvhost", ?NO_CONTENT),
    %% It's not there
    http_get("/vhosts/myvhost", ?NOT_FOUND),
    http_delete("/vhosts/myvhost", ?NOT_FOUND).

http_users_test() ->
    http_get("/users/myuser", ?NOT_FOUND),
    http_put_raw("/users/myuser", "Something not JSON", ?BAD_REQUEST),
    http_put("/users/myuser", [{flim, "flam"}], ?BAD_REQUEST),
    http_put("/users/myuser", [{password, "myuser"}], ?NO_CONTENT),
    http_put("/users/myuser", [{password, "password"}], ?NO_CONTENT),
    <<"myuser">> = http_get_check("/users/myuser", "user"),
    [<<"guest">>, <<"myuser">>] = http_get_check("/users", "users"),
    test_auth(?OK, [auth_header("myuser", "password")]),
    http_delete("/users/myuser", ?NO_CONTENT),
    test_auth(?NOT_AUTHORISED, [auth_header("myuser", "password")]),
    http_get("/users/myuser", ?NOT_FOUND).

http_permissions_validation_test() ->
    Good = [{configure, ".*"}, {write, ".*"},
            {read,      ".*"}, {scope, "client"}],
    http_put("/permissions/guest/wrong", Good, ?BAD_REQUEST),
    http_put("/permissions/wrong/%2f", Good, ?BAD_REQUEST),
    http_put("/permissions/guest/%2f",
             [{configure, ".*"}, {write, ".*"},
              {read,      ".*"}], ?BAD_REQUEST),
    http_put("/permissions/guest/%2f",
             [{configure, ".*"}, {write, ".*"},
              {read,      ".*"}, {scope, "wrong"}], ?BAD_REQUEST),
    http_put("/permissions/guest/%2f",
             [{configure, "["}, {write, ".*"},
              {read,      ".*"}, {scope, "client"}], ?BAD_REQUEST),
    http_put("/permissions/guest/%2f", Good, ?NO_CONTENT),
    ok.

http_permissions_list_test() ->
    [{struct,[{<<"vhost">>,<<"/">>},
              {<<"user">>,<<"guest">>},
              {<<"configure">>,<<".*">>},
              {<<"write">>,<<".*">>},
              {<<"read">>,<<".*">>},
              {<<"scope">>,<<"client">>}]}] =
        http_get_check("/permissions", "permissions"),

    http_put("/users/myuser1", [{password, ""}], ?NO_CONTENT),
    http_put("/users/myuser2", [{password, ""}], ?NO_CONTENT),
    http_put("/vhosts/myvhost1", [], ?NO_CONTENT),
    http_put("/vhosts/myvhost2", [], ?NO_CONTENT),

    Perms = [{configure, "foo"}, {write, "foo"},
             {read,      "foo"}, {scope, "client"}],
    http_put("/permissions/myuser1/myvhost1", Perms, ?NO_CONTENT),
    http_put("/permissions/myuser1/myvhost2", Perms, ?NO_CONTENT),
    http_put("/permissions/myuser2/myvhost1", Perms, ?NO_CONTENT),

    4 = length(http_get_check("/permissions", "permissions")),
    2 = length(http_get_check("/permissions/myuser1", "permissions")),
    1 = length(http_get_check("/permissions/myuser2", "permissions")),

    http_delete("/users/myuser1", ?NO_CONTENT),
    http_delete("/users/myuser2", ?NO_CONTENT),
    http_delete("/vhosts/myvhost1", ?NO_CONTENT),
    http_delete("/vhosts/myvhost2", ?NO_CONTENT),
    ok.

http_permissions_test() ->
    http_put("/users/myuser", [{password, "myuser"}], ?NO_CONTENT),
    http_put("/vhosts/myvhost", [], ?NO_CONTENT),

    http_put("/permissions/myuser/myvhost",
             [{configure, "foo"}, {write, "foo"},
              {read,      "foo"}, {scope, "client"}], ?NO_CONTENT),

    {struct,[{<<"vhost">>,<<"myvhost">>},
             {<<"configure">>,<<"foo">>},
             {<<"write">>,<<"foo">>},
             {<<"read">>,<<"foo">>},
             {<<"scope">>,<<"client">>}]} =
        http_get_check("/permissions/myuser/myvhost", "permission"),
    http_delete("/permissions/myuser/myvhost", ?NO_CONTENT),
    http_get("/permissions/myuser/myvhost", ?NOT_FOUND),

    http_delete("/users/myuser", ?NO_CONTENT),
    http_delete("/vhosts/myvhost", ?NO_CONTENT),
    ok.

http_connections_test() ->
    Conn = amqp_connection:start_network(),
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
    http_get("/exchanges/myvhost/foo", ?NOT_FOUND),
    http_put("/exchanges/myvhost/foo", Good, ?NOT_AUTHORISED),
    http_put("/permissions/guest/myvhost",
             [{configure, ".*"}, {write, ".*"},
              {read,      ".*"}, {scope, "client"}], ?NO_CONTENT),
    http_put("/exchanges/myvhost/foo", Good, ?NO_CONTENT),
    http_put("/exchanges/myvhost/foo", Good, ?NO_CONTENT),
    http_get("/exchanges/%2f/foo", ?NOT_FOUND),
    {struct,[{<<"name">>,<<"foo">>},
             {<<"vhost">>,<<"myvhost">>},
             {<<"type">>,<<"direct">>},
             {<<"durable">>,true},
             {<<"auto_delete">>,false},
             {<<"arguments">>,{struct,[]}}]} =
        http_get_check("/exchanges/myvhost/foo", "exchange"),

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

%% TODO this test could be more
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

    http_delete("/queues/%2f/foo", ?NO_CONTENT),
    http_delete("/queues/%2f/foo", ?NOT_FOUND),
    ok.

%%---------------------------------------------------------------------------

http_get(Path, CodeExp) ->
    {ok, {{_HTTP, CodeExp, _}, _Headers, ResBody}} =
        req(get, Path, [auth_header()]),
    decode(CodeExp, ResBody).

http_put(Path, List, CodeExp) ->
    L2 = [{K, format_for_put(V)} || {K, V} <- List],
    Enc = iolist_to_binary(mochijson2:encode({struct, L2})),
    http_put_raw(Path, Enc, CodeExp).

format_for_put(V) when is_list(V) ->
    list_to_binary(V);
format_for_put(V) ->
    V.

%% TODO Lose the sleep below. What is happening async?
http_put_raw(Path, Body, CodeExp) ->
    {ok, {{_HTTP, CodeExp, _}, _Headers, ResBody}} =
        req(put, Path, [auth_header()], Body),
    timer:sleep(100),
    decode(CodeExp, ResBody).

http_delete(Path, CodeExp) ->
    {ok, {{_HTTP, CodeExp, _}, _Headers, ResBody}} =
        req(delete, Path, [auth_header()]),
    decode(CodeExp, ResBody).

http_get_check(Path, Key) ->
    pget(list_to_binary(Key), http_get(Path, ?OK)).

req(Type, Path, Headers) ->
    httpc:request(Type, {?PREFIX ++ Path, Headers}, [], []).

req(Type, Path, Headers, Body) ->
    httpc:request(Type, {?PREFIX ++ Path, Headers, "application/json", Body},
                  [], []).

decode(Code, ResBody) ->
    case Code of
        ?OK -> {struct, Res} = mochijson2:decode(ResBody),
               Res;
        _   -> ok
    end.

auth_header() ->
    auth_header("guest", "guest").

auth_header(Username, Password) ->
    {"Authorization",
     "Basic " ++ binary_to_list(base64:encode(Username ++ ":" ++ Password))}.

%%---------------------------------------------------------------------------

pget(K, L) ->
     proplists:get_value(K, L).

equals(F1, F2) ->
    true = (abs(F1 - F2) < 0.001).
