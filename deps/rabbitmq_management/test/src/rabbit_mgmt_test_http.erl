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
%%   Copyright (c) 2010-2013 GoPivotal, Inc.  All rights reserved.
%%

-module(rabbit_mgmt_test_http).

-include("rabbit_mgmt_test.hrl").

-export([http_get/1, http_put/3, http_delete/2]).

-import(rabbit_mgmt_test_util, [assert_list/2, assert_item/2, test_item/2]).
-import(rabbit_misc, [pget/2]).

overview_test() ->
    %% Rather crude, but this req doesn't say much and at least this means it
    %% didn't blow up.
    true = 0 < length(pget(listeners, http_get("/overview"))),
    http_put("/users/myuser", [{password, <<"myuser">>},
                               {tags,     <<"management">>}], ?NO_CONTENT),
    http_get("/overview", "myuser", "myuser", ?OK),
    http_delete("/users/myuser", ?NO_CONTENT),
    %% TODO uncomment when priv works in test
    %%http_get(""),
    ok.

nodes_test() ->
    http_put("/users/user", [{password, <<"user">>},
                             {tags, <<"management">>}], ?NO_CONTENT),
    http_put("/users/monitor", [{password, <<"monitor">>},
                                {tags, <<"monitoring">>}], ?NO_CONTENT),
    DiscNode = [{type, <<"disc">>}, {running, true}],
    assert_list([DiscNode], http_get("/nodes")),
    assert_list([DiscNode], http_get("/nodes", "monitor", "monitor", ?OK)),
    http_get("/nodes", "user", "user", ?NOT_AUTHORISED),
    [Node] = http_get("/nodes"),
    Path = "/nodes/" ++ binary_to_list(pget(name, Node)),
    assert_item(DiscNode, http_get(Path, ?OK)),
    assert_item(DiscNode, http_get(Path, "monitor", "monitor", ?OK)),
    http_get(Path, "user", "user", ?NOT_AUTHORISED),
    http_delete("/users/user", ?NO_CONTENT),
    http_delete("/users/monitor", ?NO_CONTENT),
    ok.

auth_test() ->
    http_put("/users/user", [{password, <<"user">>},
                             {tags, <<"">>}], ?NO_CONTENT),
    test_auth(?NOT_AUTHORISED, []),
    test_auth(?NOT_AUTHORISED, [auth_header("user", "user")]),
    test_auth(?NOT_AUTHORISED, [auth_header("guest", "gust")]),
    test_auth(?OK, [auth_header("guest", "guest")]),
    http_delete("/users/user", ?NO_CONTENT),
    ok.

%% This test is rather over-verbose as we're trying to test understanding of
%% Webmachine
vhosts_test() ->
    assert_list([[{name, <<"/">>}]], http_get("/vhosts")),
    %% Create a new one
    http_put("/vhosts/myvhost", none, ?NO_CONTENT),
    %% PUT should be idempotent
    http_put("/vhosts/myvhost", none, ?NO_CONTENT),
    %% Check it's there
    assert_list([[{name, <<"/">>}], [{name, <<"myvhost">>}]],
                http_get("/vhosts")),
    %% Check individually
    assert_item([{name, <<"/">>}], http_get("/vhosts/%2f", ?OK)),
    assert_item([{name, <<"myvhost">>}],http_get("/vhosts/myvhost")),
    %% Delete it
    http_delete("/vhosts/myvhost", ?NO_CONTENT),
    %% It's not there
    http_get("/vhosts/myvhost", ?NOT_FOUND),
    http_delete("/vhosts/myvhost", ?NOT_FOUND).

vhosts_trace_test() ->
    http_put("/vhosts/myvhost", none, ?NO_CONTENT),
    Disabled = [{name,  <<"myvhost">>}, {tracing, false}],
    Enabled  = [{name,  <<"myvhost">>}, {tracing, true}],
    Disabled = http_get("/vhosts/myvhost"),
    http_put("/vhosts/myvhost", [{tracing, true}], ?NO_CONTENT),
    Enabled = http_get("/vhosts/myvhost"),
    http_put("/vhosts/myvhost", [{tracing, true}], ?NO_CONTENT),
    Enabled = http_get("/vhosts/myvhost"),
    http_put("/vhosts/myvhost", [{tracing, false}], ?NO_CONTENT),
    Disabled = http_get("/vhosts/myvhost"),
    http_delete("/vhosts/myvhost", ?NO_CONTENT).

users_test() ->
    assert_item([{name, <<"guest">>}, {tags, <<"administrator">>}],
                http_get("/whoami")),
    http_get("/users/myuser", ?NOT_FOUND),
    http_put_raw("/users/myuser", "Something not JSON", ?BAD_REQUEST),
    http_put("/users/myuser", [{flim, <<"flam">>}], ?BAD_REQUEST),
    http_put("/users/myuser", [{tags, <<"management">>}], ?NO_CONTENT),
    http_put("/users/myuser", [{password_hash, <<"not_hash">>}], ?BAD_REQUEST),
    http_put("/users/myuser", [{password_hash,
                                <<"IECV6PZI/Invh0DL187KFpkO5Jc=">>},
                               {tags, <<"management">>}], ?NO_CONTENT),
    http_put("/users/myuser", [{password, <<"password">>},
                               {tags, <<"administrator, foo">>}], ?NO_CONTENT),
    assert_item([{name, <<"myuser">>}, {tags, <<"administrator,foo">>}],
                http_get("/users/myuser")),
    assert_list([[{name, <<"myuser">>}, {tags, <<"administrator,foo">>}],
                 [{name, <<"guest">>}, {tags, <<"administrator">>}]],
                http_get("/users")),
    test_auth(?OK, [auth_header("myuser", "password")]),
    http_delete("/users/myuser", ?NO_CONTENT),
    test_auth(?NOT_AUTHORISED, [auth_header("myuser", "password")]),
    http_get("/users/myuser", ?NOT_FOUND),
    ok.

users_legacy_administrator_test() ->
    http_put("/users/myuser1", [{administrator, <<"true">>}], ?NO_CONTENT),
    http_put("/users/myuser2", [{administrator, <<"false">>}], ?NO_CONTENT),
    assert_item([{name, <<"myuser1">>}, {tags, <<"administrator">>}],
                http_get("/users/myuser1")),
    assert_item([{name, <<"myuser2">>}, {tags, <<"">>}],
                http_get("/users/myuser2")),
    http_delete("/users/myuser1", ?NO_CONTENT),
    http_delete("/users/myuser2", ?NO_CONTENT),
    ok.

permissions_validation_test() ->
    Good = [{configure, <<".*">>}, {write, <<".*">>}, {read, <<".*">>}],
    http_put("/permissions/wrong/guest", Good, ?BAD_REQUEST),
    http_put("/permissions/%2f/wrong", Good, ?BAD_REQUEST),
    http_put("/permissions/%2f/guest",
             [{configure, <<"[">>}, {write, <<".*">>}, {read, <<".*">>}],
             ?BAD_REQUEST),
    http_put("/permissions/%2f/guest", Good, ?NO_CONTENT),
    ok.

permissions_list_test() ->
    [[{user,<<"guest">>},
      {vhost,<<"/">>},
      {configure,<<".*">>},
      {write,<<".*">>},
      {read,<<".*">>}]] =
        http_get("/permissions"),

    http_put("/users/myuser1", [{password, <<"">>}, {tags, <<"administrator">>}],
             ?NO_CONTENT),
    http_put("/users/myuser2", [{password, <<"">>}, {tags, <<"administrator">>}],
             ?NO_CONTENT),
    http_put("/vhosts/myvhost1", none, ?NO_CONTENT),
    http_put("/vhosts/myvhost2", none, ?NO_CONTENT),

    Perms = [{configure, <<"foo">>}, {write, <<"foo">>}, {read, <<"foo">>}],
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

permissions_test() ->
    http_put("/users/myuser", [{password, <<"myuser">>}, {tags, <<"administrator">>}],
             ?NO_CONTENT),
    http_put("/vhosts/myvhost", none, ?NO_CONTENT),

    http_put("/permissions/myvhost/myuser",
             [{configure, <<"foo">>}, {write, <<"foo">>}, {read, <<"foo">>}],
             ?NO_CONTENT),

    Permission = [{user,<<"myuser">>},
                  {vhost,<<"myvhost">>},
                  {configure,<<"foo">>},
                  {write,<<"foo">>},
                  {read,<<"foo">>}],
    Default = [{user,<<"guest">>},
               {vhost,<<"/">>},
               {configure,<<".*">>},
               {write,<<".*">>},
               {read,<<".*">>}],
    Permission = http_get("/permissions/myvhost/myuser"),
    assert_list([Permission, Default], http_get("/permissions")),
    assert_list([Permission], http_get("/users/myuser/permissions")),
    http_delete("/permissions/myvhost/myuser", ?NO_CONTENT),
    http_get("/permissions/myvhost/myuser", ?NOT_FOUND),

    http_delete("/users/myuser", ?NO_CONTENT),
    http_delete("/vhosts/myvhost", ?NO_CONTENT),
    ok.

connections_test() ->
    {ok, Conn} = amqp_connection:start(#amqp_params_network{}),
    LocalPort = local_port(Conn),
    Path = binary_to_list(
             rabbit_mgmt_format:print(
               "/connections/127.0.0.1%3A~w%20->%20127.0.0.1%3A5672",
               [LocalPort])),
    http_get(Path, ?OK),
    http_delete(Path, ?NO_CONTENT),
    %% TODO rabbit_reader:shutdown/2 returns before the connection is
    %% closed. It may not be worth fixing.
    timer:sleep(200),
    http_get(Path, ?NOT_FOUND).

test_auth(Code, Headers) ->
    {ok, {{_, Code, _}, _, _}} = req(get, "/overview", Headers).

exchanges_test() ->
    %% Can pass booleans or strings
    Good = [{type, <<"direct">>}, {durable, <<"true">>}],
    http_put("/vhosts/myvhost", none, ?NO_CONTENT),
    http_get("/exchanges/myvhost/foo", ?NOT_AUTHORISED),
    http_put("/exchanges/myvhost/foo", Good, ?NOT_AUTHORISED),
    http_put("/permissions/myvhost/guest",
             [{configure, <<".*">>}, {write, <<".*">>}, {read, <<".*">>}],
             ?NO_CONTENT),
    http_get("/exchanges/myvhost/foo", ?NOT_FOUND),
    http_put("/exchanges/myvhost/foo", Good, ?NO_CONTENT),
    http_put("/exchanges/myvhost/foo", Good, ?NO_CONTENT),
    http_get("/exchanges/%2f/foo", ?NOT_FOUND),
    assert_item([{name,<<"foo">>},
                 {vhost,<<"myvhost">>},
                 {type,<<"direct">>},
                 {durable,true},
                 {auto_delete,false},
                 {internal,false},
                 {arguments,[]}],
                http_get("/exchanges/myvhost/foo")),

    http_put("/exchanges/badvhost/bar", Good, ?NOT_FOUND),
    http_put("/exchanges/myvhost/bar", [{type, <<"bad_exchange_type">>}],
             ?BAD_REQUEST),
    http_put("/exchanges/myvhost/bar", [{type, <<"direct">>},
                                        {durable, <<"troo">>}],
             ?BAD_REQUEST),
    http_put("/exchanges/myvhost/foo", [{type, <<"direct">>}],
             ?BAD_REQUEST),

    http_delete("/exchanges/myvhost/foo", ?NO_CONTENT),
    http_delete("/exchanges/myvhost/foo", ?NOT_FOUND),

    http_delete("/vhosts/myvhost", ?NO_CONTENT),
    http_get("/exchanges/badvhost", ?NOT_FOUND),
    ok.

queues_test() ->
    Good = [{durable, true}],
    http_get("/queues/%2f/foo", ?NOT_FOUND),
    http_put("/queues/%2f/foo", Good, ?NO_CONTENT),
    http_put("/queues/%2f/foo", Good, ?NO_CONTENT),
    http_get("/queues/%2f/foo", ?OK),

    http_put("/queues/badvhost/bar", Good, ?NOT_FOUND),
    http_put("/queues/%2f/bar",
             [{durable, <<"troo">>}],
             ?BAD_REQUEST),
    http_put("/queues/%2f/foo",
             [{durable, false}],
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
    http_get("/queues/badvhost", ?NOT_FOUND),
    ok.

bindings_test() ->
    XArgs = [{type, <<"direct">>}],
    QArgs = [],
    http_put("/exchanges/%2f/myexchange", XArgs, ?NO_CONTENT),
    http_put("/queues/%2f/myqueue", QArgs, ?NO_CONTENT),
    BArgs = [{routing_key, <<"routing">>}, {arguments, []}],
    http_post("/bindings/%2f/e/myexchange/q/myqueue", BArgs, ?CREATED),
    http_get("/bindings/%2f/e/myexchange/q/myqueue/routing", ?OK),
    http_get("/bindings/%2f/e/myexchange/q/myqueue/rooting", ?NOT_FOUND),
    Binding =
        [{source,<<"myexchange">>},
         {vhost,<<"/">>},
         {destination,<<"myqueue">>},
         {destination_type,<<"queue">>},
         {routing_key,<<"routing">>},
         {arguments,[]},
         {properties_key,<<"routing">>}],
    DBinding =
        [{source,<<"">>},
         {vhost,<<"/">>},
         {destination,<<"myqueue">>},
         {destination_type,<<"queue">>},
         {routing_key,<<"myqueue">>},
         {arguments,[]},
         {properties_key,<<"myqueue">>}],
    Binding = http_get("/bindings/%2f/e/myexchange/q/myqueue/routing"),
    assert_list([Binding],
                http_get("/bindings/%2f/e/myexchange/q/myqueue")),
    assert_list([Binding, DBinding],
                http_get("/queues/%2f/myqueue/bindings")),
    assert_list([Binding],
                http_get("/exchanges/%2f/myexchange/bindings/source")),
    http_delete("/bindings/%2f/e/myexchange/q/myqueue/routing", ?NO_CONTENT),
    http_delete("/bindings/%2f/e/myexchange/q/myqueue/routing", ?NOT_FOUND),
    http_delete("/exchanges/%2f/myexchange", ?NO_CONTENT),
    http_delete("/queues/%2f/myqueue", ?NO_CONTENT),
    http_get("/bindings/badvhost", ?NOT_FOUND),
    http_get("/bindings/badvhost/myqueue/myexchange/routing", ?NOT_FOUND),
    http_get("/bindings/%2f/e/myexchange/q/myqueue/routing", ?NOT_FOUND),
    ok.

bindings_post_test() ->
    XArgs = [{type, <<"direct">>}],
    QArgs = [],
    BArgs = [{routing_key, <<"routing">>}, {arguments, [{foo, <<"bar">>}]}],
    http_put("/exchanges/%2f/myexchange", XArgs, ?NO_CONTENT),
    http_put("/queues/%2f/myqueue", QArgs, ?NO_CONTENT),
    http_post("/bindings/%2f/e/myexchange/q/badqueue", BArgs, ?NOT_FOUND),
    http_post("/bindings/%2f/e/badexchange/q/myqueue", BArgs, ?NOT_FOUND),
    Headers1 = http_post("/bindings/%2f/e/myexchange/q/myqueue", [], ?CREATED),
    "../../../../%2F/e/myexchange/q/myqueue/~" = pget("location", Headers1),
    Headers2 = http_post("/bindings/%2f/e/myexchange/q/myqueue", BArgs, ?CREATED),
    PropertiesKey = "routing~V4mGFgnPNrdtRmluZIxTDA",
    PropertiesKeyBin = list_to_binary(PropertiesKey),
    "../../../../%2F/e/myexchange/q/myqueue/" ++ PropertiesKey =
        pget("location", Headers2),
    URI = "/bindings/%2F/e/myexchange/q/myqueue/" ++ PropertiesKey,
    [{source,<<"myexchange">>},
     {vhost,<<"/">>},
     {destination,<<"myqueue">>},
     {destination_type,<<"queue">>},
     {routing_key,<<"routing">>},
     {arguments,[{foo,<<"bar">>}]},
     {properties_key,PropertiesKeyBin}] = http_get(URI, ?OK),
    http_get(URI ++ "x", ?NOT_FOUND),
    http_delete(URI, ?NO_CONTENT),
    http_delete("/exchanges/%2f/myexchange", ?NO_CONTENT),
    http_delete("/queues/%2f/myqueue", ?NO_CONTENT),
    ok.

bindings_e2e_test() ->
    BArgs = [{routing_key, <<"routing">>}, {arguments, []}],
    http_post("/bindings/%2f/e/amq.direct/e/badexchange", BArgs, ?NOT_FOUND),
    http_post("/bindings/%2f/e/badexchange/e/amq.fanout", BArgs, ?NOT_FOUND),
    Headers = http_post("/bindings/%2f/e/amq.direct/e/amq.fanout", BArgs, ?CREATED),
    "../../../../%2F/e/amq.direct/e/amq.fanout/routing" =
        pget("location", Headers),
    [{source,<<"amq.direct">>},
     {vhost,<<"/">>},
     {destination,<<"amq.fanout">>},
     {destination_type,<<"exchange">>},
     {routing_key,<<"routing">>},
     {arguments,[]},
     {properties_key,<<"routing">>}] =
        http_get("/bindings/%2f/e/amq.direct/e/amq.fanout/routing", ?OK),
    http_delete("/bindings/%2f/e/amq.direct/e/amq.fanout/routing", ?NO_CONTENT),
    http_post("/bindings/%2f/e/amq.direct/e/amq.headers", BArgs, ?CREATED),
    Binding =
        [{source,<<"amq.direct">>},
         {vhost,<<"/">>},
         {destination,<<"amq.headers">>},
         {destination_type,<<"exchange">>},
         {routing_key,<<"routing">>},
         {arguments,[]},
         {properties_key,<<"routing">>}],
    Binding = http_get("/bindings/%2f/e/amq.direct/e/amq.headers/routing"),
    assert_list([Binding],
                http_get("/bindings/%2f/e/amq.direct/e/amq.headers")),
    assert_list([Binding],
                http_get("/exchanges/%2f/amq.direct/bindings/source")),
    assert_list([Binding],
                http_get("/exchanges/%2f/amq.headers/bindings/destination")),
    http_delete("/bindings/%2f/e/amq.direct/e/amq.headers/routing", ?NO_CONTENT),
    http_get("/bindings/%2f/e/amq.direct/e/amq.headers/rooting", ?NOT_FOUND),
    ok.

permissions_administrator_test() ->
    http_put("/users/isadmin", [{password, <<"isadmin">>},
                                {tags, <<"administrator">>}], ?NO_CONTENT),
    http_put("/users/notadmin", [{password, <<"notadmin">>},
                                 {tags, <<"administrator">>}], ?NO_CONTENT),
    http_put("/users/notadmin", [{password, <<"notadmin">>},
                                 {tags, <<"management">>}], ?NO_CONTENT),
    Test =
        fun(Path) ->
                http_get(Path, "notadmin", "notadmin", ?NOT_AUTHORISED),
                http_get(Path, "isadmin", "isadmin", ?OK),
                http_get(Path, "guest", "guest", ?OK)
        end,
    %% All users can get a list of vhosts. It may be filtered.
    %%Test("/vhosts"),
    Test("/vhosts/%2f"),
    Test("/vhosts/%2f/permissions"),
    Test("/users"),
    Test("/users/guest"),
    Test("/users/guest/permissions"),
    Test("/permissions"),
    Test("/permissions/%2f/guest"),
    http_delete("/users/notadmin", ?NO_CONTENT),
    http_delete("/users/isadmin", ?NO_CONTENT),
    ok.

permissions_vhost_test() ->
    QArgs = [],
    PermArgs = [{configure, <<".*">>}, {write, <<".*">>}, {read, <<".*">>}],
    http_put("/users/myuser", [{password, <<"myuser">>},
                               {tags, <<"management">>}], ?NO_CONTENT),
    http_put("/vhosts/myvhost1", none, ?NO_CONTENT),
    http_put("/vhosts/myvhost2", none, ?NO_CONTENT),
    http_put("/permissions/myvhost1/myuser", PermArgs, ?NO_CONTENT),
    http_put("/permissions/myvhost1/guest", PermArgs, ?NO_CONTENT),
    http_put("/permissions/myvhost2/guest", PermArgs, ?NO_CONTENT),
    assert_list([[{name, <<"/">>}],
                 [{name, <<"myvhost1">>}],
                 [{name, <<"myvhost2">>}]], http_get("/vhosts", ?OK)),
    assert_list([[{name, <<"myvhost1">>}]],
                http_get("/vhosts", "myuser", "myuser", ?OK)),
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
    Test2("/exchanges", "amq.default/bindings/source"),
    Test2("/exchanges", "amq.default/bindings/destination"),
    Test2("/bindings", "e/amq.default/q/myqueue"),
    Test2("/bindings", "e/amq.default/q/myqueue/myqueue"),
    http_delete("/vhosts/myvhost1", ?NO_CONTENT),
    http_delete("/vhosts/myvhost2", ?NO_CONTENT),
    http_delete("/users/myuser", ?NO_CONTENT),
    ok.

permissions_amqp_test() ->
    %% Just test that it works at all, not that it works in all possible cases.
    QArgs = [],
    PermArgs = [{configure, <<"foo.*">>}, {write, <<"foo.*">>},
                {read,      <<"foo.*">>}],
    http_put("/users/myuser", [{password, <<"myuser">>},
                               {tags, <<"management">>}], ?NO_CONTENT),
    http_put("/permissions/%2f/myuser", PermArgs, ?NO_CONTENT),
    http_put("/queues/%2f/bar-queue", QArgs, "myuser", "myuser",
             ?NOT_AUTHORISED),
    http_put("/queues/%2f/bar-queue", QArgs, "nonexistent", "nonexistent",
             ?NOT_AUTHORISED),
    http_delete("/users/myuser", ?NO_CONTENT),
    ok.

get_conn(Username, Password) ->
    {ok, Conn} = amqp_connection:start(#amqp_params_network{
                                        username = list_to_binary(Username),
                                        password = list_to_binary(Password)}),
    LocalPort = local_port(Conn),
    ConnPath = rabbit_misc:format(
                 "/connections/127.0.0.1%3A~w%20->%20127.0.0.1%3A5672",
                 [LocalPort]),
    ChPath = rabbit_misc:format(
               "/channels/127.0.0.1%3A~w%20->%20127.0.0.1%3A5672%20(1)",
               [LocalPort]),
    ConnChPath = rabbit_misc:format(
                   "/connections/127.0.0.1%3A~w%20->%20127.0.0.1%3A5672/channels",
                   [LocalPort]),
    {Conn, ConnPath, ChPath, ConnChPath}.

permissions_connection_channel_test() ->
    PermArgs = [{configure, <<".*">>}, {write, <<".*">>}, {read, <<".*">>}],
    http_put("/users/user", [{password, <<"user">>},
                             {tags, <<"management">>}], ?NO_CONTENT),
    http_put("/permissions/%2f/user", PermArgs, ?NO_CONTENT),
    http_put("/users/monitor", [{password, <<"monitor">>},
                            {tags, <<"monitoring">>}], ?NO_CONTENT),
    http_put("/permissions/%2f/monitor", PermArgs, ?NO_CONTENT),
    {Conn1, UserConn, UserCh, UserConnCh} = get_conn("user", "user"),
    {Conn2, MonConn, MonCh, MonConnCh} = get_conn("monitor", "monitor"),
    {Conn3, AdmConn, AdmCh, AdmConnCh} = get_conn("guest", "guest"),
    {ok, _Ch1} = amqp_connection:open_channel(Conn1),
    {ok, _Ch2} = amqp_connection:open_channel(Conn2),
    {ok, _Ch3} = amqp_connection:open_channel(Conn3),

    AssertLength = fun (Path, User, Len) ->
                           ?assertEqual(Len,
                                        length(http_get(Path, User, User, ?OK)))
                   end,
    [begin
         AssertLength(P, "user", 1),
         AssertLength(P, "monitor", 3),
         AssertLength(P, "guest", 3)
     end || P <- ["/connections", "/channels"]],

    AssertRead = fun(Path, UserStatus) ->
                         http_get(Path, "user", "user", UserStatus),
                         http_get(Path, "monitor", "monitor", ?OK),
                         http_get(Path, ?OK)
                 end,
    AssertRead(UserConn, ?OK),
    AssertRead(MonConn, ?NOT_AUTHORISED),
    AssertRead(AdmConn, ?NOT_AUTHORISED),
    AssertRead(UserCh, ?OK),
    AssertRead(MonCh, ?NOT_AUTHORISED),
    AssertRead(AdmCh, ?NOT_AUTHORISED),
    AssertRead(UserConnCh, ?OK),
    AssertRead(MonConnCh, ?NOT_AUTHORISED),
    AssertRead(AdmConnCh, ?NOT_AUTHORISED),

    AssertClose = fun(Path, User, Status) ->
                          http_delete(Path, User, User, Status)
                  end,
    AssertClose(UserConn, "monitor", ?NOT_AUTHORISED),
    AssertClose(MonConn, "user", ?NOT_AUTHORISED),
    AssertClose(AdmConn, "guest", ?NO_CONTENT),
    AssertClose(MonConn, "guest", ?NO_CONTENT),
    AssertClose(UserConn, "user", ?NO_CONTENT),

    http_delete("/users/user", ?NO_CONTENT),
    http_delete("/users/monitor", ?NO_CONTENT),
    http_get("/connections/foo", ?NOT_FOUND),
    http_get("/channels/foo", ?NOT_FOUND),
    ok.

unicode_test() ->
    QArgs = [],
    http_put("/queues/%2f/♫♪♫♪", QArgs, ?NO_CONTENT),
    http_get("/queues/%2f/♫♪♫♪", ?OK),
    http_delete("/queues/%2f/♫♪♫♪", ?NO_CONTENT),
    ok.

defs(Key, URI, CreateMethod, Args) ->
    defs(Key, URI, CreateMethod, Args,
         fun(URI2) -> http_delete(URI2, ?NO_CONTENT) end).

defs_v(Key, URI, CreateMethod, Args) ->
    Rep1 = fun (S, S2) -> re:replace(S, "<vhost>", S2, [{return, list}]) end,
    Rep2 = fun (L, V2) -> lists:keymap(fun (vhost) -> V2;
                                           (V)     -> V end, 2, L) end,
    %% Test against default vhost
    defs(Key, Rep1(URI, "%2f"), CreateMethod, Rep2(Args, <<"/">>)),

    %% Test against new vhost
    http_put("/vhosts/test", none, ?NO_CONTENT),
    PermArgs = [{configure, <<".*">>}, {write, <<".*">>}, {read, <<".*">>}],
    http_put("/permissions/test/guest", PermArgs, ?NO_CONTENT),
    defs(Key, Rep1(URI, "test"), CreateMethod, Rep2(Args, <<"test">>),
         fun(URI2) -> http_delete(URI2, ?NO_CONTENT),
                      http_delete("/vhosts/test", ?NO_CONTENT) end).

defs(Key, URI, CreateMethod, Args, DeleteFun) ->
    %% Create the item
    URI2 = case CreateMethod of
               put   -> http_put(URI, Args, ?NO_CONTENT),
                        URI;
               post  -> Headers = http_post(URI, Args, ?CREATED),
                        rabbit_web_dispatch_util:unrelativise(
                          URI, pget("location", Headers))
           end,
    %% Make sure it ends up in definitions
    Definitions = http_get("/definitions", ?OK),
    true = lists:any(fun(I) -> test_item(Args, I) end, pget(Key, Definitions)),

    %% Delete it
    DeleteFun(URI2),

    %% Post the definitions back, it should get recreated in correct form
    http_post("/definitions", Definitions, ?CREATED),
    assert_item(Args, http_get(URI2, ?OK)),

    %% And delete it again
    DeleteFun(URI2),

    ok.

definitions_test() ->
    rabbit_runtime_parameters_test:register(),
    rabbit_runtime_parameters_test:register_policy_validator(),

    defs_v(queues, "/queues/<vhost>/my-queue", put,
           [{name,    <<"my-queue">>},
            {durable, true}]),
    defs_v(exchanges, "/exchanges/<vhost>/my-exchange", put,
           [{name, <<"my-exchange">>},
            {type, <<"direct">>}]),
    defs_v(bindings, "/bindings/<vhost>/e/amq.direct/e/amq.fanout", post,
           [{routing_key, <<"routing">>}, {arguments, []}]),
    defs_v(policies, "/policies/<vhost>/my-policy", put,
           [{vhost,      vhost},
            {name,       <<"my-policy">>},
            {pattern,    <<".*">>},
            {definition, [{testpos, [1, 2, 3]}]},
            {priority,   1}]),
    defs_v(parameters, "/parameters/test/<vhost>/good", put,
           [{vhost,     vhost},
            {component, <<"test">>},
            {name,      <<"good">>},
            {value,     <<"ignore">>}]),
    defs(users, "/users/myuser", put,
         [{name,          <<"myuser">>},
          {password_hash, <<"WAbU0ZIcvjTpxM3Q3SbJhEAM2tQ=">>},
          {tags,          <<"management">>}]),
    defs(vhosts, "/vhosts/myvhost", put,
         [{name, <<"myvhost">>}]),
    defs(permissions, "/permissions/%2f/guest", put,
         [{user,      <<"guest">>},
          {vhost,     <<"/">>},
          {configure, <<"c">>},
          {write,     <<"w">>},
          {read,      <<"r">>}]),

    %% We just messed with guest's permissions
    http_put("/permissions/%2f/guest",
             [{configure, <<".*">>},
              {write,     <<".*">>},
              {read,      <<".*">>}], ?NO_CONTENT),

    BrokenConfig =
        [{users,       []},
         {vhosts,      []},
         {permissions, []},
         {queues,      []},
         {exchanges,   [[{name,        <<"amq.direct">>},
                         {vhost,       <<"/">>},
                         {type,        <<"definitely not direct">>},
                         {durable,     true},
                         {auto_delete, false},
                         {arguments,   []}
                        ]]},
         {bindings,    []}],
    http_post("/definitions", BrokenConfig, ?BAD_REQUEST),

    rabbit_runtime_parameters_test:unregister_policy_validator(),
    rabbit_runtime_parameters_test:unregister(),
    ok.

definitions_remove_things_test() ->
    {ok, Conn} = amqp_connection:start(#amqp_params_network{}),
    {ok, Ch} = amqp_connection:open_channel(Conn),
    amqp_channel:call(Ch, #'queue.declare'{ queue = <<"my-exclusive">>,
                                            exclusive = true }),
    http_get("/queues/%2f/my-exclusive", ?OK),
    Definitions = http_get("/definitions", ?OK),
    [] = pget(queues, Definitions),
    [] = pget(exchanges, Definitions),
    [] = pget(bindings, Definitions),
    amqp_channel:close(Ch),
    amqp_connection:close(Conn),
    ok.

definitions_server_named_queue_test() ->
    {ok, Conn} = amqp_connection:start(#amqp_params_network{}),
    {ok, Ch} = amqp_connection:open_channel(Conn),
    #'queue.declare_ok'{ queue = QName } =
        amqp_channel:call(Ch, #'queue.declare'{}),
    amqp_channel:close(Ch),
    amqp_connection:close(Conn),
    Path = "/queues/%2f/" ++ mochiweb_util:quote_plus(QName),
    http_get(Path, ?OK),
    Definitions = http_get("/definitions", ?OK),
    http_delete(Path, ?NO_CONTENT),
    http_get(Path, ?NOT_FOUND),
    http_post("/definitions", Definitions, ?CREATED),
    http_get(Path, ?OK),
    http_delete(Path, ?NO_CONTENT),
    ok.

aliveness_test() ->
    [{status, <<"ok">>}] = http_get("/aliveness-test/%2f", ?OK),
    http_get("/aliveness-test/foo", ?NOT_FOUND),
    http_delete("/queues/%2f/aliveness-test", ?NO_CONTENT),
    ok.

arguments_test() ->
    XArgs = [{type, <<"headers">>},
             {arguments, [{'alternate-exchange', <<"amq.direct">>}]}],
    QArgs = [{arguments, [{'x-expires', 1800000}]}],
    BArgs = [{routing_key, <<"">>},
             {arguments, [{'x-match', <<"all">>},
                          {foo, <<"bar">>}]}],
    http_put("/exchanges/%2f/myexchange", XArgs, ?NO_CONTENT),
    http_put("/queues/%2f/myqueue", QArgs, ?NO_CONTENT),
    http_post("/bindings/%2f/e/myexchange/q/myqueue", BArgs, ?CREATED),
    Definitions = http_get("/definitions", ?OK),
    http_delete("/exchanges/%2f/myexchange", ?NO_CONTENT),
    http_delete("/queues/%2f/myqueue", ?NO_CONTENT),
    http_post("/definitions", Definitions, ?CREATED),
    [{'alternate-exchange', <<"amq.direct">>}] =
        pget(arguments, http_get("/exchanges/%2f/myexchange", ?OK)),
    [{'x-expires', 1800000}] =
        pget(arguments, http_get("/queues/%2f/myqueue", ?OK)),
    true = lists:sort([{'x-match', <<"all">>}, {foo, <<"bar">>}]) =:=
           lists:sort(pget(arguments,
                           http_get("/bindings/%2f/e/myexchange/q/myqueue/" ++
                                    "~nXOkVwqZzUOdS9_HcBWheg", ?OK))),
    http_delete("/exchanges/%2f/myexchange", ?NO_CONTENT),
    http_delete("/queues/%2f/myqueue", ?NO_CONTENT),
    ok.

arguments_table_test() ->
    Args = [{'upstreams', [<<"amqp://localhost/%2f/upstream1">>,
                           <<"amqp://localhost/%2f/upstream2">>]}],
    XArgs = [{type, <<"headers">>},
             {arguments, Args}],
    http_put("/exchanges/%2f/myexchange", XArgs, ?NO_CONTENT),
    Definitions = http_get("/definitions", ?OK),
    http_delete("/exchanges/%2f/myexchange", ?NO_CONTENT),
    http_post("/definitions", Definitions, ?CREATED),
    Args = pget(arguments, http_get("/exchanges/%2f/myexchange", ?OK)),
    http_delete("/exchanges/%2f/myexchange", ?NO_CONTENT),
    ok.

queue_purge_test() ->
    QArgs = [],
    http_put("/queues/%2f/myqueue", QArgs, ?NO_CONTENT),
    {ok, Conn} = amqp_connection:start(#amqp_params_network{}),
    {ok, Ch} = amqp_connection:open_channel(Conn),
    Publish = fun() ->
                      amqp_channel:call(
                        Ch, #'basic.publish'{exchange = <<"">>,
                                             routing_key = <<"myqueue">>},
                        #amqp_msg{payload = <<"message">>})
              end,
    Publish(),
    Publish(),
    amqp_channel:call(
      Ch, #'queue.declare'{queue = <<"exclusive">>, exclusive = true}),
    {#'basic.get_ok'{}, _} =
        amqp_channel:call(Ch, #'basic.get'{queue = <<"myqueue">>}),
    http_delete("/queues/%2f/myqueue/contents", ?NO_CONTENT),
    http_delete("/queues/%2f/badqueue/contents", ?NOT_FOUND),
    http_delete("/queues/%2f/exclusive/contents", ?BAD_REQUEST),
    http_delete("/queues/%2f/exclusive", ?BAD_REQUEST),
    #'basic.get_empty'{} =
        amqp_channel:call(Ch, #'basic.get'{queue = <<"myqueue">>}),
    amqp_channel:close(Ch),
    amqp_connection:close(Conn),
    http_delete("/queues/%2f/myqueue", ?NO_CONTENT),
    ok.

queue_actions_test() ->
    http_put("/queues/%2f/q", [], ?NO_CONTENT),
    http_post("/queues/%2f/q/actions", [{action, sync}], ?NO_CONTENT),
    http_post("/queues/%2f/q/actions", [{action, cancel_sync}], ?NO_CONTENT),
    http_post("/queues/%2f/q/actions", [{action, change_colour}], ?BAD_REQUEST),
    http_delete("/queues/%2f/q", ?NO_CONTENT),
    ok.

exclusive_consumer_test() ->
    {ok, Conn} = amqp_connection:start(#amqp_params_network{}),
    {ok, Ch} = amqp_connection:open_channel(Conn),
    #'queue.declare_ok'{ queue = QName } =
        amqp_channel:call(Ch, #'queue.declare'{exclusive = true}),
    amqp_channel:subscribe(Ch, #'basic.consume'{queue     = QName,
                                                exclusive = true}, self()),
    timer:sleep(1000), %% Sadly we need to sleep to let the stats update
    http_get("/queues/%2f/"), %% Just check we don't blow up
    amqp_channel:close(Ch),
    amqp_connection:close(Conn),
    ok.

sorting_test() ->
    QArgs = [],
    PermArgs = [{configure, <<".*">>}, {write, <<".*">>}, {read, <<".*">>}],
    http_put("/vhosts/vh1", none, ?NO_CONTENT),
    http_put("/permissions/vh1/guest", PermArgs, ?NO_CONTENT),
    http_put("/queues/%2f/test0", QArgs, ?NO_CONTENT),
    http_put("/queues/vh1/test1", QArgs, ?NO_CONTENT),
    http_put("/queues/%2f/test2", QArgs, ?NO_CONTENT),
    http_put("/queues/vh1/test3", QArgs, ?NO_CONTENT),
    assert_list([[{name, <<"test0">>}],
                 [{name, <<"test2">>}],
                 [{name, <<"test1">>}],
                 [{name, <<"test3">>}]], http_get("/queues", ?OK)),
    assert_list([[{name, <<"test0">>}],
                 [{name, <<"test1">>}],
                 [{name, <<"test2">>}],
                 [{name, <<"test3">>}]], http_get("/queues?sort=name", ?OK)),
    assert_list([[{name, <<"test0">>}],
                 [{name, <<"test2">>}],
                 [{name, <<"test1">>}],
                 [{name, <<"test3">>}]], http_get("/queues?sort=vhost", ?OK)),
    assert_list([[{name, <<"test3">>}],
                 [{name, <<"test1">>}],
                 [{name, <<"test2">>}],
                 [{name, <<"test0">>}]], http_get("/queues?sort_reverse=true", ?OK)),
    assert_list([[{name, <<"test3">>}],
                 [{name, <<"test2">>}],
                 [{name, <<"test1">>}],
                 [{name, <<"test0">>}]], http_get("/queues?sort=name&sort_reverse=true", ?OK)),
    assert_list([[{name, <<"test3">>}],
                 [{name, <<"test1">>}],
                 [{name, <<"test2">>}],
                 [{name, <<"test0">>}]], http_get("/queues?sort=vhost&sort_reverse=true", ?OK)),
    %% Rather poor but at least test it doesn't blow up with dots
    http_get("/queues?sort=owner_pid_details.name", ?OK),
    http_delete("/queues/%2f/test0", ?NO_CONTENT),
    http_delete("/queues/vh1/test1", ?NO_CONTENT),
    http_delete("/queues/%2f/test2", ?NO_CONTENT),
    http_delete("/queues/vh1/test3", ?NO_CONTENT),
    http_delete("/vhosts/vh1", ?NO_CONTENT),
    ok.

columns_test() ->
    http_put("/queues/%2f/test", [{arguments, [{<<"foo">>, <<"bar">>}]}],
             ?NO_CONTENT),
    [[{name, <<"test">>}, {arguments, [{foo, <<"bar">>}]}]] =
        http_get("/queues?columns=arguments.foo,name", ?OK),
    [{name, <<"test">>}, {arguments, [{foo, <<"bar">>}]}] =
        http_get("/queues/%2f/test?columns=arguments.foo,name", ?OK),
    http_delete("/queues/%2f/test", ?NO_CONTENT),
    ok.

get_test() ->
    %% Real world example...
    Headers = [{<<"x-forwarding">>, array,
                [{table,
                  [{<<"uri">>, longstr,
                    <<"amqp://localhost/%2f/upstream">>}]}]}],
    http_put("/queues/%2f/myqueue", [], ?NO_CONTENT),
    {ok, Conn} = amqp_connection:start(#amqp_params_network{}),
    {ok, Ch} = amqp_connection:open_channel(Conn),
    Publish = fun (Payload) ->
                      amqp_channel:cast(
                        Ch, #'basic.publish'{exchange = <<>>,
                                             routing_key = <<"myqueue">>},
                        #amqp_msg{props = #'P_basic'{headers = Headers},
                                  payload = Payload})
              end,
    Publish(<<"1aaa">>),
    Publish(<<"2aaa">>),
    Publish(<<"3aaa">>),
    amqp_connection:close(Conn),
    [Msg] = http_post("/queues/%2f/myqueue/get", [{requeue,  false},
                                                  {count,    1},
                                                  {encoding, auto},
                                                  {truncate, 1}], ?OK),
    false         = pget(redelivered, Msg),
    <<>>          = pget(exchange,    Msg),
    <<"myqueue">> = pget(routing_key, Msg),
    <<"1">>       = pget(payload,     Msg),
    [{'x-forwarding',
      [[{uri,<<"amqp://localhost/%2f/upstream">>}]]}] =
        pget(headers, pget(properties, Msg)),

    [M2, M3] = http_post("/queues/%2f/myqueue/get", [{requeue,  true},
                                                     {count,    5},
                                                     {encoding, auto}], ?OK),
    <<"2aaa">> = pget(payload, M2),
    <<"3aaa">> = pget(payload, M3),
    2 = length(http_post("/queues/%2f/myqueue/get", [{requeue,  false},
                                                     {count,    5},
                                                     {encoding, auto}], ?OK)),
    [] = http_post("/queues/%2f/myqueue/get", [{requeue,  false},
                                               {count,    5},
                                               {encoding, auto}], ?OK),
    http_delete("/queues/%2f/myqueue", ?NO_CONTENT),
    ok.

get_fail_test() ->
    http_put("/users/myuser", [{password, <<"password">>},
                               {tags, <<"management">>}], ?NO_CONTENT),
    http_put("/queues/%2f/myqueue", [], ?NO_CONTENT),
    http_post("/queues/%2f/myqueue/get",
              [{requeue,  false},
               {count,    1},
               {encoding, auto}], "myuser", "password", ?NOT_AUTHORISED),
    http_delete("/queues/%2f/myqueue", ?NO_CONTENT),
    http_delete("/users/myuser", ?NO_CONTENT),
    ok.

publish_test() ->
    Headers = [{'x-forwarding', [[{uri,<<"amqp://localhost/%2f/upstream">>}]]}],
    Msg = msg(<<"myqueue">>, Headers, <<"Hello world">>),
    http_put("/queues/%2f/myqueue", [], ?NO_CONTENT),
    ?assertEqual([{routed, true}],
                 http_post("/exchanges/%2f/amq.default/publish", Msg, ?OK)),
    [Msg2] = http_post("/queues/%2f/myqueue/get", [{requeue,  false},
                                                   {count,    1},
                                                   {encoding, auto}], ?OK),
    assert_item(Msg, Msg2),
    http_post("/exchanges/%2f/amq.default/publish", Msg2, ?OK),
    [Msg3] = http_post("/queues/%2f/myqueue/get", [{requeue,  false},
                                                   {count,    1},
                                                   {encoding, auto}], ?OK),
    assert_item(Msg, Msg3),
    http_delete("/queues/%2f/myqueue", ?NO_CONTENT),
    ok.

publish_fail_test() ->
    Msg = msg(<<"myqueue">>, [], <<"Hello world">>),
    http_put("/queues/%2f/myqueue", [], ?NO_CONTENT),
    http_put("/users/myuser", [{password, <<"password">>},
                               {tags, <<"management">>}], ?NO_CONTENT),
    http_post("/exchanges/%2f/amq.default/publish", Msg, "myuser", "password",
              ?NOT_AUTHORISED),
    Msg2 = [{exchange,         <<"">>},
            {routing_key,      <<"myqueue">>},
            {properties,       [{user_id, <<"foo">>}]},
            {payload,          <<"Hello world">>},
            {payload_encoding, <<"string">>}],
    http_post("/exchanges/%2f/amq.default/publish", Msg2, ?BAD_REQUEST),
    Msg3 = [{exchange,         <<"">>},
            {routing_key,      <<"myqueue">>},
            {properties,       []},
            {payload,          [<<"not a string">>]},
            {payload_encoding, <<"string">>}],
    http_post("/exchanges/%2f/amq.default/publish", Msg3, ?BAD_REQUEST),
    http_delete("/users/myuser", ?NO_CONTENT),
    ok.

publish_base64_test() ->
    Msg     = msg(<<"myqueue">>, [], <<"YWJjZA==">>, <<"base64">>),
    BadMsg1 = msg(<<"myqueue">>, [], <<"flibble">>,  <<"base64">>),
    BadMsg2 = msg(<<"myqueue">>, [], <<"YWJjZA==">>, <<"base99">>),
    http_put("/queues/%2f/myqueue", [], ?NO_CONTENT),
    http_post("/exchanges/%2f/amq.default/publish", Msg, ?OK),
    http_post("/exchanges/%2f/amq.default/publish", BadMsg1, ?BAD_REQUEST),
    http_post("/exchanges/%2f/amq.default/publish", BadMsg2, ?BAD_REQUEST),
    [Msg2] = http_post("/queues/%2f/myqueue/get", [{requeue,  false},
                                                   {count,    1},
                                                   {encoding, auto}], ?OK),
    ?assertEqual(<<"abcd">>, pget(payload, Msg2)),
    http_delete("/queues/%2f/myqueue", ?NO_CONTENT),
    ok.

publish_unrouted_test() ->
    Msg = msg(<<"hmmm">>, [], <<"Hello world">>),
    ?assertEqual([{routed, false}],
                 http_post("/exchanges/%2f/amq.default/publish", Msg, ?OK)).

parameters_test() ->
    rabbit_runtime_parameters_test:register(),

    http_put("/parameters/test/%2f/good", [{value, <<"ignore">>}], ?NO_CONTENT),
    http_put("/parameters/test/%2f/maybe", [{value, <<"good">>}], ?NO_CONTENT),
    http_put("/parameters/test/%2f/maybe", [{value, <<"bad">>}], ?BAD_REQUEST),
    http_put("/parameters/test/%2f/bad", [{value, <<"good">>}], ?BAD_REQUEST),
    http_put("/parameters/test/um/good", [{value, <<"ignore">>}], ?NOT_FOUND),

    Good = [{vhost,     <<"/">>},
            {component, <<"test">>},
            {name,      <<"good">>},
            {value,     <<"ignore">>}],
    Maybe = [{vhost,     <<"/">>},
             {component, <<"test">>},
             {name,      <<"maybe">>},
             {value,     <<"good">>}],
    List = [Good, Maybe],

    assert_list(List, http_get("/parameters")),
    assert_list(List, http_get("/parameters/test")),
    assert_list(List, http_get("/parameters/test/%2f")),
    assert_list([],   http_get("/parameters/oops")),
    http_get("/parameters/test/oops", ?NOT_FOUND),

    assert_item(Good,  http_get("/parameters/test/%2f/good", ?OK)),
    assert_item(Maybe, http_get("/parameters/test/%2f/maybe", ?OK)),

    http_delete("/parameters/test/%2f/good", ?NO_CONTENT),
    http_delete("/parameters/test/%2f/maybe", ?NO_CONTENT),
    http_delete("/parameters/test/%2f/bad", ?NOT_FOUND),

    0 = length(http_get("/parameters")),
    0 = length(http_get("/parameters/test")),
    0 = length(http_get("/parameters/test/%2f")),
    rabbit_runtime_parameters_test:unregister(),
    ok.

policy_test() ->
    rabbit_runtime_parameters_test:register_policy_validator(),
    PolicyPos  = [{vhost,      <<"/">>},
                  {name,       <<"policy_pos">>},
                  {pattern,    <<".*">>},
                  {definition, [{testpos,[1,2,3]}]},
                  {priority,   10}],
    PolicyEven = [{vhost,      <<"/">>},
                  {name,       <<"policy_even">>},
                  {pattern,    <<".*">>},
                  {definition, [{testeven,[1,2,3,4]}]},
                  {priority,   10}],
    http_put(
      "/policies/%2f/policy_pos",
      lists:keydelete(key, 1, PolicyPos),
      ?NO_CONTENT),
    http_put(
      "/policies/%2f/policy_even",
      lists:keydelete(key, 1, PolicyEven),
      ?NO_CONTENT),
    assert_item(PolicyPos,  http_get("/policies/%2f/policy_pos",  ?OK)),
    assert_item(PolicyEven, http_get("/policies/%2f/policy_even", ?OK)),
    List = [PolicyPos, PolicyEven],
    assert_list(List, http_get("/policies",     ?OK)),
    assert_list(List, http_get("/policies/%2f", ?OK)),

    http_delete("/policies/%2f/policy_pos", ?NO_CONTENT),
    http_delete("/policies/%2f/policy_even", ?NO_CONTENT),
    0 = length(http_get("/policies")),
    0 = length(http_get("/policies/%2f")),
    rabbit_runtime_parameters_test:unregister_policy_validator(),
    ok.

policy_permissions_test() ->
    rabbit_runtime_parameters_test:register(),

    http_put("/users/admin",  [{password, <<"admin">>},
                               {tags, <<"administrator">>}], ?NO_CONTENT),
    http_put("/users/mon",    [{password, <<"monitor">>},
                               {tags, <<"monitoring">>}], ?NO_CONTENT),
    http_put("/users/policy", [{password, <<"policy">>},
                               {tags, <<"policymaker">>}], ?NO_CONTENT),
    http_put("/users/mgmt",   [{password, <<"mgmt">>},
                               {tags, <<"management">>}], ?NO_CONTENT),
    Perms = [{configure, <<".*">>},
             {write,     <<".*">>},
             {read,      <<".*">>}],
    http_put("/vhosts/v", none, ?NO_CONTENT),
    http_put("/permissions/v/admin",  Perms, ?NO_CONTENT),
    http_put("/permissions/v/mon",    Perms, ?NO_CONTENT),
    http_put("/permissions/v/policy", Perms, ?NO_CONTENT),
    http_put("/permissions/v/mgmt",   Perms, ?NO_CONTENT),

    Policy = [{pattern,    <<".*">>},
              {definition, [{<<"ha-mode">>, <<"all">>}]}],
    Param = [{value, <<"">>}],

    http_put("/policies/%2f/HA", Policy, ?NO_CONTENT),
    http_put("/parameters/test/%2f/good", Param, ?NO_CONTENT),

    Pos = fun (U) ->
                  http_put("/policies/v/HA",        Policy, U, U, ?NO_CONTENT),
                  http_put(
                    "/parameters/test/v/good",       Param, U, U, ?NO_CONTENT),
                  1 = length(http_get("/policies",          U, U, ?OK)),
                  1 = length(http_get("/parameters/test",   U, U, ?OK)),
                  1 = length(http_get("/parameters",        U, U, ?OK)),
                  1 = length(http_get("/policies/v",        U, U, ?OK)),
                  1 = length(http_get("/parameters/test/v", U, U, ?OK)),
                  http_get("/policies/v/HA",                U, U, ?OK),
                  http_get("/parameters/test/v/good",       U, U, ?OK)
          end,
    Neg = fun (U) ->
                  http_put("/policies/v/HA",    Policy, U, U, ?NOT_AUTHORISED),
                  http_put(
                    "/parameters/test/v/good",   Param, U, U, ?NOT_AUTHORISED),
                  http_get("/policies",                 U, U, ?NOT_AUTHORISED),
                  http_get("/policies/v",               U, U, ?NOT_AUTHORISED),
                  http_get("/parameters",               U, U, ?NOT_AUTHORISED),
                  http_get("/parameters/test",          U, U, ?NOT_AUTHORISED),
                  http_get("/parameters/test/v",        U, U, ?NOT_AUTHORISED),
                  http_get("/policies/v/HA",            U, U, ?NOT_AUTHORISED),
                  http_get("/parameters/test/v/good",   U, U, ?NOT_AUTHORISED)
          end,
    AlwaysNeg =
        fun (U) ->
                http_put("/policies/%2f/HA",  Policy, U, U, ?NOT_AUTHORISED),
                http_put(
                  "/parameters/test/%2f/good", Param, U, U, ?NOT_AUTHORISED),
                http_get("/policies/%2f/HA",          U, U, ?NOT_AUTHORISED),
                http_get("/parameters/test/%2f/good", U, U, ?NOT_AUTHORISED)
        end,

    [Neg(U) || U <- ["mon", "mgmt"]],
    [Pos(U) || U <- ["admin", "policy"]],
    [AlwaysNeg(U) || U <- ["mon", "mgmt", "admin", "policy"]],

    http_delete("/vhosts/v", ?NO_CONTENT),
    http_delete("/users/admin", ?NO_CONTENT),
    http_delete("/users/mon", ?NO_CONTENT),
    http_delete("/users/policy", ?NO_CONTENT),
    http_delete("/users/mgmt", ?NO_CONTENT),
    http_delete("/policies/%2f/HA", ?NO_CONTENT),

    rabbit_runtime_parameters_test:unregister(),
    ok.


extensions_test() ->
    [[{javascript,<<"dispatcher.js">>}]] = http_get("/extensions", ?OK),
    ok.

%%---------------------------------------------------------------------------

msg(Key, Headers, Body) ->
    msg(Key, Headers, Body, <<"string">>).

msg(Key, Headers, Body, Enc) ->
    [{exchange,         <<"">>},
     {routing_key,      Key},
     {properties,       [{delivery_mode, 2},
                         {headers,       Headers}]},
     {payload,          Body},
     {payload_encoding, Enc}].

local_port(Conn) ->
    [{sock, Sock}] = amqp_connection:info(Conn, [sock]),
    {ok, Port} = inet:port(Sock),
    Port.

%%---------------------------------------------------------------------------
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

