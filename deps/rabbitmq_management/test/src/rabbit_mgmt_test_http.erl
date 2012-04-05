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
%%   The Initial Developer of the Original Code is VMware, Inc.
%%   Copyright (c) 2010-2012 VMware, Inc.  All rights reserved.
%%

-module(rabbit_mgmt_test_http).

-include("rabbit_mgmt_test.hrl").

-export([http_get/1, http_put/3, http_delete/2]).

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
    http_put("/vhosts/myvhost", [], ?NO_CONTENT),
    %% PUT should be idempotent
    http_put("/vhosts/myvhost", [], ?NO_CONTENT),
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
    http_put("/vhosts/myvhost1", [], ?NO_CONTENT),
    http_put("/vhosts/myvhost2", [], ?NO_CONTENT),

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
    http_put("/vhosts/myvhost", [], ?NO_CONTENT),

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
    LocalPort = rabbit_mgmt_test_db:local_port(Conn),
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
    http_put("/vhosts/myvhost", [], ?NO_CONTENT),
    http_get("/exchanges/myvhost/foo", ?NOT_AUTHORISED),
    http_put("/exchanges/myvhost/foo", Good, ?NOT_AUTHORISED),
    http_put("/permissions/myvhost/guest",
             [{configure, <<".*">>}, {write, <<".*">>}, {read, <<".*">>}],
             ?NO_CONTENT),
    http_get("/exchanges/myvhost/foo", ?NOT_FOUND),
    http_put("/exchanges/myvhost/foo", Good, ?NO_CONTENT),
    http_put("/exchanges/myvhost/foo", Good, ?NO_CONTENT),
    http_get("/exchanges/%2f/foo", ?NOT_FOUND),
    [{name,<<"foo">>},
     {vhost,<<"myvhost">>},
     {type,<<"direct">>},
     {durable,true},
     {auto_delete,false},
     {internal,false},
     {arguments,[]}] =
        http_get("/exchanges/myvhost/foo"),

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
    http_put("/bindings/%2f/e/myexchange/q/badqueue/routing", [], ?NOT_FOUND),
    http_put("/bindings/%2f/e/badexchange/q/myqueue/routing", [], ?NOT_FOUND),
    http_put("/bindings/%2f/e/myexchange/q/myqueue/bad_routing", [], ?BAD_REQUEST),
    http_put("/bindings/%2f/e/myexchange/q/myqueue/routing", [], ?NO_CONTENT),
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
    "../../../../%2F/e/myexchange/q/myqueue/_" = pget("location", Headers1),
    Headers2 = http_post("/bindings/%2f/e/myexchange/q/myqueue", BArgs, ?CREATED),
    "../../../../%2F/e/myexchange/q/myqueue/routing_foo_bar" =
        pget("location", Headers2),
    [{source,<<"myexchange">>},
     {vhost,<<"/">>},
     {destination,<<"myqueue">>},
     {destination_type,<<"queue">>},
     {routing_key,<<"routing">>},
     {arguments,[{foo,<<"bar">>}]},
     {properties_key,<<"routing_foo_bar">>}] =
        http_get("/bindings/%2F/e/myexchange/q/myqueue/routing_foo_bar", ?OK),
    http_delete("/bindings/%2F/e/myexchange/q/myqueue/routing_foo_bar", ?NO_CONTENT),
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
    http_put("/bindings/%2f/e/amq.direct/e/amq.headers/routing", [], ?NO_CONTENT),
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
    http_put("/vhosts/myvhost1", [], ?NO_CONTENT),
    http_put("/vhosts/myvhost2", [], ?NO_CONTENT),
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
    LocalPort = rabbit_mgmt_test_db:local_port(Conn),
    ConnPath = binary_to_list(
                 rabbit_mgmt_format:print(
                   "/connections/127.0.0.1%3A~w%20->%20127.0.0.1%3A5672",
                   [LocalPort])),
    ChPath = binary_to_list(
               rabbit_mgmt_format:print(
                 "/channels/127.0.0.1%3A~w%20->%20127.0.0.1%3A5672%20(1)",
                 [LocalPort])),
    {Conn, ConnPath, ChPath}.

permissions_connection_channel_test() ->
    PermArgs = [{configure, <<".*">>}, {write, <<".*">>}, {read, <<".*">>}],
    http_put("/users/user", [{password, <<"user">>},
                             {tags, <<"management">>}], ?NO_CONTENT),
    http_put("/permissions/%2f/user", PermArgs, ?NO_CONTENT),
    http_put("/users/monitor", [{password, <<"monitor">>},
                            {tags, <<"monitoring">>}], ?NO_CONTENT),
    http_put("/permissions/%2f/monitor", PermArgs, ?NO_CONTENT),
    {Conn1, UserConn, UserCh} = get_conn("user", "user"),
    {Conn2, MonConn, MonCh} = get_conn("monitor", "monitor"),
    {Conn3, AdmConn, AdmCh} = get_conn("guest", "guest"),
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

definitions_test() ->
    XArgs = [{type, <<"direct">>}],
    QArgs = [],
    http_put("/queues/%2f/my-queue", QArgs, ?NO_CONTENT),
    http_put("/exchanges/%2f/my-exchange", XArgs, ?NO_CONTENT),
    http_put("/bindings/%2f/e/my-exchange/q/my-queue/routing", [], ?NO_CONTENT),
    http_put("/bindings/%2f/e/amq.direct/q/my-queue/routing", [], ?NO_CONTENT),
    http_put("/bindings/%2f/e/amq.direct/e/amq.fanout/routing", [], ?NO_CONTENT),
    Definitions = http_get("/definitions", ?OK),
    http_delete("/bindings/%2f/e/my-exchange/q/my-queue/routing", ?NO_CONTENT),
    http_delete("/bindings/%2f/e/amq.direct/q/my-queue/routing", ?NO_CONTENT),
    http_delete("/bindings/%2f/e/amq.direct/e/amq.fanout/routing", ?NO_CONTENT),
    http_delete("/queues/%2f/my-queue", ?NO_CONTENT),
    http_delete("/exchanges/%2f/my-exchange", ?NO_CONTENT),
    http_post("/definitions", Definitions, ?NO_CONTENT),
    http_delete("/bindings/%2f/e/my-exchange/q/my-queue/routing", ?NO_CONTENT),
    http_delete("/bindings/%2f/e/amq.direct/q/my-queue/routing", ?NO_CONTENT),
    http_delete("/bindings/%2f/e/amq.direct/e/amq.fanout/routing", ?NO_CONTENT),
    http_delete("/queues/%2f/my-queue", ?NO_CONTENT),
    http_delete("/exchanges/%2f/my-exchange", ?NO_CONTENT),
    ExtraConfig =
        [{users,       []},
         {vhosts,      []},
         {permissions, []},
         {queues,       [[{name,        <<"another-queue">>},
                          {vhost,       <<"/">>},
                          {durable,     true},
                          {auto_delete, false},
                          {arguments,   []}
                         ]]},
         {exchanges,   []},
         {bindings,    []}],
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
    http_post("/definitions", ExtraConfig, ?NO_CONTENT),
    http_post("/definitions", BrokenConfig, ?BAD_REQUEST),
    http_delete("/queues/%2f/another-queue", ?NO_CONTENT),
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
    http_post("/definitions", Definitions, ?NO_CONTENT),
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
    http_post("/definitions", Definitions, ?NO_CONTENT),
    [{'alternate-exchange', <<"amq.direct">>}] =
        pget(arguments, http_get("/exchanges/%2f/myexchange", ?OK)),
    [{'x-expires', 1800000}] =
        pget(arguments, http_get("/queues/%2f/myqueue", ?OK)),
    [{foo, <<"bar">>}, {'x-match', <<"all">>}] =
        pget(arguments,
             http_get("/bindings/%2f/e/myexchange/q/myqueue/" ++
                          "_foo_bar_x-match_all", ?OK)),
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
    http_post("/definitions", Definitions, ?NO_CONTENT),
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
    http_put("/vhosts/vh1", [], ?NO_CONTENT),
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
    http_put("/queues/%2f/test", [], ?NO_CONTENT),
    %% Bit lame to test backing_queue_status but at least it's
    %% something we can descend to that's always there
    [[{backing_queue_status, [{len, 0}]}, {name, <<"test">>}]] =
        http_get("/queues?columns=backing_queue_status.len,name", ?OK),
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

    http_put("/parameters/test/good", [{value, <<"ignored">>}], ?NO_CONTENT),
    http_put("/parameters/test/maybe", [{value, <<"good">>}], ?NO_CONTENT),
    http_put("/parameters/test/maybe", [{value, <<"bad">>}], ?BAD_REQUEST),
    http_put("/parameters/test/bad", [{value, <<"good">>}], ?BAD_REQUEST),

    Good = [{app_name, <<"test">>},
            {key,      <<"good">>},
            {value,    <<"ignored">>}],
    Maybe = [{app_name, <<"test">>},
             {key,      <<"maybe">>},
             {value,    <<"good">>}],
    List = [Good, Maybe],

    assert_list(List, http_get("/parameters")),
    assert_list(List, http_get("/parameters/test")),
    http_get("/parameters/oops", ?NOT_FOUND),

    assert_item(Good,  http_get("/parameters/test/good", ?OK)),
    assert_item(Maybe, http_get("/parameters/test/maybe", ?OK)),

    http_delete("/parameters/test/good", ?NO_CONTENT),
    http_delete("/parameters/test/maybe", ?NO_CONTENT),
    http_delete("/parameters/test/bad", ?NOT_FOUND),

    0 = length(http_get("/parameters")),
    0 = length(http_get("/parameters/test")),
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

%%---------------------------------------------------------------------------

pget(K, L) ->
     proplists:get_value(K, L).
