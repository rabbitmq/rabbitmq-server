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
%%   Copyright (c) 2007-2016 Pivotal Software, Inc.  All rights reserved.
%%

-module(sockjs_websocket_SUITE).

-compile(export_all).

-include_lib("common_test/include/ct.hrl").

-include_lib("eunit/include/eunit.hrl").

all() ->
    [
    connection,
    pubsub,
    disconnect,
    http_auth
    ].

init_per_suite(Config) ->
    Config1 = rabbit_ct_helpers:set_config(Config,
                                           [{rmq_nodename_suffix, ?MODULE}]),
    rabbit_ct_helpers:log_environment(),
    rabbit_ct_helpers:run_setup_steps(Config1,
                                      rabbit_ct_broker_helpers:setup_steps()).

end_per_suite(Config) ->
    rabbit_ct_helpers:run_teardown_steps(Config).

init_per_testcase(http_auth, Config) ->
    rabbit_ws_test_util:update_app_env(Config, use_http_auth, true),
    Config;
init_per_testcase(_, Config) -> Config.

end_per_testcase(http_auth, Config) ->
    rabbit_ws_test_util:update_app_env(Config, use_http_auth, false),
    Config;
end_per_testcase(_, Config) -> Config.

connection(Config) ->
    PortStr = rabbit_ws_test_util:get_web_stomp_port_str(Config),
    WS = rfc6455_client:new("ws://127.0.0.1:" ++ PortStr ++ "/stomp/0/0/websocket", self()),
    {ok, _} = rfc6455_client:open(WS),
    {ok, <<"o">>} = rfc6455_client:recv(WS),

    {close, _} = rfc6455_client:close(WS),
    ok.


sjs_send(WS, Command, Headers) ->
    sjs_send(WS, Command, Headers, <<>>).
sjs_send(WS, Command, Headers, Body) ->
    StompFrame = stomp:marshal(Command, Headers, Body),
    SockJSFrame = sockjs_json:encode([StompFrame]),
    rfc6455_client:send(WS, SockJSFrame).

sjs_recv(WS) ->
    {ok, P} = rfc6455_client:recv(WS),
    case P of
        <<"a", JsonArr/binary>> ->
            {ok, [StompFrame]} = sockjs_json:decode(JsonArr),
            {ok, stomp:unmarshal(StompFrame)};
        <<"c", JsonArr/binary>> ->
            {ok, CloseReason} = sockjs_json:decode(JsonArr),
            {close, CloseReason}
    end.

pubsub(Config) ->
    PortStr = rabbit_ws_test_util:get_web_stomp_port_str(Config),
    WS = rfc6455_client:new("ws://127.0.0.1:" ++ PortStr ++ "/stomp/0/0/websocket", self()),
    {ok, _} = rfc6455_client:open(WS),
    {ok, <<"o">>} = rfc6455_client:recv(WS),

    ok = sjs_send(WS, "CONNECT", [{"login","guest"}, {"passcode", "guest"}]),

    {ok, {<<"CONNECTED">>, _, <<>>}} = sjs_recv(WS),

    Dst = "/topic/test-" ++ stomp:list_to_hex(binary_to_list(crypto:rand_bytes(8))),

    ok = sjs_send(WS, "SUBSCRIBE", [{"destination", Dst},
                                    {"id", "s0"}]),

    ok = sjs_send(WS, "SEND", [{"destination", Dst},
                               {"content-length", "3"}], <<"a\x00a">>),

    {ok, {<<"MESSAGE">>, H, <<"a\x00a">>}} = sjs_recv(WS),
    Dst = binary_to_list(proplists:get_value(<<"destination">>, H)),

    {close, _} = rfc6455_client:close(WS),

    ok.


disconnect(Config) ->
    PortStr = rabbit_ws_test_util:get_web_stomp_port_str(Config),
    WS = rfc6455_client:new("ws://127.0.0.1:" ++ PortStr ++ "/stomp/0/0/websocket", self()),
    {ok, _} = rfc6455_client:open(WS),
    {ok, <<"o">>} = rfc6455_client:recv(WS),

    ok = sjs_send(WS, "CONNECT", [{"login","guest"}, {"passcode", "guest"}]),
    {ok, {<<"CONNECTED">>, _, <<>>}} = sjs_recv(WS),

    ok = sjs_send(WS, "DISCONNECT", []),
    {close, [1000, _]} = sjs_recv(WS),

    ok.

http_auth(Config) ->
    %% Intentionally put bad credentials in the CONNECT frame,
    %% and good credentials in the Authorization header, to
    %% confirm that the right credentials are picked.
    PortStr = rabbit_ws_test_util:get_web_stomp_port_str(Config),
    WS = rfc6455_client:new("ws://127.0.0.1:" ++ PortStr ++ "/stomp/0/0/websocket", self(),
        [{login, "guest"}, {passcode, "guest"}]),
    {ok, _} = rfc6455_client:open(WS),
    {ok, <<"o">>} = rfc6455_client:recv(WS),
    ok = sjs_send(WS, "CONNECT", [{"login", "bad"}, {"passcode", "bad"}]),
    {ok, {<<"CONNECTED">>, _, <<>>}} = sjs_recv(WS),
    {close, _} = rfc6455_client:close(WS),

    %% Confirm that if no Authorization header is provided,
    %% the default STOMP plugin credentials are used. We
    %% expect an error because the default credentials are
    %% left undefined.
    WS2 = rfc6455_client:new("ws://127.0.0.1:" ++ PortStr ++ "/stomp/0/0/websocket", self()),
    {ok, _} = rfc6455_client:open(WS2),
    {ok, <<"o">>} = rfc6455_client:recv(WS2),
    ok = sjs_send(WS2, "CONNECT", [{"login", "bad"}, {"passcode", "bad"}]),
    {ok, {<<"ERROR">>, _, _}} = sjs_recv(WS2),
    {close, _} = rfc6455_client:close(WS2).
