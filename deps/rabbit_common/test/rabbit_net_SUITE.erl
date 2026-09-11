%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2026 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
%%

-module(rabbit_net_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").

-compile(export_all).
-compile(nowarn_export_all).

all() ->
    [
     fast_close_plain_port,
     fast_close_healthy_tls,
     fast_close_recovers_from_stuck_recv
    ].

init_per_suite(Config) ->
    {ok, _} = application:ensure_all_started(ssl),
    PrivDir = ?config(priv_dir, Config),
    CertFile = filename:join(PrivDir, "cert.pem"),
    KeyFile = filename:join(PrivDir, "key.pem"),
    Cmd = lists:flatten(
            io_lib:format(
              "openssl req -x509 -newkey rsa:2048 -keyout ~ts -out ~ts "
              "-days 1 -nodes -subj /CN=localhost 2>/dev/null",
              [KeyFile, CertFile])),
    _ = os:cmd(Cmd),
    case filelib:is_file(CertFile) andalso filelib:is_file(KeyFile) of
        true  -> [{certfile, CertFile}, {keyfile, KeyFile} | Config];
        false -> {skip, "openssl is required to generate test certificates"}
    end.

end_per_suite(Config) ->
    Config.

end_per_testcase(_Testcase, Config) ->
    catch meck:unload(gen_tcp),
    Config.

%% -------------------------------------------------------------------
%% Test cases.
%% -------------------------------------------------------------------

%% A plain TCP socket is closed immediately.
fast_close_plain_port(_Config) ->
    {ok, L} = gen_tcp:listen(0, [binary, {active, false}, {reuseaddr, true}]),
    {ok, Port} = inet:port(L),
    {ok, _C} = gen_tcp:connect({127, 0, 0, 1}, Port, [binary, {active, false}], 30_000),
    {ok, S} = gen_tcp:accept(L, 30_000),
    ?assertEqual(ok, rabbit_net:fast_close(S)),
    ?assertEqual(undefined, erlang:port_info(S)),
    ok = gen_tcp:close(L).

%% A healthy TLS socket is closed promptly, without paying the timeout.
fast_close_healthy_tls(Config) ->
    {S, Client, ConnPid} = new_tls_pair(Config, []),
    T0 = erlang:monotonic_time(millisecond),
    ?assertEqual(ok, rabbit_net:fast_close(S)),
    Elapsed = erlang:monotonic_time(millisecond) - T0,
    ?assert(Elapsed < 3000),
    await_exit(ConnPid),
    stop_client(Client),
    ok.

%% Reproduces the stuck port VM bug: the `recv` in `tls_gen_connection:close/4`
%% ignores its timeout and only returns once the transport port is closed.
fast_close_recovers_from_stuck_recv(Config) ->
    {S, Client, ConnPid} = new_tls_pair(Config, []),
    TransportPort = transport_port(ConnPid),
    ok = meck:new(gen_tcp, [unstick, passthrough]),
    try
        %% Hang the length 0 `recv` from `tls_gen_connection:close/4` until the
        %% port terminates.
        ok = meck:expect(
               gen_tcp, recv,
               fun (Sock, 0, _Timeout) when Sock =:= TransportPort ->
                       Ref = erlang:monitor(port, Sock),
                       receive {'DOWN', Ref, port, Sock, _} -> {error, closed} end;
                   (Sock, Length, Timeout) ->
                       meck:passthrough([Sock, Length, Timeout])
               end),
        Timeout = 300,
        T0 = erlang:monotonic_time(millisecond),
        ?assertEqual(ok, rabbit_net:fast_close(S, Timeout)),
        Elapsed = erlang:monotonic_time(millisecond) - T0,
        %% The forced path was taken, and it stayed bounded.
        ?assert(Elapsed >= Timeout - 100),
        ?assert(Elapsed < 3000),
        %% The stuck `recv` was actually reached and intercepted.
        ?assert(meck:num_calls(gen_tcp, recv, [TransportPort, 0, '_']) >= 1),
        await_exit(ConnPid)
    after
        meck:unload(gen_tcp)
    end,
    stop_client(Client),
    ok.

%% -------------------------------------------------------------------
%% Helpers.
%% -------------------------------------------------------------------

new_tls_pair(Config, ExtraOpts) ->
    CertFile = ?config(certfile, Config),
    KeyFile = ?config(keyfile, Config),
    {ok, L} = ssl:listen(0, [{certfile, CertFile}, {keyfile, KeyFile}, binary,
                             {active, false}, {reuseaddr, true} | ExtraOpts]),
    {ok, {_, Port}} = ssl:sockname(L),
    Before = tls_server_connections(),
    Parent = self(),
    Client = spawn(fun () ->
                           {ok, C} = ssl:connect(
                                       "localhost", Port,
                                       [binary, {active, false},
                                        {verify, verify_none} | ExtraOpts], 30_000),
                           Parent ! {client_ready, self()},
                           receive stop -> ssl:close(C) end
                   end),
    {ok, T} = ssl:transport_accept(L, 30_000),
    ok = ssl:close(L),
    {ok, S} = ssl:handshake(T, 30_000),
    receive {client_ready, _} -> ok
    after 30_000 -> ct:fail(tls_client_did_not_connect)
    end,
    [ConnPid] = tls_server_connections() -- Before,
    {S, Client, ConnPid}.

stop_client(Client) ->
    Client ! stop,
    ok.

await_exit(Pid) ->
    await_exit(Pid, 300).

await_exit(Pid, 0) ->
    ct:fail({still_alive, Pid});
await_exit(Pid, N) ->
    case erlang:is_process_alive(Pid) of
        false -> ok;
        true  -> timer:sleep(100), await_exit(Pid, N - 1)
    end.

tls_server_connections() ->
    [P || P <- erlang:processes(),
          case proc_lib:get_label(P) of
              {tls, server, _}    -> true;
              {tls, server, _, _} -> true;
              _                   -> false
          end].

transport_port(ConnPid) ->
    {links, Links} = erlang:process_info(ConnPid, links),
    hd([P || P <- Links, is_port(P)]).
