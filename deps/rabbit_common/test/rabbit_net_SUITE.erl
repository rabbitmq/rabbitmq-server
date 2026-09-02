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
     fast_close_bounds_stuck_close,
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

%% -------------------------------------------------------------------
%% Test cases.
%% -------------------------------------------------------------------

%% A plain TCP socket is closed immediately.
fast_close_plain_port(_Config) ->
    {ok, L} = gen_tcp:listen(0, [binary, {active, false}, {reuseaddr, true}]),
    {ok, Port} = inet:port(L),
    {ok, _C} = gen_tcp:connect({127, 0, 0, 1}, Port, [binary, {active, false}], 5000),
    {ok, S} = gen_tcp:accept(L, 5000),
    ?assertEqual(ok, rabbit_net:fast_close(S)),
    ?assertEqual(undefined, erlang:port_info(S)),
    ok = gen_tcp:close(L).

%% A healthy TLS socket is closed promptly, without paying the timeout.
fast_close_healthy_tls(Config) ->
    {S, Client, _ConnPid} = new_tls_pair(Config, []),
    T0 = erlang:monotonic_time(millisecond),
    ?assertEqual(ok, rabbit_net:fast_close(S)),
    Elapsed = erlang:monotonic_time(millisecond) - T0,
    ?assert(Elapsed < 1000),
    stop_client(Client),
    ok.

%% When the peer stops reading, ssl:close/2 parks (waiting on the stuck
%% transport). This exercises the real ssl:close/2 path, without mocks:
%% fast_close/2 must force the transport port closed and return within its bound,
%% well under the several seconds ssl:close/2 would otherwise take.
fast_close_bounds_stuck_close(Config) ->
    SmallBuffers = [{sndbuf, 4096}, {recbuf, 4096}, {buffer, 4096},
                    {high_watermark, 2048}, {low_watermark, 1024}],
    {S, Client, _ConnPid} = new_tls_pair(Config, SmallBuffers),
    %% The peer never reads; fill the pipe so the server's close will park.
    FloodPid = spawn(fun () -> flood(S) end),
    await_backpressure(FloodPid),
    Timeout = 300,
    T0 = erlang:monotonic_time(millisecond),
    ?assertEqual(ok, rabbit_net:fast_close(S, Timeout)),
    Elapsed = erlang:monotonic_time(millisecond) - T0,
    ?assert(Elapsed >= Timeout - 100),
    ?assert(Elapsed < 3000),
    %% Kill the flood process in case it is still blocked in ssl:send, so it does
    %% not leak into later test cases.
    exit(FloodPid, kill),
    stop_client(Client),
    ok.

%% Reproduce the "stuck port" Erlang VM bug: the recv in
%% tls_gen_connection:close/4 never returns on its own (its timeout does not
%% fire), and only returns once the transport port is closed. fast_close/2 must
%% detect the stuck connection process, close its port, and return.
fast_close_recovers_from_stuck_recv(Config) ->
    {S, Client, ConnPid} = new_tls_pair(Config, []),
    TransportPort = transport_port(ConnPid),
    ok = meck:new(gen_tcp, [unstick, passthrough]),
    try
        %% close/4 calls Transport:recv(Socket, 0, Timeout); make that recv hang
        %% until the port terminates (mimicking the stuck transport port).
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
        %% The recv genuinely hung, so the forced path was taken (>= Timeout) and
        %% it was bounded (< several seconds).
        ?assert(Elapsed >= Timeout - 100),
        ?assert(Elapsed < 3000),
        %% The stuck recv was actually reached and intercepted.
        ?assert(meck:num_calls(gen_tcp, recv, [TransportPort, 0, '_']) >= 1),
        ?assertNot(erlang:is_process_alive(ConnPid))
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
                                        {verify, verify_none} | ExtraOpts], 5000),
                           Parent ! {client_ready, self()},
                           receive stop -> ssl:close(C) end
                   end),
    {ok, T} = ssl:transport_accept(L, 5000),
    ok = ssl:close(L),
    {ok, S} = ssl:handshake(T, 5000),
    receive {client_ready, _} -> ok
    after 5000 -> ct:fail(tls_client_did_not_connect)
    end,
    [ConnPid] = tls_server_connections() -- Before,
    {S, Client, ConnPid}.

stop_client(Client) ->
    Client ! stop,
    ok.

flood(S) ->
    case ssl:send(S, binary:copy(<<0>>, 4096)) of
        ok     -> flood(S);
        _Error -> ok
    end.

%% Wait until the flooding process stops making progress, i.e. it is blocked in
%% ssl:send because the peer is not reading. Only then is the send actually
%% stuck and the server's close will park.
await_backpressure(Pid) ->
    await_backpressure(Pid, reductions(Pid), 200).

await_backpressure(_Pid, _Reds, 0) ->
    ct:fail(backpressure_not_established);
await_backpressure(Pid, Reds, N) ->
    timer:sleep(20),
    case reductions(Pid) of
        Reds  -> ok;
        Reds2 -> await_backpressure(Pid, Reds2, N - 1)
    end.

reductions(Pid) ->
    case erlang:process_info(Pid, reductions) of
        {reductions, R} -> R;
        undefined       -> 0
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
