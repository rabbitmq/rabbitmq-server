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
%% The Initial Developer of the Original Code is GoPivotal, Inc.
%% Copyright (c) 2007-2018 Pivotal Software, Inc.  All rights reserved.
%%
-module(inet_tcp_proxy).

%% Transitional step until we can require Erlang/OTP 21 and
%% use the now recommended try/catch syntax for obtaining the stack trace.
-compile(nowarn_deprecated_function).

%% A TCP proxy for insertion into the Erlang distribution mechanism,
%% which allows us to simulate network partitions.

-export([start/3, reconnect/1, is_enabled/0, allow/1, block/1]).

-define(NODES_TO_BLOCK, inet_tcp_proxy__nodes_to_block).
-define(NODES_BLOCKED, inet_tcp_proxy__nodes_blocked).

%% This can't start_link because there's no supervision hierarchy we
%% can easily fit it into (we need to survive all application
%% restarts). So we have to do some horrible error handling.

start(ManagerNode, DistPort, ProxyPort) ->
    application:set_env(kernel, inet_tcp_proxy_manager_node, ManagerNode),
    Parent = self(),
    Pid = spawn(error_handler(fun() -> go(Parent, DistPort, ProxyPort) end)),
    MRef = erlang:monitor(process, Pid),
    receive
        ready ->
            erlang:demonitor(MRef),
            ok;
        {'DOWN', MRef, _, _, Reason} ->
            {error, Reason}
    end.

reconnect(Nodes) ->
    [erlang:disconnect_node(N) || N <- Nodes, N =/= node()],
    ok.

is_enabled() ->
    lists:member(?NODES_TO_BLOCK, ets:all()).

allow(Node) ->
    error_logger:warning_msg("(~s) Allowing distribution between ~s and ~s~n",
      [?MODULE, node(), Node]),
    true = ets:delete(?NODES_TO_BLOCK, Node).
block(Node) ->
    error_logger:warning_msg("(~s) BLOCKING distribution between ~s and ~s~n",
      [?MODULE, node(), Node]),
    true = ets:insert(?NODES_TO_BLOCK, {Node, block}).

%%----------------------------------------------------------------------------

error_handler(Thunk) ->
    fun () ->
            try
                Thunk()
            catch _:{{nodedown, _}, _} ->
                    %% The only other node we ever talk to is the test
                    %% runner; if that's down then the test is nearly
                    %% over; die quietly.
                    ok;
                  _:X ->
                    error_logger:error_msg(
                      "TCP proxy died with ~p~n At ~p~n",
                      [X, erlang:get_stacktrace()]),
                    io:format(standard_error,
                              "TCP proxy died with ~p~n At ~p~n",
                              [X, erlang:get_stacktrace()]),
                    timer:sleep(1000),
                    erlang:halt(1)
            end
    end.

go(Parent, Port, ProxyPort) ->
    ets:new(?NODES_TO_BLOCK, [public, named_table]),
    ets:new(?NODES_BLOCKED, [public, named_table]),
    error_logger:info_msg(
      "(~s) Listening on proxy port ~p~n",
      [?MODULE, ProxyPort]),
    {ok, Sock} = gen_tcp:listen(ProxyPort, [inet,
                                            {reuseaddr, true}]),
    Parent ! ready,
    accept_loop(Sock, Port).

accept_loop(ListenSock, Port) ->
    {ok, Sock} = gen_tcp:accept(ListenSock),
    Proxy = spawn(error_handler(fun() -> run_it(Sock, Port) end)),
    ok = gen_tcp:controlling_process(Sock, Proxy),
    accept_loop(ListenSock, Port).

run_it(SockIn, Port) ->
    case {inet:peername(SockIn), inet:sockname(SockIn)} of
        {{ok, {_Addr, SrcPort}}, {ok, {Addr, OtherPort}}} ->
            {ok, Remote, This} = inet_tcp_proxy_manager:lookup(SrcPort),
            error_logger:info_msg(
              "(~s) => Incoming proxied connection from node ~s (port ~b) "
              "to node ~s (port ~b)~n",
              [?MODULE, Remote, SrcPort, This, OtherPort]),
            case node() of
                This  -> ok;
                _     -> exit({not_me, node(), This})
            end,
            {ok, SockOut} = gen_tcp:connect(Addr, Port, [inet]),
            run_loop({SockIn, SockOut}, Remote, []);
        _ ->
            ok
    end.

run_loop(Sockets, RemoteNode, Buf0) ->
    Block = [{RemoteNode, block}] =:= ets:lookup(?NODES_TO_BLOCK, RemoteNode),
    WasBlocked = [{RemoteNode, blocked}] =:= ets:lookup(?NODES_BLOCKED,
                                                        RemoteNode),
    receive
        {tcp, Sock, Data} ->
            Buf = [Data | Buf0],
            case {WasBlocked, Block} of
                {false, true} ->
                    true = ets:insert(?NODES_BLOCKED, {RemoteNode, blocked}),
                    error_logger:warning_msg(
                      "(~s) Distribution BLOCKED between ~s and ~s~n",
                      [?MODULE, node(), RemoteNode]);
                {true, false} ->
                    true = ets:delete(?NODES_BLOCKED, RemoteNode),
                    error_logger:warning_msg(
                      "(~s) Distribution allowed between ~s and ~s~n",
                      [?MODULE, node(), RemoteNode]);
                _ ->
                    ok
            end,
            case Block of
                false -> gen_tcp:send(other(Sock, Sockets), lists:reverse(Buf)),
                         run_loop(Sockets, RemoteNode, []);
                true  -> run_loop(Sockets, RemoteNode, Buf)
            end;
        {tcp_closed, Sock} ->
            error_logger:info_msg(
              "(~s) Distribution closed between ~s and ~s~n",
              [?MODULE, node(), RemoteNode]),
            gen_tcp:close(other(Sock, Sockets));
        X ->
            exit({weirdness, X})
    end.

other(A, {A, B}) -> B;
other(B, {A, B}) -> A.
