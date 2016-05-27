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
%% Copyright (c) 2007-2016 Pivotal Software, Inc.  All rights reserved.
%%
-module(inet_tcp_proxy).

%% A TCP proxy for insertion into the Erlang distribution mechanism,
%% which allows us to simulate network partitions.

-export([start/3, reconnect/1, is_enabled/0, allow/1, block/1]).

-define(TABLE, ?MODULE).

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
    lists:member(?TABLE, ets:all()).

allow(Node) ->
    rabbit_log:info("(~s) Allowing distribution between ~s and ~s~n",
      [?MODULE, node(), Node]),
    ets:delete(?TABLE, Node).
block(Node) ->
    rabbit_log:info("(~s) BLOCKING distribution between ~s and ~s~n",
      [?MODULE, node(), Node]),
    ets:insert(?TABLE, {Node, block}).

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
                    io:format(user, "TCP proxy died with ~p~n At ~p~n",
                              [X, erlang:get_stacktrace()]),
                    erlang:halt(1)
            end
    end.

go(Parent, Port, ProxyPort) ->
    ets:new(?TABLE, [public, named_table]),
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
        {{ok, {_Addr, SrcPort}}, {ok, {Addr, _OtherPort}}} ->
            {ok, Remote, This} = inet_tcp_proxy_manager:lookup(SrcPort),
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
    Block = [{RemoteNode, block}] =:= ets:lookup(?TABLE, RemoteNode),
    receive
        {tcp, Sock, Data} ->
            Buf = [Data | Buf0],
            case {Block, get(dist_was_blocked)} of
                {true, false} ->
                    put(dist_was_blocked, Block),
                    rabbit_log:warning(
                      "(~s) Distribution BLOCKED between ~s and ~s~n",
                      [?MODULE, node(), RemoteNode]);
                {false, S} when S =:= true orelse S =:= undefined ->
                    put(dist_was_blocked, Block),
                    rabbit_log:warning(
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
            gen_tcp:close(other(Sock, Sockets));
        X ->
            exit({weirdness, X})
    end.

other(A, {A, B}) -> B;
other(B, {A, B}) -> A.
