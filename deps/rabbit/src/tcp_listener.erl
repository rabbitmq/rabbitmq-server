%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2025 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
%%

-module(tcp_listener).

%% Represents a running TCP listener (a process that listens for inbound
%% TCP or TLS connections). Every protocol supported typically has one
%% or two listeners, plain TCP and (optionally) TLS, but there can
%% be more, e.g. when multiple network interfaces are involved.
%%
%% A listener has 6 properties (is a tuple of 6):
%%
%%  * IP address
%%  * Port
%%  * Node
%%  * Label (human-friendly name, e.g. AMQP 0-9-1)
%%  * Startup callback
%%  * Shutdown callback
%%
%% Listeners use Ranch in embedded mode to accept and "bridge" client
%% connections with protocol entry points such as rabbit_reader.
%%
%% Listeners are tracked in a Mnesia table so that they can be
%%
%%  * Shut down
%%  * Listed (e.g. in the management UI)
%%
%% Every tcp_listener process has callbacks that are executed on start
%% and termination. Those must take care of listener registration
%% among other things.
%%
%% Listeners are supervised by tcp_listener_sup (one supervisor per protocol).
%%
%% See also rabbit_networking and tcp_listener_sup.

-behaviour(gen_server).

-export([start_link/5]).

-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-record(state, {on_shutdown, label, ip, port}).

%%----------------------------------------------------------------------------

-type mfargs() :: {atom(), atom(), [any()]}.

-spec start_link
        (inet:ip_address(), inet:port_number(),
         mfargs(), mfargs(), string()) ->
                           rabbit_types:ok_pid_or_error().

start_link(IPAddress, Port,
           OnStartup, OnShutdown, Label) ->
    gen_server:start_link(
      ?MODULE, {IPAddress, Port,
                OnStartup, OnShutdown, Label}, []).

%%--------------------------------------------------------------------

init({IPAddress, Port, {M, F, A}, OnShutdown, Label}) ->
    process_flag(trap_exit, true),
    logger:info("started ~ts on ~ts:~tp", [Label, rabbit_misc:ntoab(IPAddress), Port]),
    apply(M, F, A ++ [IPAddress, Port]),
    State0 = #state{
        on_shutdown = OnShutdown,
        label = Label,
        ip = IPAddress,
        port = Port
    },
    {ok, obfuscate_state(State0)}.

handle_call(_Request, _From, State) ->
    {noreply, State}.

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, #state{on_shutdown = OnShutdown, label = Label, ip = IPAddress, port = Port}) ->
    logger:info("stopped ~ts on ~ts:~tp", [Label, rabbit_misc:ntoab(IPAddress), Port]),
    try
        OnShutdown(IPAddress, Port)
    catch _:Error ->
        logger:error("Failed to stop ~ts on ~ts:~tp: ~tp",
                     [Label, rabbit_misc:ntoab(IPAddress), Port, Error])
    end.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

obfuscate_state(#state{on_shutdown = OnShutdown} = State) ->
    {M, F, A} = OnShutdown,
    State#state{
        %% avoids arguments from being logged in case of an exception
        on_shutdown = fun(IPAddress, Port) ->
          apply(M, F, A ++ [IPAddress, Port])
        end
    }.
