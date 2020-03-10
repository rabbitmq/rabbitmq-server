%% The contents of this file are subject to the Mozilla Public License
%% Version 1.1 (the "License"); you may not use this file except in
%% compliance with the License. You may obtain a copy of the License
%% at https://www.mozilla.org/MPL/
%%
%% Software distributed under the License is distributed on an "AS IS"
%% basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See
%% the License for the specific language governing rights and
%% limitations under the License.
%%
%% The Original Code is RabbitMQ.
%%
%% The Initial Developer of the Original Code is GoPivotal, Inc.
%% Copyright (c) 2007-2020 VMware, Inc. or its affiliates.  All rights reserved.
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

-record(state, {on_startup, on_shutdown, label, ip, port}).

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

init({IPAddress, Port, {M,F,A} = OnStartup, OnShutdown, Label}) ->
    process_flag(trap_exit, true),
    error_logger:info_msg(
      "started ~s on ~s:~p~n",
      [Label, rabbit_misc:ntoab(IPAddress), Port]),
    apply(M, F, A ++ [IPAddress, Port]),
    {ok, #state{on_startup = OnStartup, on_shutdown = OnShutdown,
                label = Label, ip=IPAddress, port=Port}}.

handle_call(_Request, _From, State) ->
    {noreply, State}.

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, #state{on_shutdown = {M,F,A}, label=Label, ip=IPAddress, port=Port}) ->
    error_logger:info_msg("stopped ~s on ~s:~p~n",
                          [Label, rabbit_misc:ntoab(IPAddress), Port]),
    apply(M, F, A ++ [IPAddress, Port]).

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.
