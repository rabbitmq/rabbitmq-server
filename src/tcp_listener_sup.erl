%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2020 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(tcp_listener_sup).

%% Supervises TCP listeners. There is a separate supervisor for every
%% protocol. In case of AMQP 0-9-1, it resides under rabbit_sup. Plugins
%% that provide protocol support (e.g. STOMP) have an instance of this supervisor in their
%% app supervision tree.
%%
%% See also rabbit_networking and tcp_listener.

-behaviour(supervisor).

-export([start_link/10]).
-export([init/1]).

-type mfargs() :: {atom(), atom(), [any()]}.

-spec start_link
        (inet:ip_address(), inet:port_number(), module(), [gen_tcp:listen_option()],
         module(), any(), mfargs(), mfargs(), integer(), string()) ->
                           rabbit_types:ok_pid_or_error().

start_link(IPAddress, Port, Transport, SocketOpts, ProtoSup, ProtoOpts, OnStartup, OnShutdown,
           ConcurrentAcceptorCount, Label) ->
    supervisor:start_link(
      ?MODULE, {IPAddress, Port, Transport, SocketOpts, ProtoSup, ProtoOpts, OnStartup, OnShutdown,
                ConcurrentAcceptorCount, Label}).

init({IPAddress, Port, Transport, SocketOpts, ProtoSup, ProtoOpts, OnStartup, OnShutdown,
      ConcurrentAcceptorCount, Label}) ->
    {ok, AckTimeout} = application:get_env(rabbit, ssl_handshake_timeout),
    MaxConnections = rabbit_misc:get_env(rabbit, connection_max, infinity),
    RanchListenerOpts = #{
      num_acceptors => ConcurrentAcceptorCount,
      max_connections => MaxConnections,
      handshake_timeout => AckTimeout,
      connection_type => supervisor,
      socket_opts => [{ip, IPAddress},
                      {port, Port} |
                      SocketOpts]
     },
    Flags = {one_for_all, 10, 10},
    OurChildSpecStart = {tcp_listener, start_link, [IPAddress, Port, OnStartup, OnShutdown, Label]},
    OurChildSpec = {tcp_listener, OurChildSpecStart, transient, 16#ffffffff, worker, [tcp_listener]},
    RanchChildSpec = ranch:child_spec(rabbit_networking:ranch_ref(IPAddress, Port),
        Transport, RanchListenerOpts,
        ProtoSup, ProtoOpts),
    {ok, {Flags, [RanchChildSpec, OurChildSpec]}}.
