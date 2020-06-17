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

%%----------------------------------------------------------------------------

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
    {ok, {{one_for_all, 10, 10}, [
        ranch:child_spec({acceptor, IPAddress, Port},
            Transport, RanchListenerOpts,
            ProtoSup, ProtoOpts),
        {tcp_listener, {tcp_listener, start_link,
                        [IPAddress, Port,
                         OnStartup, OnShutdown, Label]},
         transient, 16#ffffffff, worker, [tcp_listener]}]}}.
