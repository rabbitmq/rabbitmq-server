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
-module(rabbit_amqp1_0).

-export([connection_info_local/1,
         emit_connection_info_local/3,
         emit_connection_info_all/4,
         list/0]).

emit_connection_info_all(Nodes, Items, Ref, AggregatorPid) ->
    Pids = [spawn_link(Node, rabbit_amqp1_0, emit_connection_info_local,
                       [Items, Ref, AggregatorPid])
            || Node <- Nodes],
    rabbit_control_misc:await_emitters_termination(Pids),
    ok.

emit_connection_info_local(Items, Ref, AggregatorPid) ->
    rabbit_control_misc:emitting_map_with_exit_handler(
      AggregatorPid, Ref,
      fun(Pid) ->
              rabbit_amqp1_0_reader:info(Pid, Items)
      end,
      list()).

connection_info_local(Items) ->
    Connections = list(),
    [rabbit_amqp1_0_reader:info(Pid, Items) || Pid <- Connections].

list() ->
    [ReaderPid
     || {_, TcpPid, _, [tcp_listener_sup]} <- supervisor:which_children(rabbit_sup),
        {_, RanchLPid, _, [ranch_listener_sup]} <- supervisor:which_children(TcpPid),
        {_, RanchCPid, _, [ranch_conns_sup]} <- supervisor:which_children(RanchLPid),
        {rabbit_connection_sup, ConnPid, _, _} <- supervisor:which_children(RanchCPid),
        {reader, ReaderPid, _, _} <- supervisor:which_children(ConnPid)
    ].
