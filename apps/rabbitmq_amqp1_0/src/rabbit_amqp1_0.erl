%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2021 VMware, Inc. or its affiliates.  All rights reserved.
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
