%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2023 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries.  All rights reserved.
%%
-module(rabbit_amqp1_0).

-export([emit_connection_info_local/3,
         emit_connection_info_all/4,
         list_local/0,
         register_connection/1]).

-define(PROCESS_GROUP_NAME, rabbit_amqp10_connections).

emit_connection_info_all(Nodes, Items, Ref, AggregatorPid) ->
    Pids = [spawn_link(Node, rabbit_amqp1_0, emit_connection_info_local,
                       [Items, Ref, AggregatorPid])
            || Node <- Nodes],
    rabbit_control_misc:await_emitters_termination(Pids),
    ok.

emit_connection_info_local(Items, Ref, AggregatorPid) ->
    ConnectionPids = list_local(),
    rabbit_control_misc:emitting_map_with_exit_handler(
      AggregatorPid,
      Ref,
      fun(Pid) ->
              rabbit_amqp1_0_reader:info(Pid, Items)
      end,
      ConnectionPids).

-spec list_local() -> [pid()].
list_local() ->
    pg:get_local_members(node(), ?PROCESS_GROUP_NAME).

-spec register_connection(pid()) -> ok.
register_connection(Pid) ->
    ok = pg:join(node(), ?PROCESS_GROUP_NAME, Pid).
