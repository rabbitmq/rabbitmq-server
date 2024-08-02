%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2024 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
%%
-module(rabbit_amqp1_0).

-export([list_local/0,
         register_connection/1]).

%% Below 2 functions are deprecated.
%% They could be called in 3.13 / 4.0 mixed version clusters by the old 3.13 CLI command
%% rabbitmqctl list_amqp10_connections
-export([emit_connection_info_local/3,
         emit_connection_info_all/4]).

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
              rabbit_amqp_reader:info(Pid, Items)
      end,
      ConnectionPids).

-spec list_local() -> [pid()].
list_local() ->
    pg:which_groups(pg_scope()).

-spec register_connection(pid()) -> ok.
register_connection(Pid) ->
    ok = pg:join(pg_scope(), Pid, Pid).

pg_scope() ->
    rabbit:pg_local_scope(amqp_connection).
