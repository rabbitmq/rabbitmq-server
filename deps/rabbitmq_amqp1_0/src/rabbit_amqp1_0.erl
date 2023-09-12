%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2023 VMware, Inc. or its affiliates.  All rights reserved.
%%
-module(rabbit_amqp1_0).

-export([emit_connection_info_local/3,
         emit_connection_info_all/4,
         list/0,
         register_connection/1,
         unregister_connection/1]).

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

-spec list() -> [pid()].
list() ->
    pg_local:get_members(rabbit_amqp10_connections).

-spec register_connection(pid()) -> ok.
register_connection(Pid) ->
    pg_local:join(rabbit_amqp10_connections, Pid).

-spec unregister_connection(pid()) -> ok.
unregister_connection(Pid) ->
    pg_local:leave(rabbit_amqp10_connections, Pid).
