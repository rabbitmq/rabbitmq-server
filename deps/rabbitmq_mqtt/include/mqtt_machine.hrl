%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2020-2023 VMware, Inc. or its affiliates.  All rights reserved.
%%

%%TODO decrease per connection memory overhead
%% since the Raft process memory can grow a few GBs with
%% millions of connections.
%% 1. Use binaries instead of string()s for the ConnectionId
%% 2. Use new Erlang 24 function erlang:monitor/3 with tag being the ConnectionId
%%    so that we can get rid of pids fields because we won't to lookup the ConnectionId
%%    by PID anymore.
-record(machine_state, {
          %% client ID to connection PID
          client_ids = #{},
          %% connection PID to list of client IDs
          pids = #{},
          %% add acouple of fields for future extensibility
          reserved_1,
          reserved_2}).

