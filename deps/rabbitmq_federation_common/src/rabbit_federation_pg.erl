%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2025 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
%%

-module(rabbit_federation_pg).

-export([start_scope/1, stop_scope/1]).

start_scope(Scope) ->
  rabbit_log_federation:debug("Starting pg scope ~ts", [Scope]),
  _ = pg:start_link(Scope).

stop_scope(Scope) ->
  case whereis(Scope) of
      Pid when is_pid(Pid) ->
          rabbit_log_federation:debug("Stopping pg scope ~ts", [Scope]),
          exit(Pid, normal);
      _ ->
          ok
  end.
