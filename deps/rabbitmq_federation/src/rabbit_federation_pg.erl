%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2025 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
%%

-module(rabbit_federation_pg).

-include("rabbit_federation.hrl").

-export([start_scope/0, stop_scope/0]).

start_scope() ->
  rabbit_log_federation:debug("Starting pg scope ~ts", [?FEDERATION_PG_SCOPE]),
  _ = pg:start_link(?FEDERATION_PG_SCOPE).

stop_scope() ->
  case whereis(?FEDERATION_PG_SCOPE) of
      Pid when is_pid(Pid) ->
          rabbit_log_federation:debug("Stopping pg scope ~ts", [?FEDERATION_PG_SCOPE]),
          exit(Pid, normal);
      _ ->
          ok
  end.
