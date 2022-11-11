%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2022 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(rabbit_db).

-include_lib("kernel/include/logger.hrl").
-include_lib("stdlib/include/assert.hrl").

-export([run/1]).

%% -------------------------------------------------------------------
%% run().
%% -------------------------------------------------------------------

-spec run(Funs) -> Ret when
      Funs :: #{mnesia := Fun},
      Fun :: fun(() -> Ret),
      Ret :: any().
%% @doc Runs the function corresponding to the used database engine.
%%
%% @returns the return value of `Fun'.

run(Funs)
  when is_map(Funs) andalso is_map_key(mnesia, Funs) ->
    #{mnesia := MnesiaFun} = Funs,
    run_with_mnesia(MnesiaFun).

run_with_mnesia(Fun) ->
    Fun().
