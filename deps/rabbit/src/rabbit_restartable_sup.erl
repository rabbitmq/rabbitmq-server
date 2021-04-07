%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2021 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(rabbit_restartable_sup).

-behaviour(supervisor2).

-export([start_link/3]).

-export([init/1]).

-include_lib("rabbit_common/include/rabbit.hrl").

-define(DELAY, 2).

%%----------------------------------------------------------------------------

-spec start_link(atom(), rabbit_types:mfargs(), boolean()) ->
                           rabbit_types:ok_pid_or_error().

start_link(Name, {_M, _F, _A} = Fun, Delay) ->
    supervisor2:start_link({local, Name}, ?MODULE, [Fun, Delay]).

init([{Mod, _F, _A} = Fun, Delay]) ->
    {ok, {{one_for_one, 10, 10},
          [{Mod, Fun, case Delay of
                          true  -> {transient, 1};
                          false -> transient
                      end, ?WORKER_WAIT, worker, [Mod]}]}}.
