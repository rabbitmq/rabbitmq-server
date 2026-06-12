%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2026 Broadcom. All Rights Reserved. The term "Broadcom" refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.

%% Per-queue supervisor for a single rabbit_fifo_dlx_worker process.
%% Started as a temporary child of rabbit_fifo_dlx_sup_sup (simple_one_for_one).
%% This isolation prevents a burst of DLX worker crashes from taking down
%% the top-level supervisor and stranding unrelated queues.
-module(rabbit_fifo_dlx_worker_sup).

-behaviour(supervisor).

-export([start_link/1, init/1]).

start_link(QRef) ->
    supervisor:start_link(?MODULE, [QRef]).

init([QRef]) ->
    DlxWorkerSupPid = self(),
    SupFlags = #{strategy => one_for_one,
                 intensity => 0,
                 period => 1},
    ChildSpec = #{id => rabbit_fifo_dlx_worker,
                  start => {rabbit_fifo_dlx_worker, start_link, [QRef, DlxWorkerSupPid]},
                  type => worker,
                  restart => transient,
                  shutdown => 5000},
    {ok, {SupFlags, [ChildSpec]}}.
