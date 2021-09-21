%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2021 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(rabbit_amqqueue_sup).

-behaviour(supervisor2).

-export([start_link/2]).

-export([init/1]).

-include_lib("rabbit_common/include/rabbit.hrl").

%%----------------------------------------------------------------------------

-spec start_link(amqqueue:amqqueue(), rabbit_prequeue:start_mode()) ->
          {'ok', pid(), pid()}.

start_link(Q, StartMode) ->
    Marker = spawn_link(fun() -> receive stop -> ok end end),
    ChildSpec = {rabbit_amqqueue,
                 {rabbit_prequeue, start_link, [Q, StartMode, Marker]},
                 intrinsic, ?CLASSIC_QUEUE_WORKER_WAIT, worker,
                 [rabbit_amqqueue_process, rabbit_mirror_queue_slave]},
    {ok, SupPid} = supervisor2:start_link(?MODULE, []),
    {ok, QPid} = supervisor2:start_child(SupPid, ChildSpec),
    unlink(Marker),
    Marker ! stop,
    {ok, SupPid, QPid}.

init([]) -> {ok, {{one_for_one, 5, 10}, []}}.
