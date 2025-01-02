%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2025 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
%%

-module(rabbit_tracing_consumer_sup).

-behaviour(supervisor).

-include_lib("rabbit_common/include/rabbit.hrl").

-export([start_link/1]).
-export([init/1]).

start_link(Args) -> supervisor:start_link(?MODULE, Args).

%%----------------------------------------------------------------------------

init(Args) ->
    {ok, {{one_for_one, 3, 10},
          [{consumer, {rabbit_tracing_consumer, start_link, [Args]},
            transient, ?WORKER_WAIT, worker,
            [rabbit_tracing_consumer]}]}}.
