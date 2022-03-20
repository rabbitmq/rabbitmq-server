%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2022 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(rabbit_tracing_sup).

-behaviour(supervisor).

-include_lib("rabbit_common/include/rabbit.hrl").

-define(SUPERVISOR, ?MODULE).

-export([start_link/0, start_child/2, stop_child/1]).
-export([init/1]).

%%----------------------------------------------------------------------------

start_link() ->
    supervisor:start_link({local, ?SUPERVISOR}, ?MODULE, []).

start_child(Id, Args) ->
    supervisor:start_child(
      ?SUPERVISOR,
      {Id, {rabbit_tracing_consumer_sup, start_link, [Args]},
       temporary, ?SUPERVISOR_WAIT, supervisor,
       [rabbit_tracing_consumer_sup]}).

stop_child(Id) ->
    supervisor:terminate_child(?SUPERVISOR, Id),
    supervisor:delete_child(?SUPERVISOR, Id),
    ok.

%%----------------------------------------------------------------------------

init([]) -> {ok, {{one_for_one, 3, 10},
                  [{traces, {rabbit_tracing_traces, start_link, []},
                    transient, ?WORKER_WAIT, worker,
                    [rabbit_tracing_traces]}]}}.
