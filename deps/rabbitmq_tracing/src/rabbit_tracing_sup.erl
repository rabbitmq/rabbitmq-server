%% The contents of this file are subject to the Mozilla Public License
%% Version 1.1 (the "License"); you may not use this file except in
%% compliance with the License. You may obtain a copy of the License
%% at https://www.mozilla.org/MPL/
%%
%% Software distributed under the License is distributed on an "AS IS"
%% basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See
%% the License for the specific language governing rights and
%% limitations under the License.
%%
%% The Original Code is RabbitMQ.
%%
%% The Initial Developer of the Original Code is GoPivotal, Inc.
%% Copyright (c) 2007-2020 VMware, Inc. or its affiliates.  All rights reserved.
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
