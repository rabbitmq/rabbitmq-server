%% The contents of this file are subject to the Mozilla Public License
%% Version 2.0 (the "License"); you may not use this file except in
%% compliance with the License. You may obtain a copy of the License
%% at https://www.mozilla.org/en-US/MPL/2.0/
%%
%% Software distributed under the License is distributed on an "AS IS"
%% basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See
%% the License for the specific language governing rights and
%% limitations under the License.
%%
%% The Original Code is RabbitMQ.
%%
%% The Initial Developer of the Original Code is Pivotal Software, Inc.
%% Copyright (c) 2021 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(rabbit_stream_prometheus).

-behaviour(application).

-export([start/2]).
-export([stop/1]).

-behaviour(supervisor).

-export([init/1]).

start(_Type, _Args) ->
    prometheus_registry:register_collectors([prometheus_rabbitmq_stream_collector]),
    prometheus_registry:register_collectors('per-object',
                                            [prometheus_rabbitmq_stream_collector]),
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

stop(_State) ->
    ok.

init([]) ->
    {ok, {{one_for_one, 3, 10}, []}}.
