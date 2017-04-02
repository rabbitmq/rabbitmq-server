%% The contents of this file are subject to the Mozilla Public License
%% Version 1.1 (the "License"); you may not use this file except in
%% compliance with the License. You may obtain a copy of the License
%% at http://www.mozilla.org/MPL/
%%
%% Software distributed under the License is distributed on an "AS IS"
%% basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See
%% the License for the specific language governing rights and
%% limitations under the License.
%%
%% The Original Code is RabbitMQ Federation.
%%
%% The Initial Developer of the Original Code is GoPivotal, Inc.
%% Copyright (c) 2007-2017 Pivotal Software, Inc.  All rights reserved.
%%

-module(rabbit_tracing_consumer_sup).

-behaviour(supervisor).

-include_lib("rabbit_common/include/rabbit.hrl").

-export([start_link/1]).
-export([init/1]).

start_link(Args) -> supervisor2:start_link(?MODULE, Args).

%%----------------------------------------------------------------------------

init(Args) ->
    {ok, {{one_for_one, 3, 10},
          [{consumer, {rabbit_tracing_consumer, start_link, [Args]},
            transient, ?WORKER_WAIT, worker,
            [rabbit_tracing_consumer]}]}}.
