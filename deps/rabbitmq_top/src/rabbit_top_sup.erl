%%   The contents of this file are subject to the Mozilla Public License
%%   Version 1.1 (the "License"); you may not use this file except in
%%   compliance with the License. You may obtain a copy of the License at
%%   http://www.mozilla.org/MPL/
%%
%%   Software distributed under the License is distributed on an "AS IS"
%%   basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See the
%%   License for the specific language governing rights and limitations
%%   under the License.
%%
%%   The Original Code is RabbitMQ.
%%
%%   The Initial Developer of the Original Code is VMware, Inc.
%%   Copyright (c) 2011-2012 VMware, Inc.  All rights reserved.
%%

-module(rabbit_top_sup).

-behaviour(supervisor).

-include_lib("rabbit_common/include/rabbit.hrl").

-export([start_link/0]).
-export([init/1]).

start_link() ->
     supervisor:start_link({local, ?MODULE}, ?MODULE, []).

init([]) ->
    Top = {rabbit_top_worker,
           {rabbit_top_worker, start_link, []},
           permanent, ?WORKER_WAIT, worker, [rabbit_top_worker]},
    {ok, {{one_for_one, 10, 10}, [Top]}}.

