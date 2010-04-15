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
%%   The Initial Developers of the Original Code are LShift Ltd,
%%   Cohesive Financial Technologies LLC, and Rabbit Technologies Ltd.
%%
%%   Portions created before 22-Nov-2008 00:00:00 GMT by LShift Ltd,
%%   Cohesive Financial Technologies LLC, or Rabbit Technologies Ltd
%%   are Copyright (C) 2007-2008 LShift Ltd, Cohesive Financial
%%   Technologies LLC, and Rabbit Technologies Ltd.
%%
%%   Portions created by LShift Ltd are Copyright (C) 2007-2010 LShift
%%   Ltd. Portions created by Cohesive Financial Technologies LLC are
%%   Copyright (C) 2007-2010 Cohesive Financial Technologies
%%   LLC. Portions created by Rabbit Technologies Ltd are Copyright
%%   (C) 2007-2010 Rabbit Technologies Ltd.
%%
%%   All Rights Reserved.
%%
%%   Contributor(s): ______________________________________.
%%

-module(worker_pool_sup).

-behaviour(supervisor).

-export([start_link/0, start_link/1]).

-export([init/1]).

%%----------------------------------------------------------------------------

-ifdef(use_specs).

-spec(start_link/0 :: () -> {'ok', pid()} | 'ignore' | {'error', any()}).
-spec(start_link/1 ::
        (non_neg_integer()) -> {'ok', pid()} | 'ignore' | {'error', any()}).

-endif.

%%----------------------------------------------------------------------------

-define(SERVER, ?MODULE).

%%----------------------------------------------------------------------------

start_link() ->
    start_link(erlang:system_info(schedulers)).

start_link(WCount) ->
    supervisor:start_link({local, ?SERVER}, ?MODULE, [WCount]).

%%----------------------------------------------------------------------------

init([WCount]) ->
    {ok, {{one_for_one, 10, 10},
          [{worker_pool, {worker_pool, start_link, []}, transient,
            16#ffffffff, worker, [worker_pool]} |
           [{N, {worker_pool_worker, start_link, [N]}, transient, 16#ffffffff,
             worker, [worker_pool_worker]} || N <- lists:seq(1, WCount)]]}}.
