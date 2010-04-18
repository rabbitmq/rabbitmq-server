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

-module(rabbit_msg_store_gc).

-behaviour(gen_server2).

-export([start_link/4, gc/3, stop/1]).

-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-record(gcstate,
        {dir,
         index_state,
         index_module,
         parent,
         file_summary_ets
        }).

-include("rabbit.hrl").

%%----------------------------------------------------------------------------

start_link(Dir, IndexState, IndexModule, FileSummaryEts) ->
    gen_server2:start_link(
      ?MODULE, [self(), Dir, IndexState, IndexModule, FileSummaryEts],
      [{timeout, infinity}]).

gc(Server, Source, Destination) ->
    gen_server2:cast(Server, {gc, Source, Destination}).

stop(Server) ->
    gen_server2:call(Server, stop, infinity).

%%----------------------------------------------------------------------------

init([Parent, Dir, IndexState, IndexModule, FileSummaryEts]) ->
    {ok, #gcstate { dir = Dir, index_state = IndexState,
                    index_module = IndexModule, parent = Parent,
                    file_summary_ets = FileSummaryEts},
     hibernate,
     {backoff, ?HIBERNATE_AFTER_MIN, ?HIBERNATE_AFTER_MIN, ?DESIRED_HIBERNATE}}.

handle_call(stop, _From, State) ->
    {stop, normal, ok, State}.

handle_cast({gc, Source, Destination}, State =
                #gcstate { parent = Parent, dir = Dir, index_module = Index,
                           index_state = IndexState,
                           file_summary_ets = FileSummaryEts }) ->
    Reclaimed = rabbit_msg_store:gc(Source, Destination,
                                    {FileSummaryEts, Dir, Index, IndexState}),
    ok = rabbit_msg_store:gc_done(Parent, Reclaimed, Source, Destination),
    {noreply, State, hibernate}.

handle_info({file_handle_cache, maximum_eldest_since_use, Age}, State) ->
    ok = file_handle_cache:set_maximum_since_use(Age),
    {noreply, State, hibernate};

handle_info(Info, State) ->
    {stop, {unhandled_info, Info}, State}.

terminate(_Reason, State) ->
    State.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.
