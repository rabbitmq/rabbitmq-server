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

-export([start_link/4, gc/3, no_readers/2, stop/1]).

-export([set_maximum_since_use/2]).

-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3, prioritise_cast/2]).

-record(gcstate,
        {dir,
         index_state,
         index_module,
         parent,
         file_summary_ets,
         scheduled
        }).

-include("rabbit.hrl").

%%----------------------------------------------------------------------------

-ifdef(use_specs).

-spec(start_link/4 :: (file:filename(), any(), atom(), ets:tid()) ->
                           rabbit_types:ok_pid_or_error()).
-spec(gc/3 :: (pid(), non_neg_integer(), non_neg_integer()) -> 'ok').
-spec(no_readers/2 :: (pid(), non_neg_integer()) -> 'ok').
-spec(stop/1 :: (pid()) -> 'ok').
-spec(set_maximum_since_use/2 :: (pid(), non_neg_integer()) -> 'ok').

-endif.

%%----------------------------------------------------------------------------

start_link(Dir, IndexState, IndexModule, FileSummaryEts) ->
    gen_server2:start_link(
      ?MODULE, [self(), Dir, IndexState, IndexModule, FileSummaryEts],
      [{timeout, infinity}]).

gc(Server, Source, Destination) ->
    gen_server2:cast(Server, {gc, Source, Destination}).

no_readers(Server, File) ->
    gen_server2:cast(Server, {no_readers, File}).

stop(Server) ->
    gen_server2:call(Server, stop, infinity).

set_maximum_since_use(Pid, Age) ->
    gen_server2:cast(Pid, {set_maximum_since_use, Age}).

%%----------------------------------------------------------------------------

init([Parent, Dir, IndexState, IndexModule, FileSummaryEts]) ->
    ok = file_handle_cache:register_callback(?MODULE, set_maximum_since_use,
                                             [self()]),
    {ok, #gcstate { dir              = Dir,
                    index_state      = IndexState,
                    index_module     = IndexModule,
                    parent           = Parent,
                    file_summary_ets = FileSummaryEts,
                    scheduled        = undefined },
     hibernate,
     {backoff, ?HIBERNATE_AFTER_MIN, ?HIBERNATE_AFTER_MIN, ?DESIRED_HIBERNATE}}.

prioritise_cast({set_maximum_since_use, _Age}, _State) ->
    8;
prioritise_cast(_Msg, _State) ->
    0.

handle_call(stop, _From, State) ->
    {stop, normal, ok, State}.

handle_cast({gc, Source, Destination},
            State = #gcstate { scheduled = undefined }) ->
    {noreply, attempt_gc(State #gcstate { scheduled = {Source, Destination} }),
     hibernate};

handle_cast({no_readers, File},
            State = #gcstate { scheduled = {Source, Destination} })
  when File =:= Source orelse File =:= Destination ->
    {noreply, attempt_gc(State), hibernate};

handle_cast({no_readers, _File}, State) ->
    {noreply, State, hibernate};

handle_cast({set_maximum_since_use, Age}, State) ->
    ok = file_handle_cache:set_maximum_since_use(Age),
    {noreply, State, hibernate}.

handle_info(Info, State) ->
    {stop, {unhandled_info, Info}, State}.

terminate(_Reason, State) ->
    State.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

attempt_gc(State = #gcstate { dir              = Dir,
                              index_state      = IndexState,
                              index_module     = Index,
                              parent           = Parent,
                              file_summary_ets = FileSummaryEts,
                              scheduled        = {Source, Destination} }) ->
    case rabbit_msg_store:gc(Source, Destination,
                             {FileSummaryEts, Dir, Index, IndexState}) of
        concurrent_readers -> State;
        Reclaimed          -> ok = rabbit_msg_store:gc_done(
                                     Parent, Reclaimed, Source, Destination),
                              State #gcstate { scheduled = undefined }
    end.
