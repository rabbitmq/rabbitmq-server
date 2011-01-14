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

-export([start_link/1, combine/3, delete/2, no_readers/2, stop/1]).

-export([set_maximum_since_use/2]).

-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3, prioritise_cast/2]).

-record(state,
        { pending_no_readers,
          on_action,
          msg_store_state
        }).

-include("rabbit.hrl").

%%----------------------------------------------------------------------------

-ifdef(use_specs).

-spec(start_link/1 :: (rabbit_msg_store:gc_state()) ->
                           rabbit_types:ok_pid_or_error()).
-spec(combine/3 :: (pid(), rabbit_msg_store:file_num(),
                    rabbit_msg_store:file_num()) -> 'ok').
-spec(delete/2 :: (pid(), rabbit_msg_store:file_num()) -> 'ok').
-spec(no_readers/2 :: (pid(), rabbit_msg_store:file_num()) -> 'ok').
-spec(stop/1 :: (pid()) -> 'ok').
-spec(set_maximum_since_use/2 :: (pid(), non_neg_integer()) -> 'ok').

-endif.

%%----------------------------------------------------------------------------

start_link(MsgStoreState) ->
    gen_server2:start_link(?MODULE, [MsgStoreState],
                           [{timeout, infinity}]).

combine(Server, Source, Destination) ->
    gen_server2:cast(Server, {combine, Source, Destination}).

delete(Server, File) ->
    gen_server2:cast(Server, {delete, File}).

no_readers(Server, File) ->
    gen_server2:cast(Server, {no_readers, File}).

stop(Server) ->
    gen_server2:call(Server, stop, infinity).

set_maximum_since_use(Pid, Age) ->
    gen_server2:cast(Pid, {set_maximum_since_use, Age}).

%%----------------------------------------------------------------------------

init([MsgStoreState]) ->
    ok = file_handle_cache:register_callback(?MODULE, set_maximum_since_use,
                                             [self()]),
    {ok, #state { pending_no_readers = dict:new(),
                  on_action          = [],
                  msg_store_state    = MsgStoreState }, hibernate,
     {backoff, ?HIBERNATE_AFTER_MIN, ?HIBERNATE_AFTER_MIN, ?DESIRED_HIBERNATE}}.

prioritise_cast({set_maximum_since_use, _Age}, _State) -> 8;
prioritise_cast(_Msg,                          _State) -> 0.

handle_call(stop, _From, State) ->
    {stop, normal, ok, State}.

handle_cast({combine, Source, Destination}, State) ->
    {noreply, attempt_action(combine, [Source, Destination], State), hibernate};

handle_cast({delete, File}, State) ->
    {noreply, attempt_action(delete, [File], State), hibernate};

handle_cast({no_readers, File},
            State = #state { pending_no_readers = Pending }) ->
    {noreply, case dict:find(File, Pending) of
                  error ->
                      State;
                  {ok, {Action, Files}} ->
                      Pending1 = dict:erase(File, Pending),
                      attempt_action(
                        Action, Files,
                        State #state { pending_no_readers = Pending1 })
              end, hibernate};

handle_cast({set_maximum_since_use, Age}, State) ->
    ok = file_handle_cache:set_maximum_since_use(Age),
    {noreply, State, hibernate}.

handle_info(Info, State) ->
    {stop, {unhandled_info, Info}, State}.

terminate(_Reason, State) ->
    State.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

attempt_action(Action, Files,
               State = #state { pending_no_readers = Pending,
                                on_action          = Thunks,
                                msg_store_state    = MsgStoreState }) ->
    Thunks1 = run_thunks(Thunks),
    case [File || File <- Files,
                  rabbit_msg_store:has_readers(File, MsgStoreState)] of
        []         -> Thunks2 = case do_action(Action, Files, MsgStoreState) of
                                    ok    -> Thunks1;
                                    Thunk -> [Thunk | Thunks1]
                                end,
                      State #state { on_action = Thunks2 };
        [File | _] -> Pending1 = dict:store(File, {Action, Files}, Pending),
                      State #state { pending_no_readers = Pending1,
                                     on_action          = Thunks1 }
    end.

do_action(combine, [Source, Destination], MsgStoreState) ->
    rabbit_msg_store:combine_files(Source, Destination, MsgStoreState);
do_action(delete, [File], MsgStoreState) ->
    rabbit_msg_store:delete_file(File, MsgStoreState).

run_thunks(Thunks) ->
    lists:foldl(fun (Thunk, Thunks1) ->
                        case Thunk() of
                            ok     -> Thunks1;
                            Thunk1 -> [Thunk1 | Thunks1]
                        end
                end, [], Thunks).
