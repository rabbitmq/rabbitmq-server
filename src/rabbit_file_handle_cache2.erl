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
%%   Portions created by LShift Ltd are Copyright (C) 2007-2009 LShift
%%   Ltd. Portions created by Cohesive Financial Technologies LLC are
%%   Copyright (C) 2007-2009 Cohesive Financial Technologies
%%   LLC. Portions created by Rabbit Technologies Ltd are Copyright
%%   (C) 2007-2009 Rabbit Technologies Ltd.
%%
%%   All Rights Reserved.
%%
%%   Contributor(s): ______________________________________.
%%

-module(rabbit_file_handle_cache2).

-behaviour(gen_server2).

-export([start_link/0, new_client/1, get_file_handle/3, release_file_handle/2,
         close_file_handle/3, with_file_handle_at/5]).

-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-define(SERVER, ?MODULE).
-define(ETS_HANDLES_NAME, rabbit_file_handle_cache_handles).
-define(ETS_AGE_NAME, rabbit_file_handle_cache_ages).
-define(MAX_FILE_HANDLES, 900). %% unlimit -a on debian default gives 1024
-define(ISSUE_PERIOD, 10000). %% 10 seconds
-define(HIBERNATE_AFTER_MIN, 1000).
-define(DESIRED_HIBERNATE, 10000).

-record(server_state,
        { request_queue,
          handles,
          ages,
          max_handles
        }).

-record(client_state,
        { callback,
          handles
        }).

-record(hdl,
        { key,
          handle,
          offset,
          timer_ref,
          released_at
        }).

%%----------------------------------------------------------------------------
%% Public API
%%----------------------------------------------------------------------------

start_link() ->
    gen_server2:start_link({local, ?SERVER}, ?MODULE, [?MAX_FILE_HANDLES], []).

new_client(Callback = {_M, _F, _A}) ->
    ok = gen_server2:call(?SERVER, new_client, infinity),
    #client_state { callback = Callback,
                    handles = dict:new() }.

get_file_handle(Path, Mode, CState = #client_state { handles = Handles }) ->
    case obtain_file_handle(Path, Mode, CState) of
        not_available -> {not_available, CState};
        {Mode1, Hdl, _Offset} ->
            Handles1 = dict:store({Path, Mode1}, {Hdl, unknown}, Handles),
            {Hdl, CState #client_state { handles = Handles1 }}
    end.

release_file_handle({release_handle, Key = {_From, Path, Mode}},
                    CState = #client_state { handles = Handles }) ->
    case dict:find({Path, Mode}, Handles) of
        error -> %% oh well, it must have already gone
            CState;
        {value, {_Hdl, Offset}} ->
            Handles1 = dict:erase({Path, Mode}, Handles),
            gen_server2:cast(?SERVER, {release_handle, Key, Offset}),
            CState #client_state { handles = Handles1 }
    end.

close_file_handle(Path, Mode, CState = #client_state { handles = Handles }) ->
    Mode1 = lists:usort(Mode),
    case dict:find({Path, Mode1}, Handles) of
        error -> %% oh well, it must have already gone
             CState;
        {value, _} ->
            gen_server2:cast(?SERVER, {close_handle, {self(), Path, Mode1}})
    end.

with_file_handle_at(Path, Mode, Offset, Fun, CState =
                    #client_state { handles = Handles }) ->
    case obtain_file_handle(Path, Mode, CState) of
        not_available -> {not_available, CState};
        {Mode1, Hdl, OldOffset} ->
            SeekRes =
                case Offset == OldOffset orelse not is_integer(Offset) of
                    true -> ok;
                    false -> case file:position(Hdl, Offset) of
                                 {ok, _} -> ok;
                                 KO -> KO
                             end
                end,
            case SeekRes of
                ok -> {NewOffset, Result} = Fun(Hdl),
                      {Result, CState #client_state {
                                 handles = dict:store({Path, Mode1},
                                                      {Hdl, NewOffset},
                                                      Handles) }};
                KO1 -> {KO1, CState}
            end
    end.

%%----------------------------------------------------------------------------
%% Client-side helpers
%%----------------------------------------------------------------------------

obtain_file_handle(Path, Mode, #client_state { handles = Handles,
                                               callback = Callback }) ->
    Mode1 = lists:usort(Mode),
    case dict:find(Mode1, Handles) of
        error ->
            case gen_server2:call(?SERVER,
                                  {get_handle, Path, Mode1, Callback}) of
                {Hdl, Offset} -> {Mode1, Hdl, Offset};
                exiting -> not_available
            end;
        {value, {Hdl, Offset}} ->
            {Mode1, Hdl, Offset}
    end.

%%----------------------------------------------------------------------------
%% gen_server callbacks
%%----------------------------------------------------------------------------

init([MaxFileHandles]) ->
    Handles = ets:new(?ETS_HANDLES_NAME,
                      [ordered_set, private, {keypos, #hdl.key}]),
    Ages = ets:new(?ETS_AGE_NAME, [ordered_set, private]),
    {ok, #server_state { request_queue = queue:new(),
                         handles = Handles,
                         ages = Ages,
                         max_handles = MaxFileHandles },
     {backoff, ?HIBERNATE_AFTER_MIN, ?HIBERNATE_AFTER_MIN, ?DESIRED_HIBERNATE}}.

handle_call(new_client, From, State) ->
    _MRef = erlang:monitor(process, From),
    {reply, ok, State, hibernate};
handle_call({get_handle, Path, Mode, Callback = {_M, _F, _A}}, From,
            State = #server_state { handles = Handles,
                                    ages = Ages,
                                    request_queue = Reqs }) ->
    Key = {From, Path, Mode},
    State1 =
        case ets:lookup(Handles, Key) of
            [Obj = #hdl { handle = Hdl, offset = Offset,
                          timer_ref = TRef, released_at = ReleasedAt }] ->
                gen_server2:reply(From, {Hdl, Offset}),
                ok = stop_timer(TRef),
                {ok, TRef1} = start_timer(Callback, Key),
                true = ets:insert(Handles,
                                  Obj #hdl { offset = unknown,
                                             timer_ref = TRef1,
                                             released_at = not_released }),
                true = ets:delete(Ages, ReleasedAt),
                State;
            [] ->
                process_request_queue(
                  State #server_state { request_queue =
                                        queue:in({Key, Callback}, Reqs) })
        end,
    {noreply, State1, hibernate}.

handle_cast({release_handle, Key = {_From, _Path, _Mode}, Offset},
            State = #server_state { handles = Handles,
                                    ages = Ages }) ->
    [Obj = #hdl { timer_ref = TRef, released_at = ReleasedAtOld }] =
        ets:lookup(Handles, Key),
    ok = stop_timer(TRef),
    ok = case ReleasedAtOld of
             not_released ->
                 ReleasedAt = now(),
                 true = ets:insert_new(Ages, {ReleasedAt, Key}),
                 true = ets:insert(Handles, Obj #hdl { released_at = ReleasedAt,
                                                       offset = Offset,
                                                       timer_ref = no_timer }),
                 ok;
             _ ->
                 ok
         end,
    State1 = process_request_queue(State),
    {noreply, State1, hibernate};
handle_cast({close_handle, Key = {_From, _Path, _Mode}},
            State = #server_state { handles = Handles,
                                    ages = Ages }) ->
    [Obj] = ets:lookup(Handles, Key),
    ok = close_handle(Obj, Handles, Ages),
    State1 = process_request_queue(State),
    {noreply, State1, hibernate}.

handle_info({'DOWN', _MRef, process, Pid, _Reason},
            State = #server_state { handles = Handles, ages = Ages,
                                    request_queue = Reqs }) ->
    Reqs1 = queue:filter(fun ({{From, _Path, _Mode}, _Callback}) ->
                                 From /= Pid
                         end, Reqs),
    lists:foreach(fun (Obj) ->
                          ok = close_handle(Obj, Handles, Ages)
                  end, ets:match_object(Handles, #hdl { key = {Pid, '_', '_'},
                                                        _ = '_' })),
    {noreply, State #server_state { request_queue = Reqs1 }}.

terminate(_Reason, State = #server_state { ages = Ages,
                                           request_queue = Reqs }) ->
    Size = ets:info(Ages, size),
    Size = free_upto(Size, State),
    lists:foreach(fun ({{From, _Path, _Mode}, _Callback}) ->
                          gen_server2:reply(From, exiting)
                  end, queue:to_list(Reqs)),
    State #server_state { request_queue = queue:new() }.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%----------------------------------------------------------------------------
%% Server-side Helpers
%%----------------------------------------------------------------------------

start_timer({M,F,A}, Key) ->
    timer:apply_after(?ISSUE_PERIOD, M, F, A ++ [{release_handle, Key}]).

stop_timer(no_timer) ->
    ok;
stop_timer(TRef) ->
    timer:cancel(TRef),
    ok.

close_handle(#hdl { key = Key, timer_ref = TRef, released_at = ReleasedAt,
                    handle = Hdl },
             Handles, Ages) ->
    ok = timer:stop(TRef),
    ok = file:sync(Hdl),
    ok = file:close(Hdl),
    true = ets:delete(Handles, Key),
    true = ets:delete(Ages, ReleasedAt),
    ok.

process_request_queue(State = #server_state { max_handles = MaxHandles,
                                              handles = Handles,
                                              request_queue = Reqs }) ->
    Tokens = MaxHandles - ets:info(Handles, size),
    Requests = queue:len(Reqs),
    OpenCount = case Tokens >= Requests of
                      true  -> Requests;
                      false -> Tokens + free_upto(Requests - Tokens, State)
                end,
    open_requested(OpenCount, State).

open_requested(0, State) ->
    State;
open_requested(N, State = #server_state { handles = Handles,
                                          request_queue = Reqs }) ->
    case queue:out(Reqs) of
        {empty, _Reqs} -> State;
        {{value, {Key = {From, Path, Mode}, Callback}}, Reqs1} ->
            {ok, Hdl} = file:open(Path, Mode),
            gen_server2:reply(From, {Hdl, 0}),
            {ok, TRef} = start_timer(Callback, Key),
            true = ets:insert_new(Handles, #hdl { key = Key,
                                                  handle = Hdl,
                                                  offset = unknown,
                                                  timer_ref = TRef,
                                                  released_at = not_released }),
            open_requested(N - 1, State #server_state { request_queue = Reqs1 })
    end.

free_upto(N, State) ->
    free_upto(N, 0, State).

free_upto(0, Count, _State) ->
    Count;
free_upto(N, Count, State = #server_state { handles = Handles,
                                            ages = Ages }) ->
    case ets:first(Ages) of
        '$end_of_table' ->
            Count;
        {ReleasedAt, Key} ->
            [#hdl { handle = Hdl, timer_ref = no_timer,
                    released_at = ReleasedAt }] = ets:lookup(Handles, Key),
            ok = file:sync(Hdl),
            ok = file:close(Hdl),
            true = ets:delete(Ages, ReleasedAt),
            true = ets:delete(Handles, Key),
            free_upto(N - 1, Count + 1, State)
    end.
