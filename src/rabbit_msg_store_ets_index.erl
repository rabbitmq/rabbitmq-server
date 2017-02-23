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
%% The Original Code is RabbitMQ.
%%
%% The Initial Developer of the Original Code is GoPivotal, Inc.
%% Copyright (c) 2007-2016 Pivotal Software, Inc.  All rights reserved.
%%

-module(rabbit_msg_store_ets_index).

-include("rabbit_msg_store.hrl").

-behaviour(rabbit_msg_store_index).

-export([new/1, recover/1,
         lookup/2, insert/2, update/2, update_fields/3, delete/2,
         delete_object/2, cleanup_undefined_file/1, terminate/1]).

-define(MSG_LOC_NAME, rabbit_msg_store_ets_index).
-define(FILENAME, "msg_store_index.ets").

-record(state, { table, dir }).

new(Dir) ->
    file:delete(filename:join(Dir, ?FILENAME)),
    Tid = ets:new(?MSG_LOC_NAME, [set, public, {keypos, #msg_location.msg_id}]),
    #state { table = Tid, dir = Dir }.

recover(Dir) ->
    Path = filename:join(Dir, ?FILENAME),
    case ets:file2tab(Path) of
        {ok, Tid}  -> file:delete(Path),
                      {ok, #state { table = Tid, dir = Dir }};
        Error      -> Error
    end.

lookup(Key, State) ->
    case ets:lookup(State #state.table, Key) of
        []      -> not_found;
        [Entry] -> Entry
    end.

insert(Obj, State) ->
    true = ets:insert_new(State #state.table, Obj),
    ok.

update(Obj, State) ->
    true = ets:insert(State #state.table, Obj),
    ok.

update_fields(Key, Updates, State) ->
    true = ets:update_element(State #state.table, Key, Updates),
    ok.

delete(Key, State) ->
    true = ets:delete(State #state.table, Key),
    ok.

delete_object(Obj, State) ->
    true = ets:delete_object(State #state.table, Obj),
    ok.

cleanup_undefined_file(State) ->
    MatchHead = #msg_location { file = undefined, _ = '_' },
    ets:select_delete(State #state.table, [{MatchHead, [], [true]}]),
    ok.

terminate(#state { table = MsgLocations, dir = Dir }) ->
    case ets:tab2file(MsgLocations, filename:join(Dir, ?FILENAME),
                      [{extended_info, [object_count]}]) of
        ok           -> ok;
        {error, Err} ->
            rabbit_log:error("Unable to save message store index"
                             " for directory ~p.~nError: ~p~n",
                             [Dir, Err])
    end,
    ets:delete(MsgLocations).
