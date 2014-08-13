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
%% Copyright (c) 2007-2014 GoPivotal, Inc.  All rights reserved.
%%

%% We use a gen_server simply so that during the terminate/2 call
%% (i.e., during shutdown), we can sync/flush the dets table to disk.

-module(rabbit_recovery_terms).

-behaviour(gen_server).

-export([start/0, stop/0, store/2, read/1, clear/0]).

-export([start_link/0]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-export([upgrade_recovery_terms/0, persistent_bytes/0]).

-rabbit_upgrade({upgrade_recovery_terms, local, []}).
-rabbit_upgrade({persistent_bytes, local, [upgrade_recovery_terms]}).

%%----------------------------------------------------------------------------

-ifdef(use_specs).

-spec(start() -> rabbit_types:ok_or_error(term())).
-spec(stop() -> rabbit_types:ok_or_error(term())).
-spec(store(file:filename(), term()) -> rabbit_types:ok_or_error(term())).
-spec(read(file:filename()) -> rabbit_types:ok_or_error2(term(), not_found)).
-spec(clear() -> 'ok').

-endif. % use_specs

%%----------------------------------------------------------------------------

-define(SERVER, ?MODULE).

start() -> rabbit_sup:start_child(?MODULE).

stop() -> rabbit_sup:stop_child(?MODULE).

store(DirBaseName, Terms) -> dets:insert(?MODULE, {DirBaseName, Terms}).

read(DirBaseName) ->
    case dets:lookup(?MODULE, DirBaseName) of
        [{_, Terms}] -> {ok, Terms};
        _            -> {error, not_found}
    end.

clear() ->
    ok = dets:delete_all_objects(?MODULE),
    flush().

start_link() -> gen_server:start_link(?MODULE, [], []).

%%----------------------------------------------------------------------------

upgrade_recovery_terms() ->
    open_table(),
    try
        QueuesDir = filename:join(rabbit_mnesia:dir(), "queues"),
        Dirs = case rabbit_file:list_dir(QueuesDir) of
                   {ok, Entries} -> Entries;
                   {error, _}    -> []
               end,
        [begin
             File = filename:join([QueuesDir, Dir, "clean.dot"]),
             case rabbit_file:read_term_file(File) of
                 {ok, Terms} -> ok  = store(Dir, Terms);
                 {error, _}  -> ok
             end,
             file:delete(File)
         end || Dir <- Dirs],
        ok
    after
        close_table()
    end.

persistent_bytes()      -> dets_upgrade(fun persistent_bytes/1).
persistent_bytes(Props) -> Props ++ [{persistent_bytes, 0}].

dets_upgrade(Fun)->
    open_table(),
    try
        ok = dets:foldl(fun ({DirBaseName, Terms}, Acc) ->
                                store(DirBaseName, Fun(Terms)),
                                Acc
                        end, ok, ?MODULE),
        ok
    after
        close_table()
    end.

%%----------------------------------------------------------------------------

init(_) ->
    process_flag(trap_exit, true),
    open_table(),
    {ok, undefined}.

handle_call(Msg, _, State) -> {stop, {unexpected_call, Msg}, State}.

handle_cast(Msg, State) -> {stop, {unexpected_cast, Msg}, State}.

handle_info(_Info, State) -> {noreply, State}.

terminate(_Reason, _State) ->
    close_table().

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%----------------------------------------------------------------------------

open_table() ->
    File = filename:join(rabbit_mnesia:dir(), "recovery.dets"),
    {ok, _} = dets:open_file(?MODULE, [{file,      File},
                                       {ram_file,  true},
                                       {auto_save, infinity}]).

flush() -> ok = dets:sync(?MODULE).

close_table() ->
    ok = flush(),
    ok = dets:close(?MODULE).
