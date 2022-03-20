%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2022 VMware, Inc. or its affiliates.  All rights reserved.
%%

%% We use a gen_server simply so that during the terminate/2 call
%% (i.e., during shutdown), we can sync/flush the dets table to disk.

-module(rabbit_recovery_terms).

-behaviour(gen_server).

-export([start/1, stop/1, store/3, read/2, clear/1]).

-export([start_link/1]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-export([upgrade_recovery_terms/0, persistent_bytes/0]).
-export([open_global_table/0, close_global_table/0,
         read_global/1, delete_global_table/0]).
-export([open_table/1, close_table/1]).

-rabbit_upgrade({upgrade_recovery_terms, local, []}).
-rabbit_upgrade({persistent_bytes, local, [upgrade_recovery_terms]}).

-include_lib("rabbit_common/include/rabbit.hrl").

%%----------------------------------------------------------------------------

-spec start(rabbit_types:vhost()) -> rabbit_types:ok_or_error(term()).

start(VHost) ->
    case rabbit_vhost_sup_sup:get_vhost_sup(VHost) of
      {ok, VHostSup} ->
            {ok, _} = supervisor2:start_child(
                        VHostSup,
                        {?MODULE,
                         {?MODULE, start_link, [VHost]},
                         transient, ?WORKER_WAIT, worker,
                         [?MODULE]});
        %% we can get here if a vhost is added and removed concurrently
        %% e.g. some integration tests do it
        {error, {no_such_vhost, VHost}} ->
            rabbit_log:error("Failed to start a recovery terms manager for vhost ~s: vhost no longer exists!",
                             [VHost])
    end,
    ok.

-spec stop(rabbit_types:vhost()) -> rabbit_types:ok_or_error(term()).

stop(VHost) ->
    case rabbit_vhost_sup_sup:get_vhost_sup(VHost) of
        {ok, VHostSup} ->
            case supervisor:terminate_child(VHostSup, ?MODULE) of
                ok -> supervisor:delete_child(VHostSup, ?MODULE);
                E  -> E
            end;
        %% see start/1
        {error, {no_such_vhost, VHost}} ->
            rabbit_log:error("Failed to stop a recovery terms manager for vhost ~s: vhost no longer exists!",
                             [VHost]),

            ok
    end.

-spec store(rabbit_types:vhost(), file:filename(), term()) -> rabbit_types:ok_or_error(term()).

store(VHost, DirBaseName, Terms) ->
    dets:insert(VHost, {DirBaseName, Terms}).

-spec read(rabbit_types:vhost(), file:filename()) -> rabbit_types:ok_or_error2(term(), not_found).

read(VHost, DirBaseName) ->
    case dets:lookup(VHost, DirBaseName) of
        [{_, Terms}] -> {ok, Terms};
        _            -> {error, not_found}
    end.

-spec clear(rabbit_types:vhost()) -> 'ok'.

clear(VHost) ->
    try
        dets:delete_all_objects(VHost)
    %% see start/1
    catch _:badarg ->
            rabbit_log:error("Failed to clear recovery terms for vhost ~s: table no longer exists!",
                             [VHost]),
            ok
    end,
    flush(VHost).

start_link(VHost) ->
    gen_server:start_link(?MODULE, [VHost], []).

%%----------------------------------------------------------------------------

upgrade_recovery_terms() ->
    open_global_table(),
    try
        QueuesDir = filename:join(rabbit_mnesia:dir(), "queues"),
        Dirs = case rabbit_file:list_dir(QueuesDir) of
                   {ok, Entries} -> Entries;
                   {error, _}    -> []
               end,
        [begin
             File = filename:join([QueuesDir, Dir, "clean.dot"]),
             case rabbit_file:read_term_file(File) of
                 {ok, Terms} -> ok  = store_global_table(Dir, Terms);
                 {error, _}  -> ok
             end,
             file:delete(File)
         end || Dir <- Dirs],
        ok
    after
        close_global_table()
    end.

persistent_bytes()      -> dets_upgrade(fun persistent_bytes/1).
persistent_bytes(Props) -> Props ++ [{persistent_bytes, 0}].

dets_upgrade(Fun)->
    open_global_table(),
    try
        ok = dets:foldl(fun ({DirBaseName, Terms}, Acc) ->
                                store_global_table(DirBaseName, Fun(Terms)),
                                Acc
                        end, ok, ?MODULE),
        ok
    after
        close_global_table()
    end.

open_global_table() ->
    File = filename:join(rabbit_mnesia:dir(), "recovery.dets"),
    {ok, _} = dets:open_file(?MODULE, [{file,      File},
                                       {ram_file,  true},
                                       {auto_save, infinity}]),
    ok.

close_global_table() ->
    try
        dets:sync(?MODULE),
        dets:close(?MODULE)
    %% see clear/1
    catch _:badarg ->
            rabbit_log:error("Failed to clear global recovery terms: table no longer exists!",
                             []),
            ok
    end.

store_global_table(DirBaseName, Terms) ->
    dets:insert(?MODULE, {DirBaseName, Terms}).

read_global(DirBaseName) ->
    case dets:lookup(?MODULE, DirBaseName) of
        [{_, Terms}] -> {ok, Terms};
        _            -> {error, not_found}
    end.

delete_global_table() ->
    file:delete(filename:join(rabbit_mnesia:dir(), "recovery.dets")).

%%----------------------------------------------------------------------------

init([VHost]) ->
    process_flag(trap_exit, true),
    open_table(VHost),
    {ok, VHost}.

handle_call(Msg, _, State) -> {stop, {unexpected_call, Msg}, State}.

handle_cast(Msg, State) -> {stop, {unexpected_cast, Msg}, State}.

handle_info(_Info, State) -> {noreply, State}.

terminate(_Reason, VHost) ->
    close_table(VHost).

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%----------------------------------------------------------------------------

-spec open_table(vhost:name()) -> rabbit_types:ok_or_error(any()).

open_table(VHost) ->
    open_table(VHost, 10).

-spec open_table(vhost:name(), non_neg_integer()) -> rabbit_types:ok_or_error(any()).

open_table(VHost, RetriesLeft) ->
    VHostDir = rabbit_vhost:msg_store_dir_path(VHost),
    File = filename:join(VHostDir, "recovery.dets"),
    Opts = [{file,      File},
            {ram_file,  true},
            {auto_save, infinity}],
    case dets:open_file(VHost, Opts) of
        {ok, _}        -> ok;
        {error, Error} ->
          case RetriesLeft of
                0 ->
                    {error, Error};
                N when is_integer(N) ->
                    _ = file:delete(File),
                    %% Wait before retrying
                    DelayInMs = 1000,
                    rabbit_log:warning("Failed to open a recovery terms DETS file at ~p. Will delete it and retry in ~p ms (~p retries left)",
                                       [File, DelayInMs, RetriesLeft]),
                    timer:sleep(DelayInMs),
                    open_table(VHost, RetriesLeft - 1)
          end
    end.

-spec flush(vhost:name()) -> rabbit_types:ok_or_error(any()).

flush(VHost) ->
    try
        dets:sync(VHost)
    %% see clear/1
    catch _:badarg ->
            rabbit_log:error("Failed to sync recovery terms table for vhost ~s: the table no longer exists!",
                             [VHost]),
            ok
    end.

-spec close_table(vhost:name()) -> rabbit_types:ok_or_error(any()).

close_table(VHost) ->
    try
        ok = flush(VHost),
        ok = dets:close(VHost)
    %% see clear/1
    catch _:badarg ->
            rabbit_log:error("Failed to close recovery terms table for vhost ~s: the table no longer exists!",
                             [VHost]),
            ok
    end.
