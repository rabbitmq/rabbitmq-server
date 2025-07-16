%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2025 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
%%

-module(rabbit_disk_usage).

%% Functions for calculating disk usage of a given directory.

-include_lib("kernel/include/file.hrl").

-export([scan/1]).

%% @doc Calculates the disk usage in bytes of the given directory.
%%
%% On especially large directories this can be an expensive operation since
%% each sub-directory is scanned recursively and each file's metadata must be
%% read.
-spec scan(Dir) -> {ok, Size} | {error, Error} when
    Dir :: filename:filename_all(),
    Size :: non_neg_integer(),
    Error :: not_directory | file:posix() | badarg.
scan(Dir) ->
    case file:read_file_info(Dir) of
        {ok, #file_info{type = directory, size = S}} ->
            {ok, Gatherer} = gatherer:start_link(),
            scan_directory(Dir, Gatherer),
            Size = sum(Gatherer, S),
            gatherer:stop(Gatherer),
            {ok, Size};
        {ok, #file_info{}} ->
            {error, not_directory};
        {error, _} = Err ->
            Err
    end.

scan_directory(Dir, Gatherer) ->
    gatherer:fork(Gatherer),
    worker_pool:submit_async(fun() -> scan_directory0(Dir, Gatherer) end).

scan_directory0(Dir, Gatherer) ->
    link(Gatherer),
    Size = case file:list_dir_all(Dir) of
               {ok, Entries} ->
                   lists:foldl(
                     fun(Entry, Acc) ->
                             Path = filename:join(Dir, Entry),
                             case file:read_file_info(Path) of
                                 {ok, #file_info{type = directory,
                                                 size = S}} ->
                                     scan_directory(Path, Gatherer),
                                     Acc + S;
                                 {ok, #file_info{size = S}} ->
                                     Acc + S;
                                 _ ->
                                     Acc
                             end
                     end, 0, Entries);
               _ ->
                   0
           end,
    gatherer:in(Gatherer, Size),
    gatherer:finish(Gatherer),
    unlink(Gatherer),
    ok.

-spec sum(pid(), non_neg_integer()) -> non_neg_integer().
sum(Gatherer, Size) ->
    case gatherer:out(Gatherer) of
        empty ->
            Size;
        {value, S} ->
            sum(Gatherer, Size + S)
    end.
