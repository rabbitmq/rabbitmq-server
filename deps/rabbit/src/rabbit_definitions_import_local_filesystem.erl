%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2023 VMware, Inc. or its affiliates.  All rights reserved.
%%

%% This module is responsible for loading definition from a local filesystem
%% (a JSON file or a conf.d-style directory of files).
%%
%% See also
%%
%%  * rabbit.schema (core Cuttlefish schema mapping file)
%%  * rabbit_definitions
%%  * rabbit_definitions_import_http
%%  * rabbit_definitions_hashing
-module(rabbit_definitions_import_local_filesystem).
-include_lib("rabbit_common/include/rabbit.hrl").

-export([
    is_enabled/0,
    %% definition source options
    load/1,
    %% classic arguments specific to this source
    load/2,
    load_with_hashing/3,
    load_with_hashing/4,
    location/0,

    %% tests and REPL
    compiled_definitions_from_local_path/2
]).



-import(rabbit_misc, [pget/2, pget/3]).
-import(rabbit_data_coercion, [to_binary/1]).
-import(rabbit_definitions, [import_raw/1]).

%%
%% API
%%

-spec is_enabled() -> boolean().
is_enabled() ->
    is_enabled_via_classic_option() or is_enabled_via_modern_option().

-spec load(Proplist :: list() | map()) -> ok | {error, term()}.
load(Proplist) when is_list(Proplist) ->
    case pget(local_path, Proplist, undefined) of
        undefined -> {error, "local definition file path is not configured: local_path is not set"};
        Path      ->
            rabbit_log:debug("Asked to import definitions from a local file or directory at '~ts'", [Path]),
            IsDir = filelib:is_dir(Path),
            case IsDir of
                true ->
                    load_from_local_path(true, Path);
                false ->
                    load_from_single_file(Path)
            end
    end;
load(Map) when is_map(Map) ->
    load(maps:to_list(Map)).

-spec load(IsDir :: boolean(), Path :: file:name_all()) -> ok | {error, term()}.
load(IsDir, Path) when is_boolean(IsDir) ->
    load_from_local_path(IsDir, Path).

-spec load_with_hashing(Proplist :: list() | map(), PreviousHash :: binary() | 'undefined', Algo :: crypto:sha1() | crypto:sha2()) -> binary() | 'undefined'.
load_with_hashing(Proplist, PreviousHash, Algo) ->
    case pget(local_path, Proplist, undefined) of
        undefined -> {error, "local definition file path is not configured: local_path is not set"};
        Path      ->
            IsDir = filelib:is_dir(Path),
            load_with_hashing(IsDir, Path, PreviousHash, Algo)
    end.

-spec load_with_hashing(IsDir :: boolean(), Path :: file:name_all(), PreviousHash :: binary() | 'undefined', Algo :: crypto:sha1() | crypto:sha2()) -> binary() | 'undefined'.
load_with_hashing(IsDir, Path, PreviousHash, Algo) when is_boolean(IsDir) ->
    rabbit_log:debug("Loading definitions with content hashing enabled, path: ~ts, is directory?: ~tp, previous hash value: ~ts",
                     [Path, IsDir, rabbit_misc:hexify(PreviousHash)]),
    case compiled_definitions_from_local_path(IsDir, Path) of
        %% the directory is empty or no files could be read
        [] ->
            rabbit_definitions_hashing:hash(Algo, undefined);
        Defs ->
            case rabbit_definitions_hashing:hash(Algo, Defs) of
                PreviousHash -> PreviousHash;
                Other        ->
                    rabbit_log:debug("New hash: ~ts", [rabbit_misc:hexify(Other)]),
                    _ = load_from_local_path(IsDir, Path),
                    Other
            end
    end.

location() ->
    case location_from_classic_option() of
        undefined -> location_from_modern_option();
        Value     -> Value
    end.

-spec load_from_local_path(IsDir :: boolean(), Path :: file:name_all()) -> ok | {error, term()}.
load_from_local_path(true, Dir) ->
    rabbit_log:info("Applying definitions from directory ~ts", [Dir]),
    load_from_files(file:list_dir(Dir), Dir);
load_from_local_path(false, File) ->
    rabbit_log:info("Applying definitions from regular file at ~ts", [File]),
    load_from_single_file(File).

%%
%% Implementation
%%

-spec is_enabled_via_classic_option() -> boolean().
is_enabled_via_classic_option() ->
    %% Classic way of defining a local filesystem definition source
    case application:get_env(rabbit, load_definitions) of
        undefined   -> false;
        {ok, none}  -> false;
        {ok, _Path} -> true
    end.

-spec is_enabled_via_modern_option() -> boolean().
is_enabled_via_modern_option() ->
    %% Modern way of defining a local filesystem definition source
    case application:get_env(rabbit, definitions) of
        undefined   -> false;
        {ok, none}  -> false;
        {ok, []}    -> false;
        {ok, Proplist} ->
            case pget(import_backend, Proplist, undefined) of
                undefined -> false;
                ?MODULE   -> true;
                _         -> false
            end
    end.

location_from_classic_option() ->
    case application:get_env(rabbit, load_definitions) of
        undefined  -> undefined;
        {ok, none} -> undefined;
        {ok, Path} -> Path
    end.

location_from_modern_option() ->
    case application:get_env(rabbit, definitions) of
        undefined  -> undefined;
        {ok, none} -> undefined;
        {ok, Proplist} ->
            pget(local_path, Proplist)
    end.

-spec compiled_definitions_from_local_path(IsDir :: boolean(), Dir :: file:name_all()) -> [binary()] | {error, any()}.
compiled_definitions_from_local_path(true = _IsDir, Dir) ->
    case file:list_dir(Dir) of
        {ok, Filenames0} ->
            Filenames1  = lists:sort(Filenames0),
            Filenames2  = [filename:join(Dir, F) || F <- Filenames1],
            ReadResults = [rabbit_misc:raw_read_file(F) || F <- Filenames2],
            Successes   = lists:filter(
                fun ({error, _}) -> false;
                    (_)          -> true
                end, ReadResults),
            [Body || {ok, Body} <- Successes];
        {error, E} ->
            rabbit_log:error("Could not list files in '~ts', error: ~tp", [Dir, E]),
            {error, {could_not_read_defs, {Dir, E}}}
    end;
compiled_definitions_from_local_path(false = _IsDir, Path) ->
    case read_file_contents(Path) of
        {error, _} -> [];
        Body       -> [Body]
    end.

-spec read_file_contents(Path :: file:name_all()) -> binary() | {error, any()}.
read_file_contents(Path) ->
    case rabbit_misc:raw_read_file(Path) of
        {ok, Body} ->
            Body;
        {error, E} ->
            rabbit_log:error("Could not read definitions from file at '~ts', error: ~tp", [Path, E]),
            {error, {could_not_read_defs, {Path, E}}}
    end.

load_from_files({ok, Filenames0}, Dir) ->
    Filenames1 = lists:sort(Filenames0),
    Filenames2 = [filename:join(Dir, F) || F <- Filenames1],
    load_from_multiple_files(Filenames2);
load_from_files({error, E}, Dir) ->
    rabbit_log:error("Could not read definitions from directory ~ts, Error: ~tp", [Dir, E]),
    {error, {could_not_read_defs, E}}.

load_from_multiple_files([]) ->
    ok;
load_from_multiple_files([File|Rest]) ->
    case load_from_single_file(File) of
        ok         -> load_from_multiple_files(Rest);
        {error, E} -> {error, {failed_to_import_definitions, File, E}}
    end.

load_from_single_file(Path) ->
    rabbit_log:debug("Will try to load definitions from a local file or directory at '~ts'", [Path]),

    case file:read_file_info(Path, [raw]) of
        {ok, FileInfo} ->
            %% same check is used by Cuttlefish validation, this is to be extra defensive
            IsReadable = (element(4, FileInfo) == read) or (element(4, FileInfo) == read_write),
            case IsReadable of
                true ->
                    case rabbit_misc:raw_read_file(Path) of
                        {ok, Body} ->
                            rabbit_log:info("Applying definitions from file at '~ts'", [Path]),
                            import_raw(Body);
                        {error, E} ->
                            rabbit_log:error("Could not read definitions from file at '~ts', error: ~tp", [Path, E]),
                            {error, {could_not_read_defs, {Path, E}}}
                    end;
                false ->
                    Msg = rabbit_misc:format("local definition file '~ts' does not exist or cannot be read by the node", [Path]),
                    {error, Msg}
            end;
        _ ->
            Msg = rabbit_misc:format("local definition file '~ts' does not exist or cannot be read by the node", [Path]),
            {error, {could_not_read_defs, Msg}}
    end.
