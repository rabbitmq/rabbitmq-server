%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2021 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(rabbit_definitions_import_local_filesystem).
-include_lib("rabbit_common/include/rabbit.hrl").

-export([
    is_enabled/0,
    %% definition source options
    load/1,
    %% classic arguments specific to this source
    load/2,
    load_with_hashing/3,
    location/0
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

load(Proplist) when is_list(Proplist) ->
    case pget(local_path, Proplist, undefined) of
        undefined -> {error, "local definition file path is not configured: local_path is not set"};
        Path      ->
            rabbit_log:debug("Asked to import definitions from a local file or directory at '~s'", [Path]),
            case file:read_file_info(Path, [raw]) of
                {ok, FileInfo} ->
                    %% same check is used by Cuttlefish validation, this is to be extra defensive
                    IsReadable = (element(4, FileInfo) == read) or (element(4, FileInfo) == read_write),
                    case IsReadable of
                        true ->
                            load_from_single_file(Path);
                        false ->
                            Msg = rabbit_misc:format("local definition file '~s' does not exist or cannot be read by the node", [Path]),
                            {error, Msg}
                    end;
                _ ->
                    Msg = rabbit_misc:format("local definition file '~s' does not exist or cannot be read by the node", [Path]),
                    {error, {could_not_read_defs, Msg}}
            end
    end.

load(IsDir, Path) ->
    load_from_local_path(IsDir, Path).

load_with_hashing(Defs, undefined = _Hash, _Algo) when is_list(Defs) ->
    load(Defs);
load_with_hashing(Defs, PreviousHash, Algo) ->
    case rabbit_definitions_hashing:hash(Algo, Defs) of
        PreviousHash -> ok;
        _            -> load(Defs)
    end.

location() ->
    case location_from_classic_option() of
        undefined -> location_from_modern_option();
        Value     -> Value
    end.

load_from_local_path(true, Dir) ->
    rabbit_log:info("Applying definitions from directory ~s", [Dir]),
    load_from_files(file:list_dir(Dir), Dir);
load_from_local_path(false, File) ->
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


load_from_files({ok, Filenames0}, Dir) ->
    Filenames1 = lists:sort(Filenames0),
    Filenames2 = [filename:join(Dir, F) || F <- Filenames1],
    load_from_multiple_files(Filenames2);
load_from_files({error, E}, Dir) ->
    rabbit_log:error("Could not read definitions from directory ~s, Error: ~p", [Dir, E]),
    {error, {could_not_read_defs, E}}.

load_from_multiple_files([]) ->
    ok;
load_from_multiple_files([File|Rest]) ->
    case load_from_single_file(File) of
        ok         -> load_from_multiple_files(Rest);
        {error, E} -> {error, {failed_to_import_definitions, File, E}}
    end.

load_from_single_file(Path) ->
    rabbit_log:debug("Will try to load definitions from a local file or directory at '~s'", [Path]),
    case rabbit_misc:raw_read_file(Path) of
        {ok, Body} ->
            rabbit_log:info("Applying definitions from file at '~s'", [Path]),
            import_raw(Body);
        {error, E} ->
            rabbit_log:error("Could not read definitions from file at '~s', error: ~p", [Path, E]),
            {error, {could_not_read_defs, {Path, E}}}
    end.
