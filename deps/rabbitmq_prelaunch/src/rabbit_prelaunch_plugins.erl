%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2026 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.

-module(rabbit_prelaunch_plugins).

-include_lib("rabbit_common/include/rabbit.hrl").
-include_lib("kernel/include/file.hrl").
-include_lib("kernel/include/logger.hrl").
-include_lib("stdlib/include/zip.hrl").

-export([with_unpacked_plugins/2]).

with_unpacked_plugins(#{os_type := OSType, plugins_path := PluginsPath} = Context, Fun) ->
    Separator = case OSType of
                    {win32, _} -> ";";
                    _ -> ":"
                end,
    {Plugins, Problems} = rabbit_plugin_discovery:list(string:lexemes(PluginsPath, Separator)),
    lists:foreach(
      fun({Path, Reason}) ->
              ?LOG_WARNING("Could not use plugin ~ts: ~tp", [Path, Reason])
      end, Problems),
    Archives = [Plugin || Plugin = #plugin{type = ez} <- Plugins],
    Stage = case Archives of
                [] -> undefined;
                _ -> make_stage(maps:get(plugins_expand_dir, Context))
            end,
    OriginalPath = code:get_path(),
    Loaded = application:loaded_applications(),
    Names = [Name || #plugin{name = Name} <- Plugins],
    try
        Sources = [unpack(Plugin, Stage) || Plugin <- Archives],
        lists:foreach(
          fun(#plugin{type = dir, location = Dir}) ->
                  true = code:add_patha(filename:join(Dir, "ebin"));
             (#plugin{type = ez}) -> ok
          end, Plugins),
        Fun(Context#{schema_apps => Names, unpacked_archives => Sources})
    after
        lists:foreach(
          fun application:unload/1,
          [App || {App, _, _} <- application:loaded_applications() -- Loaded,
                  lists:member(App, Names)]),
        true = code:set_path(OriginalPath),
        case Stage of
            undefined -> ok;
            _ -> ok = file:del_dir_r(Stage)
        end
    end.

make_stage(ExpandDir) ->
    Stage = filename:join(ExpandDir, ".schemas-" ++
                          integer_to_list(erlang:unique_integer([positive, monotonic]))),
    ok = filelib:ensure_dir(Stage),
    case file:make_dir(Stage) of
        ok ->
            ok = file:change_mode(Stage, 8#700),
            Stage;
        {error, eexist} ->
            make_stage(ExpandDir);
        {error, Reason} ->
            throw({error, {cannot_create_schema_directory, Stage, Reason}})
    end.

unpack(#plugin{name = Name, location = Archive}, Stage) ->
    {ok, Files} = zip:list_dir(Archive),
    [Root | _] = [Root || #zip_file{name = Path} <- Files,
                         [Root, "ebin", File] <- [filename:split(Path)],
                         filename:extension(File) =:= ".app"],
    lists:foreach(
      fun(#zip_file{name = Path, info = #file_info{type = Type}}) ->
              Parts = string:lexemes(Path, "/"),
              Valid = filename:pathtype(Path) =:= relative
                  andalso (Type =:= regular orelse Type =:= directory)
                  andalso not lists:any(fun(C) -> C =:= $\\ orelse C =:= $: end, Path)
                  andalso not lists:member("..", Parts)
                  andalso not lists:member(".", Parts)
                  andalso Parts =/= []
                  andalso hd(Parts) =:= Root,
              case Valid of
                  true -> ok;
                  false -> throw({error, {invalid_plugin_archive_path, Archive, Path}})
              end;
         (#zip_comment{}) -> ok
      end, Files),
    Dest = filename:join(Stage, binary_to_list(binary:encode_hex(atom_to_binary(Name)))),
    ok = file:make_dir(Dest),
    case zip:extract(Archive, [{cwd, Dest}]) of
        {ok, _} ->
            true = code:add_patha(filename:join([Dest, Root, "ebin"])),
            {Dest, Archive};
        {error, Reason} ->
            throw({error, {failed_to_unzip_plugin, Name, Archive, Reason}})
    end.
