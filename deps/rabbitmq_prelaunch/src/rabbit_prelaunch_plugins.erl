%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2026 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.

-module(rabbit_prelaunch_plugins).

-include_lib("kernel/include/file.hrl").
-include_lib("kernel/include/logger.hrl").
-include_lib("stdlib/include/zip.hrl").

-export([with_unpacked_plugins/2]).

with_unpacked_plugins(#{os_type := OSType, plugins_path := PluginsPath} = Context, Fun) ->
    Separator = case OSType of
                    {win32, _} -> ";";
                    _ -> ":"
                end,
    Archives = [filename:join(Dir, File)
                || Dir <- string:lexemes(PluginsPath, Separator),
                   File <- filelib:wildcard("*.ez", Dir)],
    Plugins = lists:foldl(fun select_archive/2, #{}, Archives),
    case lists:sort(maps:values(Plugins)) of
        [] ->
            Fun(Context);
        Selected ->
            Stage = make_stage(maps:get(plugins_expand_dir, Context)),
            OriginalPath = code:get_path(),
            Loaded = application:loaded_applications(),
            try
                Sources = [unpack(Plugin, Stage) || Plugin <- Selected],
                Dirs = [Dir || {Dir, _} <- Sources],
                ExpandedPath = string:join([PluginsPath | Dirs], Separator),
                Fun(Context#{plugins_path => ExpandedPath, unpacked_archives => Sources})
            after
                lists:foreach(
                  fun application:unload/1,
                  [App || {App, _, _} <- application:loaded_applications() -- Loaded,
                          maps:is_key(App, Plugins)]),
                lists:foreach(
                  fun code:del_path/1,
                  [Path || Path <- code:get_path() -- OriginalPath,
                           lists:prefix(filename:split(Stage), filename:split(Path))]),
                ok = file:del_dir_r(Stage)
            end
    end.

select_archive(Archive, Plugins) ->
    case archive_info(Archive) of
        {ok, {Name, Version, _, _} = Info} ->
            case code:lib_dir(Name) of
                Dir when is_list(Dir) ->
                    case filelib:is_dir(Dir) of
                        true -> Plugins;
                        false -> select_version(Name, Version, Info, Plugins)
                    end;
                _ ->
                    select_version(Name, Version, Info, Plugins)
            end;
        {error, Reason} ->
            ?LOG_WARNING("Could not read plugin archive ~ts: ~tp", [Archive, Reason]),
            Plugins
    end.

select_version(Name, Version, Info, Plugins) ->
    case maps:find(Name, Plugins) of
        {ok, {_, ExistingVersion, _, _}} ->
            case rabbit_semver:gt(Version, ExistingVersion) of
                true -> Plugins#{Name => Info};
                false -> Plugins
            end;
        error ->
            Plugins#{Name => Info}
    end.

archive_info(Archive) ->
    try
        {ok, Files} = zip:list_dir(Archive),
        [{Root, AppPath} | _] =
            [{Root, Path} || #zip_file{name = Path} <- Files,
                            [Root, "ebin", File] <- [filename:split(Path)],
                            filename:extension(File) =:= ".app"],
        {ok, [{AppPath, Binary}]} = zip:extract(Archive, [memory, {file_list, [AppPath]}]),
        {ok, Tokens, _} = erl_scan:string(binary_to_list(Binary)),
        {ok, {application, Name, Props}} = erl_parse:parse_term(Tokens),
        true = is_atom(Name) andalso is_list(Props),
        {ok, {Name, proplists:get_value(vsn, Props, "0"), Archive, Root}}
    catch
        error:Reason -> {error, Reason}
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

unpack({Name, _, Archive, Root}, Stage) ->
    {ok, Files} = zip:list_dir(Archive),
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
