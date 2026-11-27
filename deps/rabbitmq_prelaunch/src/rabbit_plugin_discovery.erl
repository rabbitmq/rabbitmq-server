%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2026 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
%%

-module(rabbit_plugin_discovery).

-include_lib("rabbit_common/include/rabbit.hrl").

-export([list/1, read_app_file/1]).

-spec list([file:name()]) -> {[#plugin{}], [{file:name(), term()}]}.
list(Dirs) ->
    {AllPlugins, LoadingProblems} = discover_plugins(lists:usort(Dirs)),
    {Plugins, DuplicateProblems} = remove_duplicate_plugins(AllPlugins),
    {Plugins, LoadingProblems ++ DuplicateProblems}.

plugin_info({ez, EZ}) ->
    case read_app_file(EZ) of
        {application, Name, Props} -> mkplugin(Name, Props, ez, EZ);
        {error, Reason}            -> {error, EZ, Reason}
    end;
plugin_info({app, App}) ->
    case file:read_file(App) of
        {ok, Binary} ->
            case parse_binary(Binary) of
                {application, Name, Props} ->
                    mkplugin(Name, Props, dir,
                             filename:absname(
                               filename:dirname(filename:dirname(App))));
                {error, Reason} ->
                    {error, App, Reason}
            end;
        {error, Reason} ->
            {error, App, {invalid_app, Reason}}
    end.

mkplugin(Name, Props, Type, Location) ->
    Version = proplists:get_value(vsn, Props, "0"),
    Description = proplists:get_value(description, Props, ""),
    Dependencies = proplists:get_value(applications, Props, []),
    BrokerVersions = proplists:get_value(broker_version_requirements, Props, []),
    DepsVersions = proplists:get_value(dependency_version_requirements, Props, []),
    #plugin{name = Name, version = Version, description = Description,
            dependencies = Dependencies, location = Location, type = Type,
            broker_version_requirements = BrokerVersions,
            dependency_version_requirements = DepsVersions}.

read_app_file(EZ) ->
    case zip:list_dir(EZ) of
        {ok, [_|ZippedFiles]} ->
            case find_app_files(ZippedFiles) of
                [AppPath|_] ->
                    {ok, [{AppPath, AppFile}]} =
                        zip:extract(EZ, [{file_list, [AppPath]}, memory]),
                    parse_binary(AppFile);
                [] ->
                    {error, no_app_file}
            end;
        {error, Reason} ->
            {error, {invalid_ez, Reason}}
    end.

find_app_files(ZippedFiles) ->
    {ok, RE} = re:compile("^.*/ebin/.*.app$"),
    [Path || {zip_file, Path, _, _, _, _} <- ZippedFiles,
             re:run(Path, RE, [{capture, none}]) =:= match].

parse_binary(Bin) ->
    try
        {ok, Ts, _} = erl_scan:string(binary_to_list(Bin)),
        {ok, Term} = erl_parse:parse_term(Ts),
        Term
    catch
        error:{badmatch, {error, Reason}} -> {error, {invalid_app, Reason}};
        error:{badmatch, {error, Reason, _}} -> {error, {invalid_app, Reason}};
        Err -> {error, {invalid_app, Err}}
    end.

%% Search for files using glob in a given dir. Returns full filenames of those files.
full_path_wildcard(Glob, Dir) ->
    [filename:join([Dir, File]) || File <- filelib:wildcard(Glob, Dir)].

%% Returns list off all .ez files in a given set of directories
list_ezs([]) ->
    [];
list_ezs([Dir|Rest]) ->
    [{ez, EZ} || EZ <- full_path_wildcard("*.ez", Dir)] ++ list_ezs(Rest).

%% Returns list of all files that look like OTP applications in a
%% given set of directories.
list_free_apps([]) ->
    [];
list_free_apps([Dir|Rest]) ->
    [{app, App} || App <- full_path_wildcard("*/ebin/*.app", Dir)]
        ++ list_free_apps(Rest).

compare_by_name_and_version(#plugin{name = Name, version = VersionA},
                            #plugin{name = Name, version = VersionB}) ->
    rabbit_semver:lte(VersionA, VersionB);
compare_by_name_and_version(#plugin{name = NameA},
                            #plugin{name = NameB}) ->
    NameA =< NameB.

-spec discover_plugins([Directory]) -> {[#plugin{}], [Problem]} when
      Directory :: file:name(),
      Problem :: {file:name(), term()}.
discover_plugins(PluginsDirs) ->
    EZs = list_ezs(PluginsDirs),
    FreeApps = list_free_apps(PluginsDirs),
    read_plugins_info(EZs ++ FreeApps, {[], []}).

read_plugins_info([], Acc) ->
    Acc;
read_plugins_info([Path|Paths], {Plugins, Problems}) ->
    case plugin_info(Path) of
        #plugin{} = Plugin ->
            read_plugins_info(Paths, {[Plugin|Plugins], Problems});
        {error, Location, Reason} ->
            read_plugins_info(Paths, {Plugins, [{Location, Reason}|Problems]})
    end.

remove_duplicate_plugins(Plugins) ->
    %% Reverse order ensures that if there are several versions of the
    %% same plugin, the most recent one comes first.
    Sorted = lists:reverse(
               lists:sort(fun compare_by_name_and_version/2, Plugins)),
    remove_duplicate_plugins(Sorted, {[], []}).

remove_duplicate_plugins([], Acc) ->
    Acc;
remove_duplicate_plugins([Best = #plugin{name = Name}, Offender = #plugin{name = Name} | Rest],
                  {Plugins0, Problems0}) ->
    Problems1 = [{Offender#plugin.location, duplicate_plugin}|Problems0],
    remove_duplicate_plugins([Best|Rest], {Plugins0, Problems1});
remove_duplicate_plugins([Plugin|Rest], {Plugins0, Problems0}) ->
    Plugins1 = [Plugin|Plugins0],
    remove_duplicate_plugins(Rest, {Plugins1, Problems0}).
