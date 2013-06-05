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
%% The Initial Developer of the Original Code is VMware, Inc.
%% Copyright (c) 2011-2013 VMware, Inc.  All rights reserved.
%%

-module(rabbit_plugins).
-include("rabbit.hrl").

-export([setup/0, active/0, read_enabled/1, list/1, dependencies/3]).

%%----------------------------------------------------------------------------

-ifdef(use_specs).

-type(plugin_name() :: atom()).

-spec(setup/0 :: () -> [plugin_name()]).
-spec(active/0 :: () -> [plugin_name()]).
-spec(list/1 :: (string()) -> [#plugin{}]).
-spec(read_enabled/1 :: (file:filename()) -> [plugin_name()]).
-spec(dependencies/3 :: (boolean(), [plugin_name()], [#plugin{}]) ->
                             [plugin_name()]).

-endif.

%%----------------------------------------------------------------------------

%% @doc Prepares the file system and installs all enabled plugins.
setup() ->
    {ok, PluginDir}   = application:get_env(rabbit, plugins_dir),
    {ok, ExpandDir}   = application:get_env(rabbit, plugins_expand_dir),
    {ok, EnabledFile} = application:get_env(rabbit, enabled_plugins_file),
    prepare_plugins(EnabledFile, PluginDir, ExpandDir).

%% @doc Lists the plugins which are currently running.
active() ->
    {ok, ExpandDir} = application:get_env(rabbit, plugins_expand_dir),
    InstalledPlugins = [ P#plugin.name || P <- list(ExpandDir) ],
    [App || {App, _, _} <- rabbit_misc:which_applications(),
            lists:member(App, InstalledPlugins)].

%% @doc Get the list of plugins which are ready to be enabled.
list(PluginsDir) ->
    EZs = [{ez, EZ} || EZ <- filelib:wildcard("*.ez", PluginsDir)],
    FreeApps = [{app, App} ||
                   App <- filelib:wildcard("*/ebin/*.app", PluginsDir)],
    {Plugins, Problems} =
        lists:foldl(fun ({error, EZ, Reason}, {Plugins1, Problems1}) ->
                            {Plugins1, [{EZ, Reason} | Problems1]};
                        (Plugin = #plugin{}, {Plugins1, Problems1}) ->
                            {[Plugin|Plugins1], Problems1}
                    end, {[], []},
                    [plugin_info(PluginsDir, Plug) || Plug <- EZs ++ FreeApps]),
    case Problems of
        [] -> ok;
        _  -> error_logger:warning_msg(
                "Problem reading some plugins: ~p~n", [Problems])
    end,
    Plugins.

%% @doc Read the list of enabled plugins from the supplied term file.
read_enabled(PluginsFile) ->
    case rabbit_file:read_term_file(PluginsFile) of
        {ok, [Plugins]} -> Plugins;
        {ok, []}        -> [];
        {ok, [_|_]}     -> throw({error, {malformed_enabled_plugins_file,
                                          PluginsFile}});
        {error, enoent} -> [];
        {error, Reason} -> throw({error, {cannot_read_enabled_plugins_file,
                                          PluginsFile, Reason}})
    end.

%% @doc Calculate the dependency graph from <i>Sources</i>.
%% When Reverse =:= true the bottom/leaf level applications are returned in
%% the resulting list, otherwise they're skipped.
dependencies(Reverse, Sources, AllPlugins) ->
    {ok, G} = rabbit_misc:build_acyclic_graph(
                fun (App, _Deps) -> [{App, App}] end,
                fun (App,  Deps) -> [{App, Dep} || Dep <- Deps] end,
                lists:ukeysort(
                  1, [{Name, Deps} ||
                         #plugin{name         = Name,
                                 dependencies = Deps} <- AllPlugins] ++
                      [{Dep,   []} ||
                          #plugin{dependencies = Deps} <- AllPlugins,
                          Dep                          <- Deps])),
    Dests = case Reverse of
                false -> digraph_utils:reachable(Sources, G);
                true  -> digraph_utils:reaching(Sources, G)
            end,
    true = digraph:delete(G),
    Dests.

%%----------------------------------------------------------------------------

prepare_plugins(EnabledFile, PluginsDistDir, ExpandDir) ->
    AllPlugins = list(PluginsDistDir),
    Enabled = read_enabled(EnabledFile),
    ToUnpack = dependencies(false, Enabled, AllPlugins),
    ToUnpackPlugins = lookup_plugins(ToUnpack, AllPlugins),

    case Enabled -- plugin_names(ToUnpackPlugins) of
        []      -> ok;
        Missing -> error_logger:warning_msg(
                     "The following enabled plugins were not found: ~p~n",
                     [Missing])
    end,

    %% Eliminate the contents of the destination directory
    case delete_recursively(ExpandDir) of
        ok          -> ok;
        {error, E1} -> throw({error, {cannot_delete_plugins_expand_dir,
                                      [ExpandDir, E1]}})
    end,
    case filelib:ensure_dir(ExpandDir ++ "/") of
        ok          -> ok;
        {error, E2} -> throw({error, {cannot_create_plugins_expand_dir,
                                      [ExpandDir, E2]}})
    end,

    [prepare_plugin(Plugin, ExpandDir) || Plugin <- ToUnpackPlugins],

    [prepare_dir_plugin(PluginAppDescPath) ||
        PluginAppDescPath <- filelib:wildcard(ExpandDir ++ "/*/ebin/*.app")].

prepare_dir_plugin(PluginAppDescPath) ->
    code:add_path(filename:dirname(PluginAppDescPath)),
    list_to_atom(filename:basename(PluginAppDescPath, ".app")).

%%----------------------------------------------------------------------------

delete_recursively(Fn) ->
    case rabbit_file:recursive_delete([Fn]) of
        ok                 -> ok;
        {error, {Path, E}} -> {error, {cannot_delete, Path, E}};
        Error              -> Error
    end.

prepare_plugin(#plugin{type = ez, location = Location}, ExpandDir) ->
    zip:unzip(Location, [{cwd, ExpandDir}]);
prepare_plugin(#plugin{type = dir, name = Name, location = Location},
               ExpandDir) ->
    rabbit_file:recursive_copy(Location, filename:join([ExpandDir, Name])).

plugin_info(Base, {ez, EZ0}) ->
    EZ = filename:join([Base, EZ0]),
    case read_app_file(EZ) of
        {application, Name, Props} -> mkplugin(Name, Props, ez, EZ);
        {error, Reason}            -> {error, EZ, Reason}
    end;
plugin_info(Base, {app, App0}) ->
    App = filename:join([Base, App0]),
    case rabbit_file:read_term_file(App) of
        {ok, [{application, Name, Props}]} ->
            mkplugin(Name, Props, dir,
                     filename:absname(
                       filename:dirname(filename:dirname(App))));
        {error, Reason} ->
            {error, App, {invalid_app, Reason}}
    end.

mkplugin(Name, Props, Type, Location) ->
    Version = proplists:get_value(vsn, Props, "0"),
    Description = proplists:get_value(description, Props, ""),
    Dependencies =
        filter_applications(proplists:get_value(applications, Props, [])),
    #plugin{name = Name, version = Version, description = Description,
            dependencies = Dependencies, location = Location, type = Type}.

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
        Err -> {error, {invalid_app, Err}}
    end.

filter_applications(Applications) ->
    [Application || Application <- Applications,
                    not is_available_app(Application)].

is_available_app(Application) ->
    case application:load(Application) of
        {error, {already_loaded, _}} -> true;
        ok                           -> application:unload(Application),
                                        true;
        _                            -> false
    end.

plugin_names(Plugins) ->
    [Name || #plugin{name = Name} <- Plugins].

lookup_plugins(Names, AllPlugins) ->
    [P || P = #plugin{name = Name} <- AllPlugins, lists:member(Name, Names)].
