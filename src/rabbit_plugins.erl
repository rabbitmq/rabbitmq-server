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
%% Copyright (c) 2011-2015 Pivotal Software, Inc.  All rights reserved.
%%

-module(rabbit_plugins).
-include("rabbit.hrl").

-export([setup/0, active/0, read_enabled/1, list/1, dependencies/3]).
-export([ensure/1]).

%%----------------------------------------------------------------------------

-ifdef(use_specs).

-type(plugin_name() :: atom()).

-spec(setup/0 :: () -> [plugin_name()]).
-spec(active/0 :: () -> [plugin_name()]).
-spec(list/1 :: (string()) -> [#plugin{}]).
-spec(read_enabled/1 :: (file:filename()) -> [plugin_name()]).
-spec(dependencies/3 :: (boolean(), [plugin_name()], [#plugin{}]) ->
                             [plugin_name()]).
-spec(ensure/1  :: (string()) -> {'ok', [atom()], [atom()]} | {error, any()}).
-endif.

%%----------------------------------------------------------------------------

ensure(FileJustChanged0) ->
    {ok, OurFile0} = application:get_env(rabbit, enabled_plugins_file),
    FileJustChanged = filename:nativename(FileJustChanged0),
    OurFile = filename:nativename(OurFile0),
    case OurFile of
        FileJustChanged ->
            Enabled = read_enabled(OurFile),
            Wanted = prepare_plugins(Enabled),
            Current = active(),
            Start = Wanted -- Current,
            Stop = Current -- Wanted,
            rabbit:start_apps(Start),
            %% We need sync_notify here since mgmt will attempt to look at all
            %% the modules for the disabled plugins - if they are unloaded
            %% that won't work.
            ok = rabbit_event:sync_notify(plugins_changed, [{enabled,  Start},
                                                            {disabled, Stop}]),
            rabbit:stop_apps(Stop),
            clean_plugins(Stop),
            rabbit_log:info("Plugins changed; enabled ~p, disabled ~p~n",
                            [Start, Stop]),
            {ok, Start, Stop};
        _ ->
            {error, {enabled_plugins_mismatch, FileJustChanged, OurFile}}
    end.

%% @doc Prepares the file system and installs all enabled plugins.
setup() ->
    {ok, ExpandDir}   = application:get_env(rabbit, plugins_expand_dir),

    %% Eliminate the contents of the destination directory
    case delete_recursively(ExpandDir) of
        ok          -> ok;
        {error, E1} -> throw({error, {cannot_delete_plugins_expand_dir,
                                      [ExpandDir, E1]}})
    end,

    {ok, EnabledFile} = application:get_env(rabbit, enabled_plugins_file),
    Enabled = read_enabled(EnabledFile),
    prepare_plugins(Enabled).

%% @doc Lists the plugins which are currently running.
active() ->
    {ok, ExpandDir} = application:get_env(rabbit, plugins_expand_dir),
    InstalledPlugins = plugin_names(list(ExpandDir)),
    [App || {App, _, _} <- rabbit_misc:which_applications(),
            lists:member(App, InstalledPlugins)].

%% @doc Get the list of plugins which are ready to be enabled.
list(PluginsDir) ->
    EZs = [{ez, EZ} || EZ <- filelib:wildcard("*.ez", PluginsDir)],
    FreeApps = [{app, App} ||
                   App <- filelib:wildcard("*/ebin/*.app", PluginsDir)],
    {AvailablePlugins, Problems} =
        lists:foldl(fun ({error, EZ, Reason}, {Plugins1, Problems1}) ->
                            {Plugins1, [{EZ, Reason} | Problems1]};
                        (#plugin{name = rabbit_common}, {Plugins1, Problems1}) ->
                            {Plugins1, Problems1};
                        (Plugin = #plugin{}, {Plugins1, Problems1}) ->
                            {[Plugin|Plugins1], Problems1}
                    end, {[], []},
                    [plugin_info(PluginsDir, Plug) || Plug <- EZs ++ FreeApps]),
    case Problems of
        [] -> ok;
        _  -> rabbit_log:warning(
                "Problem reading some plugins: ~p~n", [Problems])
    end,
    Plugins = lists:filter(fun(P) -> not plugin_provided_by_otp(P) end,
                           AvailablePlugins),
    ensure_dependencies(Plugins).

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
                fun ({App, _Deps}) -> [{App, App}] end,
                fun ({App,  Deps}) -> [{App, Dep} || Dep <- Deps] end,
                [{Name, Deps} || #plugin{name         = Name,
                                         dependencies = Deps} <- AllPlugins]),
    Dests = case Reverse of
                false -> digraph_utils:reachable(Sources, G);
                true  -> digraph_utils:reaching(Sources, G)
            end,
    true = digraph:delete(G),
    Dests.

%% For a few known cases, an externally provided plugin can be trusted.
%% In this special case, it overrides the plugin.
plugin_provided_by_otp(#plugin{name = eldap}) ->
    %% eldap was added to Erlang/OTP R15B01 (ERTS 5.9.1). In this case,
    %% we prefer this version to the plugin.
    rabbit_misc:version_compare(erlang:system_info(version), "5.9.1", gte);
plugin_provided_by_otp(_) ->
    false.

%% Make sure we don't list OTP apps in here, and also that we detect
%% missing dependencies.
ensure_dependencies(Plugins) ->
    Names = plugin_names(Plugins),
    NotThere = [Dep || #plugin{dependencies = Deps} <- Plugins,
                       Dep                          <- Deps,
                       not lists:member(Dep, Names)],
    {OTP, Missing} = lists:partition(fun is_loadable/1, lists:usort(NotThere)),
    case Missing of
        [] -> ok;
        _  -> Blame = [Name || #plugin{name         = Name,
                                       dependencies = Deps} <- Plugins,
                               lists:any(fun (Dep) ->
                                                 lists:member(Dep, Missing)
                                         end, Deps)],
              throw({error, {missing_dependencies, Missing, Blame}})
    end,
    [P#plugin{dependencies = Deps -- OTP}
     || P = #plugin{dependencies = Deps} <- Plugins].

is_loadable(App) ->
    case application:load(App) of
        {error, {already_loaded, _}} -> true;
        ok                           -> application:unload(App),
                                        true;
        _                            -> false
    end.

%%----------------------------------------------------------------------------

prepare_plugins(Enabled) ->
    {ok, PluginsDistDir} = application:get_env(rabbit, plugins_dir),
    {ok, ExpandDir} = application:get_env(rabbit, plugins_expand_dir),

    AllPlugins = list(PluginsDistDir),
    Wanted = dependencies(false, Enabled, AllPlugins),
    WantedPlugins = lookup_plugins(Wanted, AllPlugins),

    case filelib:ensure_dir(ExpandDir ++ "/") of
        ok          -> ok;
        {error, E2} -> throw({error, {cannot_create_plugins_expand_dir,
                                      [ExpandDir, E2]}})
    end,

    [prepare_plugin(Plugin, ExpandDir) || Plugin <- WantedPlugins],

    [prepare_dir_plugin(PluginAppDescPath) ||
        PluginAppDescPath <- filelib:wildcard(ExpandDir ++ "/*/ebin/*.app")],
    Wanted.

clean_plugins(Plugins) ->
    {ok, ExpandDir} = application:get_env(rabbit, plugins_expand_dir),
    [clean_plugin(Plugin, ExpandDir) || Plugin <- Plugins].

clean_plugin(Plugin, ExpandDir) ->
    {ok, Mods} = application:get_key(Plugin, modules),
    application:unload(Plugin),
    [begin
         code:soft_purge(Mod),
         code:delete(Mod),
         false = code:is_loaded(Mod)
     end || Mod <- Mods],
    delete_recursively(rabbit_misc:format("~s/~s", [ExpandDir, Plugin])).

prepare_dir_plugin(PluginAppDescPath) ->
    PluginEbinDir = filename:dirname(PluginAppDescPath),
    Plugin = filename:basename(PluginAppDescPath, ".app"),
    code:add_patha(PluginEbinDir),
    case filelib:wildcard(PluginEbinDir++ "/*.beam") of
        [] ->
            ok;
        [BeamPath | _] ->
            Module = list_to_atom(filename:basename(BeamPath, ".beam")),
            case code:ensure_loaded(Module) of
                {module, _} ->
                    ok;
                {error, badfile} ->
                    rabbit_log:error("Failed to enable plugin \"~s\": "
                                     "it may have been built with an "
                                     "incompatible (more recent?) "
                                     "version of Erlang~n", [Plugin]),
                    throw({plugin_built_with_incompatible_erlang, Plugin});
                Error ->
                    throw({plugin_module_unloadable, Plugin, Error})
            end
    end.

%%----------------------------------------------------------------------------

delete_recursively(Fn) ->
    case rabbit_file:recursive_delete([Fn]) of
        ok                 -> ok;
        {error, {Path, E}} -> {error, {cannot_delete, Path, E}}
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
    Dependencies = proplists:get_value(applications, Props, []),
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

plugin_names(Plugins) ->
    [Name || #plugin{name = Name} <- Plugins].

lookup_plugins(Names, AllPlugins) ->
    [P || P = #plugin{name = Name} <- AllPlugins, lists:member(Name, Names)].
