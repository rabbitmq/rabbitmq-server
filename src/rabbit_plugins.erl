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
-include_lib("rabbit_common/include/rabbit.hrl").

-export([setup/0, active/0, read_enabled/1, list/1, list/2, dependencies/3]).
-export([ensure/1]).

%%----------------------------------------------------------------------------

-type plugin_name() :: atom().

-spec setup() -> [plugin_name()].
-spec active() -> [plugin_name()].
-spec list(string()) -> [#plugin{}].
-spec list(string(), boolean()) -> [#plugin{}].
-spec read_enabled(file:filename()) -> [plugin_name()].
-spec dependencies(boolean(), [plugin_name()], [#plugin{}]) ->
                             [plugin_name()].
-spec ensure(string()) -> {'ok', [atom()], [atom()]} | {error, any()}.

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

-spec plugins_expand_dir() -> file:filename().
plugins_expand_dir() ->
    case application:get_env(rabbit, plugins_expand_dir) of
        {ok, ExpandDir} ->
            ExpandDir;
        _ ->
            filename:join([rabbit_mnesia:dir(), "plugins_expand_dir"])
    end.

-spec plugins_dist_dir() -> file:filename().
plugins_dist_dir() ->
    case application:get_env(rabbit, plugins_dir) of
        {ok, PluginsDistDir} ->
            PluginsDistDir;
        _ ->
            filename:join([rabbit_mnesia:dir(), "plugins_dir_stub"])
    end.

-spec enabled_plugins() -> [atom()].
enabled_plugins() ->
    case application:get_env(rabbit, enabled_plugins_file) of
        {ok, EnabledFile} ->
            read_enabled(EnabledFile);
        _ ->
            []
    end.

%% @doc Prepares the file system and installs all enabled plugins.
setup() ->
    ExpandDir = plugins_expand_dir(),
    %% Eliminate the contents of the destination directory
    case delete_recursively(ExpandDir) of
        ok          -> ok;
        {error, E1} -> throw({error, {cannot_delete_plugins_expand_dir,
                                      [ExpandDir, E1]}})
    end,

    Enabled = enabled_plugins(),
    prepare_plugins(Enabled).

%% @doc Lists the plugins which are currently running.
active() ->
    InstalledPlugins = plugin_names(list(plugins_expand_dir())),
    [App || {App, _, _} <- rabbit_misc:which_applications(),
            lists:member(App, InstalledPlugins)].

%% @doc Get the list of plugins which are ready to be enabled.
list(PluginsPath) ->
    list(PluginsPath, false).

list(PluginsPath, IncludeRequiredDeps) ->
    {AllPlugins, LoadingProblems} = discover_plugins(split_path(PluginsPath)),
    {UniquePlugins, DuplicateProblems} = remove_duplicate_plugins(AllPlugins),
    Plugins1 = maybe_keep_required_deps(IncludeRequiredDeps, UniquePlugins),
    Plugins2 = remove_otp_overrideable_plugins(Plugins1),
    maybe_report_plugin_loading_problems(LoadingProblems ++ DuplicateProblems),
    ensure_dependencies(Plugins2).

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
    ExpandDir = plugins_expand_dir(),

    AllPlugins = list(plugins_dist_dir()),
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
    ExpandDir = plugins_expand_dir(),
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

plugin_info({ez, EZ}) ->
    case read_app_file(EZ) of
        {application, Name, Props} -> mkplugin(Name, Props, ez, EZ);
        {error, Reason}            -> {error, EZ, Reason}
    end;
plugin_info({app, App}) ->
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

%% Split PATH-like value into its components.
split_path(PathString) ->
    Delimiters = case os:type() of
                     {unix, _} -> ":";
                     {win32, _} -> ";"
                 end,
    string:tokens(PathString, Delimiters).

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
    ec_semver:lte(VersionA, VersionB);
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

maybe_keep_required_deps(true, Plugins) ->
    Plugins;
maybe_keep_required_deps(false, Plugins) ->
    %% We load the "rabbit" application to be sure we can get the
    %% "applications" key. This is required for rabbitmq-plugins for
    %% instance.
    application:load(rabbit),
    {ok, RabbitDeps} = application:get_key(rabbit, applications),
    lists:filter(fun(#plugin{name = Name}) ->
                         not lists:member(Name, RabbitDeps)
                 end,
                 Plugins).

remove_otp_overrideable_plugins(Plugins) ->
    lists:filter(fun(P) -> not plugin_provided_by_otp(P) end,
                 Plugins).

maybe_report_plugin_loading_problems([]) ->
    ok;
maybe_report_plugin_loading_problems(Problems) ->
    rabbit_log:warning("Problem reading some plugins: ~p~n", [Problems]).
