%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2011-2021 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(rabbit_plugins).
-include_lib("rabbit_common/include/rabbit.hrl").
-include_lib("stdlib/include/zip.hrl").

-export([setup/0, active/0, read_enabled/1, list/1, list/2, dependencies/3, running_plugins/0]).
-export([ensure/1]).
-export([validate_plugins/1, format_invalid_plugins/1]).
-export([is_strictly_plugin/1, strictly_plugins/2, strictly_plugins/1]).
-export([plugins_dir/0, plugin_names/1, plugins_expand_dir/0, enabled_plugins_file/0]).

% Export for testing purpose.
-export([is_version_supported/2, validate_plugins/2]).
%%----------------------------------------------------------------------------

-type plugin_name() :: atom().

%%----------------------------------------------------------------------------

-spec ensure(string()) -> {'ok', [atom()], [atom()]} | {error, any()}.

ensure(FileJustChanged) ->
    case rabbit:is_running() of
        true  -> ensure1(FileJustChanged);
        false -> {error, rabbit_not_running}
    end.

ensure1(FileJustChanged0) ->
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
            %% The app_utils module stops the apps in reverse order, so we should
            %% pass them here in dependency order.
            rabbit:stop_apps(lists:reverse(Stop)),
            clean_plugins(Stop),
            case {Start, Stop} of
                {[], []} ->
                    ok;
                {[], _} ->
                    _ = rabbit_log:info("Plugins changed; disabled ~p~n",
                                    [Stop]);
                {_, []} ->
                    _ = rabbit_log:info("Plugins changed; enabled ~p~n",
                                    [Start]);
                {_, _} ->
                    _ = rabbit_log:info("Plugins changed; enabled ~p, disabled ~p~n",
                                    [Start, Stop])
            end,
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

-spec plugins_dir() -> file:filename().
plugins_dir() ->
    case application:get_env(rabbit, plugins_dir) of
        {ok, PluginsDistDir} ->
            PluginsDistDir;
        _ ->
            filename:join([rabbit_mnesia:dir(), "plugins_dir_stub"])
    end.

-spec enabled_plugins_file() -> file:filename().
enabled_plugins_file() ->
     case application:get_env(rabbit, enabled_plugins_file) of
        {ok, Val} ->
            Val;
        _ ->
            filename:join([rabbit_mnesia:dir(), "enabled_plugins"])
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

-spec setup() -> [plugin_name()].

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

-spec active() -> [plugin_name()].

active() ->
    InstalledPlugins = plugin_names(list(plugins_dir())),
    [App || {App, _, _} <- rabbit_misc:which_applications(),
            lists:member(App, InstalledPlugins)].

%% @doc Get the list of plugins which are ready to be enabled.

-spec list(string()) -> [#plugin{}].

list(PluginsPath) ->
    list(PluginsPath, false).

-spec list(string(), boolean()) -> [#plugin{}].

list(PluginsPath, IncludeRequiredDeps) ->
    {AllPlugins, LoadingProblems} = discover_plugins(split_path(PluginsPath)),
    {UniquePlugins, DuplicateProblems} = remove_duplicate_plugins(AllPlugins),
    Plugins1 = maybe_keep_required_deps(IncludeRequiredDeps, UniquePlugins),
    Plugins2 = remove_plugins(Plugins1),
    maybe_report_plugin_loading_problems(LoadingProblems ++ DuplicateProblems),
    ensure_dependencies(Plugins2).

%% @doc Read the list of enabled plugins from the supplied term file.

-spec read_enabled(file:filename()) -> [plugin_name()].

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

-spec dependencies(boolean(), [plugin_name()], [#plugin{}]) ->
                             [plugin_name()].

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
    OrderedDests = digraph_utils:postorder(digraph_utils:subgraph(G, Dests)),
    true = digraph:delete(G),
    OrderedDests.

%% Filter real plugins from application dependencies

-spec is_strictly_plugin(#plugin{}) -> boolean().

is_strictly_plugin(#plugin{extra_dependencies = ExtraDeps}) ->
    lists:member(rabbit, ExtraDeps).

-spec strictly_plugins([plugin_name()], [#plugin{}]) -> [plugin_name()].

strictly_plugins(Plugins, AllPlugins) ->
    lists:filter(
      fun(Name) ->
              is_strictly_plugin(lists:keyfind(Name, #plugin.name, AllPlugins))
      end, Plugins).

-spec strictly_plugins([plugin_name()]) -> [plugin_name()].

strictly_plugins(Plugins) ->
    AllPlugins = list(plugins_dir()),
    lists:filter(
      fun(Name) ->
              is_strictly_plugin(lists:keyfind(Name, #plugin.name, AllPlugins))
      end, Plugins).

%% For a few known cases, an externally provided plugin can be trusted.
%% In this special case, it overrides the plugin.
is_plugin_provided_by_otp(#plugin{name = eldap}) ->
    %% eldap was added to Erlang/OTP R15B01 (ERTS 5.9.1). In this case,
    %% we prefer this version to the plugin.
    rabbit_misc:version_compare(erlang:system_info(version), "5.9.1", gte);
is_plugin_provided_by_otp(_) ->
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
    [P#plugin{dependencies = Deps -- OTP,
              extra_dependencies = Deps -- (Deps -- OTP)}
     || P = #plugin{dependencies = Deps} <- Plugins].

is_loadable(App) ->
    case application:load(App) of
        {error, {already_loaded, _}} -> true;
        ok                           -> application:unload(App),
                                        true;
        _                            -> false
    end.


%% List running plugins along with their version.
-spec running_plugins() -> {ok, [{atom(), Vsn :: string()}]}.
running_plugins() ->
    ActivePlugins = active(),
    {ok, [{App, Vsn} || {App, _ , Vsn} <- rabbit_misc:which_applications(), lists:member(App, ActivePlugins)]}.

%%----------------------------------------------------------------------------

prepare_plugins(Enabled) ->
    ExpandDir = plugins_expand_dir(),
    AllPlugins = list(plugins_dir()),
    Wanted = dependencies(false, Enabled, AllPlugins),
    WantedPlugins = lookup_plugins(Wanted, AllPlugins),
    {ValidPlugins, Problems} = validate_plugins(WantedPlugins),
    maybe_warn_about_invalid_plugins(Problems),
    case filelib:ensure_dir(ExpandDir ++ "/") of
        ok          -> ok;
        {error, E2} -> throw({error, {cannot_create_plugins_expand_dir,
                                      [ExpandDir, E2]}})
    end,
    [prepare_plugin(Plugin, ExpandDir) || Plugin <- ValidPlugins],
    Wanted.

maybe_warn_about_invalid_plugins([]) ->
    ok;
maybe_warn_about_invalid_plugins(InvalidPlugins) ->
    %% TODO: error message formatting
    _ = rabbit_log:warning(format_invalid_plugins(InvalidPlugins)).


format_invalid_plugins(InvalidPlugins) ->
    lists:flatten(["Failed to enable some plugins: \r\n"
                   | [format_invalid_plugin(Plugin)
                      || Plugin <- InvalidPlugins]]).

format_invalid_plugin({Name, Errors}) ->
    [io_lib:format("    ~p:~n", [Name])
     | [format_invalid_plugin_error(Err) || Err <- Errors]].

format_invalid_plugin_error({missing_dependency, Dep}) ->
    io_lib:format("        Dependency is missing or invalid: ~p~n", [Dep]);
%% a plugin doesn't support the effective broker version
format_invalid_plugin_error({broker_version_mismatch, Version, Required}) ->
    io_lib:format("        Plugin doesn't support current server version."
                  " Actual broker version: ~p, supported by the plugin: ~p~n",
                  [Version, format_required_versions(Required)]);
%% one of dependencies of a plugin doesn't match its version requirements
format_invalid_plugin_error({{dependency_version_mismatch, Version, Required}, Name}) ->
    io_lib:format("        Version '~p' of dependency '~p' is unsupported."
                  " Version ranges supported by the plugin: ~p~n",
                  [Version, Name, Required]);
format_invalid_plugin_error(Err) ->
    io_lib:format("        Unknown error ~p~n", [Err]).

format_required_versions(Versions) ->
    lists:map(fun(V) ->
                      case re:run(V, "^[0-9]*\.[0-9]*\.", [{capture, all, list}]) of
                          {match, [Sub]} ->
                              lists:flatten(io_lib:format("~s-~sx", [V, Sub]));
                          _ ->
                              V
                      end
              end, Versions).

validate_plugins(Plugins) ->
    application:load(rabbit),
    RabbitVersion = RabbitVersion = case application:get_key(rabbit, vsn) of
                                        undefined -> "0.0.0";
                                        {ok, Val} -> Val
                                    end,
    validate_plugins(Plugins, RabbitVersion).

validate_plugins(Plugins, BrokerVersion) ->
    lists:foldl(
        fun(#plugin{name = Name,
                    broker_version_requirements = BrokerVersionReqs,
                    dependency_version_requirements = DepsVersions} = Plugin,
            {Plugins0, Errors}) ->
            case is_version_supported(BrokerVersion, BrokerVersionReqs) of
                true  ->
                    case BrokerVersion of
                        "0.0.0" ->
                            _ = rabbit_log:warning(
                                "Running development version of the broker."
                                " Requirement ~p for plugin ~p is ignored.",
                                [BrokerVersionReqs, Name]);
                        _ -> ok
                    end,
                    case check_plugins_versions(Name, Plugins0, DepsVersions) of
                        ok           -> {[Plugin | Plugins0], Errors};
                        {error, Err} -> {Plugins0, [{Name, Err} | Errors]}
                    end;
                false ->
                    Error = [{broker_version_mismatch, BrokerVersion, BrokerVersionReqs}],
                    {Plugins0, [{Name, Error} | Errors]}
            end
        end,
        {[],[]},
        Plugins).

check_plugins_versions(PluginName, AllPlugins, RequiredVersions) ->
    ExistingVersions = [{Name, Vsn}
                        || #plugin{name = Name, version = Vsn} <- AllPlugins],
    Problems = lists:foldl(
        fun({Name, Versions}, Acc) ->
            case proplists:get_value(Name, ExistingVersions) of
                undefined -> [{missing_dependency, Name} | Acc];
                Version   ->
                    case is_version_supported(Version, Versions) of
                        true  ->
                            case Version of
                                "" ->
                                    _ = rabbit_log:warning(
                                        "~p plugin version is not defined."
                                        " Requirement ~p for plugin ~p is ignored",
                                        [Versions, PluginName]);
                                _  -> ok
                            end,
                            Acc;
                        false ->
                            [{{dependency_version_mismatch, Version, Versions}, Name} | Acc]
                    end
            end
        end,
        [],
        RequiredVersions),
    case Problems of
        [] -> ok;
        _  -> {error, Problems}
    end.

is_version_supported("", _)        -> true;
is_version_supported("0.0.0", _)   -> true;
is_version_supported(_Version, []) -> true;
is_version_supported(VersionFull, ExpectedVersions) ->
    %% Pre-release version should be supported in plugins,
    %% therefore preview part should be removed
    Version = remove_version_preview_part(VersionFull),
    case lists:any(fun(ExpectedVersion) ->
                       rabbit_misc:strict_version_minor_equivalent(ExpectedVersion,
                                                                   Version)
                       andalso
                       rabbit_misc:version_compare(ExpectedVersion, Version, lte)
                   end,
                   ExpectedVersions) of
        true  -> true;
        false -> false
    end.

remove_version_preview_part(Version) ->
    {Ver, _Preview} = rabbit_semver:parse(Version),
    iolist_to_binary(rabbit_semver:format({Ver, {[], []}})).

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
                    _ = rabbit_log:error("Failed to enable plugin \"~s\": "
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

find_unzipped_app_file(ExpandDir, Files) ->
    StripComponents = length(filename:split(ExpandDir)),
    [ X || X <- Files,
           [_AppName, "ebin", MaybeAppFile] <-
               [lists:nthtail(StripComponents, filename:split(X))],
           lists:suffix(".app", MaybeAppFile)
    ].

prepare_plugin(#plugin{type = ez, name = Name, location = Location}, ExpandDir) ->
    case zip:unzip(Location, [{cwd, ExpandDir}]) of
        {ok, Files} ->
            case find_unzipped_app_file(ExpandDir, Files) of
                [PluginAppDescPath|_] ->
                    prepare_dir_plugin(PluginAppDescPath);
                _ ->
                    _ = rabbit_log:error("Plugin archive '~s' doesn't contain an .app file~n", [Location]),
                    throw({app_file_missing, Name, Location})
            end;
        {error, Reason} ->
            _ = rabbit_log:error("Could not unzip plugin archive '~s': ~p~n", [Location, Reason]),
            throw({failed_to_unzip_plugin, Name, Location, Reason})
    end;
prepare_plugin(#plugin{type = dir, location = Location, name = Name},
               _ExpandDir) ->
    case filelib:wildcard(Location ++ "/ebin/*.app") of
        [PluginAppDescPath|_] ->
            prepare_dir_plugin(PluginAppDescPath);
        _ ->
            _ = rabbit_log:error("Plugin directory '~s' doesn't contain an .app file~n", [Location]),
            throw({app_file_missing, Name, Location})
    end.

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
        Err -> {error, {invalid_app, Err}}
    end.

plugin_names(Plugins) ->
    [Name || #plugin{name = Name} <- Plugins].

lookup_plugins(Names, AllPlugins) ->
    %% Preserve order of Names
    lists:map(
        fun(Name) ->
            lists:keyfind(Name, #plugin.name, AllPlugins)
        end,
        Names).

%% Split PATH-like value into its components.
split_path(PathString) ->
    Delimiters = case os:type() of
                     {unix, _} -> ":";
                     {win32, _} -> ";"
                 end,
    lists:usort(string:tokens(PathString, Delimiters)).

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

maybe_keep_required_deps(true, Plugins) ->
    Plugins;
maybe_keep_required_deps(false, Plugins) ->
    RabbitDeps = list_all_deps([rabbit]),
    lists:filter(fun
                     (#plugin{name = Name}) ->
                         not lists:member(Name, RabbitDeps);
                     (Name) when is_atom(Name) ->
                         not lists:member(Name, RabbitDeps)
                 end,
                 Plugins).

list_all_deps(Applications) ->
    list_all_deps(Applications, []).

list_all_deps([Application | Applications], Deps) ->
    %% We load the application to be sure we can get the "applications" key.
    %% This is required for rabbitmq-plugins for instance.
    application:load(Application),
    NewDeps = [Application | Deps],
    case application:get_key(Application, applications) of
        {ok, ApplicationDeps} ->
            RemainingApplications0 = ApplicationDeps ++ Applications,
            RemainingApplications = RemainingApplications0 -- NewDeps,
            list_all_deps(RemainingApplications, NewDeps);
        undefined ->
            list_all_deps(Applications, NewDeps)
    end;
list_all_deps([], Deps) ->
    Deps.

remove_plugins(Plugins) ->
    %% We want to filter out all Erlang applications in the plugins
    %% directories which are not actual RabbitMQ plugin.
    %%
    %% A RabbitMQ plugin must depend on `rabbit`. We also want to keep
    %% all applications they depend on, except Erlang/OTP applications.
    %% In the end, we will skip:
    %%   * Erlang/OTP applications
    %%   * All applications which do not depend on `rabbit` and which
    %%     are not direct or indirect dependencies of plugins.
    ActualPlugins = [Plugin
                     || #plugin{dependencies = Deps} = Plugin <- Plugins,
                        lists:member(rabbit, Deps)],
    %% As said above, we want to keep all non-plugins which are
    %% dependencies of plugins.
    PluginDeps = lists:usort(
                   lists:flatten(
                     [resolve_deps(Plugins, Plugin)
                      || Plugin <- ActualPlugins])),
    lists:filter(
      fun(#plugin{name = Name} = Plugin) ->
              IsOTPApp = is_plugin_provided_by_otp(Plugin),
              IsAPlugin =
              lists:member(Plugin, ActualPlugins) orelse
              lists:member(Name, PluginDeps),
              if
                  IsOTPApp ->
                      _ = rabbit_log:debug(
                        "Plugins discovery: "
                        "ignoring ~s, Erlang/OTP application",
                        [Name]);
                  not IsAPlugin ->
                      _ = rabbit_log:debug(
                        "Plugins discovery: "
                        "ignoring ~s, not a RabbitMQ plugin",
                        [Name]);
                  true ->
                      ok
              end,
              not (IsOTPApp orelse not IsAPlugin)
      end, Plugins).

resolve_deps(Plugins, #plugin{dependencies = Deps}) ->
    IndirectDeps = [case lists:keyfind(Dep, #plugin.name, Plugins) of
                        false     -> [];
                        DepPlugin -> resolve_deps(Plugins, DepPlugin)
                    end
                    || Dep <- Deps],
    Deps ++ IndirectDeps.

maybe_report_plugin_loading_problems([]) ->
    ok;
maybe_report_plugin_loading_problems(Problems) ->
    io:format(standard_error,
              "Problem reading some plugins: ~p~n",
              [Problems]).
