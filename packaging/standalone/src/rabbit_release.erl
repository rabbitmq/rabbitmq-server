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
%% Copyright (c) 2007-2012 GoPivotal, Inc.  All rights reserved.
%%
-module(rabbit_release).

-export([start/0]).

-include("rabbit.hrl").

-define(BaseApps, [rabbit]).
-define(ERROR_CODE, 1).

%% We need to calculate all the ERTS apps we need to ship with a
%% standalone rabbit. To acomplish that we need to unpack and load the plugins
%% apps that are shiped with rabbit.
%% Once we get that we generate an erlang release inside a tarball.
%% Our make file will work with that release to generate our final rabbitmq
%% package.
start() ->
    %% Determine our various directories
    [PluginsDistDir, UnpackedPluginDir, RabbitHome] =
        init:get_plain_arguments(),
    RootName = UnpackedPluginDir ++ "/rabbit",

    %% extract the plugins so we can load their apps later
    prepare_plugins(PluginsDistDir, UnpackedPluginDir),

    %% add the plugin ebin folder to the code path.
    add_plugins_to_path(UnpackedPluginDir),

    PluginAppNames = [P#plugin.name ||
                      P <- rabbit_plugins:list(PluginsDistDir, false)],

    %% Build the entire set of dependencies - this will load the
    %% applications along the way
    AllApps = case catch sets:to_list(expand_dependencies(PluginAppNames)) of
                  {failed_to_load_app, App, Err} ->
                      terminate("failed to load application ~s:~n~p",
                                [App, Err]);
                  AppList ->
                      AppList
              end,

    %% we need a list of ERTS apps we need to ship with rabbit
    RabbitMQAppNames = lists:umerge(
                         lists:sort(sets:to_list(expand_dependencies([rabbit]))),
                         [P#plugin.name ||
                          P <- rabbit_plugins:list(PluginsDistDir, true)])
                       -- PluginAppNames,
    {ok, SslAppsConfig} = application:get_env(rabbit, ssl_apps),

    BaseApps = lists:umerge([
      lists:sort(RabbitMQAppNames),
      lists:sort(SslAppsConfig),
      lists:sort(AllApps -- PluginAppNames)]),

    AppVersions = [determine_version(App) || App <- BaseApps],
    RabbitVersion = proplists:get_value(rabbit, AppVersions),

    %% Build the overall release descriptor
    RDesc = {release,
             {"rabbit", RabbitVersion},
             {erts, erlang:system_info(version)},
             AppVersions},

    %% Write it out to $RABBITMQ_PLUGINS_EXPAND_DIR/rabbit.rel
    rabbit_file:write_file(RootName ++ ".rel", io_lib:format("~p.~n", [RDesc])),

    %% Compile the script
    systools:make_script(RootName),
    systools:script2boot(RootName),
    %% Make release tarfile
    make_tar(RootName, RabbitHome),
    rabbit_misc:quit(0).

make_tar(Release, RabbitHome) ->
    systools:make_tar(Release,
                      [
                       {dirs,
                        [docs, escript, etc, include, plugins, sbin, share]},
                       {erts, code:root_dir()},
                       {outdir, RabbitHome}
                      ]).

determine_version(App) ->
    application:load(App),
    {ok, Vsn} = application:get_key(App, vsn),
    {App, Vsn}.

delete_recursively(Fn) ->
    case rabbit_file:recursive_delete([Fn]) of
        ok                 -> ok;
        {error, {Path, E}} -> {error, {cannot_delete, Path, E}};
        Error              -> Error
    end.

prepare_plugins(PluginsDistDir, DestDir) ->
    %% Eliminate the contents of the destination directory
    case delete_recursively(DestDir) of
        ok         -> ok;
        {error, E} -> terminate("Could not delete dir ~s (~p)", [DestDir, E])
    end,
    case filelib:ensure_dir(DestDir ++ "/") of
        ok          -> ok;
        {error, E2} -> terminate("Could not create dir ~s (~p)", [DestDir, E2])
    end,

    [prepare_plugin(Plugin, DestDir) ||
        Plugin <- rabbit_plugins:list(PluginsDistDir, true)].

prepare_plugin(#plugin{type = ez, location = Location}, PluginDestDir) ->
    zip:unzip(Location, [{cwd, PluginDestDir}]);
prepare_plugin(#plugin{type = dir, name = Name, location = Location},
               PluginsDestDir) ->
    rabbit_file:recursive_copy(Location,
                               filename:join([PluginsDestDir, Name])).

expand_dependencies(Pending) ->
    expand_dependencies(sets:new(), Pending).
expand_dependencies(Current, []) ->
    Current;
expand_dependencies(Current, [Next|Rest]) ->
    case sets:is_element(Next, Current) of
        true ->
            expand_dependencies(Current, Rest);
        false ->
            case application:load(Next) of
                ok ->
                    ok;
                {error, {already_loaded, _}} ->
                    ok;
                {error, Reason} ->
                    throw({failed_to_load_app, Next, Reason})
            end,
            {ok, Required} = application:get_key(Next, applications),
            Unique = [A || A <- Required, not(sets:is_element(A, Current))],
            expand_dependencies(sets:add_element(Next, Current), Rest ++ Unique)
    end.

add_plugins_to_path(PluginDir) ->
    [add_plugin_to_path(PluginName) ||
        PluginName <- filelib:wildcard(PluginDir ++ "/*/ebin/*.app")].

add_plugin_to_path(PluginAppDescFn) ->
    %% Add the plugin ebin directory to the load path
    PluginEBinDirN = filename:dirname(PluginAppDescFn),
    code:add_path(PluginEBinDirN).

terminate(Fmt, Args) ->
    io:format("ERROR: " ++ Fmt ++ "~n", Args),
    rabbit_misc:quit(?ERROR_CODE).
