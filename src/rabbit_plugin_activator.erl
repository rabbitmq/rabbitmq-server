%%   The contents of this file are subject to the Mozilla Public License
%%   Version 1.1 (the "License"); you may not use this file except in
%%   compliance with the License. You may obtain a copy of the License at
%%   http://www.mozilla.org/MPL/
%%
%%   Software distributed under the License is distributed on an "AS IS"
%%   basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See the
%%   License for the specific language governing rights and limitations
%%   under the License.
%%
%%   The Original Code is RabbitMQ.
%%
%%   The Initial Developers of the Original Code are LShift Ltd,
%%   Cohesive Financial Technologies LLC, and Rabbit Technologies Ltd.
%%
%%   Portions created before 22-Nov-2008 00:00:00 GMT by LShift Ltd,
%%   Cohesive Financial Technologies LLC, or Rabbit Technologies Ltd
%%   are Copyright (C) 2007-2008 LShift Ltd, Cohesive Financial
%%   Technologies LLC, and Rabbit Technologies Ltd.
%%
%%   Portions created by LShift Ltd are Copyright (C) 2007-2009 LShift
%%   Ltd. Portions created by Cohesive Financial Technologies LLC are
%%   Copyright (C) 2007-2009 Cohesive Financial Technologies
%%   LLC. Portions created by Rabbit Technologies Ltd are Copyright
%%   (C) 2007-2009 Rabbit Technologies Ltd.
%%
%%   All Rights Reserved.
%%
%%   Contributor(s): ______________________________________.
%%

-module(rabbit_plugin_activator).

-export([start/0, stop/0]).

-define(DefaultPluginDir, "plugins").
-define(DefaultUnpackedPluginDir, "priv/plugins").
-define(DefaultRabbitEBin, "ebin").
-define(BaseApps, [rabbit]).

%%----------------------------------------------------------------------------

start() ->
    %% Ensure Rabbit is loaded so we can access it's environment
    application:load(rabbit),

    %% Determine our various directories
    PluginDir         = get_env(plugins_dir,        ?DefaultPluginDir),
    UnpackedPluginDir = get_env(plugins_expand_dir, ?DefaultUnpackedPluginDir),
    RabbitEBin        = get_env(rabbit_ebin,        ?DefaultRabbitEBin),

    %% Unpack any .ez plugins
    unpack_ez_plugins(PluginDir, UnpackedPluginDir),

    %% Build a list of required apps based on the fixed set, and any plugins
    RequiredApps = ?BaseApps ++
        find_plugins(PluginDir) ++
        find_plugins(UnpackedPluginDir),

    %% Build the entire set of dependencies - this will load the
    %% applications along the way
    AllApps = case catch sets:to_list(expand_dependencies(RequiredApps)) of
                  {unknown_app, {App, Err}} -> 
                      io:format("ERROR: Failed to load application " ++
                                "~s: ~p~n", [App, Err]),
                      halt(1);
                  AppList ->
                      AppList
              end,
    AppVersions = [determine_version(App) || App <- AllApps],
    {value, {rabbit, RabbitVersion}} = lists:keysearch(rabbit, 1, AppVersions),

    %% Build the overall release descriptor
    RDesc = {release, 
             {"rabbit", RabbitVersion}, 
             {erts, erlang:system_info(version)},
             AppVersions},

    %% Write it out to ebin/rabbit.rel
    file:write_file(RabbitEBin ++ "/rabbit.rel",
                    io_lib:format("~p.~n", [RDesc])),

    %% Compile the script
    case systools:make_script(RabbitEBin ++ "/rabbit", [local, silent]) of
        {ok, Module, Warnings} -> 
            %% This gets lots of spurious no-source warnings when we
            %% have .ez files, so we want to supress them to prevent
            %% hiding real issues.
            WarningStr = Module:format_warning(
                           [W || W <- Warnings, 
                                 case W of 
                                     {warning, {source_not_found, _}} -> false; 
                                     _                                -> true 
                                 end]),
            case length(WarningStr) of
                0 -> ok;
                _ -> io:format("~s", [WarningStr])
            end,
            ok;
        {error, Module, Error} ->
            io:format("Boot file generation failed: ~s~n",
                      [Module:format_error(Error)]),
            halt(1)
    end,
    halt(),
    ok.

stop() ->
    ok.

get_env(Key, Default) ->
    case application:get_env(rabbit, Key) of
        {ok, V} -> V;
        _       -> Default
    end.

determine_version(App) ->
    application:load(App), 
    {ok, Vsn} = application:get_key(App, vsn), 
    {App, Vsn}.

assert_dir(Dir) ->
    case filelib:is_dir(Dir) of
        true  -> ok;
        false ->
            ok = filelib:ensure_dir(Dir),
            ok = file:make_dir(Dir)
    end.
delete_dir(Dir) ->
    case filelib:is_dir(Dir) of
        true ->
            case file:list_dir(Dir) of
                {ok, Files} ->
                    [case Dir ++ "/" ++ F of
                         Fn ->
                             case filelib:is_dir(Fn) and not(is_symlink(Fn)) of
                                 true  -> delete_dir(Fn);
                                 false -> file:delete(Fn)
                             end
                     end || F <- Files]
            end,
            ok = file:del_dir(Dir);
        false ->
            ok
    end.
is_symlink(Name) ->
    case file:read_link(Name) of
        {ok, _} -> true;
        _       -> false
    end.

unpack_ez_plugins(PluginSrcDir, PluginDestDir) ->
    %% Eliminate the contents of the destination directory
    delete_dir(PluginDestDir),

    assert_dir(PluginDestDir),
    [unpack_ez_plugin(PluginName, PluginDestDir) ||
        PluginName <- filelib:wildcard(PluginSrcDir ++ "/*.ez")].

unpack_ez_plugin(PluginFn, PluginDestDir) ->
    zip:unzip(PluginFn, [{cwd, PluginDestDir}]),
    ok.

find_plugins(PluginDir) ->
    [prepare_dir_plugin(PluginName) ||
        PluginName <- filelib:wildcard(PluginDir ++ "/*/ebin/*.app")].

prepare_dir_plugin(PluginAppDescFn) ->
    %% Add the plugin ebin directory to the load path
    PluginEBinDirN = filename:dirname(PluginAppDescFn),
    code:add_path(PluginEBinDirN),

    %% We want the second-last token
    NameTokens = string:tokens(PluginAppDescFn,"/."),
    PluginNameString = lists:nth(length(NameTokens) - 1, NameTokens),
    list_to_atom(PluginNameString).

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
                X -> 
                    throw({unknown_app, {Next, X}})
            end,
            {ok, Required} = application:get_key(Next, applications),
            Unique = [A || A <- Required, not(sets:is_element(A, Current))],
            expand_dependencies(sets:add_element(Next, Current), Rest ++ Unique)
    end.
