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
%% Copyright (c) 2007-2012 VMware, Inc.  All rights reserved.
%%

-module(rabbit_prelaunch).

-export([start/0, stop/0]).

-include("rabbit.hrl").

-define(BaseApps, [rabbit]).
-define(ERROR_CODE, 1).

%%----------------------------------------------------------------------------
%% Specs
%%----------------------------------------------------------------------------

-ifdef(use_specs).

-spec(start/0 :: () -> no_return()).
-spec(stop/0 :: () -> 'ok').
%% Shut dialyzer up
-spec(terminate/1 :: (string()) -> no_return()).
-spec(terminate/2 :: (string(), [any()]) -> no_return()).

-endif.

%%----------------------------------------------------------------------------

start() ->
    io:format("Activating RabbitMQ plugins ...~n"),

    %% Determine our various directories
    [EnabledPluginsFile, PluginsDistDir, UnpackedPluginDir, NodeStr] =
        init:get_plain_arguments(),
    RootName = UnpackedPluginDir ++ "/rabbit",

    prepare_plugins(EnabledPluginsFile, PluginsDistDir, UnpackedPluginDir),

    %% Build a list of required apps based on the fixed set, and any plugins
    PluginApps = find_plugins(UnpackedPluginDir),
    RequiredApps = ?BaseApps ++ PluginApps,

    %% Build the entire set of dependencies - this will load the
    %% applications along the way
    AllApps = case catch sets:to_list(expand_dependencies(RequiredApps)) of
                  {failed_to_load_app, App, Err} ->
                      terminate("failed to load application ~s:~n~p",
                                [App, Err]);
                  AppList ->
                      AppList
              end,
    AppVersions = [determine_version(App) || App <- AllApps],
    RabbitVersion = proplists:get_value(rabbit, AppVersions),

    %% Build the overall release descriptor
    RDesc = {release,
             {"rabbit", RabbitVersion},
             {erts, erlang:system_info(version)},
             AppVersions},

    %% Write it out to $RABBITMQ_PLUGINS_EXPAND_DIR/rabbit.rel
    rabbit_file:write_file(RootName ++ ".rel", io_lib:format("~p.~n", [RDesc])),

    %% We exclude mochiweb due to its optional use of fdsrv.
    XRefExclude = [mochiweb],

    %% Compile the script
    ScriptFile = RootName ++ ".script",
    case systools:make_script(RootName, [local, silent,
                                         {exref, AllApps -- XRefExclude}]) of
        {ok, Module, Warnings} ->
            %% This gets lots of spurious no-source warnings when we
            %% have .ez files, so we want to supress them to prevent
            %% hiding real issues. On Ubuntu, we also get warnings
            %% about kernel/stdlib sources being out of date, which we
            %% also ignore for the same reason.
            WarningStr = Module:format_warning(
                           [W || W <- Warnings,
                                 case W of
                                     {warning, {source_not_found, _}} -> false;
                                     {warning, {obj_out_of_date, {_,_,WApp,_,_}}}
                                       when WApp == mnesia;
                                            WApp == stdlib;
                                            WApp == kernel;
                                            WApp == sasl;
                                            WApp == crypto;
                                            WApp == os_mon -> false;
                                     _ -> true
                                 end]),
            case length(WarningStr) of
                0 -> ok;
                _ -> S = string:copies("*", 80),
                     io:format("~n~s~n~s~s~n~n", [S, WarningStr, S])
            end,
            ok;
        {error, Module, Error} ->
            terminate("generation of boot script file ~s failed:~n~s",
                      [ScriptFile, Module:format_error(Error)])
    end,

    case post_process_script(ScriptFile) of
        ok -> ok;
        {error, Reason} ->
            terminate("post processing of boot script file ~s failed:~n~w",
                      [ScriptFile, Reason])
    end,
    case systools:script2boot(RootName) of
        ok    -> ok;
        error -> terminate("failed to compile boot script file ~s",
                           [ScriptFile])
    end,
    io:format("~w plugins activated:~n", [length(PluginApps)]),
    [io:format("* ~s-~s~n", [App, proplists:get_value(App, AppVersions)])
     || App <- PluginApps],
    io:nl(),

    ok = duplicate_node_check(NodeStr),

    terminate(0),
    ok.

stop() ->
    ok.

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

prepare_plugins(EnabledPluginsFile, PluginsDistDir, DestDir) ->
    AllPlugins = rabbit_plugins:find_plugins(PluginsDistDir),
    Enabled = rabbit_plugins:read_enabled_plugins(EnabledPluginsFile),
    ToUnpack = rabbit_plugins:calculate_required_plugins(Enabled, AllPlugins),
    ToUnpackPlugins = rabbit_plugins:lookup_plugins(ToUnpack, AllPlugins),

    Missing = Enabled -- rabbit_plugins:plugin_names(ToUnpackPlugins),
    case Missing of
        [] -> ok;
        _  -> io:format("Warning: the following enabled plugins were "
                        "not found: ~p~n", [Missing])
    end,

    %% Eliminate the contents of the destination directory
    case delete_recursively(DestDir) of
        ok         -> ok;
        {error, E} -> terminate("Could not delete dir ~s (~p)", [DestDir, E])
    end,
    case filelib:ensure_dir(DestDir ++ "/") of
        ok          -> ok;
        {error, E2} -> terminate("Could not create dir ~s (~p)", [DestDir, E2])
    end,

    [prepare_plugin(Plugin, DestDir) || Plugin <- ToUnpackPlugins].

prepare_plugin(#plugin{type = ez, location = Location}, PluginDestDir) ->
    zip:unzip(Location, [{cwd, PluginDestDir}]);
prepare_plugin(#plugin{type = dir, name = Name, location = Location},
               PluginsDestDir) ->
    rabbit_file:recursive_copy(Location,
                               filename:join([PluginsDestDir, Name])).

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
                {error, Reason} ->
                    throw({failed_to_load_app, Next, Reason})
            end,
            {ok, Required} = application:get_key(Next, applications),
            Unique = [A || A <- Required, not(sets:is_element(A, Current))],
            expand_dependencies(sets:add_element(Next, Current), Rest ++ Unique)
    end.

post_process_script(ScriptFile) ->
    case file:consult(ScriptFile) of
        {ok, [{script, Name, Entries}]} ->
            NewEntries = lists:flatmap(fun process_entry/1, Entries),
            case file:open(ScriptFile, [write]) of
                {ok, Fd} ->
                    io:format(Fd, "%% script generated at ~w ~w~n~p.~n",
                              [date(), time(), {script, Name, NewEntries}]),
                    file:close(Fd),
                    ok;
                {error, OReason} ->
                    {error, {failed_to_open_script_file_for_writing, OReason}}
            end;
        {error, Reason} ->
            {error, {failed_to_load_script, Reason}}
    end.

process_entry(Entry = {apply,{application,start_boot,[mnesia,permanent]}}) ->
    [{apply,{rabbit,maybe_hipe_compile,[]}},
     {apply,{rabbit,prepare,[]}}, Entry];
process_entry(Entry) ->
    [Entry].

%% Check whether a node with the same name is already running
duplicate_node_check([]) ->
    %% Ignore running node while installing windows service
    ok;
duplicate_node_check(NodeStr) ->
    Node = rabbit_nodes:make(NodeStr),
    {NodeName, NodeHost} = rabbit_nodes:parts(Node),
    case rabbit_nodes:names(NodeHost) of
        {ok, NamePorts}  ->
            case proplists:is_defined(NodeName, NamePorts) of
                true -> io:format("node with name ~p "
                                  "already running on ~p~n",
                                  [NodeName, NodeHost]),
                        io:format(rabbit_nodes:diagnostics([Node]) ++ "~n"),
                        terminate(?ERROR_CODE);
                false -> ok
            end;
        {error, EpmdReason} ->
            terminate("epmd error for host ~p: ~p (~s)~n",
                      [NodeHost, EpmdReason,
                       case EpmdReason of
                           address -> "unable to establish tcp connection";
                           timeout -> "timed out establishing tcp connection";
                           _       -> inet:format_error(EpmdReason)
                       end])
    end.

terminate(Fmt, Args) ->
    io:format("ERROR: " ++ Fmt ++ "~n", Args),
    terminate(?ERROR_CODE).

terminate(Status) ->
    case os:type() of
        {unix,  _} -> halt(Status);
        {win32, _} -> init:stop(Status),
                      receive
                      after infinity -> ok
                      end
    end.
