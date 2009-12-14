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
%% Specs
%%----------------------------------------------------------------------------

-ifdef(use_specs).

-spec(start/0 :: () -> no_return()).
-spec(stop/0 :: () -> 'ok').

-endif.

%%----------------------------------------------------------------------------

start() ->
    %% Ensure Rabbit is loaded so we can access it's environment
    application:load(rabbit),

    %% Determine our various directories
    PluginDir         = get_env(plugins_dir,        ?DefaultPluginDir),
    UnpackedPluginDir = get_env(plugins_expand_dir, ?DefaultUnpackedPluginDir),
    RabbitEBin        = get_env(rabbit_ebin,        ?DefaultRabbitEBin),

    RootName = RabbitEBin ++ "/rabbit",

    %% Unpack any .ez plugins
    unpack_ez_plugins(PluginDir, UnpackedPluginDir),

    %% Build a list of required apps based on the fixed set, and any plugins
    RequiredApps = ?BaseApps ++
        find_plugins(PluginDir) ++
        find_plugins(UnpackedPluginDir),

    %% Build the entire set of dependencies - this will load the
    %% applications along the way
    AllApps = case catch sets:to_list(expand_dependencies(RequiredApps)) of
                  {failed_to_load_app, App, Err} ->
                      error("failed to load application ~s:~n~p", [App, Err]);
                  AppList ->
                      AppList
              end,
    AppVersions = [determine_version(App) || App <- AllApps],
    {rabbit, RabbitVersion} = proplists:lookup(rabbit, AppVersions),

    %% Build the overall release descriptor
    RDesc = {release,
             {"rabbit", RabbitVersion},
             {erts, erlang:system_info(version)},
             AppVersions},

    %% Write it out to ebin/rabbit.rel
    file:write_file(RootName ++ ".rel", io_lib:format("~p.~n", [RDesc])),

    %% Compile the script
    ScriptFile = RootName ++ ".script",
    case systools:make_script(RootName, [local, silent]) of
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
                                            WApp == os_mon -> false;
                                     _ -> true
                                 end]),
            case length(WarningStr) of
                0 -> ok;
                _ -> io:format("~s", [WarningStr])
            end,
            ok;
        {error, Module, Error} ->
            error("generation of boot script file ~s failed:~n~s",
                  [ScriptFile, Module:format_error(Error)])
    end,

    case post_process_script(ScriptFile, boot_steps(AllApps)) of
        ok -> ok;
        {error, Reason} ->
            error("post processing of boot script file ~s failed:~n~w",
                  [ScriptFile, Reason])
    end,
    case systools:script2boot(RootName) of
        ok    -> ok;
        error -> error("failed to compile boot script file ~s", [ScriptFile])
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

boot_steps(AllApps) ->
    [application:load(App) || App <- AllApps],
    Modules = lists:usort(
                lists:append([Modules
                              || {ok, Modules} <- [application:get_key(App, modules)
                                                   || App <- AllApps]])),
    UnsortedSteps =
        lists:flatmap(fun (Module) ->
                              [{{Module, FunSpec}, Attributes}
                               || {rabbit_boot_step, [{FunSpec, Attributes}]}
                                      <- Module:module_info(attributes)]
                      end, Modules),
    sort_boot_steps(UnsortedSteps).

sort_boot_steps(UnsortedSteps) ->
    G = digraph:new([acyclic]),
    [digraph:add_vertex(G, ModFunSpec, Step) || Step = {ModFunSpec, _Attrs} <- UnsortedSteps],
    lists:foreach(fun ({ModFunSpec, Attributes}) ->
                          [add_boot_step_dep(G, ModFunSpec, PostModFunSpec)
                           || {post, PostModFunSpec} <- Attributes],
                          [add_boot_step_dep(G, PreModFunSpec, ModFunSpec)
                           || {pre, PreModFunSpec} <- Attributes]
                  end, UnsortedSteps),
    SortedStepsRev = [begin
                          {ModFunSpec, Step} = digraph:vertex(G, ModFunSpec),
                          Step
                      end || ModFunSpec <- digraph_utils:topsort(G)],
    SortedSteps = lists:reverse(SortedStepsRev),
    digraph:delete(G),
    check_boot_steps(SortedSteps).

add_boot_step_dep(G, RunsSecond, RunsFirst) ->
    case digraph:add_edge(G, RunsSecond, RunsFirst) of
        {error, Reason} ->
            error("Could not add boot step dependency of ~s on ~s:~n~s",
                  [format_modfunspec(RunsSecond), format_modfunspec(RunsFirst),
                   case Reason of
                       {bad_vertex, V} ->
                           io_lib:format("Boot step not registered: ~s~n",
                                         [format_modfunspec(V)]);
                       {bad_edge, [First | Rest]} ->
                           [io_lib:format("Cyclic dependency: ~s", [format_modfunspec(First)]),
                            [io_lib:format(" depends on ~s", [format_modfunspec(Next)])
                             || Next <- Rest],
                            io_lib:format(" depends on ~s~n", [format_modfunspec(First)])]
                   end]);
        _ ->
            ok
    end.

check_boot_steps(SortedSteps) ->
    case [ModFunSpec || {ModFunSpec = {Module, {Fun, Arity}}, _} <- SortedSteps,
                        not erlang:function_exported(Module, Fun, Arity)] of
        [] ->
            SortedSteps;
        MissingFunctions ->
            error("Boot steps not exported:~s~n",
                  [[[" ", format_modfunspec(MFS)] || MFS <- MissingFunctions]])
    end.

format_modfunspec({Module, {Fun, Arity}}) ->
    lists:flatten(io_lib:format("~w:~w/~b", [Module, Fun, Arity])).

assert_dir(Dir) ->
    case filelib:is_dir(Dir) of
        true  -> ok;
        false -> ok = filelib:ensure_dir(Dir),
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
                {error, Reason} ->
                    throw({failed_to_load_app, Next, Reason})
            end,
            {ok, Required} = application:get_key(Next, applications),
            Unique = [A || A <- Required, not(sets:is_element(A, Current))],
            expand_dependencies(sets:add_element(Next, Current), Rest ++ Unique)
    end.

post_process_script(ScriptFile, BootSteps) ->
    case file:consult(ScriptFile) of
        {ok, [{script, Name, Entries}]} ->
            NewEntries = lists:flatmap(fun (Entry) ->
                                               process_entry(Entry, BootSteps)
                                       end, Entries),
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

process_entry(Entry = {apply,{application,start_boot,[stdlib,permanent]}}, _BootSteps) ->
    [Entry, {apply,{rabbit,prepare,[]}}];
process_entry(Entry = {progress, started}, BootSteps) ->
    [{apply,{rabbit,finish_boot,[BootSteps]}}, Entry];
process_entry(Entry, _BootSteps) ->
    [Entry].

error(Fmt, Args) ->
    io:format("ERROR: " ++ Fmt ++ "~n", Args),
    halt(1).
