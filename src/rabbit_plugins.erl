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
%% Copyright (c) 2011 VMware, Inc.  All rights reserved.
%%

-module(rabbit_plugins).
-include("rabbit.hrl").

-export([start/0, stop/0, find_plugins/1, read_enabled_plugins/1,
         lookup_plugins/2, calculate_required_plugins/2]).

-define(VERBOSE_OPT, "-v").
-define(ENABLED_OPT, "-E").
-define(ENABLED_ALL_OPT, "-e").

%%----------------------------------------------------------------------------

-ifdef(use_specs).

-spec(start/0 :: () -> no_return()).
-spec(stop/0 :: () -> 'ok').
-spec(find_plugins/1 :: (file:filename()) -> [#plugin{}]).
-spec(read_enabled_plugins/1 :: (file:filename()) -> [atom()]).
-spec(lookup_plugins/2 :: ([atom()], [#plugin{}]) -> [#plugin{}]).
-spec(calculate_required_plugins/2 :: ([atom()], [#plugin{}]) -> [atom()]).

-endif.

%%----------------------------------------------------------------------------

start() ->
    {ok, [[EnabledPluginsFile|_]|_]} =
        init:get_argument(enabled_plugins_file),
    put(enabled_plugins_file, EnabledPluginsFile),
    {ok, [[PluginsDistDir|_]|_]} = init:get_argument(plugins_dist_dir),
    put(plugins_dist_dir, PluginsDistDir),
    {[Command0 | Args], Opts} =
        case rabbit_misc:get_options([{flag, ?VERBOSE_OPT},
                                      {flag, ?ENABLED_OPT},
                                      {flag, ?ENABLED_ALL_OPT}],
                                     init:get_plain_arguments()) of
            {[], _Opts}    -> usage();
            CmdArgsAndOpts -> CmdArgsAndOpts
        end,
    Command = list_to_atom(Command0),

    case catch action(Command, Args, Opts) of
        ok ->
            rabbit_misc:quit(0);
        {'EXIT', {function_clause, [{?MODULE, action, _} | _]}} ->
            print_error("invalid command '~s'",
                        [string:join([atom_to_list(Command) | Args], " ")]),
            usage();
        {error, Reason} ->
            print_error("~p", [Reason]),
            rabbit_misc:quit(2);
        Other ->
            print_error("~p", [Other]),
            rabbit_misc:quit(2)
    end.

stop() ->
    ok.

print_error(Format, Args) ->
    rabbit_misc:format_stderr("Error: " ++ Format ++ "~n", Args).

usage() ->
    io:format("~s", [rabbit_plugins_usage:usage()]),
    rabbit_misc:quit(1).

%%----------------------------------------------------------------------------

action(list, [], Opts) ->
    action(list, [".*"], Opts);
action(list, [Pat], Opts) ->
    format_plugins(Pat, Opts);

action(enable, ToEnable0, _Opts) ->
    case ToEnable0 of
        [] -> throw("Not enough arguments for 'enable'");
        _  -> ok
    end,
    AllPlugins = find_plugins(),
    Enabled = read_enabled_plugins(),
    EnabledPlugins = lookup_plugins(Enabled, AllPlugins),
    ImplicitlyEnabled = calculate_required_plugins(Enabled, AllPlugins),
    ToEnable = [list_to_atom(Name) || Name <- ToEnable0],
    ToEnablePlugins = lookup_plugins(ToEnable, AllPlugins),
    Missing = ToEnable -- plugin_names(ToEnablePlugins),
    case Missing of
        [] -> ok;
        _  -> io:format("Warning: the following plugins could not be "
                        "found: ~p~n", [Missing])
    end,
    NewEnabled = plugin_names(merge_plugin_lists(EnabledPlugins,
                                                 ToEnablePlugins)),
    write_enabled_plugins(NewEnabled),
    case NewEnabled -- ImplicitlyEnabled of
        [] -> io:format("Plugin configuration unchanged.~n");
        _  -> NewImplicitlyEnabled =
                  calculate_required_plugins(NewEnabled, AllPlugins),
              io:format("The following plugins have been enabled: ~p~n",
                        [NewImplicitlyEnabled -- ImplicitlyEnabled]),
              io:format("Plugin configuration has changed. "
                        "You should restart RabbitMQ.~n")
    end;

action(disable, ToDisable0, _Opts) ->
    case ToDisable0 of
        [] -> throw("Not enough arguments for 'disable'");
        _  -> ok
    end,
    ToDisable = [list_to_atom(Name) || Name <- ToDisable0],
    Enabled = read_enabled_plugins(),
    AllPlugins = find_plugins(),
    Missing = ToDisable -- plugin_names(AllPlugins),
    case Missing of
        [] -> ok;
        _  -> io:format("Warning: the following plugins could not be "
                        "found: ~p~n", [Missing])
    end,
    ToDisable1 = ToDisable -- Missing,
    ToDisable2 = calculate_dependencies(true, ToDisable1, AllPlugins),
    NewEnabled = Enabled -- ToDisable2,
    case length(Enabled) =:= length(NewEnabled) of
        true  -> io:format("Plugin configuration unchanged.~n");
        false -> ImplicitlyEnabled =
                     calculate_required_plugins(Enabled, AllPlugins),
                 NewImplicitlyEnabled =
                     calculate_required_plugins(NewEnabled, AllPlugins),
                 io:format("The following plugins have been disabled: ~p~n",
                           [ImplicitlyEnabled -- NewImplicitlyEnabled]),
                 write_enabled_plugins(NewEnabled),
                 io:format("Plugin configuration has changed. "
                           "You should restart RabbitMQ.~n")
    end.

%%----------------------------------------------------------------------------

%% Get the #plugin{}s ready to be enabled.
find_plugins() ->
    find_plugins(get(plugins_dist_dir)).
find_plugins(PluginsDistDir) ->
    EZs = [{ez, EZ} || EZ <- filelib:wildcard("*.ez", PluginsDistDir)],
    FreeApps = [{app, App} ||
                   App <- filelib:wildcard("*/ebin/*.app", PluginsDistDir)],
    {Plugins, Problems} =
        lists:foldl(fun ({error, EZ, Reason}, {Plugins1, Problems1}) ->
                            {Plugins1, [{EZ, Reason} | Problems1]};
                        (Plugin = #plugin{}, {Plugins1, Problems1}) ->
                            {[Plugin|Plugins1], Problems1}
                    end, {[], []},
                    [get_plugin_info(PluginsDistDir, Plug) ||
                        Plug <- EZs ++ FreeApps]),
    case Problems of
        [] -> ok;
        _  -> io:format("Warning: Problem reading some plugins: ~p~n",
                        [Problems])
    end,
    Plugins.

%% Get the #plugin{} from an .ez.
get_plugin_info(Base, {ez, EZ0}) ->
    EZ = filename:join([Base, EZ0]),
    case read_app_file(EZ) of
        {application, Name, Props} -> mkplugin(Name, Props, ez, EZ);
        {error, Reason}            -> {error, EZ, Reason}
    end;
%% Get the #plugin{} from an .app.
get_plugin_info(Base, {app, App0}) ->
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

%% Read the .app file from an ez.
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

%% Return the path of the .app files in ebin/.
find_app_files(ZippedFiles) ->
    {ok, RE} = re:compile("^.*/ebin/.*.app$"),
    [Path || {zip_file, Path, _, _, _, _} <- ZippedFiles,
             re:run(Path, RE, [{capture, none}]) =:= match].

%% Parse a binary into a term.
parse_binary(Bin) ->
    try
        {ok, Ts, _} = erl_scan:string(binary:bin_to_list(Bin)),
        {ok, Term} = erl_parse:parse_term(Ts),
        Term
    catch
        Err -> {error, {invalid_app, Err}}
    end.

%% Pretty print a list of plugins.
format_plugins(Pattern, Opts) ->
    Verbose = proplists:get_bool(?VERBOSE_OPT, Opts),
    OnlyEnabled = proplists:get_bool(?ENABLED_OPT, Opts),
    OnlyEnabledAll = proplists:get_bool(?ENABLED_ALL_OPT, Opts),

    AvailablePlugins = find_plugins(),
    EnabledExplicitly = read_enabled_plugins(),
    EnabledImplicitly =
        calculate_required_plugins(EnabledExplicitly, AvailablePlugins) --
        EnabledExplicitly,
    {ok, RE} = re:compile(Pattern),
    Plugins = [ Plugin ||
                  Plugin = #plugin{name = Name} <- AvailablePlugins,
                  re:run(atom_to_list(Name), RE, [{capture, none}]) =:= match,
                  if OnlyEnabled -> lists:member(Name, EnabledExplicitly);
                     true        -> true
                  end,
                  if OnlyEnabledAll ->
                          lists:member(Name, EnabledImplicitly) or
                              lists:member(Name, EnabledExplicitly);
                     true ->
                          true
                  end],
    MaxWidth = lists:max([length(atom_to_list(Name)) ||
                             #plugin{name = Name} <- Plugins] ++ [0]),
    [ format_plugin(P, EnabledExplicitly, EnabledImplicitly, Verbose,
                    MaxWidth) || P <- Plugins],
    ok.

format_plugin(#plugin{name = Name, version = Version,
                      description = Description, dependencies = Dependencies},
              EnabledExplicitly, EnabledImplicitly, Verbose, MaxWidth) ->
    Glyph = case {lists:member(Name, EnabledExplicitly),
                  lists:member(Name, EnabledImplicitly)} of
                {true, false} -> "[E]";
                {false, true} -> "[e]";
                _             -> "[ ]"
            end,
    case Verbose of
        false ->
            io:format("~s ~-" ++ integer_to_list(MaxWidth) ++
                          "w ~s~n", [Glyph, Name, Version]);
        true ->
            io:format("~s ~w~n", [Glyph, Name]),
            io:format("    Version:    \t~s~n", [Version]),
            case Dependencies of
                [] -> ok;
                _  -> io:format("    Dependencies:\t~p~n", [Dependencies])
            end,
            io:format("    Description:\t~s~n", [Description]),
            io:format("~n")
    end.

usort_plugins(Plugins) ->
    lists:usort(fun plugins_cmp/2, Plugins).

%% Merge two plugin lists.  In case of duplicates, only keep highest
%% version.
merge_plugin_lists(Ps1, Ps2) ->
    filter_duplicates(usort_plugins(Ps1 ++ Ps2)).

filter_duplicates([P1 = #plugin{name = N, version = V1},
                   P2 = #plugin{name = N, version = V2} | Ps]) ->
    if V1 < V2 -> filter_duplicates([P2 | Ps]);
       true    -> filter_duplicates([P1 | Ps])
    end;
filter_duplicates([P | Ps]) ->
    [P | filter_duplicates(Ps)];
filter_duplicates(Ps) ->
    Ps.

plugins_cmp(#plugin{name = N1, version = V1},
            #plugin{name = N2, version = V2}) ->
    {N1, V1} =< {N2, V2}.

%% Filter applications that can be loaded *right now*.
filter_applications(Applications) ->
    [Application || Application <- Applications,
                    case application:load(Application) of
                        {error, {already_loaded, _}} -> false;
                        ok -> application:unload(Application),
                              false;
                        _  -> true
                    end].

%% Return the names of the given plugins.
plugin_names(Plugins) ->
    [Name || #plugin{name = Name} <- Plugins].

%% Find plugins by name in a list of plugins.
lookup_plugins(Names, AllPlugins) ->
    [P || P = #plugin{name = Name} <- AllPlugins, lists:member(Name, Names)].

%% Read the enabled plugin names from disk.
read_enabled_plugins() ->
    read_enabled_plugins(get(enabled_plugins_file)).

read_enabled_plugins(FileName) ->
    case rabbit_file:read_term_file(FileName) of
        {ok, [Plugins]} -> Plugins;
        {error, enoent} -> [];
        {error, Reason} -> throw({error, {cannot_read_enabled_plugins_file,
                                          FileName, Reason}})
    end.

%% Write the enabled plugin names on disk.
write_enabled_plugins(Plugins) ->
    FileName = get(enabled_plugins_file),
    case rabbit_file:write_term_file(FileName, [Plugins]) of
        ok              -> ok;
        {error, Reason} -> throw({error, {cannot_write_enabled_plugins_file,
                                          FileName, Reason}})
    end.

calculate_required_plugins(Sources, AllPlugins) ->
    calculate_dependencies(false, Sources, AllPlugins).

calculate_dependencies(Reverse, Sources, AllPlugins) ->
    {ok, G} = rabbit_misc:build_acyclic_graph(
                fun (App, _Deps) -> [{App, App}] end,
                fun (App,  Deps) -> [{App, Dep} || Dep <- Deps] end,
                [{Name, Deps}
                 || #plugin{name = Name, dependencies = Deps} <- AllPlugins]),
    Dests = case Reverse of
                false -> digraph_utils:reachable(Sources, G);
                true  -> digraph_utils:reaching(Sources, G)
            end,
    true = digraph:delete(G),
    Dests.
