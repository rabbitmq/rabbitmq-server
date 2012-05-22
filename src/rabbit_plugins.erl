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
%% Copyright (c) 2011-2012 VMware, Inc.  All rights reserved.
%%

-module(rabbit_plugins).
-include("rabbit.hrl").

-export([start/0, stop/0, find_plugins/1, read_enabled_plugins/1,
         lookup_plugins/2, calculate_required_plugins/2, plugin_names/1]).

-define(VERBOSE_OPT, "-v").
-define(MINIMAL_OPT, "-m").
-define(ENABLED_OPT, "-E").
-define(ENABLED_ALL_OPT, "-e").

-define(VERBOSE_DEF, {?VERBOSE_OPT, flag}).
-define(MINIMAL_DEF, {?MINIMAL_OPT, flag}).
-define(ENABLED_DEF, {?ENABLED_OPT, flag}).
-define(ENABLED_ALL_DEF, {?ENABLED_ALL_OPT, flag}).

-define(GLOBAL_DEFS, []).

-define(COMMANDS,
        [{list, [?VERBOSE_DEF, ?MINIMAL_DEF, ?ENABLED_DEF, ?ENABLED_ALL_DEF]},
         enable,
         disable]).

%%----------------------------------------------------------------------------

-ifdef(use_specs).

-spec(start/0 :: () -> no_return()).
-spec(stop/0 :: () -> 'ok').
-spec(find_plugins/1 :: (file:filename()) -> [#plugin{}]).
-spec(read_enabled_plugins/1 :: (file:filename()) -> [atom()]).
-spec(lookup_plugins/2 :: ([atom()], [#plugin{}]) -> [#plugin{}]).
-spec(calculate_required_plugins/2 :: ([atom()], [#plugin{}]) -> [atom()]).
-spec(plugin_names/1 :: ([#plugin{}]) -> [atom()]).

-endif.

%%----------------------------------------------------------------------------

start() ->
    {ok, [[PluginsFile|_]|_]} =
        init:get_argument(enabled_plugins_file),
    {ok, [[PluginsDir|_]|_]} = init:get_argument(plugins_dist_dir),
    {Command, Opts, Args} =
        case rabbit_misc:parse_arguments(?COMMANDS, ?GLOBAL_DEFS,
                                         init:get_plain_arguments())
        of
            {ok, Res}  -> Res;
            no_command -> print_error("could not recognise command", []),
                          usage()
        end,

    PrintInvalidCommandError =
        fun () ->
                print_error("invalid command '~s'",
                            [string:join([atom_to_list(Command) | Args], " ")])
        end,

    case catch action(Command, Args, Opts, PluginsFile, PluginsDir) of
        ok ->
            rabbit_misc:quit(0);
        {'EXIT', {function_clause, [{?MODULE, action, _} | _]}} ->
            PrintInvalidCommandError(),
            usage();
        {'EXIT', {function_clause, [{?MODULE, action, _, _} | _]}} ->
            PrintInvalidCommandError(),
            usage();
        {error, Reason} ->
            print_error("~p", [Reason]),
            rabbit_misc:quit(2);
        {error_string, Reason} ->
            print_error("~s", [Reason]),
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

action(list, [], Opts, PluginsFile, PluginsDir) ->
    action(list, [".*"], Opts, PluginsFile, PluginsDir);
action(list, [Pat], Opts, PluginsFile, PluginsDir) ->
    format_plugins(Pat, Opts, PluginsFile, PluginsDir);

action(enable, ToEnable0, _Opts, PluginsFile, PluginsDir) ->
    case ToEnable0 of
        [] -> throw({error_string, "Not enough arguments for 'enable'"});
        _  -> ok
    end,
    AllPlugins = find_plugins(PluginsDir),
    Enabled = read_enabled_plugins(PluginsFile),
    ImplicitlyEnabled = calculate_required_plugins(Enabled, AllPlugins),
    ToEnable = [list_to_atom(Name) || Name <- ToEnable0],
    Missing = ToEnable -- plugin_names(AllPlugins),
    case Missing of
        [] -> ok;
        _  -> throw({error_string,
                     fmt_list("The following plugins could not be found:",
                              Missing)})
    end,
    NewEnabled = lists:usort(Enabled ++ ToEnable),
    write_enabled_plugins(PluginsFile, NewEnabled),
    NewImplicitlyEnabled = calculate_required_plugins(NewEnabled, AllPlugins),
    maybe_warn_mochiweb(NewImplicitlyEnabled),
    case NewEnabled -- ImplicitlyEnabled of
        [] -> io:format("Plugin configuration unchanged.~n");
        _  -> print_list("The following plugins have been enabled:",
                         NewImplicitlyEnabled -- ImplicitlyEnabled),
              report_change()
    end;

action(disable, ToDisable0, _Opts, PluginsFile, PluginsDir) ->
    case ToDisable0 of
        [] -> throw({error_string, "Not enough arguments for 'disable'"});
        _  -> ok
    end,
    ToDisable = [list_to_atom(Name) || Name <- ToDisable0],
    Enabled = read_enabled_plugins(PluginsFile),
    AllPlugins = find_plugins(PluginsDir),
    Missing = ToDisable -- plugin_names(AllPlugins),
    case Missing of
        [] -> ok;
        _  -> print_list("Warning: the following plugins could not be found:",
                         Missing)
    end,
    ToDisableDeps = calculate_dependencies(true, ToDisable, AllPlugins),
    NewEnabled = Enabled -- ToDisableDeps,
    case length(Enabled) =:= length(NewEnabled) of
        true  -> io:format("Plugin configuration unchanged.~n");
        false -> ImplicitlyEnabled =
                     calculate_required_plugins(Enabled, AllPlugins),
                 NewImplicitlyEnabled =
                     calculate_required_plugins(NewEnabled, AllPlugins),
                 print_list("The following plugins have been disabled:",
                            ImplicitlyEnabled -- NewImplicitlyEnabled),
                 write_enabled_plugins(PluginsFile, NewEnabled),
                 report_change()
    end.

%%----------------------------------------------------------------------------

%% Get the #plugin{}s ready to be enabled.
find_plugins(PluginsDir) ->
    EZs = [{ez, EZ} || EZ <- filelib:wildcard("*.ez", PluginsDir)],
    FreeApps = [{app, App} ||
                   App <- filelib:wildcard("*/ebin/*.app", PluginsDir)],
    {Plugins, Problems} =
        lists:foldl(fun ({error, EZ, Reason}, {Plugins1, Problems1}) ->
                            {Plugins1, [{EZ, Reason} | Problems1]};
                        (Plugin = #plugin{}, {Plugins1, Problems1}) ->
                            {[Plugin|Plugins1], Problems1}
                    end, {[], []},
                    [get_plugin_info(PluginsDir, Plug) ||
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
        {ok, Ts, _} = erl_scan:string(binary_to_list(Bin)),
        {ok, Term} = erl_parse:parse_term(Ts),
        Term
    catch
        Err -> {error, {invalid_app, Err}}
    end.

%% Pretty print a list of plugins.
format_plugins(Pattern, Opts, PluginsFile, PluginsDir) ->
    Verbose = proplists:get_bool(?VERBOSE_OPT, Opts),
    Minimal = proplists:get_bool(?MINIMAL_OPT, Opts),
    Format = case {Verbose, Minimal} of
                 {false, false} -> normal;
                 {true,  false} -> verbose;
                 {false, true}  -> minimal;
                 {true,  true}  -> throw({error_string,
                                          "Cannot specify -m and -v together"})
             end,
    OnlyEnabled    = proplists:get_bool(?ENABLED_OPT,     Opts),
    OnlyEnabledAll = proplists:get_bool(?ENABLED_ALL_OPT, Opts),

    AvailablePlugins = find_plugins(PluginsDir),
    EnabledExplicitly = read_enabled_plugins(PluginsFile),
    EnabledImplicitly =
        calculate_required_plugins(EnabledExplicitly, AvailablePlugins) --
        EnabledExplicitly,
    {ok, RE} = re:compile(Pattern),
    Plugins = [ Plugin ||
                  Plugin = #plugin{name = Name} <- AvailablePlugins,
                  re:run(atom_to_list(Name), RE, [{capture, none}]) =:= match,
                  if OnlyEnabled    ->  lists:member(Name, EnabledExplicitly);
                     OnlyEnabledAll -> (lists:member(Name, EnabledExplicitly) or
                                        lists:member(Name, EnabledImplicitly));
                     true           -> true
                  end],
    Plugins1 = usort_plugins(Plugins),
    MaxWidth = lists:max([length(atom_to_list(Name)) ||
                             #plugin{name = Name} <- Plugins1] ++ [0]),
    [format_plugin(P, EnabledExplicitly, EnabledImplicitly, Format,
                   MaxWidth) || P <- Plugins1],
    ok.

format_plugin(#plugin{name = Name, version = Version,
                      description = Description, dependencies = Deps},
              EnabledExplicitly, EnabledImplicitly, Format, MaxWidth) ->
    Glyph = case {lists:member(Name, EnabledExplicitly),
                  lists:member(Name, EnabledImplicitly)} of
                {true, false} -> "[E]";
                {false, true} -> "[e]";
                _             -> "[ ]"
            end,
    case Format of
        minimal -> io:format("~s~n", [Name]);
        normal  -> io:format("~s ~-" ++ integer_to_list(MaxWidth) ++
                                 "w ~s~n", [Glyph, Name, Version]);
        verbose -> io:format("~s ~w~n", [Glyph, Name]),
                   io:format("    Version:    \t~s~n", [Version]),
                   case Deps of
                       [] -> ok;
                       _  -> io:format("    Dependencies:\t~p~n", [Deps])
                   end,
                   io:format("    Description:\t~s~n", [Description]),
                   io:format("~n")
    end.

print_list(Header, Plugins) ->
    io:format(fmt_list(Header, Plugins)).

fmt_list(Header, Plugins) ->
    lists:flatten(
      [Header, $\n, [io_lib:format("  ~s~n", [P]) || P <- Plugins]]).

usort_plugins(Plugins) ->
    lists:usort(fun plugins_cmp/2, Plugins).

plugins_cmp(#plugin{name = N1, version = V1},
            #plugin{name = N2, version = V2}) ->
    {N1, V1} =< {N2, V2}.

%% Filter out applications that can be loaded *right now*.
filter_applications(Applications) ->
    [Application || Application <- Applications,
                    not is_available_app(Application)].

%% Return whether is application is already available (and hence
%% doesn't need enabling).
is_available_app(Application) ->
    case application:load(Application) of
        {error, {already_loaded, _}} -> true;
        ok                           -> application:unload(Application),
                                        true;
        _                            -> false
    end.

%% Return the names of the given plugins.
plugin_names(Plugins) ->
    [Name || #plugin{name = Name} <- Plugins].

%% Find plugins by name in a list of plugins.
lookup_plugins(Names, AllPlugins) ->
    [P || P = #plugin{name = Name} <- AllPlugins, lists:member(Name, Names)].

%% Read the enabled plugin names from disk.
read_enabled_plugins(PluginsFile) ->
    case rabbit_file:read_term_file(PluginsFile) of
        {ok, [Plugins]} -> Plugins;
        {ok, []}        -> [];
        {ok, [_|_]}     -> throw({error, {malformed_enabled_plugins_file,
                                          PluginsFile}});
        {error, enoent} -> [];
        {error, Reason} -> throw({error, {cannot_read_enabled_plugins_file,
                                          PluginsFile, Reason}})
    end.

%% Write the enabled plugin names on disk.
write_enabled_plugins(PluginsFile, Plugins) ->
    case rabbit_file:write_term_file(PluginsFile, [Plugins]) of
        ok              -> ok;
        {error, Reason} -> throw({error, {cannot_write_enabled_plugins_file,
                                          PluginsFile, Reason}})
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

maybe_warn_mochiweb(Enabled) ->
    V = erlang:system_info(otp_release),
    case lists:member(mochiweb, Enabled) andalso V < "R13B01" of
        true ->
            Stars = string:copies("*", 80),
            io:format("~n~n~s~n"
                      "  Warning: Mochiweb enabled and Erlang version ~s "
                      "detected.~n"
                      "  Enabling plugins that depend on Mochiweb is not "
                      "supported on this Erlang~n"
                      "  version. At least R13B01 is required.~n~n"
                      "  RabbitMQ will not start successfully in this "
                      "configuration. You *must*~n"
                      "  disable the Mochiweb plugin, or upgrade Erlang.~n"
                      "~s~n~n~n", [Stars, V, Stars]);
        false ->
            ok
    end.

report_change() ->
    io:format("Plugin configuration has changed. "
              "Restart RabbitMQ for changes to take effect.~n"),
    case os:type() of
        {win32, _OsName} ->
             io:format("If you have RabbitMQ running as a service then you must"
                       " reinstall by running~n  rabbitmq-service.bat stop~n"
                       "  rabbitmq-service.bat install~n"
                       "  rabbitmq-service.bat start~n~n");
        _ ->
             ok
    end.
