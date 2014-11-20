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
%% Copyright (c) 2011-2014 GoPivotal, Inc.  All rights reserved.
%%

-module(rabbit_plugins_main).
-include("rabbit.hrl").
-include("rabbit_cli.hrl").

-export([start/0, stop/0, action/6]).

-define(GLOBAL_DEFS(Node), [?NODE_DEF(Node)]).

-define(COMMANDS,
        [{list, [?VERBOSE_DEF, ?MINIMAL_DEF, ?ENABLED_DEF, ?ENABLED_ALL_DEF]},
         {enable, [?OFFLINE_DEF, ?ONLINE_DEF]},
         {disable, [?OFFLINE_DEF, ?ONLINE_DEF]},
         {set, [?OFFLINE_DEF, ?ONLINE_DEF]},
         {sync, []}]).

%%----------------------------------------------------------------------------

-ifdef(use_specs).

-spec(start/0 :: () -> no_return()).
-spec(stop/0 :: () -> 'ok').

-endif.

%%----------------------------------------------------------------------------

-record(cli, {file, dir, all, enabled, implicit}).


start() ->
    {ok, [[PluginsFile|_]|_]} = init:get_argument(enabled_plugins_file),
    {ok, [[PluginsDir |_]|_]} = init:get_argument(plugins_dist_dir),
    rabbit_cli:main(
      fun (Args, NodeStr) ->
              parse_arguments(Args, NodeStr)
      end,
      fun (Command, Node, Args, Opts) ->
              action(Command, Node, Args, Opts, PluginsFile, PluginsDir)
      end, rabbit_plugins_usage).

stop() ->
    ok.

%%----------------------------------------------------------------------------

parse_arguments(CmdLine, NodeStr) ->
    rabbit_cli:parse_arguments(
      ?COMMANDS, ?GLOBAL_DEFS(NodeStr), ?NODE_OPT, CmdLine).

action(Command, Node, Args, Opts, PluginsFile, PluginsDir) ->
    All = rabbit_plugins:list(PluginsDir),
    Enabled = rabbit_plugins:read_enabled(PluginsFile),
    case Enabled -- plugin_names(All) of
        []      -> ok;
        Missing -> io:format("WARNING - plugins currently enabled but "
                             "missing: ~p~n~n", [Missing])
    end,
    Implicit = rabbit_plugins:dependencies(false, Enabled, All),
    State = #cli{file     = PluginsFile,
                 dir      = PluginsDir,
                 all      = All,
                 enabled  = Enabled,
                 implicit = Implicit},
    action(Command, Node, Args, Opts, State).

action(list, Node, [], Opts, State) ->
    action(list, Node, [".*"], Opts, State);
action(list, Node, [Pat], Opts, State) ->
    format_plugins(Node, Pat, Opts, State);

action(enable, Node, ToEnable0, Opts, State = #cli{all      = All,
                                                   implicit = Implicit,
                                                   enabled  = Enabled}) ->
    case ToEnable0 of
        [] -> throw({error_string, "Not enough arguments for 'enable'"});
        _  -> ok
    end,
    ToEnable = [list_to_atom(Name) || Name <- ToEnable0],
    Missing = ToEnable -- plugin_names(All),
    case Missing of
        [] -> ok;
        _  -> throw({error_string, fmt_missing(Missing)})
    end,
    NewEnabled = lists:usort(Enabled ++ ToEnable),
    NewImplicit = write_enabled_plugins(NewEnabled, State),
    case NewEnabled -- Implicit of
        [] -> io:format("Plugin configuration unchanged.~n");
        _  -> print_list("The following plugins have been enabled:",
                         NewImplicit -- Implicit)
    end,
    action_change(Opts, Node, Implicit, NewImplicit, State);

action(set, Node, NewEnabled0, Opts, State = #cli{all      = All,
                                                  implicit = Implicit}) ->
    NewEnabled = [list_to_atom(Name) || Name <- NewEnabled0],
    Missing = NewEnabled -- plugin_names(All),
    case Missing of
        [] -> ok;
        _  -> throw({error_string, fmt_missing(Missing)})
    end,
    NewImplicit = write_enabled_plugins(NewEnabled, State),
    case NewImplicit of
        [] -> io:format("All plugins are now disabled.~n");
        _  -> print_list("The following plugins are now enabled:",
                         NewImplicit)
    end,
    action_change(Opts, Node, Implicit, NewImplicit, State);

action(disable, Node, ToDisable0, Opts, State = #cli{all      = All,
                                                     implicit = Implicit,
                                                     enabled  = Enabled}) ->
    case ToDisable0 of
        [] -> throw({error_string, "Not enough arguments for 'disable'"});
        _  -> ok
    end,
    ToDisable = [list_to_atom(Name) || Name <- ToDisable0],
    Missing = ToDisable -- plugin_names(All),
    case Missing of
        [] -> ok;
        _  -> print_list("Warning: the following plugins could not be found:",
                         Missing)
    end,
    ToDisableDeps = rabbit_plugins:dependencies(true, ToDisable, All),
    NewEnabled = Enabled -- ToDisableDeps,
    NewImplicit = write_enabled_plugins(NewEnabled, State),
    case length(Enabled) =:= length(NewEnabled) of
        true  -> io:format("Plugin configuration unchanged.~n");
        false -> print_list("The following plugins have been disabled:",
                            Implicit -- NewImplicit)
    end,
    action_change(Opts, Node, Implicit, NewImplicit, State);

action(sync, Node, [], _Opts, State) ->
    sync(Node, true, State).

%%----------------------------------------------------------------------------

%% Pretty print a list of plugins.
format_plugins(Node, Pattern, Opts, #cli{all      = All,
                                         enabled  = Enabled,
                                         implicit = Implicit}) ->
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

    EnabledImplicitly = Implicit -- Enabled,
    {StatusMsg, Running} =
        case rabbit_cli:rpc_call(Node, rabbit_plugins, active, []) of
            {badrpc, _} -> {"[failed to contact ~s - status not shown]", []};
            Active      -> {"* = running on ~s", Active}
        end,
    {ok, RE} = re:compile(Pattern),
    Plugins = [ Plugin ||
                  Plugin = #plugin{name = Name} <- All,
                  re:run(atom_to_list(Name), RE, [{capture, none}]) =:= match,
                  if OnlyEnabled    -> lists:member(Name, Enabled);
                     OnlyEnabledAll -> lists:member(Name, Enabled) or
                                           lists:member(Name,EnabledImplicitly);
                     true           -> true
                  end],
    Plugins1 = usort_plugins(Plugins),
    MaxWidth = lists:max([length(atom_to_list(Name)) ||
                             #plugin{name = Name} <- Plugins1] ++ [0]),
    case Format of
        minimal -> ok;
        _       -> io:format(" Configured: E = explicitly enabled; "
                             "e = implicitly enabled~n"
                             " | Status:   ~s~n"
                             " |/~n", [rabbit_misc:format(StatusMsg, [Node])])
    end,
    [format_plugin(P, Enabled, EnabledImplicitly, Running,
                   Format, MaxWidth) || P <- Plugins1],
    ok.

format_plugin(#plugin{name = Name, version = Version,
                      description = Description, dependencies = Deps},
              Enabled, EnabledImplicitly, Running, Format,
              MaxWidth) ->
    EnabledGlyph = case {lists:member(Name, Enabled),
                         lists:member(Name, EnabledImplicitly)} of
                       {true, false} -> "E";
                       {false, true} -> "e";
                       _             -> " "
                   end,
    RunningGlyph = case lists:member(Name, Running) of
                       true  -> "*";
                       false -> " "
                   end,
    Glyph = rabbit_misc:format("[~s~s]", [EnabledGlyph, RunningGlyph]),
    Opt = fun (_F, A, A) -> ok;
              ( F, A, _) -> io:format(F, [A])
          end,
    case Format of
        minimal -> io:format("~s~n", [Name]);
        normal  -> io:format("~s ~-" ++ integer_to_list(MaxWidth) ++ "w ",
                             [Glyph, Name]),
                   Opt("~s", Version, undefined),
                   io:format("~n");
        verbose -> io:format("~s ~w~n", [Glyph, Name]),
                   Opt("     Version:     \t~s~n", Version,     undefined),
                   Opt("     Dependencies:\t~p~n", Deps,        []),
                   Opt("     Description: \t~s~n", Description, undefined),
                   io:format("~n")
    end.

print_list(Header, Plugins) ->
    io:format(fmt_list(Header, Plugins)).

fmt_list(Header, Plugins) ->
    lists:flatten(
      [Header, $\n, [io_lib:format("  ~s~n", [P]) || P <- Plugins]]).

fmt_missing(Missing) ->
    fmt_list("The following plugins could not be found:", Missing).

usort_plugins(Plugins) ->
    lists:usort(fun plugins_cmp/2, Plugins).

plugins_cmp(#plugin{name = N1, version = V1},
            #plugin{name = N2, version = V2}) ->
    {N1, V1} =< {N2, V2}.

%% Return the names of the given plugins.
plugin_names(Plugins) ->
    [Name || #plugin{name = Name} <- Plugins].

%% Write the enabled plugin names on disk.
write_enabled_plugins(Plugins, #cli{file = File,
                                    all  = All}) ->
    case rabbit_file:write_term_file(File, [Plugins]) of
        ok              -> rabbit_plugins:dependencies(false, Plugins, All);
        {error, Reason} -> throw({error, {cannot_write_enabled_plugins_file,
                                          File, Reason}})
    end.

action_change(Opts, Node, Old, New, State) ->
    action_change0(proplists:get_bool(?OFFLINE_OPT, Opts),
                   proplists:get_bool(?ONLINE_OPT, Opts),
                   Node, Old, New, State).

action_change0(true, _Online, _Node, Same, Same, _State) ->
    %% Definitely nothing to do
    ok;
action_change0(true, _Online, _Node, _Old, _New, _State) ->
    io:format("Offline change; changes will take effect at broker restart.~n");
action_change0(false, Online, Node, _Old, _New, State) ->
    sync(Node, Online, State).

sync(Node, ForceOnline, #cli{file = File}) ->
    rpc_call(Node, ForceOnline, rabbit_plugins, ensure, [File]).

rpc_call(Node, Online, Mod, Fun, Args) ->
    io:format("~nApplying plugin configuration to ~s...", [Node]),
    case rabbit_cli:rpc_call(Node, Mod, Fun, Args) of
        {ok, [], []} ->
            io:format(" nothing to do.~n", []);
        {ok, Start, []} ->
            io:format(" started ~b plugin~s.~n", [length(Start), plur(Start)]);
        {ok, [], Stop} ->
            io:format(" stopped ~b plugin~s.~n", [length(Stop), plur(Stop)]);
        {ok, Start, Stop} ->
            io:format(" stopped ~b plugin~s and started ~b plugin~s.~n",
                      [length(Stop), plur(Stop), length(Start), plur(Start)]);
        {badrpc, nodedown} = Error ->
            io:format(" failed.~n", []),
            case Online of
                true  -> Error;
                false -> io:format(
                           " * Could not contact node ~s.~n"
                           "   Changes will take effect at broker restart.~n"
                           " * Options: --online  - fail if broker cannot be "
                           "contacted.~n"
                           "            --offline - do not try to contact "
                           "broker.~n",
                           [Node])
            end;
        Error ->
            io:format(" failed.~n", []),
            Error
    end.

plur([_]) -> "";
plur(_)   -> "s".
