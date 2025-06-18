-module(rabbit_cli_backend).

-include_lib("kernel/include/logger.hrl").

-include_lib("rabbit_common/include/logging.hrl").
-include_lib("rabbit_common/include/resource.hrl").

-include("src/rabbit_cli_backend.hrl").

-export([run_command/1]).

%% TODO:
%% * Implémenter "list exchanges" plus proprement
%% * Implémenter "rabbitmqctl list_exchanges" pour la compatibilité

run_command(ContextMap) when is_map(ContextMap) ->
    Context = map_to_context(ContextMap),
    run_command(Context);
run_command(#rabbit_cli{} = Context) ->
    maybe
        %% We can query the argparse definition from the remote node to know
        %% the commands it supports and proceed with the execution.
        ArgparseDef = final_argparse_def(Context),
        Context1 = Context#rabbit_cli{argparse_def = ArgparseDef},

        {ok, ArgMap, CmdPath, Command} ?= final_parse(Context1),
        Context2 = Context1#rabbit_cli{arg_map = ArgMap,
                                       cmd_path = CmdPath,
                                       command = Command},

        do_run_command(Context2)
    end.

%% -------------------------------------------------------------------
%% Argparse definition handling.
%% -------------------------------------------------------------------

final_argparse_def(
  #rabbit_cli{argparse_def = PartialArgparseDef}) ->
    FullArgparseDef = rabbit_cli_commands:discovered_argparse_def(),
    ArgparseDef1 = merge_argparse_def(PartialArgparseDef, FullArgparseDef),
    ArgparseDef1.

merge_argparse_def(ArgparseDef1, ArgparseDef2) ->
    Args1 = maps:get(arguments, ArgparseDef1, []),
    Args2 = maps:get(arguments, ArgparseDef2, []),
    Args = merge_arguments(Args1, Args2),
    Cmds1 = maps:get(commands, ArgparseDef1, #{}),
    Cmds2 = maps:get(commands, ArgparseDef2, #{}),
    Cmds = merge_commands(Cmds1, Cmds2),
    maps:merge(
      ArgparseDef1,
      ArgparseDef2#{arguments => Args, commands => Cmds}).

merge_arguments(Args1, Args2) ->
    Args1 ++ Args2.

merge_commands(Cmds1, Cmds2) ->
    maps:merge(Cmds1, Cmds2).

final_parse(
  #rabbit_cli{progname = ProgName, args = Args, argparse_def = ArgparseDef}) ->
    Options = #{progname => ProgName},
    argparse:parse(Args, ArgparseDef, Options).

%% -------------------------------------------------------------------
%% Command execution.
%% -------------------------------------------------------------------

do_run_command(
  #rabbit_cli{command = #{handler := {Module, Function}}} = Context) ->
    erlang:apply(Module, Function, [Context]).

map_to_context(ContextMap) ->
    #rabbit_cli{scriptname = maps:get(scriptname, ContextMap),
                progname = maps:get(progname, ContextMap),
                args = maps:get(args, ContextMap),
                argparse_def = maps:get(argparse_def, ContextMap),
                arg_map = maps:get(arg_map, ContextMap),
                cmd_path = maps:get(cmd_path, ContextMap),
                command = maps:get(command, ContextMap)}.
