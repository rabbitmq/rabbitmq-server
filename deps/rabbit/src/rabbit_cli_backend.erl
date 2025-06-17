-module(rabbit_cli_backend).

-include_lib("kernel/include/logger.hrl").

-include_lib("rabbit_common/include/logging.hrl").
-include_lib("rabbit_common/include/resource.hrl").

-include("src/rabbit_cli_backend.hrl").

-export([final_argparse_def/0, run_command/1]).

%% -------------------------------------------------------------------
%% Commands discovery.
%% -------------------------------------------------------------------

final_argparse_def() ->
    #{argparse_def := ArgparseDef} = get_discovered_commands(),
    ArgparseDef.

get_discovered_commands() ->
    Key = {?MODULE, discovered_commands},
    try
        persistent_term:get(Key)
    catch
        error:badarg ->
            Commands = discover_commands(),
            ArgparseDef = commands_to_cli_argparse_def(Commands),
            Cache = #{commands => Commands,
                      argparse_def => ArgparseDef},
            persistent_term:put(Key, Cache),
            Cache
    end.

discover_commands() ->
    %% Extract the commands from module attributes like feature flags and boot
    %% steps.
    ?LOG_DEBUG(
      "Commands: query commands in loaded applications",
      #{domain => ?RMQLOG_DOMAIN_CMD}),
    T0 = erlang:monotonic_time(),
    AttrsPerApp = rabbit_misc:rabbitmq_related_module_attributes(
                    rabbitmq_command),
    T1 = erlang:monotonic_time(),
    ?LOG_DEBUG(
      "Commands: time to find supported commands: ~tp us",
      [erlang:convert_time_unit(T1 - T0, native, microsecond)],
      #{domain => ?RMQLOG_DOMAIN_CMD}),
    AttrsPerApp.

commands_to_cli_argparse_def(Commands) ->
    lists:foldl(
      fun({_App, _Mod, Entries}, Acc0) ->
              lists:foldl(
                fun
                    ({#{cli := Path}, Def}, Acc1) ->
                        Def1 = expand_argparse_def(Def),
                        M1 = lists:foldr(
                               fun
                                   (Cmd, undefined) ->
                                       #{commands => #{Cmd => Def1}};
                                   (Cmd, M0) ->
                                       #{commands => #{Cmd => M0}}
                               end, undefined, Path),
                        rabbit_cli:merge_argparse_def(Acc1, M1);
                    (_, Acc1) ->
                        Acc1
                end, Acc0, Entries)
      end, #{}, Commands).

expand_argparse_def(Def) when is_map(Def) ->
    Def;
expand_argparse_def(Defs) when is_list(Defs) ->
    lists:foldl(
      fun
          (argparse_def_record_stream, Acc) ->
              Def = rabbit_cli_io:argparse_def(record_stream),
              rabbit_cli:merge_argparse_def(Acc, Def);
          (argparse_def_file_input, Acc) ->
              Def = rabbit_cli_io:argparse_def(file_input),
              rabbit_cli:merge_argparse_def(Acc, Def);
          (Def, Acc) ->
              Def1 = expand_argparse_def(Def),
              rabbit_cli:merge_argparse_def(Acc, Def1)
      end, #{}, Defs).

%% -------------------------------------------------------------------
%% Commands execution.
%% -------------------------------------------------------------------

run_command(ContextMap) ->
    Context = map_to_context(ContextMap),
    do_run_command(Context).

do_run_command(
  #rabbit_cli{command = #{handler := {Module, Function}}} = Context) ->
    erlang:apply(Module, Function, [Context]).

map_to_context(ContextMap) ->
    #rabbit_cli{progname = maps:get(progname, ContextMap),
                args = maps:get(args, ContextMap),
                argparse_def = maps:get(argparse_def, ContextMap),
                arg_map = maps:get(arg_map, ContextMap),
                cmd_path = maps:get(cmd_path, ContextMap),
                command = maps:get(command, ContextMap)}.
