-module(rabbit_cli).

-include_lib("kernel/include/logger.hrl").

-export([main/1]).

main(Args) ->
    Ret = run_cli(Args),
    io:format(standard_error, "Ret: ~p~n", [Ret]),
    erlang:halt().

run_cli(Args) ->
    maybe
        Progname = escript:script_name(),
        ok ?= add_rabbitmq_code_path(Progname),

        {ok, IO} ?= rabbit_cli_io:start_link(Progname),

        try
            do_run_cli(Progname, Args, IO)
        after
            rabbit_cli_io:stop(IO)
        end
    end.

do_run_cli(Progname, Args, IO) ->
    maybe
        PartialArgparseDef = argparse_def(),
        {ok,
         PartialArgMap,
         PartialCmdPath,
         PartialCommand} ?= initial_parse(Progname, Args, PartialArgparseDef),

        case rabbit_cli_transport:connect(PartialArgMap, IO) of
            {ok, Connection} ->
                %% We can query the argparse definition from the remote node
                %% to know the commands it supports and proceed with the
                %% execution.
                maybe
                    ArgparseDef = get_final_argparse_def(
                                    Connection, PartialArgparseDef),
                    {ok,
                     ArgMap,
                     CmdPath,
                     Command} ?= final_parse(Progname, Args, ArgparseDef),

                    run_remote_command(
                      Connection, ArgparseDef,
                      Progname, ArgMap, CmdPath, Command,
                      IO)
                end;
            {error, _} ->
                %% We can't reach the remote node. Let's fallback
                %% to a local execution.
                run_local_command(
                  PartialArgparseDef,
                  Progname, PartialArgMap, PartialCmdPath,
                  PartialCommand, IO)
        end
    end.

add_rabbitmq_code_path(Progname) ->
    ScriptDir = filename:dirname(Progname),
    PluginsDir0 = filename:join([ScriptDir, "..", "plugins"]),
    PluginsDir1 = case filelib:is_dir(PluginsDir0) of
                      true ->
                          PluginsDir0
                  end,
    Glob = filename:join([PluginsDir1, "*", "ebin"]),
    AppDirs = filelib:wildcard(Glob),
    lists:foreach(fun code:add_path/1, AppDirs),
    ok.

argparse_def() ->
    Aliases0 = #{"lx" => "list_exchanges -v",
                 "list_exchanges" => "list exchanges"},
    Aliases1 = maps:map(
                 fun(Alias, CommandStr) ->
                         #{help => <<"alias">>,
                           handler => fun(ArgMap) ->
                                              handle_alias(Alias, CommandStr, ArgMap)
                                      end}
                 end, Aliases0),
    #{arguments =>
      [
       #{name => help,
         long => "-help",
         short => $h,
         type => boolean,
         help => "Display help and exit"},
       #{name => node,
         long => "-node",
         short => $n,
         type => string,
         nargs => 1,
         help => "Name of the node to control"},
       #{name => verbose,
         long => "-verbose",
         short => $v,
         action => count,
         help =>
         "Be verbose; can be specified multiple times to increase verbosity"},
       #{name => version,
         long => "-version",
         short => $V,
         help =>
         "Display version and exit"}
      ],

      commands => Aliases1}.

initial_parse(Progname, Args, ArgparseDef) ->
    Options = #{progname => Progname},
    case partial_parse(Args, ArgparseDef, Options) of
        {ok, ArgMap, CmdPath, Command, _RemainingArgs} ->
            {ok, ArgMap, CmdPath, Command};
        {error, _} = Error->
            Error
    end.

partial_parse(Args, ArgparseDef, Options) ->
    partial_parse(Args, ArgparseDef, Options, []).

partial_parse(Args, ArgparseDef, Options, RemainingArgs) ->
    case argparse:parse(Args, ArgparseDef, Options) of
        {ok, ArgMap, CmdPath, Command} ->
            RemainingArgs1 = lists:reverse(RemainingArgs),
            {ok, ArgMap, CmdPath, Command, RemainingArgs1};
        {error, {_CmdPath, undefined, Arg, <<>>}} ->
            Args1 = Args -- [Arg],
            RemainingArgs1 = [Arg | RemainingArgs],
            partial_parse(Args1, ArgparseDef, Options, RemainingArgs1);
        {error, _} = Error ->
            Error
    end.

get_final_argparse_def(Connection, PartialArgparseDef) ->
    ArgparseDef1 = PartialArgparseDef,
    ArgparseDef2 = rabbit_cli_transport:rpc(
                     Connection, rabbit_cli_commands, argparse_def, []),
    ArgparseDef = merge_argparse_def(ArgparseDef1, ArgparseDef2),
    ArgparseDef.

merge_argparse_def(ArgparseDef1, ArgparseDef2) ->
    Args1 = maps:get(arguments, ArgparseDef1, []),
    Args2 = maps:get(arguments, ArgparseDef2, []),
    Args = merge_arguments(Args1, Args2),
    Cmds1 = maps:get(commands, ArgparseDef1, #{}),
    Cmds2 = maps:get(commands, ArgparseDef2, #{}),
    Cmds = merge_commands(Cmds1, Cmds2),
    maps:merge(ArgparseDef1, ArgparseDef2#{arguments => Args, commands => Cmds}).

merge_arguments(Args1, Args2) ->
    Args1 ++ Args2.

merge_commands(Cmds1, Cmds2) ->
    maps:merge(Cmds1, Cmds2).

final_parse(Progname, Args, ArgparseDef) ->
    Options = #{progname => Progname},
    argparse:parse(Args, ArgparseDef, Options).

run_remote_command(
  Connection, ArgparseDef, Progname, ArgMap, _CmdPath,
  #{help := <<"alias">>} = Command,
  IO) ->
    {ok, ArgMap1, CmdPath1, Command1} = expand_alias(
                                          ArgparseDef, Progname, ArgMap,
                                          Command),
    run_remote_command(
      Connection, ArgparseDef, Progname, ArgMap1, CmdPath1, Command1, IO);
run_remote_command(
  _Nodename, ArgparseDef, _Progname, #{help := true}, CmdPath, _Command, IO) ->
    rabbit_cli_io:display_help(IO, CmdPath, ArgparseDef);
run_remote_command(
  Connection, _ArgparseDef, Progname, ArgMap, CmdPath, Command, _IO) ->
    rabbit_cli_transport:rpc_with_io(
      Connection,
      rabbit_cli_commands, run_command,
      [Progname, ArgMap, CmdPath, Command]).

run_local_command(
  ArgparseDef, _Progname, #{help := true}, CmdPath, _Command, IO) ->
    rabbit_cli_io:display_help(IO, CmdPath, ArgparseDef);
run_local_command(
  _ArgparseDef, Progname, ArgMap, CmdPath, Command, IO) ->
    rabbit_cli_commands:run_command(
      Progname, ArgMap, CmdPath, Command, IO).

handle_alias(Alias, CommandStr, ArgMap) ->
    {alias, Alias, CommandStr, ArgMap}.

expand_alias(ArgparseDef, Progname, ArgMap, #{handler := Fun} = _Command) ->
    {alias, _Alias, CommandStr, ArgMap} = Fun(ArgMap),
    Args = string:lexemes(CommandStr, " "),
    Options = #{progname => Progname},
    case argparse:parse(Args, ArgparseDef, Options) of
        {ok, ArgMap1, CmdPath1, Command1} ->
            ArgMap2 = maps:merge(ArgMap1, ArgMap),
            {ok, ArgMap2, CmdPath1, Command1};
        {error, _} = Error ->
            Error
    end.
