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
            parse_command_pass1(Progname, Args, IO)
        after
            rabbit_cli_io:stop(IO)
        end
    end.

parse_command_pass1(Progname, Args, IO) ->
    maybe
        PartialArgparseDef = argparse_def(),
        {ok,
         PartialArgMap,
         PartialCmdPath,
         PartialCommand} ?= initial_parse(Progname, Args, PartialArgparseDef),

        case rabbit_cli_transport:connect(PartialArgMap) of
            {ok, Connection} ->
                %% We can query the argparse definition from the remote node
                %% to know the commands it supports and proceed with the
                %% execution.
                maybe
                    ArgparseDef = get_final_argparse_def(Connection),
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

      commands => #{}}.

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

get_final_argparse_def(Connection) ->
    ArgparseDef1 = argparse_def(),
    ArgparseDef2 = rabbit_cli_transport:rpc(
                     Connection, rabbit_cli_commands, argparse_def, []),
    ArgparseDef = maps:merge(ArgparseDef1, ArgparseDef2),
    ArgparseDef.

final_parse(Progname, Args, ArgparseDef) ->
    Options = #{progname => Progname},
    argparse:parse(Args, ArgparseDef, Options).

run_remote_command(
  _Nodename, ArgparseDef, _Progname, #{help := true}, CmdPath, _Command, IO) ->
    rabbit_cli_io:display_help(IO, CmdPath, ArgparseDef);
run_remote_command(
  Connection, _ArgparseDef, Progname, ArgMap, CmdPath, Command, IO) ->
    rabbit_cli_transport:rpc(
      Connection,
      rabbit_cli_commands, run_command,
      [Progname, ArgMap, CmdPath, Command, IO]).

run_local_command(
  ArgparseDef, _Progname, #{help := true}, CmdPath, _Command, IO) ->
    rabbit_cli_io:display_help(IO, CmdPath, ArgparseDef);
run_local_command(
  _ArgparseDef, Progname, ArgMap, CmdPath, Command, IO) ->
    rabbit_cli_commands:run_command(
      Progname, ArgMap, CmdPath, Command, IO).
