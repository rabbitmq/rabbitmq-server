-module(rabbit_cli).

-include_lib("kernel/include/logger.hrl").

-export([main/1,
         merge_argparse_def/2,
         handle_alias/1,
         noop/1]).

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
    PartialArgparseDef = argparse_def(),
    Context0 = #{progname => Progname,
                 args => Args,
                 io => IO,
                 argparse_def => PartialArgparseDef},
    maybe
        {ok,
         PartialArgMap,
         PartialCmdPath,
         PartialCommand} ?= initial_parse(Context0),
        Context1 = Context0#{arg_map => PartialArgMap,
                             cmd_path => PartialCmdPath,
                             command => PartialCommand},

        Context2 = case rabbit_cli_transport:connect(Context1) of
                       {ok, Connection} ->
                           Context1#{connection => Connection};
                       {error, _} ->
                           Context1
                   end,

        %% We can query the argparse definition from the remote node to know
        %% the commands it supports and proceed with the execution.
        ArgparseDef = get_final_argparse_def(Context2),
        Context3 = Context2#{argparse_def => ArgparseDef},
        {ok,
         ArgMap,
         CmdPath,
         Command} ?= final_parse(Context3),
        Context4 = Context3#{arg_map => ArgMap,
                             cmd_path => CmdPath,
                             command => Command},

        run_command(Context4)
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
                 fun(_Alias, CommandStr) ->
                         Args = string:lexemes(CommandStr, " "),
                         #{alias => Args,
                           help => hidden,
                           handler => {?MODULE, handle_alias}}
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

      commands => Aliases1,
      handler => {?MODULE, noop}}.

initial_parse(
  #{progname := Progname, args := Args, argparse_def := ArgparseDef}) ->
    Options = #{progname => Progname},
    case partial_parse(Args, ArgparseDef, Options) of
        {ok, ArgMap, CmdPath, Command, _RemainingArgs} ->
            {ok, ArgMap, CmdPath, Command};
        {error, _} = Error->
            Error
    end.

partial_parse(Args, ArgparseDef, Options) ->
    ArgparseDef1 = maps:remove(commands, ArgparseDef),
    partial_parse(Args, ArgparseDef1, Options, []).

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

get_final_argparse_def(
  #{connection := Connection, argparse_def := PartialArgparseDef}) ->
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
    maps:merge(
      ArgparseDef1,
      ArgparseDef2#{arguments => Args, commands => Cmds}).

merge_arguments(Args1, Args2) ->
    Args1 ++ Args2.

merge_commands(Cmds1, Cmds2) ->
    maps:merge(Cmds1, Cmds2).

final_parse(
  #{progname := Progname, args := Args, argparse_def := ArgparseDef}) ->
    Options = #{progname => Progname},
    argparse:parse(Args, ArgparseDef, Options).

run_command(#{arg_map := #{help := true}} = Context) ->
    rabbit_cli_io:display_help(Context);
run_command(#{connection := Connection} = Context) ->
    rabbit_cli_transport:run_command(Connection, Context);
run_command(Context) ->
    rabbit_cli_commands:run_command(Context).

noop(_Context) ->
    ok.

handle_alias(
  #{progname := Progname,
    argparse_def := ArgparseDef,
    arg_map := ArgMap,
    command := #{alias := Args}} = Context) ->
    Options = #{progname => Progname},
    case argparse:parse(Args, ArgparseDef, Options) of
        {ok, ArgMap1, CmdPath1, Command1} ->
            ArgMap2 = maps:merge(ArgMap1, ArgMap),
            Context1 = Context#{arg_map => ArgMap2,
                                cmd_path => CmdPath1,
                                command => Command1},
            rabbit_cli_commands:do_run_command(Context1);
        {error, _} = Error ->
            Error
    end.
