-module(rabbit_cli).

-include_lib("kernel/include/logger.hrl").
-include_lib("stdlib/include/assert.hrl").

-export([main/1,
         merge_argparse_def/2,
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

        {ok, Config} ?= read_config_file(Context1),
        Context2 = Context1#{config => Config},

        Context3 = case rabbit_cli_transport:connect(Context2) of
                       {ok, Connection} ->
                           Context2#{connection => Connection};
                       {error, _} ->
                           Context2
                   end,

        %% We can query the argparse definition from the remote node to know
        %% the commands it supports and proceed with the execution.
        {ok, ArgparseDef} ?= get_final_argparse_def(Context3),
        Context4 = Context3#{argparse_def => ArgparseDef},
        {ok,
         ArgMap,
         CmdPath,
         Command} ?= final_parse(Context4),
        Context5 = Context4#{arg_map => ArgMap,
                             cmd_path => CmdPath,
                             command => Command},

        run_command(Context5)
    end.

%% -------------------------------------------------------------------
%% RabbitMQ code directory.
%% -------------------------------------------------------------------

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

%% -------------------------------------------------------------------
%% Arguments definition and parsing.
%% -------------------------------------------------------------------

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
         type => boolean,
         help =>
         "Display version and exit"}
      ],

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

get_final_argparse_def(#{argparse_def := PartialArgparseDef} = Context) ->
    maybe
        {ok, FullArgparseDef} ?= get_full_argparse_def(Context),
        ArgparseDef1 = merge_argparse_def(PartialArgparseDef, FullArgparseDef),
        {ok, ArgparseDef1}
    end.

get_full_argparse_def(#{connection := Connection}) ->
    RemoteArgparseDef = rabbit_cli_transport:rpc(
                          Connection, rabbit_cli_commands, argparse_def, []),
    {ok, RemoteArgparseDef};
get_full_argparse_def(_) ->
    LocalArgparseDef = rabbit_cli_commands:argparse_def(),
    {ok, LocalArgparseDef}.

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

%% -------------------------------------------------------------------
%% Configuation file.
%% -------------------------------------------------------------------

read_config_file(_Context) ->
    ConfigFilename = get_config_filename(),
    case filelib:is_regular(ConfigFilename) of
        true ->
            SchemaFilename = get_config_schema_filename(),
            Schema = cuttlefish_schema:files([SchemaFilename]),
            case cuttlefish_conf:files([ConfigFilename]) of
                {errorlist, Errors} ->
                    io:format(standard_error, "Errors1 = ~p~n", [Errors]),
                    {error, config};
                Config0 ->
                    case cuttlefish_generator:map(Schema, Config0) of
                        {error, _Phase, {errorlist, Errors}} ->
                            io:format(
                              standard_error, "Errors2 = ~p~n", [Errors]),
                            {error, config};
                        Config1 ->
                            Config2 = proplists:get_value(
                                        rabbitmqctl, Config1, []),
                            Config3 = maps:from_list(Config2),
                            {ok, Config3}
                    end
            end;
        false ->
            {ok, #{}}
    end.

get_config_schema_filename() ->
    ok = application:load(rabbit),
    RabbitPrivDir = code:priv_dir(rabbit),
    RabbitmqctlSchema = filename:join(
                          [RabbitPrivDir, "schema", "rabbitmqctl.schema"]),
    RabbitmqctlSchema.

get_config_filename() ->
    {OsFamily, _} = os:type(),
    get_config_filename(OsFamily).

get_config_filename(unix) ->
    XdgConfigHome = case os:getenv("XDG_CONFIG_HOME") of
                        false ->
                            HomeDir = os:getenv("HOME"),
                            ?assertNotEqual(false, HomeDir),
                            filename:join([HomeDir, ".config"]);
                        Value ->
                            Value
                    end,
    ConfigFilename = filename:join(
                       [XdgConfigHome, "rabbitmq", "rabbitmqctl.conf"]),
    ConfigFilename.

%% -------------------------------------------------------------------
%% Command execution.
%% -------------------------------------------------------------------

run_command(#{connection := Connection} = Context) ->
    rabbit_cli_transport:run_command(Connection, Context);
run_command(Context) ->
    rabbit_cli_commands:run_command(Context).

noop(_Context) ->
    ok.
