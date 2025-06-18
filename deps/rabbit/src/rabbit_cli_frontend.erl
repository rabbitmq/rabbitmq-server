-module(rabbit_cli_frontend).

-include_lib("kernel/include/logger.hrl").
-include_lib("stdlib/include/assert.hrl").

-export([main/1,
         noop/1]).

-record(?MODULE, {scriptname,
                  progname,
                  group_leader,
                  args,
                  argparse_def,
                  arg_map,
                  cmd_path,
                  command,
                  connection}).

main(Args) ->
    ScriptName = escript:script_name(),
    add_rabbitmq_code_path(ScriptName),
    configure_logging(),

    Ret = run_cli(ScriptName, Args),
    ?LOG_NOTICE("CLI: run_cli() return value: ~p", [Ret]),
    %% FIXME: Ensures everything written to stdout/stderr was flushed.
    erlang:halt().

%% -------------------------------------------------------------------
%% CLI frontend setup.
%% -------------------------------------------------------------------

add_rabbitmq_code_path(ScriptName) ->
    ScriptDir = filename:dirname(ScriptName),
    PluginsDir0 = filename:join([ScriptDir, "..", "plugins"]),
    PluginsDir1 = case filelib:is_dir(PluginsDir0) of
                      true ->
                          PluginsDir0
                  end,
    Glob = filename:join([PluginsDir1, "*", "ebin"]),
    AppDirs = filelib:wildcard(Glob),
    lists:foreach(fun code:add_path/1, AppDirs).

configure_logging() ->
    Config = #{level => debug,
               config => #{type => standard_error},
               filters => [{progress_reports,
                            {fun logger_filters:progress/2, stop}}],
               formatter => {rabbit_logger_text_fmt,
                             #{single_line => false,
                               use_colors => true}}},
    ok = logger:add_handler(rmq_cli, rabbit_logger_std_h, Config),
    ok = logger:remove_handler(default),
    ok.

%% -------------------------------------------------------------------
%% Preparation for remote command execution.
%% -------------------------------------------------------------------

run_cli(ScriptName, Args) ->
    ProgName = filename:basename(ScriptName, ".escript"),
    GroupLeader = erlang:group_leader(),
    Context = #?MODULE{scriptname = ScriptName,
                       progname = ProgName,
                       args = Args,
                       group_leader = GroupLeader},
    init_local_args(Context).

init_local_args(Context) ->
    maybe
        LocalArgparseDef = initial_argparse_def(),
        Context1 = Context#?MODULE{argparse_def = LocalArgparseDef},

        {ok,
         PartialArgMap,
         PartialCmdPath,
         PartialCommand} ?= initial_parse(Context1),
        Context2 = Context1#?MODULE{arg_map = PartialArgMap,
                                    cmd_path = PartialCmdPath,
                                    command = PartialCommand},
        set_log_level(Context2)
    end.

set_log_level(#?MODULE{arg_map = #{verbose := Verbosity}} = Context)
  when Verbosity >= 3 ->
    logger:set_primary_config(level, debug),
    connect_to_node(Context);
set_log_level(#?MODULE{} = Context) ->
    connect_to_node(Context).

connect_to_node(#?MODULE{arg_map = ArgMap} = Context) ->
    Ret = case ArgMap of
              #{node := NodenameOrUri} ->
                  rabbit_cli_transport2:connect(NodenameOrUri);
              _ ->
                  rabbit_cli_transport2:connect()
          end,
    Context1 = case Ret of
                   {ok, Connection} ->
                       Context#?MODULE{connection = Connection};
                   {error, _Reason} ->
                       Context#?MODULE{connection = none}
               end,
    run_command(Context1).

%% -------------------------------------------------------------------
%% Arguments definition and parsing.
%% -------------------------------------------------------------------

initial_argparse_def() ->
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
  #?MODULE{progname = ProgName, args = Args, argparse_def = ArgparseDef}) ->
    Options = #{progname => ProgName},
    case partial_parse(Args, ArgparseDef, Options) of
        {ok, ArgMap, CmdPath, Command, _RemainingArgs} ->
            {ok, ArgMap, CmdPath, Command};
        {error, _} = Error ->
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

%% -------------------------------------------------------------------
%% Command execution.
%% -------------------------------------------------------------------

%% TODO: Send a list of supported features:
%% * support for some messages, like Erlang I/O protocol, file read/write
%%   support
%% * type of terminal (or no terminal)
%% * capabilities of the terminal
%% * is plain test or HTTP
%% * evolutions in the communication between the frontend and the backend

run_command(#?MODULE{connection = Connection} = Context)
  when Connection =/= none ->
    ContextMap = context_to_map(Context),
    rabbit_cli_transport2:rpc(
      Connection, rabbit_cli_backend, run_command, [ContextMap]);
run_command(Context) ->
    %% TODO: If we can't connect to a node, try to parse args locally and run
    %% the command on this CLI node.
    %% FIXME: Load applications first, otherwise module attributes are
    %% unavailable.
    %% FIXME: Do we need to spawn a process?
    ContextMap = context_to_map(Context),
    rabbit_cli_backend:run_command(ContextMap).

context_to_map(Context) ->
    Fields = [Field || Field <- record_info(fields, ?MODULE),
                       %% We don’t need or want to communicate the connection
                       %% state or the group leader to the backend.
                       Field =/= connection orelse
                       Field =/= group_leader],
    record_to_map(Fields, Context, 2, #{}).

record_to_map([Field | Rest], Record, Index, Map) ->
    Value = element(Index, Record),
    Map1 = Map#{Field => Value},
    record_to_map(Rest, Record, Index + 1, Map1);
record_to_map([], _Record, _Index, Map) ->
    Map.

noop(_Context) ->
    ok.
