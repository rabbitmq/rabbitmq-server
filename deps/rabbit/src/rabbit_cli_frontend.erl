-module(rabbit_cli_frontend).

-include_lib("kernel/include/logger.hrl").
-include_lib("stdlib/include/assert.hrl").

-export([main/1,
         merge_argparse_def/2,
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
    Ret = run_cli(ScriptName, Args),
    io:format(standard_error, "CLI run_cli() -> ~p~n", [Ret]),
    erlang:halt().

run_cli(ScriptName, Args) ->
    ProgName = filename:basename(ScriptName, ".escript"),
    GroupLeader = erlang:group_leader(),
    Context = #?MODULE{scriptname = ScriptName,
                       progname = ProgName,
                       args = Args,
                       group_leader = GroupLeader},
    add_rabbitmq_code_path(Context).

add_rabbitmq_code_path(#?MODULE{scriptname = ScriptName} = Context) ->
    ScriptDir = filename:dirname(ScriptName),
    PluginsDir0 = filename:join([ScriptDir, "..", "plugins"]),
    PluginsDir1 = case filelib:is_dir(PluginsDir0) of
                      true ->
                          PluginsDir0
                  end,
    Glob = filename:join([PluginsDir1, "*", "ebin"]),
    AppDirs = filelib:wildcard(Glob),
    lists:foreach(fun code:add_path/1, AppDirs),
    init_local_args(Context).

init_local_args(Context) ->
    maybe
        LocalArgparseDef = local_argparse_def(),
        Context1 = Context#?MODULE{argparse_def = LocalArgparseDef},

        {ok,
         PartialArgMap,
         PartialCmdPath,
         PartialCommand} ?= initial_parse(Context1),
        Context2 = Context1#?MODULE{arg_map = PartialArgMap,
                                    cmd_path = PartialCmdPath,
                                    command = PartialCommand},
        connect_to_node(Context2)
    end.

connect_to_node(#?MODULE{arg_map = #{node := NodenameOrUri}} = Context) ->
    maybe
        %% TODO: Send a list of supported features:
        %% * support for some messages, like Erlang I/O protocol, file
        %%   read/write support
        %% * type of terminal (or no terminal)
        %% * capabilities of the terminal
        %% * is plain test or HTTP
        %% * evolutions in the communication between the frontend and the
        %%   backend
        {ok, Connection} ?= rabbit_cli_transport2:connect(NodenameOrUri),
        Context1 = Context#?MODULE{connection = Connection},
        init_final_args(Context1)
    end;
connect_to_node(#?MODULE{} = Context) ->
    maybe
        {ok, Connection} ?= rabbit_cli_transport2:connect(),
        Context1 = Context#?MODULE{connection = Connection},
        init_final_args(Context1)
    end.

init_final_args(Context) ->
    maybe
        %% We can query the argparse definition from the remote node to know
        %% the commands it supports and proceed with the execution.
        {ok, ArgparseDef} ?= final_argparse_def(Context),
        Context1 = Context#?MODULE{argparse_def = ArgparseDef},

        {ok,
         ArgMap,
         CmdPath,
         Command} ?= final_parse(Context1),
        Context2 = Context1#?MODULE{arg_map = ArgMap,
                                    cmd_path = CmdPath,
                                    command = Command},

        run_command(Context2)
    end.

%% -------------------------------------------------------------------
%% Arguments definition and parsing.
%% -------------------------------------------------------------------

local_argparse_def() ->
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

final_argparse_def(#?MODULE{argparse_def = PartialArgparseDef} = Context) ->
    maybe
        {ok, FullArgparseDef} ?= get_full_argparse_def(Context),
        ArgparseDef1 = merge_argparse_def(PartialArgparseDef, FullArgparseDef),
        {ok, ArgparseDef1}
    end.

get_full_argparse_def(#?MODULE{connection = Connection}) ->
    %% TODO: Handle an undef failure when the remote node is too old.
    RemoteArgparseDef = rabbit_cli_transport2:rpc(
                          Connection,
                          rabbit_cli_backend, final_argparse_def, []),
    {ok, RemoteArgparseDef};
get_full_argparse_def(_) ->
    LocalArgparseDef = rabbit_cli_backend:final_argparse_def(),
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
  #?MODULE{progname = ProgName, args = Args, argparse_def = ArgparseDef}) ->
    Options = #{progname => ProgName},
    argparse:parse(Args, ArgparseDef, Options).

%% -------------------------------------------------------------------
%% Command execution.
%% -------------------------------------------------------------------

run_command(#?MODULE{connection = Connection} = Context) ->
    ContextMap = context_to_map(Context),
    rabbit_cli_transport2:rpc(
      Connection, rabbit_cli_backend, run_command, [ContextMap]);
run_command(Context) ->
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
