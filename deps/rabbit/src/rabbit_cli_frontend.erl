-module(rabbit_cli_frontend).

-include_lib("kernel/include/logger.hrl").
-include_lib("stdlib/include/assert.hrl").

-include("src/rabbit_cli_backend.hrl").

-export([main/1,
         noop/1]).

-record(?MODULE, {scriptname,
                  connection,
                  pager}).

-spec main(Args) -> no_return() when
      Args :: argparse:args().

main(Args) ->
    ScriptName = escript:script_name(),
    add_rabbitmq_code_path(ScriptName),
    configure_logging(),

    Ret = run_cli(ScriptName, Args),
    ?LOG_DEBUG("CLI: run_cli() return value: ~p", [Ret]),

    flush_log_messages(),
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

-define(LOG_HANDLER_NAME, rmq_cli).

configure_logging() ->
    Config = #{level => debug,
               config => #{type => standard_error},
               filters => [{progress_reports,
                            {fun logger_filters:progress/2, stop}}],
               formatter => {rabbit_logger_text_fmt,
                             #{single_line => false,
                               use_colors => true}}},
    ok = logger:add_handler(?LOG_HANDLER_NAME, rabbit_logger_std_h, Config),
    ok = logger:remove_handler(default),
    ok.

flush_log_messages() ->
    _ = rabbit_logger_std_h:filesync(?LOG_HANDLER_NAME),
    ok.

%% -------------------------------------------------------------------
%% Preparation for remote command execution.
%% -------------------------------------------------------------------

run_cli(ScriptName, Args) ->
    ProgName0 = filename:basename(ScriptName, ".bat"),
    ProgName1 = filename:basename(ProgName0, ".escript"),
    Terminal = collect_terminal_info(),
    Priv = #?MODULE{scriptname = ScriptName},
    Context = #rabbit_cli{progname = ProgName1,
                          args = Args,
                          os = os:type(),
                          env = os:env(),
                          terminal = Terminal,
                          priv = Priv},
    init_local_args(Context).

collect_terminal_info() ->
    IoOpts = io:getopts(),
    Term = eterminfo:get_term_type_or_default(),
    TermInfo = case eterminfo:read_by_infocmp(Term) of
                   {ok, TI} ->
                       TI;
                   _ ->
                       case eterminfo:read_by_file(Term) of
                           {ok, TI} ->
                               TI;
                           _ ->
                               undefined
                       end
               end,
    #{stdout => proplists:get_value(stdout, IoOpts),
      stderr => proplists:get_value(stderr, IoOpts),
      stdin => proplists:get_value(stdin, IoOpts),

      name => Term,
      info => TermInfo}.

init_local_args(Context) ->
    maybe
        LocalArgparseDef = initial_argparse_def(),
        Context1 = Context#rabbit_cli{argparse_def = LocalArgparseDef},

        {ok,
         PartialArgMap,
         PartialCmdPath,
         PartialCommand} ?= initial_parse(Context1),
        Context2 = Context1#rabbit_cli{arg_map = PartialArgMap,
                                       cmd_path = PartialCmdPath,
                                       command = PartialCommand},
        set_log_level(Context2)
    end.

set_log_level(#rabbit_cli{arg_map = #{verbose := Verbosity}} = Context)
  when Verbosity >= 3 ->
    _ = logger:set_primary_config(level, debug),
    connect_to_node(Context);
set_log_level(#rabbit_cli{} = Context) ->
    connect_to_node(Context).

connect_to_node(
  #rabbit_cli{arg_map = ArgMap, priv = Priv} = Context) ->
    Ret = case ArgMap of
              #{node := NodenameOrUri} ->
                  rabbit_cli_transport2:connect(NodenameOrUri);
              _ ->
                  rabbit_cli_transport2:connect()
          end,
    Priv1 = case Ret of
                {ok, Connection} ->
                    Priv#?MODULE{connection = Connection};
                {error, _Reason} ->
                    Priv#?MODULE{connection = none}
            end,
    Context1 = Context#rabbit_cli{priv = Priv1},
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
  #rabbit_cli{progname = ProgName, args = Args, argparse_def = ArgparseDef}) ->
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

noop(_Context) ->
    ok.

%% -------------------------------------------------------------------
%% Command execution.
%% -------------------------------------------------------------------

%% Run command:
%% * start backend (remote if connection, local otherwise); backend starts
%%   execution of command
%% * loop to react to signals and messages from backend
%%
%% TODO: Send a list of supported features:
%% * support for some messages, like Erlang I/O protocol, file read/write
%%   support
%% * type of terminal (or no terminal)
%% * capabilities of the terminal
%% * is plain test or HTTP
%% * evolutions in the communication between the frontend and the backend

run_command(
  #rabbit_cli{priv = #?MODULE{connection = Connection}} = Context)
  when Connection =/= none ->
    maybe
        process_flag(trap_exit, true),
        ContextMap = context_to_map(Context),
        {ok, _Backend} ?= rabbit_cli_transport2:run_command(
                           Connection, ContextMap),
        main_loop(Context)
    end;
run_command(#rabbit_cli{} = Context) ->
    %% TODO: If we can't connect to a node, try to parse args locally and run
    %% the command on this CLI node.
    %% FIXME: Load applications first, otherwise module attributes are
    %% unavailable.
    %% FIXME: run_command() relies on rabbit_cli_backend_sup.
    maybe
        process_flag(trap_exit, true),
        ContextMap = context_to_map(Context),
        {ok, _Backend} ?= rabbit_cli_backend:run_command(ContextMap, self()),
        main_loop(Context)
    end.

context_to_map(Context) ->
    Fields = [Field || Field <- record_info(fields, rabbit_cli),
                       %% We don't need or want to communicate anything that
                       %% is private to the backend.
                       Field =/= priv],
    record_to_map(Fields, Context, 2, #{}).

record_to_map([Field | Rest], Record, Index, Map) ->
    Value = element(Index, Record),
    Map1 = Map#{Field => Value},
    record_to_map(Rest, Record, Index + 1, Map1);
record_to_map([], _Record, _Index, Map) ->
    Map.

main_loop(
  #rabbit_cli{priv = #?MODULE{connection = Connection,
                              pager = Pager} = Priv} = Context) ->
    ?LOG_DEBUG("CLI: frontend main loop (pager: ~0p)...", [Pager]),
    Timeout = case is_port(Pager) of
                  false ->
                      infinity;
                  true ->
                      100
              end,
    receive
        {'EXIT', Pager, Reason} ->
            ?LOG_DEBUG("CLI: EXIT signal from pager: ~p", [Reason]),
            Priv1 = Priv#?MODULE{pager = undefined},
            Context1 = Context#rabbit_cli{priv = Priv1},
            terminate(Reason, Context1);
        {'EXIT', LinkedPid, Reason} ->
            ?LOG_DEBUG(
               "CLI: EXIT signal from linked process ~0p: ~p",
               [LinkedPid, Reason]),
            case Pager of
                undefined ->
                    terminate(Reason, Context);
                _ ->
                    ?LOG_DEBUG("CLI: waiting for pager to exit"),
                    main_loop(Context)
            end;
        {frontend_request, From, Request} ->
            {reply, Reply, Context1} = handle_request(Request, Context),
            _ = rabbit_cli_transport2:gen_reply(Connection, From, Reply),
            main_loop(Context1);
        {io_request, From, ReplyAs, Request}
          when element(1, Request) =:= put_chars andalso is_port(Pager) ->
            Chars0 = case Request of
                         {put_chars, unicode, M, F, A} ->
                             erlang:apply(M, F, A);
                         {put_chars, unicode, C} ->
                             C
                     end,
            Chars1 = re:replace(Chars0, "\n", "\r\n"),
            Bin = unicode:characters_to_binary(Chars1),
            erlang:port_command(Pager, Bin),
            IoReply = {io_reply, ReplyAs, ok},
            From ! IoReply,
            main_loop(Context);
        {io_request, _From, _ReplyAs, _Request} = IoRequest ->
            GroupLeader = erlang:group_leader(),
            GroupLeader ! IoRequest,
            main_loop(Context);
        Info ->
            ?LOG_ALERT("CLI: unknown info: ~0p", [Info]),
            main_loop(Context)
    after Timeout ->
              erlang:port_command(Pager, <<>>),
              main_loop(Context)
    end.

terminate(Reason, _Context) ->
    ?LOG_DEBUG("CLI: frontend terminating: ~0p", [Reason]),
    ok.

handle_request({read_file, Filename}, Context) ->
    {reply, file:read_file(Filename), Context};
handle_request({write_file, Filename, Bytes}, Context) ->
    {reply, file:write_file(Filename, Bytes), Context};
handle_request(set_interactive_mode, Context) ->
    Ret = shell:start_interactive({noshell, raw}),
    ?LOG_DEBUG("CLI: interactive mode: ~p", [Ret]),
    {reply, Ret, Context};
handle_request(
  set_paging_mode, #rabbit_cli{env = Env, priv = Priv} = Context) ->
    Cmd = case proplists:get_value("PAGER", Env) of
              Value when is_list(Value) ->
                  Value;
              undefined ->
                  "less"
          end,
    ?LOG_DEBUG("CLI: start pager \"~ts\"", [Cmd]),
    Pager = erlang:open_port(
              {spawn, Cmd},
              [stream, exit_status, binary, use_stdio, out, hide, {env, Env}]),
    Priv1 = Priv#?MODULE{pager = Pager},
    Context1 = Context#rabbit_cli{priv = Priv1},
    {reply, ok, Context1}.
