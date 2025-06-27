-module(rabbit_cli_frontend).

-behaviour(gen_event).

-include_lib("kernel/include/logger.hrl").
-include_lib("stdlib/include/assert.hrl").

-include("src/rabbit_cli_backend.hrl").

-export([main/1,
         noop/1]).
-export([init/1,
         handle_call/2,
         handle_event/2,
         terminate/2,
         code_change/3]).

-record(?MODULE, {scriptname,
                  connection,
                  backend,
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
    case is_legacy_progname(ProgName1) of
        false ->
            run_cli(ScriptName, ProgName1, Args);
        true ->
            run_legacy_cli(Args)
    end.

is_legacy_progname("rabbitmqctl") ->
    true;
is_legacy_progname("rabbitmq-diagnostics") ->
    true;
is_legacy_progname("rabbitmq-plugins") ->
    true;
is_legacy_progname("rabbitmq-queues") ->
    true;
is_legacy_progname("rabbitmq-streams") ->
    true;
is_legacy_progname("rabbitmq-upgrade") ->
    true;
is_legacy_progname(_Progname) ->
    false.

run_cli(ScriptName, ProgName, Args) ->
    Terminal = collect_terminal_info(),
    configure_signal_handler(),
    Priv = #?MODULE{scriptname = ScriptName},
    Context = #rabbit_cli{progname = ProgName,
                          args = Args,
                          os = os:type(),
                          env = os:env(),
                          terminal = Terminal,
                          priv = Priv},
    init_local_args(Context).

run_legacy_cli(Args) ->
    'Elixir.RabbitMQCtl':main(Args).

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

configure_signal_handler() ->
    gen_event:add_handler(erl_signal_server, ?MODULE, self()),
    Signals = [sigwinch, siginfo],
    lists:foreach(
      fun(Signal) ->
              try
                  os:set_signal(Signal, handle)
              catch
                  _:badarg ->
                      ?LOG_DEBUG("Signal ~s not supported", [Signal])
              end
      end, Signals),
    ok.

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
                  rabbit_cli_transport:connect(NodenameOrUri);
              _ ->
                  rabbit_cli_transport:connect()
          end,
    Priv1 = case Ret of
                {ok, Connection} ->
                    Priv#?MODULE{connection = Connection};
                {error, Reason} ->
                    ?LOG_DEBUG(
                       "CLI: failed to establish a connection to a RabbitMQ "
                       "node: ~0p",
                       [Reason]),
                    Priv#?MODULE{connection = none}
            end,
    ClientInfo = rabbit_cli_transport:get_client_info(
                   Priv1#?MODULE.connection),
    Context1 = Context#rabbit_cli{client = ClientInfo,
                                  priv = Priv1},
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
  #rabbit_cli{priv = #?MODULE{connection = Connection} = Priv} = Context)
  when Connection =/= none ->
    maybe
        process_flag(trap_exit, true),
        ContextMap = context_to_map(Context),
        {ok, Backend} ?= rabbit_cli_transport:run_command(
                           Connection, ContextMap),
        Priv1 = Priv#?MODULE{backend = Backend},
        Context1 = Context#rabbit_cli{priv = Priv1},
        main_loop(Context1)
    end;
run_command(#rabbit_cli{} = Context) ->
    %% FIXME: Load applications first, otherwise module attributes are
    %% unavailable.
    maybe
        process_flag(trap_exit, true),
        prepare_offline_exec(Context),
        ContextMap = context_to_map(Context),
        {ok, _Backend} ?= rabbit_cli_backend:run_command(ContextMap, self()),
        main_loop(Context)
    end.

prepare_offline_exec(_Context) ->
    ?LOG_DEBUG("CLI: prepare for offline execution"),
    Env = rabbit_env:get_context(),
    rabbit_env:context_to_code_path(Env),
    rabbit_env:context_to_app_env_vars(Env),
    PluginsDir = rabbit_plugins:plugins_dir(),
    Plugins = rabbit_plugins:plugin_names(
                rabbit_plugins:list(PluginsDir, true)),
    Apps = [rabbit_common, rabbit | Plugins],
    lists:foreach(
      fun(App) -> _ = application:load(App) end,
      Apps),
    ?LOG_DEBUG("CLI: ready for offline execution: ~p", [application:loaded_applications()]),
    ok.

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
                              backend = Backend,
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
            terminate_cli(Reason, Context1);
        {'EXIT', LinkedPid, Reason} ->
            ?LOG_DEBUG(
               "CLI: EXIT signal from linked process ~0p: ~p",
               [LinkedPid, Reason]),
            case Pager of
                undefined ->
                    terminate_cli(Reason, Context);
                _ ->
                    ?LOG_DEBUG("CLI: waiting for pager to exit"),
                    main_loop(Context)
            end;
        {frontend_request, From, Request} ->
            {reply, Reply, Context1} = handle_request(Request, Context),
            _ = rabbit_cli_transport:gen_reply(Connection, From, Reply),
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
        {signal, Signal} = Event ->
            ?LOG_DEBUG("CLI: got Unix signal: ~ts", [Signal]),
            _ = rabbit_cli_transport:send(Connection, Backend, Event),
            main_loop(Context);
        Info ->
            ?LOG_ALERT("CLI: unknown info: ~0p", [Info]),
            main_loop(Context)
    after Timeout ->
              erlang:port_command(Pager, <<>>),
              main_loop(Context)
    end.

terminate_cli(Reason, _Context) ->
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
    {reply, ok, Context1};
handle_request({display_siginfo, {Format, Args}}, Context) ->
    io:format(standard_error, Format, Args),
    {reply, ok, Context};
handle_request({display_siginfo, String}, Context) ->
    io:format(standard_error, "~ts", [String]),
    {reply, ok, Context};
handle_request(get_window_size, Context) ->
    Size = get_window_size(Context),
    {reply, Size, Context}.

get_window_size(#rabbit_cli{env = Env}) ->
    DefaultSize = #{lines => 25, cols => 80},
    Cmd = "stty size",
    Port = erlang:open_port(
             {spawn, Cmd},
             [stream, use_stdio, in, hide, {env, Env}]),
    get_window_size_loop(Port, DefaultSize).

get_window_size_loop(Port, Size) ->
    receive
        {Port, {data, Output}} ->
            [LinesStr, ColsStr] = string:lexemes(string:trim(Output), " "),
            Lines = list_to_integer(LinesStr),
            Cols = list_to_integer(ColsStr),
            Size1 = Size#{lines => Lines,
                          cols => Cols},
            get_window_size_loop(Port, Size1);
        {'EXIT', Port, _Reason} ->
            Size
    end.

%% -------------------------------------------------------------------
%% gen_event callbacks (signal handler).
%% -------------------------------------------------------------------

init(Parent) ->
    {ok, Parent}.

handle_call(Request, Parent) ->
    ?LOG_DEBUG("CLI: (signal) unknown request: ~0p", [Request]),
    {ok, ok, Parent}.

handle_event(Signal, Parent)
  when Signal =:= sigwinch orelse Signal =:= siginfo ->
    ?LOG_DEBUG("CLI: (signal) signal = ~0p", [Signal]),
    Parent ! {signal, Signal},
    {ok, Parent};
handle_event(Event, Parent) ->
    ?LOG_DEBUG("CLI: (signal) unknown event: ~0p", [Event]),
    {ok, Parent}.

terminate(Reason, _Parent) ->
    ?LOG_DEBUG("CLI: (signal) terminate: ~0p", [Reason]),
    ok.

code_change(_OldVsn, Parent, _Extra) ->
    {ok, Parent}.
