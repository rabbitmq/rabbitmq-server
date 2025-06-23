-module(rabbit_cli_commands).

-include_lib("kernel/include/logger.hrl").

-include_lib("rabbit_common/include/logging.hrl").
-include_lib("rabbit_common/include/resource.hrl").

-include("src/rabbit_cli_backend.hrl").

-export([discover_commands/0,
         discovered_commands/0,
         discovered_argparse_def/0,
         merge_argparse_def/2,
         expect_legacy/1]).
-export([cmd_noop/1,
         cmd_hello/1,
         cmd_crash/1,
         cmd_import_definitions/1,
         cmd_top/1]).

-rabbitmq_command(
   {#{cli => ["noop"]},
    #{help => "No-op",
      handler => {?MODULE, cmd_noop}}}).

-rabbitmq_command(
   {#{cli => ["hello"]},
    #{help => "Say hello!",
      handler => {?MODULE, cmd_hello}}}).

-rabbitmq_command(
   {#{cli => ["crash"]},
    #{help => "Crash",
      handler => {?MODULE, cmd_crash}}}).

-rabbitmq_command(
   {#{cli => ["declare", "exchange"],
      http => {put, ["exchanges", vhost, exchange]}},
    #{help => "Declare new exchange",
      arguments => [
                    #{name => vhost,
                      long => "-vhost",
                      type => binary,
                      default => <<"/">>,
                      help => "Name of the vhost owning the new exchange"},
                    #{name => exchange,
                      type => binary,
                      help => "Name of the exchange to declare"}
                   ],
      handler => {?MODULE, cmd_declare_exchange}}}).

-rabbitmq_command(
   {#{cli => ["import", "definitions"]},
    [argparse_def_file_input,
     #{help => "Import definitions",
       handler => {?MODULE, cmd_import_definitions}}]}).

-rabbitmq_command(
   {#{cli => ["top"]},
    [#{help => "Top-like interactive view",
       handler => {?MODULE, cmd_top}}]}).

%% -------------------------------------------------------------------
%% Commands discovery.
%% -------------------------------------------------------------------

discover_commands() ->
    _ = discovered_commands_and_argparse_def(),
    ok.

discovered_commands_and_argparse_def() ->
    Key = {?MODULE, discovered_commands},
    try
        persistent_term:get(Key)
    catch
        error:badarg ->
            Commands = do_discover_commands(),
            ArgparseDef = commands_to_cli_argparse_def(Commands),
            Cache = #{commands => Commands,
                      argparse_def => ArgparseDef},
            persistent_term:put(Key, Cache),
            Cache
    end.

discovered_commands() ->
    #{commands := Commands} = discovered_commands_and_argparse_def(),
    Commands.

discovered_argparse_def() ->
    #{argparse_def := ArgparseDef} = discovered_commands_and_argparse_def(),
    ArgparseDef.

do_discover_commands() ->
    %% Extract the commands from module attributes like feature flags and boot
    %% steps.
    %% TODO: Write shell completion scripts for various shells as part of that.
    %% TODO: Generate manpages? When/how? With eDoc?
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
                        merge_argparse_def(Acc1, M1);
                    (_, Acc1) ->
                        Acc1
                end, Acc0, Entries)
      end, #{}, Commands).

%% -------------------------------------------------------------------
%% Argparse helpers.
%% -------------------------------------------------------------------

expand_argparse_def(Def) when is_map(Def) ->
    Def;
expand_argparse_def(Defs) when is_list(Defs) ->
    lists:foldl(
      fun
          (argparse_def_record_stream, Acc) ->
              Def = rabbit_cli_io:argparse_def(record_stream),
              merge_argparse_def(Acc, Def);
          (argparse_def_file_input, Acc) ->
              Def = rabbit_cli_io:argparse_def(file_input),
              merge_argparse_def(Acc, Def);
          (Mod, Acc) when is_atom(Mod) ->
              Def = Mod:argparse_def(),
              merge_argparse_def(Acc, Def);
          (Def, Acc) ->
              Def1 = expand_argparse_def(Def),
              merge_argparse_def(Acc, Def1)
      end, #{}, Defs).

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

merge_arguments(Args1, []) ->
    Args1;
merge_arguments([], Args2) ->
    Args2;
merge_arguments(Args1, Args2) ->
    merge_arguments(Args1, Args2, []).

merge_arguments([#{name := Name} = Arg1 | Rest1], Args2, Acc) ->
    Ret = lists:partition(
            fun(#{name := N}) -> N =:= Name end,
            Args2),
    {Arg, Rest2} = case Ret of
                      {[Arg2], NotMatching} ->
                          {Arg2, NotMatching};
                      {[], Args2} ->
                          {Arg1, Args2}
                  end,
    Acc1 = [Arg | Acc],
    merge_arguments(Rest1, Rest2, Acc1);
merge_arguments([], Args2, Acc) ->
    lists:reverse(Acc) ++ Args2.

merge_commands(Cmds1, Cmds2) ->
    maps:merge(Cmds1, Cmds2).

%% -------------------------------------------------------------------
%% Helpers.
%% -------------------------------------------------------------------

expect_legacy(#rabbit_cli{progname = <<"rabbitmqctl">>}) ->
    true;
expect_legacy(#rabbit_cli{progname = <<"rabbitmq-diagnostics">>}) ->
    true;
expect_legacy(#rabbit_cli{progname = <<"rabbitmq-plugins">>}) ->
    true;
expect_legacy(#rabbit_cli{progname = <<"rabbitmq-queues">>}) ->
    true;
expect_legacy(#rabbit_cli{progname = <<"rabbitmq-streams">>}) ->
    true;
expect_legacy(#rabbit_cli{progname = <<"rabbitmq-upgrade">>}) ->
    true;
expect_legacy(_Context) ->
    false.

%% -------------------------------------------------------------------
%% XXX
%% -------------------------------------------------------------------

cmd_noop(_) ->
    ok.

cmd_hello(_) ->
    Name = io:get_line("Name: "),
    io:format("Hello ~s!~n", [string:trim(Name)]),
    ok.

cmd_crash(_) ->
    erlang:exit(oops).

cmd_import_definitions(#{progname := Progname, arg_map := ArgMap}) ->
    {ok, IO} = rabbit_cli_io:start_link(Progname),
    %% TODO: Use a wrapper above `file' to proxy through transport.
    Ret = case rabbit_cli_io:read_file(IO, ArgMap) of
              {ok, Data} ->
                  rabbit_cli_io:format(IO, "Import definitions:~n  ~s~n", [Data]),
                  ok;
              {error, _} = Error ->
                  Error
          end,
    rabbit_cli_io:stop(IO),
    Ret.

cmd_top(#{io := IO} = Context) ->
    Top = spawn_link(fun() -> run_top(IO) end),
    wait_quit(Context, Top).

run_top(IO) ->
    receive
        quit ->
            ok
    after 1000 ->
              rabbit_cli_io:format(IO, "Refresh~n", []),
              run_top(IO)
    end.

wait_quit(#{arg_map := _ArgMap, io := _IO}, Top) ->
    receive
        {keypress, _} ->
            erlang:unlink(Top),
            Top ! quit,
            ok
    end.
