-module(rabbit_cli_commands).

-include_lib("kernel/include/logger.hrl").

-include_lib("rabbit_common/include/logging.hrl").
-include_lib("rabbit_common/include/resource.hrl").

-include("src/rabbit_cli_backend.hrl").

-export([discover_commands/0,
         discovered_commands/0,
         discovered_argparse_def/0,
         merge_argparse_def/2]).
-export([cmd_generate_completion_script/1,
         cmd_noop/1,
         cmd_hello/1,
         cmd_crash/1,
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
   {#{cli => ["top"]},
    [#{help => "Top-like interactive view",
       handler => {?MODULE, cmd_top}}]}).

-rabbitmq_command(
   {#{cli => ["generate", "completion"]},
    #{help => "Generate a completion script for the given shell",
      arguments => [
                    #{name => shell,
                      type => {binary, [<<"fish">>]},
                      required => true,
                      help => "Name of the shell to target"}
                   ],
      handler => {?MODULE, cmd_generate_completion_script}}}).

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
          (Mod, Acc) when is_atom(Mod) ->
              Def = Mod:argparse_def(),
              merge_argparse_def(Acc, Def);
          ({Mod, Function, Args}, Acc) when is_atom(Mod) ->
              Def = erlang:apply(Mod, Function, Args),
              merge_argparse_def(Acc, Def);
          (Def, Acc) when is_map(Def) ->
              Def1 = expand_argparse_def(Def),
              merge_argparse_def(Acc, Def1)
      end, #{}, Defs).

merge_argparse_def(ArgparseDef1, ArgparseDef2)
  when is_map(ArgparseDef1) andalso is_map(ArgparseDef2) ->
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
%% Completion files.
%% -------------------------------------------------------------------

cmd_generate_completion_script(
  #rabbit_cli{arg_map = #{shell := Shell}} = Context) ->
    ?LOG_DEBUG("Generating completion script for shell `~ts`", [Shell]),
    generate_completion_script(Context, Shell).

generate_completion_script(
  #rabbit_cli{progname = Progname, argparse_def = ArgparseDef} = Context,
  <<"fish">>) ->
    Chunk1 = io_lib:format(
               """
               # Clear any existing completion rules.
               complete -c ~ts -e

               # Disable filename completion.
               complete -c ~ts -f

               """, [Progname, Progname]),

    Chunk2 = completion_for_fish(Context, ArgparseDef, []),

    io:format("~ts~n~ts", [Chunk1, Chunk2]).

completion_for_fish(Context, ArgparseDef, CmdPath) ->
    Chunk1 = format_arguments_for_fish(Context, ArgparseDef, CmdPath),
    Chunk2 = format_commands_for_fish(Context, ArgparseDef, CmdPath),
    [Chunk1, Chunk2].

format_arguments_for_fish(
  #rabbit_cli{progname = Progname},
  #{arguments := Arguments},
  CmdPath) when Arguments =/= [] ->
    Chunk = lists:map(
              fun(Arg) ->
                      Cond = case CmdPath of
                                 [] ->
                                     "";
                                 _ ->
                                     format_cmdpath_cond_for_fish(
                                       CmdPath, [])
                             end,
                      Option = format_arg_for_fish(Arg),
                      Desc = format_desc_for_fish(Arg),
                      io_lib:format(
                        "complete -c ~ts~ts~ts~ts~n",
                        [Progname, Cond, Option, Desc])
              end, Arguments),
    [io_lib:nl(), Chunk];
format_arguments_for_fish(_Context, _ArgparseDef, _CmdPath) ->
    "".

format_commands_for_fish(
  #rabbit_cli{progname = Progname} = Context,
  #{commands := Commands},
  CmdPath) when Commands =/= #{} ->
    CmdNames = lists:sort(maps:keys(Commands)),
    Chunk1 = lists:map(
               fun(CmdName) ->
                       Command = maps:get(CmdName, Commands),
                       Cond = format_cmdpath_cond_for_fish(CmdPath, CmdNames),
                       Desc = format_desc_for_fish(Command),
                       io_lib:format(
                         "complete -c ~ts~ts -a ~ts~ts~n",
                         [Progname, Cond, CmdName, Desc])
               end, CmdNames),
    Chunk2 = lists:map(
               fun(CmdName) ->
                       Command = maps:get(CmdName, Commands),
                       completion_for_fish(
                         Context, Command, CmdPath ++ [CmdName])
               end, CmdNames),
    [io_lib:nl(), Chunk1, Chunk2];
format_commands_for_fish(_Context, _ArgparseDef, _CmdPath) ->
    "".

format_cmdpath_cond_for_fish([], _CmdNames) ->
     " -n __fish_use_subcommand";
format_cmdpath_cond_for_fish(CmdPath, CmdNames) ->
    CondA = lists:map(
              fun(CmdName) ->
                      io_lib:format(
                        "__fish_seen_subcommand_from ~ts",
                        [CmdName])
              end, CmdPath),
    CondB = case CmdNames of
                [] ->
                    [];
                _ ->
                    CondB0 = lists:map(
                               fun(CmdName) ->
                                       io_lib:format("~ts", [CmdName])
                               end, CmdNames),
                    CondB1 = string:join(CondB0, " "),
                    CondB2 = io_lib:format(
                               "not __fish_seen_subcommand_from ~ts",
                               [CondB1]),
                    [CondB2]
            end,
    Cond1 = string:join(CondA ++ CondB, " && "),
    Cond2 = lists:flatten(Cond1),
    io_lib:format(" -n ~0p", [Cond2]).

format_arg_for_fish(Arg) ->
    Long = case Arg of
               #{long := [$- | Name]} ->
                   io_lib:format(" -l ~ts", [Name]);
               #{long := Name} ->
                   io_lib:format(" -o ~ts", [Name]);
               _ ->
                   ""
           end,
    Short = case Arg of
                #{short := Char} ->
                    io_lib:format(" -s ~tc", [Char]);
                _ ->
                    ""
            end,
    IsRequired = case Arg of
                     #{required := R} ->
                         R;
                     #{long := _} ->
                         false;
                     #{short := _} ->
                         false;
                     _ ->
                         true
                 end,
    Required = case IsRequired of
                   true ->
                       " -r";
                   false ->
                       ""
               end,
    Type = maps:get(type, Arg, string),
    ArgArg = case Type of
                 {ErlType, [H | _] = Choices}
                   when ErlType =:= atom orelse
                        ErlType =:= binary orelse
                        (ErlType =:= string andalso is_list(H)) ->
                     AA0 = lists:map(
                             fun(Choice) ->
                                     io_lib:format("~ts", [Choice])
                             end, Choices),
                     AA1 = string:join(AA0, " "),
                     AA2 = lists:flatten(AA1),
                     AA3 = io_lib:format(" -a ~p", [AA2]),
                     AA3;
                 _ ->
                     ""
             end,
    [Long, Short, Required, ArgArg].

format_desc_for_fish(#{help := Help}) ->
    io_lib:format(" -d ~0p", [Help]);
format_desc_for_fish(_) ->
    "".

%% -------------------------------------------------------------------
%% XXX
%% -------------------------------------------------------------------

cmd_noop(_) ->
    ok.

cmd_hello(_) ->
    Name = io:get_line("Name: "),
    io:format("Hello ~s!~n", [string:trim(Name)]),
    ok.

-spec cmd_crash(#rabbit_cli{}) -> no_return().

cmd_crash(_) ->
    erlang:exit(oops).

cmd_top(#{io := IO} = Context) ->
    Top = spawn_link(fun() -> run_top(IO) end),
    wait_quit(Context, Top).

run_top(IO) ->
    receive
        quit ->
            ok
    after 1000 ->
              _ = rabbit_cli_io:format(IO, "Refresh~n", []),
              run_top(IO)
    end.

wait_quit(#{arg_map := _ArgMap, io := _IO}, Top) ->
    receive
        {keypress, _} ->
            erlang:unlink(Top),
            Top ! quit,
            ok
    end.
