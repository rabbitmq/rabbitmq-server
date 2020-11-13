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
        add_rabbitmq_code_path(Progname),

        PartialArgparseDef = argparse_def(),
        {ok,
         PartialArgMap,
         PartialCmdPath,
         PartialCommand} ?= initial_parse(Progname, Args, PartialArgparseDef),

        %% Get remote node name and prepare Erlang distribution.
        Nodename = lookup_rabbitmq_nodename(PartialArgMap),
        {ok, _} ?= net_kernel:start(
                     undefined, #{name_domain => shortnames}),

        {ok, IO} ?= rabbit_cli_io:start_link(Progname),
        try
            %% Can we reach the remote node?
            case net_kernel:connect_node(Nodename) of
                true ->
                    maybe
                        %% We can query the argparse definition from the
                        %% remote node to know the commands it supports and
                        %% proceed with the execution.
                        ArgparseDef = get_final_argparse_def(Nodename),
                        {ok,
                         ArgMap,
                         CmdPath,
                         Command} ?= final_parse(Progname, Args, ArgparseDef),
                        run_command(
                          Nodename, ArgparseDef,
                          Progname, ArgMap, CmdPath, Command,
                          IO)
                    end;
                false ->
                    %% We can't reach the remote node. Let's fallback
                    %% to a local execution.
                    run_command(
                      undefined, PartialArgparseDef,
                      Progname, PartialArgMap, PartialCmdPath,
                      PartialCommand, IO)
            end
        after
            rabbit_cli_io:stop(IO)
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

get_final_argparse_def(Nodename) ->
    ArgparseDef1 = argparse_def(),
    ArgparseDef2 = erpc:call(Nodename, rabbit_cli_commands, argparse_def, []),
    ArgparseDef = maps:merge(ArgparseDef1, ArgparseDef2),
    ArgparseDef.

final_parse(Progname, Args, ArgparseDef) ->
    Options = #{progname => Progname},
    argparse:parse(Args, ArgparseDef, Options).

lookup_rabbitmq_nodename(#{node := Nodename}) ->
    Nodename1 = complete_nodename(Nodename),
    Nodename1;
lookup_rabbitmq_nodename(_) ->
    GuessedNodename0 = guess_rabbitmq_nodename(),
    GuessedNodename1 = complete_nodename(GuessedNodename0),
    GuessedNodename1.

guess_rabbitmq_nodename() ->
    case net_adm:names() of
        {ok, NamesAndPorts} ->
            Names0 = [Name || {Name, _Port} <- NamesAndPorts],
            Names1 = lists:sort(Names0),
            Names2 = lists:filter(
                       fun
                           ("rabbit" ++ _) -> true;
                           (_) ->             false
                       end, Names1),
            case Names2 of
                [First | _] ->
                    First;
                [] ->
                    "rabbit"
            end;
        {error, address} ->
            "rabbit"
    end.

complete_nodename(Nodename) ->
    case re:run(Nodename, "@", [{capture, none}]) of
        nomatch ->
            {ok, ThisHost} = inet:gethostname(),
            list_to_atom(Nodename ++ "@" ++ ThisHost);
        match ->
            list_to_atom(Nodename)
    end.

run_command(
  _Nodename, ArgparseDef, _Progname, #{help := true}, CmdPath, _Command, IO) ->
    rabbit_cli_io:display_help(IO, CmdPath, ArgparseDef);
run_command(Nodename, _ArgparseDef, Progname, ArgMap, CmdPath, Command, IO) ->
    erpc:call(
      Nodename,
      rabbit_cli_commands, run_command,
      [Progname, ArgMap, CmdPath, Command, IO]).
