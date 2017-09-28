%% The contents of this file are subject to the Mozilla Public License
%% Version 1.1 (the "License"); you may not use this file except in
%% compliance with the License. You may obtain a copy of the License
%% at http://www.mozilla.org/MPL/
%%
%% Software distributed under the License is distributed on an "AS IS"
%% basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See
%% the License for the specific language governing rights and
%% limitations under the License.
%%
%% The Original Code is RabbitMQ.
%%
%% The Initial Developer of the Original Code is GoPivotal, Inc.
%% Copyright (c) 2007-2017 Pivotal Software, Inc.  All rights reserved.
%%

-module(rabbit_cli).
-include("rabbit_cli.hrl").

-export([main/3, start_distribution/0, start_distribution/1,
         parse_arguments/4, mutually_exclusive_flags/3,
         rpc_call/4, rpc_call/5, rpc_call/7]).

%%----------------------------------------------------------------------------

-type option_name() :: string().
-type option_value() :: string() | node() | boolean().
-type optdef() :: flag | {option, string()}.
-type parse_result() :: {'ok', {atom(), [{option_name(), option_value()}], [string()]}} |
                        'no_command'.

-spec main
        (fun (([string()], string()) -> parse_result()),
         fun ((atom(), atom(), [any()], [any()]) -> any()),
         atom()) ->
            no_return().
-spec start_distribution() -> {'ok', pid()} | {'error', any()}.
-spec start_distribution(string()) -> {'ok', pid()} | {'error', any()}.
-spec usage(atom()) -> no_return().
-spec parse_arguments
        ([{atom(), [{string(), optdef()}]} | atom()],
         [{string(), optdef()}], string(), [string()]) ->
          parse_result().

-spec mutually_exclusive_flags([{option_name(), option_value()}], term(), [{option_name(), term()}]) -> {ok, term()} | {error, string()}.

-spec rpc_call(node(), atom(), atom(), [any()]) -> any().
-spec rpc_call(node(), atom(), atom(), [any()], number()) -> any().
-spec rpc_call
        (node(), atom(), atom(), [any()], reference(), pid(), number()) ->
            any().

ensure_cli_distribution() ->
    case start_distribution() of
        {ok, _} ->
            ok;
        {error, Error} ->
            print_error("Failed to initialize erlang distribution: ~p.",
                        [Error]),
            rabbit_misc:quit(?EX_TEMPFAIL)
    end.

%%----------------------------------------------------------------------------

main(ParseFun, DoFun, UsageMod) ->
    error_logger:tty(false),
    ensure_cli_distribution(),
    {ok, [[NodeStr|_]|_]} = init:get_argument(nodename),
    {Command, Opts, Args} =
        case ParseFun(init:get_plain_arguments(), NodeStr) of
            {ok, Res}  -> Res;
            no_command -> print_error("could not recognise command", []),
                          usage(UsageMod)
        end,
    Node = proplists:get_value(?NODE_OPT, Opts),
    PrintInvalidCommandError =
        fun () ->
                print_error("invalid command '~s'",
                            [string:join([atom_to_list(Command) | Args], " ")])
        end,

    %% The reason we don't use a try/catch here is that rpc:call turns
    %% thrown errors into normal return values
    case catch DoFun(Command, Node, Args, Opts) of
        ok ->
            rabbit_misc:quit(?EX_OK);
        {ok, Result} ->
            rabbit_control_misc:print_cmd_result(Command, Result),
            rabbit_misc:quit(?EX_OK);
        {'EXIT', {function_clause, [{?MODULE, action, _}    | _]}} -> %% < R15
            PrintInvalidCommandError(),
            usage(UsageMod);
        {'EXIT', {function_clause, [{?MODULE, action, _, _} | _]}} -> %% >= R15
            PrintInvalidCommandError(),
            usage(UsageMod);
        {error, {missing_dependencies, Missing, Blame}} ->
            print_error("dependent plugins ~p not found; used by ~p.",
                        [Missing, Blame]),
            rabbit_misc:quit(?EX_CONFIG);
        {'EXIT', {badarg, _}} ->
            print_error("invalid parameter: ~p", [Args]),
            usage(UsageMod, ?EX_DATAERR);
        {error, {Problem, Reason}} when is_atom(Problem), is_binary(Reason) ->
            %% We handle this common case specially to avoid ~p since
            %% that has i18n issues
            print_error("~s: ~s", [Problem, Reason]),
            rabbit_misc:quit(?EX_SOFTWARE);
        {error, Reason} ->
            print_error("~p", [Reason]),
            rabbit_misc:quit(?EX_SOFTWARE);
        {error_string, Reason} ->
            print_error("~s", [Reason]),
            rabbit_misc:quit(?EX_SOFTWARE);
        {badrpc, {'EXIT', Reason}} ->
            print_error("~p", [Reason]),
            rabbit_misc:quit(?EX_SOFTWARE);
        {badrpc, Reason} ->
            case Reason of
                timeout ->
                    print_error("operation ~w on node ~w timed out", [Command, Node]),
                    rabbit_misc:quit(?EX_TEMPFAIL);
                _ ->
                    print_error("unable to connect to node ~w: ~w", [Node, Reason]),
                    print_badrpc_diagnostics([Node]),
                    case Command of
                        stop -> rabbit_misc:quit(?EX_OK);
                        _    -> rabbit_misc:quit(?EX_UNAVAILABLE)
                    end
            end;
        {badrpc_multi, Reason, Nodes} ->
            print_error("unable to connect to nodes ~p: ~w", [Nodes, Reason]),
            print_badrpc_diagnostics(Nodes),
            rabbit_misc:quit(?EX_UNAVAILABLE);
        function_clause ->
            print_error("operation ~w used with invalid parameter: ~p",
                        [Command, Args]),
            usage(UsageMod);
        {refused, Username, _, _} ->
            print_error("failed to authenticate user \"~s\"", [Username]),
            rabbit_misc:quit(?EX_NOUSER);
        Other ->
            print_error("~p", [Other]),
            rabbit_misc:quit(?EX_SOFTWARE)
    end.

start_distribution_anon(0, LastError) ->
    {error, LastError};
start_distribution_anon(TriesLeft, _) ->
    NameCandidate = generate_cli_node_name(),
    case net_kernel:start([NameCandidate, name_type()]) of
        {ok, _} = Result ->
            Result;
        {error, Reason} ->
            start_distribution_anon(TriesLeft - 1, Reason)
    end.

%% Tries to start distribution with random name chosen from limited list of candidates - to
%% prevent atom table pollution on target nodes.
start_distribution() ->
    rabbit_nodes:ensure_epmd(),
    start_distribution_anon(10, undefined).

start_distribution(Name) ->
    rabbit_nodes:ensure_epmd(),
    net_kernel:start([Name, name_type()]).

name_type() ->
    case os:getenv("RABBITMQ_USE_LONGNAME") of
        "true" -> longnames;
        _      -> shortnames
    end.

generate_cli_node_name() ->
    Base = rabbit_misc:format("rabbitmq-cli-~2..0b", [rand_compat:uniform(100)]),
    NameAsList =
        case {name_type(), inet_db:res_option(domain)} of
            {longnames, []} ->
                %% Distribution will fail to start if it's unable to
                %% determine FQDN of a node (with at least one dot in
                %% a name).
                %% CLI is always an initiator of connection, so it
                %% doesn't matter if the name will not resolve.
                Base ++ "@" ++ inet_db:gethostname() ++ ".no-domain";
            _ ->
                Base
        end,
    list_to_atom(NameAsList).

usage(Mod) ->
    usage(Mod, ?EX_USAGE).

usage(Mod, ExitCode) ->
    io:format("~s", [Mod:usage()]),
    rabbit_misc:quit(ExitCode).

%%----------------------------------------------------------------------------

parse_arguments(Commands, GlobalDefs, NodeOpt, CmdLine) ->
    case parse_arguments(Commands, GlobalDefs, CmdLine) of
        {ok, {Cmd, Opts0, Args}} ->
            Opts = [case K of
                        NodeOpt -> {NodeOpt, rabbit_nodes:make(V)};
                        _       -> {K, V}
                    end || {K, V} <- Opts0],
            {ok, {Cmd, Opts, Args}};
        E ->
            E
    end.

%% Takes:
%%    * A list of [{atom(), [{string(), optdef()]} | atom()], where the atom()s
%%      are the accepted commands and the optional [string()] is the list of
%%      accepted options for that command
%%    * A list [{string(), optdef()}] of options valid for all commands
%%    * The list of arguments given by the user
%%
%% Returns either {ok, {atom(), [{string(), string()}], [string()]} which are
%% respectively the command, the key-value pairs of the options and the leftover
%% arguments; or no_command if no command could be parsed.
parse_arguments(Commands, GlobalDefs, As) ->
    lists:foldl(maybe_process_opts(GlobalDefs, As), no_command, Commands).

maybe_process_opts(GDefs, As) ->
    fun({C, Os}, no_command) ->
            process_opts(atom_to_list(C), dict:from_list(GDefs ++ Os), As);
       (C, no_command) ->
            (maybe_process_opts(GDefs, As))({C, []}, no_command);
       (_, {ok, Res}) ->
            {ok, Res}
    end.

process_opts(C, Defs, As0) ->
    KVs0 = dict:map(fun (_, flag)        -> false;
                        (_, {option, V}) -> V
                    end, Defs),
    process_opts(Defs, C, As0, not_found, KVs0, []).

%% Consume flags/options until you find the correct command. If there are no
%% arguments or the first argument is not the command we're expecting, fail.
%% Arguments to this are: definitions, cmd we're looking for, args we
%% haven't parsed, whether we have found the cmd, options we've found,
%% plain args we've found.
process_opts(_Defs, C, [], found, KVs, Outs) ->
    {ok, {list_to_atom(C), dict:to_list(KVs), lists:reverse(Outs)}};
process_opts(_Defs, _C, [], not_found, _, _) ->
    no_command;
process_opts(Defs, C, [A | As], Found, KVs, Outs) ->
    OptType = case dict:find(A, Defs) of
                  error             -> none;
                  {ok, flag}        -> flag;
                  {ok, {option, _}} -> option
              end,
    case {OptType, C, Found} of
        {flag, _, _}     -> process_opts(
                              Defs, C, As, Found, dict:store(A, true, KVs),
                              Outs);
        {option, _, _}   -> case As of
                                []        -> no_command;
                                [V | As1] -> process_opts(
                                               Defs, C, As1, Found,
                                               dict:store(A, V, KVs), Outs)
                            end;
        {none, A, _}     -> process_opts(Defs, C, As, found, KVs, Outs);
        {none, _, found} -> process_opts(Defs, C, As, found, KVs, [A | Outs]);
        {none, _, _}     -> no_command
    end.

mutually_exclusive_flags(CurrentOptionValues, Default, FlagsAndValues) ->
    PresentFlags = lists:filtermap(fun({OptName, _} = _O) ->
                                           proplists:get_bool(OptName, CurrentOptionValues)
                                   end,
                             FlagsAndValues),
    case PresentFlags of
        [] ->
            {ok, Default};
        [{_, Value}] ->
            {ok, Value};
        _ ->
            Names = [ [$', N, $']  || {N, _} <- PresentFlags ],
            CommaSeparated = string:join(rabbit_misc:lists_droplast(Names), ", "),
            AndOneMore = lists:last(Names),
            Msg = io_lib:format("Options ~s and ~s are mutually exclusive", [CommaSeparated, AndOneMore]),
            {error, lists:flatten(Msg)}
    end.

%%----------------------------------------------------------------------------

fmt_stderr(Format, Args) -> rabbit_misc:format_stderr(Format ++ "~n", Args).

print_error(Format, Args) -> fmt_stderr("Error: " ++ Format, Args).

print_badrpc_diagnostics(Nodes) ->
    fmt_stderr(rabbit_nodes:diagnostics(Nodes), []).

%% If the server we are talking to has non-standard net_ticktime, and
%% our connection lasts a while, we could get disconnected because of
%% a timeout unless we set our ticktime to be the same. So let's do
%% that.
rpc_call(Node, Mod, Fun, Args) ->
    rabbit_misc:rpc_call(Node, Mod, Fun, Args).

rpc_call(Node, Mod, Fun, Args, Timeout) ->
    rabbit_misc:rpc_call(Node, Mod, Fun, Args, Timeout).

rpc_call(Node, Mod, Fun, Args, Ref, Pid, Timeout) ->
    rabbit_misc:rpc_call(Node, Mod, Fun, Args, Ref, Pid, Timeout).
