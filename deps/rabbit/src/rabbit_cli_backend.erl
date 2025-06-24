-module(rabbit_cli_backend).

-behaviour(gen_statem).

-include_lib("kernel/include/logger.hrl").

-include_lib("rabbit_common/include/logging.hrl").
-include_lib("rabbit_common/include/resource.hrl").

-include("src/rabbit_cli_backend.hrl").

-export([run_command/2,
         read_stdin/1,
         read_file/2,
         write_file/3,
         start_link/3]).
-export([init/1,
         callback_mode/0,
         handle_event/4,
         terminate/3,
         code_change/4]).

-record(?MODULE, {caller}).

%% TODO:
%% * Implémenter "list exchanges" plus proprement
%% * Implémenter "rabbitmqctl list_exchanges" pour la compatibilité

run_command(ContextMap, Caller) when is_map(ContextMap) ->
    Context = map_to_context(ContextMap),
    run_command(Context, Caller);
run_command(#rabbit_cli{} = Context, Caller) when is_pid(Caller) ->
    GroupLeader = erlang:group_leader(),
    rabbit_cli_backend_sup:start_backend(Context, Caller, GroupLeader).

map_to_context(ContextMap) ->
    #rabbit_cli{progname = maps:get(progname, ContextMap),
                args = maps:get(args, ContextMap),
                argparse_def = maps:get(argparse_def, ContextMap),
                arg_map = maps:get(arg_map, ContextMap),
                cmd_path = maps:get(cmd_path, ContextMap),
                command = maps:get(command, ContextMap)}.

start_link(Context, Caller, GroupLeader) ->
    Args = #{context => Context,
             caller => Caller,
             group_leader => GroupLeader},
    gen_statem:start_link(?MODULE, Args, []).

read_stdin(Context) ->
    read_stdin(Context, []).

read_stdin(Context, Data) ->
    case io:get_chars("", 4096) of
        Chunk when is_list(Chunk) ->
            Data1 = [Data, Chunk],
            read_stdin(Context, Data1);
        eof ->
            {ok, list_to_binary(Data)};
        {error, _} = Error ->
            Error
    end.

read_file(Context, Filename) ->
    Request = {read_file, Filename},
    send_frontend_request(Context, Request).

write_file(Context, Filename, Bytes) ->
    Request = {write_file, Filename, Bytes},
    send_frontend_request(Context, Request).

send_frontend_request(
  #rabbit_cli{priv = #?MODULE{caller = Caller}}, Request) ->
    Mref = erlang:monitor(process, Caller),
    Caller ! {frontend_request, {self(), Mref}, Request},
    receive
        {Mref, Reply} ->
            erlang:demonitor(Mref, [flush]),
            Reply;
        {'DOWN', Mref, _, _, Reason} ->
            exit(Reason)
    end.

%% -------------------------------------------------------------------
%% gen_statem callbacks.
%% -------------------------------------------------------------------

init(#{context := Context, caller := Caller, group_leader := GroupLeader}) ->
    process_flag(trap_exit, true),
    erlang:link(Caller),
    erlang:group_leader(GroupLeader, self()),
    Priv = #?MODULE{caller = Caller},
    Context1 = Context#rabbit_cli{priv = Priv},
    {ok, standing_by, Context1, {next_event, internal, parse_command}}.

callback_mode() ->
    handle_event_function.

handle_event(internal, parse_command, standing_by, Context) ->
    %% We can query the argparse definition from the remote node to know
    %% the commands it supports and proceed with the execution.
    ArgparseDef = final_argparse_def(Context),
    Context1 = Context#rabbit_cli{argparse_def = ArgparseDef},

    case final_parse(Context1) of
        {ok, ArgMap, CmdPath, Command} ->
            Context2 = Context1#rabbit_cli{arg_map = ArgMap,
                                           cmd_path = CmdPath,
                                           command = Command},
            {next_state, command_parsed, Context2,
             {next_event, internal, run_command}};
        {error, Reason} ->
            {stop, {failed_to_parse_command, Reason}}
    end;
handle_event(
  internal, run_command, command_parsed,
  #rabbit_cli{arg_map = #{help := true}} = Context) ->
    display_help(Context),
    {stop, {shutdown, ok}, Context};
handle_event(internal, run_command, command_parsed, Context) ->
    Ret = do_run_command(Context),
    {stop, {shutdown, Ret}, Context}.

terminate(Reason, _State, _Data) ->
    ?LOG_DEBUG("CLI: backend terminating: ~0p", [Reason]),
    ok.

code_change(_Vsn, State, Data, _Extra) ->
    {ok, State, Data}.

%% -------------------------------------------------------------------
%% Argparse definition handling.
%% -------------------------------------------------------------------

final_argparse_def(
  #rabbit_cli{argparse_def = PartialArgparseDef}) ->
    FullArgparseDef = rabbit_cli_commands:discovered_argparse_def(),
    ArgparseDef1 = rabbit_cli_commands:merge_argparse_def(
                     PartialArgparseDef, FullArgparseDef),
    ArgparseDef1.

final_parse(
  #rabbit_cli{progname = ProgName, args = Args, argparse_def = ArgparseDef}) ->
    Options = #{progname => ProgName},
    argparse:parse(Args, ArgparseDef, Options).

%% -------------------------------------------------------------------
%% Command execution.
%% -------------------------------------------------------------------

do_run_command(
  #rabbit_cli{command = #{handler := {Module, Function}}} = Context) ->
    erlang:apply(Module, Function, [Context]).

display_help(#rabbit_cli{progname = Progname,
                         argparse_def = ArgparseDef,
                         arg_map = #{help := true},
                         cmd_path = CmdPath}) ->
    Options = #{progname => Progname,
                %% Work around bug in argparse;
                %% See https://github.com/erlang/otp/pull/9160
                command => tl(CmdPath)},
    Help = argparse:help(ArgparseDef, Options),
    io:format("~s~n", [Help]),
    ok.
