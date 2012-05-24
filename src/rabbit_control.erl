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
%% The Initial Developer of the Original Code is VMware, Inc.
%% Copyright (c) 2007-2012 VMware, Inc.  All rights reserved.
%%

-module(rabbit_control).
-include("rabbit.hrl").

-export([start/0, stop/0, action/5]).

-define(RPC_TIMEOUT, infinity).
-define(EXTERNAL_CHECK_INTERVAL, 1000).

-define(QUIET_OPT, "-q").
-define(NODE_OPT, "-n").
-define(VHOST_OPT, "-p").

-define(QUIET_DEF, {?QUIET_OPT, flag}).
-define(NODE_DEF(Node), {?NODE_OPT, {option, Node}}).
-define(VHOST_DEF, {?VHOST_OPT, {option, "/"}}).

-define(GLOBAL_DEFS(Node), [?QUIET_DEF, ?NODE_DEF(Node)]).

-define(COMMANDS,
        [stop,
         stop_app,
         start_app,
         wait,
         reset,
         force_reset,
         rotate_logs,

         cluster,
         force_cluster,
         cluster_status,

         add_user,
         delete_user,
         change_password,
         clear_password,
         set_user_tags,
         list_users,

         add_vhost,
         delete_vhost,
         list_vhosts,
         {set_permissions, [?VHOST_DEF]},
         {clear_permissions, [?VHOST_DEF]},
         {list_permissions, [?VHOST_DEF]},
         list_user_permissions,

         set_parameter,
         clear_parameter,
         list_parameters,

         {list_queues, [?VHOST_DEF]},
         {list_exchanges, [?VHOST_DEF]},
         {list_bindings, [?VHOST_DEF]},
         {list_connections, [?VHOST_DEF]},
         list_channels,
         {list_consumers, [?VHOST_DEF]},
         status,
         environment,
         report,
         eval,

         close_connection,
         {trace_on, [?VHOST_DEF]},
         {trace_off, [?VHOST_DEF]},
         set_vm_memory_high_watermark
        ]).

-define(GLOBAL_QUERIES,
        [{"Connections", rabbit_networking, connection_info_all,
          connection_info_keys},
         {"Channels",  rabbit_channel,  info_all, info_keys}]).

-define(VHOST_QUERIES,
        [{"Queues",    rabbit_amqqueue, info_all, info_keys},
         {"Exchanges", rabbit_exchange, info_all, info_keys},
         {"Bindings",  rabbit_binding,  info_all, info_keys},
         {"Consumers", rabbit_amqqueue, consumers_all, consumer_info_keys},
         {"Permissions", rabbit_auth_backend_internal, list_vhost_permissions,
          vhost_perms_info_keys}]).

%%----------------------------------------------------------------------------

-ifdef(use_specs).

-spec(start/0 :: () -> no_return()).
-spec(stop/0 :: () -> 'ok').
-spec(action/5 ::
        (atom(), node(), [string()], [{string(), any()}],
         fun ((string(), [any()]) -> 'ok'))
        -> 'ok').
-spec(usage/0 :: () -> no_return()).

-endif.

%%----------------------------------------------------------------------------

start() ->
    {ok, [[NodeStr|_]|_]} = init:get_argument(nodename),
    {Command, Opts, Args} =
        case rabbit_misc:parse_arguments(?COMMANDS, ?GLOBAL_DEFS(NodeStr),
                                         init:get_plain_arguments())
        of
            {ok, Res}  -> Res;
            no_command -> print_error("could not recognise command", []),
                          usage()
        end,
    Opts1 = [case K of
                 ?NODE_OPT -> {?NODE_OPT, rabbit_nodes:make(V)};
                 _         -> {K, V}
             end || {K, V} <- Opts],
    Quiet = proplists:get_bool(?QUIET_OPT, Opts1),
    Node = proplists:get_value(?NODE_OPT, Opts1),
    Inform = case Quiet of
                 true  -> fun (_Format, _Args1) -> ok end;
                 false -> fun (Format, Args1) ->
                                  io:format(Format ++ " ...~n", Args1)
                          end
             end,
    PrintInvalidCommandError =
        fun () ->
                print_error("invalid command '~s'",
                            [string:join([atom_to_list(Command) | Args], " ")])
        end,

    %% The reason we don't use a try/catch here is that rpc:call turns
    %% thrown errors into normal return values
    case catch action(Command, Node, Args, Opts, Inform) of
        ok ->
            case Quiet of
                true  -> ok;
                false -> io:format("...done.~n")
            end,
            rabbit_misc:quit(0);
        {'EXIT', {function_clause, [{?MODULE, action, _}    | _]}} -> %% < R15
            PrintInvalidCommandError(),
            usage();
        {'EXIT', {function_clause, [{?MODULE, action, _, _} | _]}} -> %% >= R15
            PrintInvalidCommandError(),
            usage();
        {'EXIT', {badarg, _}} ->
            print_error("invalid parameter: ~p", [Args]),
            usage();
        {error, Reason} ->
            print_error("~p", [Reason]),
            rabbit_misc:quit(2);
        {error_string, Reason} ->
            print_error("~s", [Reason]),
            rabbit_misc:quit(2);
        {badrpc, {'EXIT', Reason}} ->
            print_error("~p", [Reason]),
            rabbit_misc:quit(2);
        {badrpc, Reason} ->
            print_error("unable to connect to node ~w: ~w", [Node, Reason]),
            print_badrpc_diagnostics(Node),
            rabbit_misc:quit(2);
        Other ->
            print_error("~p", [Other]),
            rabbit_misc:quit(2)
    end.

fmt_stderr(Format, Args) -> rabbit_misc:format_stderr(Format ++ "~n", Args).

print_report(Node, {Descr, Module, InfoFun, KeysFun}) ->
    io:format("~s:~n", [Descr]),
    print_report0(Node, {Module, InfoFun, KeysFun}, []).

print_report(Node, {Descr, Module, InfoFun, KeysFun}, VHostArg) ->
    io:format("~s on ~s:~n", [Descr, VHostArg]),
    print_report0(Node, {Module, InfoFun, KeysFun}, VHostArg).

print_report0(Node, {Module, InfoFun, KeysFun}, VHostArg) ->
    case Results = rpc_call(Node, Module, InfoFun, VHostArg) of
        [_|_] -> InfoItems = rpc_call(Node, Module, KeysFun, []),
                 display_row([atom_to_list(I) || I <- InfoItems]),
                 display_info_list(Results, InfoItems);
        _     -> ok
    end,
    io:nl().

print_error(Format, Args) -> fmt_stderr("Error: " ++ Format, Args).

print_badrpc_diagnostics(Node) ->
    fmt_stderr(rabbit_nodes:diagnostics([Node]), []).

stop() ->
    ok.

usage() ->
    io:format("~s", [rabbit_ctl_usage:usage()]),
    rabbit_misc:quit(1).

%%----------------------------------------------------------------------------

action(stop, Node, Args, _Opts, Inform) ->
    Inform("Stopping and halting node ~p", [Node]),
    Res = call(Node, {rabbit, stop_and_halt, []}),
    case {Res, Args} of
        {ok, [PidFile]} -> wait_for_process_death(
                             read_pid_file(PidFile, false));
        {ok, [_, _| _]} -> exit({badarg, Args});
        _               -> ok
    end,
    Res;

action(stop_app, Node, [], _Opts, Inform) ->
    Inform("Stopping node ~p", [Node]),
    call(Node, {rabbit, stop, []});

action(start_app, Node, [], _Opts, Inform) ->
    Inform("Starting node ~p", [Node]),
    call(Node, {rabbit, start, []});

action(reset, Node, [], _Opts, Inform) ->
    Inform("Resetting node ~p", [Node]),
    call(Node, {rabbit_mnesia, reset, []});

action(force_reset, Node, [], _Opts, Inform) ->
    Inform("Forcefully resetting node ~p", [Node]),
    call(Node, {rabbit_mnesia, force_reset, []});

action(cluster, Node, ClusterNodeSs, _Opts, Inform) ->
    ClusterNodes = lists:map(fun list_to_atom/1, ClusterNodeSs),
    Inform("Clustering node ~p with ~p",
           [Node, ClusterNodes]),
    rpc_call(Node, rabbit_mnesia, cluster, [ClusterNodes]);

action(force_cluster, Node, ClusterNodeSs, _Opts, Inform) ->
    ClusterNodes = lists:map(fun list_to_atom/1, ClusterNodeSs),
    Inform("Forcefully clustering node ~p with ~p (ignoring offline nodes)",
           [Node, ClusterNodes]),
    rpc_call(Node, rabbit_mnesia, force_cluster, [ClusterNodes]);

action(wait, Node, [PidFile], _Opts, Inform) ->
    Inform("Waiting for ~p", [Node]),
    wait_for_application(Node, PidFile, rabbit, Inform);
action(wait, Node, [PidFile, App], _Opts, Inform) ->
    Inform("Waiting for ~p on ~p", [App, Node]),
    wait_for_application(Node, PidFile, list_to_atom(App), Inform);

action(status, Node, [], _Opts, Inform) ->
    Inform("Status of node ~p", [Node]),
    display_call_result(Node, {rabbit, status, []});

action(cluster_status, Node, [], _Opts, Inform) ->
    Inform("Cluster status of node ~p", [Node]),
    display_call_result(Node, {rabbit_mnesia, status, []});

action(environment, Node, _App, _Opts, Inform) ->
    Inform("Application environment of node ~p", [Node]),
    display_call_result(Node, {rabbit, environment, []});

action(rotate_logs, Node, [], _Opts, Inform) ->
    Inform("Reopening logs for node ~p", [Node]),
    call(Node, {rabbit, rotate_logs, [""]});
action(rotate_logs, Node, Args = [Suffix], _Opts, Inform) ->
    Inform("Rotating logs to files with suffix \"~s\"", [Suffix]),
    call(Node, {rabbit, rotate_logs, Args});

action(close_connection, Node, [PidStr, Explanation], _Opts, Inform) ->
    Inform("Closing connection \"~s\"", [PidStr]),
    rpc_call(Node, rabbit_networking, close_connection,
             [rabbit_misc:string_to_pid(PidStr), Explanation]);

action(add_user, Node, Args = [Username, _Password], _Opts, Inform) ->
    Inform("Creating user \"~s\"", [Username]),
    call(Node, {rabbit_auth_backend_internal, add_user, Args});

action(delete_user, Node, Args = [_Username], _Opts, Inform) ->
    Inform("Deleting user \"~s\"", Args),
    call(Node, {rabbit_auth_backend_internal, delete_user, Args});

action(change_password, Node, Args = [Username, _Newpassword], _Opts, Inform) ->
    Inform("Changing password for user \"~s\"", [Username]),
    call(Node, {rabbit_auth_backend_internal, change_password, Args});

action(clear_password, Node, Args = [Username], _Opts, Inform) ->
    Inform("Clearing password for user \"~s\"", [Username]),
    call(Node, {rabbit_auth_backend_internal, clear_password, Args});

action(set_user_tags, Node, [Username | TagsStr], _Opts, Inform) ->
    Tags = [list_to_atom(T) || T <- TagsStr],
    Inform("Setting tags for user \"~s\" to ~p", [Username, Tags]),
    rpc_call(Node, rabbit_auth_backend_internal, set_tags,
             [list_to_binary(Username), Tags]);

action(list_users, Node, [], _Opts, Inform) ->
    Inform("Listing users", []),
    display_info_list(
      call(Node, {rabbit_auth_backend_internal, list_users, []}),
      rabbit_auth_backend_internal:user_info_keys());

action(add_vhost, Node, Args = [_VHostPath], _Opts, Inform) ->
    Inform("Creating vhost \"~s\"", Args),
    call(Node, {rabbit_vhost, add, Args});

action(delete_vhost, Node, Args = [_VHostPath], _Opts, Inform) ->
    Inform("Deleting vhost \"~s\"", Args),
    call(Node, {rabbit_vhost, delete, Args});

action(list_vhosts, Node, Args, _Opts, Inform) ->
    Inform("Listing vhosts", []),
    ArgAtoms = default_if_empty(Args, [name]),
    display_info_list(call(Node, {rabbit_vhost, info_all, []}), ArgAtoms);

action(list_user_permissions, Node, Args = [_Username], _Opts, Inform) ->
    Inform("Listing permissions for user ~p", Args),
    display_info_list(call(Node, {rabbit_auth_backend_internal,
                                  list_user_permissions, Args}),
                      rabbit_auth_backend_internal:user_perms_info_keys());

action(list_queues, Node, Args, Opts, Inform) ->
    Inform("Listing queues", []),
    VHostArg = list_to_binary(proplists:get_value(?VHOST_OPT, Opts)),
    ArgAtoms = default_if_empty(Args, [name, messages]),
    display_info_list(rpc_call(Node, rabbit_amqqueue, info_all,
                               [VHostArg, ArgAtoms]),
                      ArgAtoms);

action(list_exchanges, Node, Args, Opts, Inform) ->
    Inform("Listing exchanges", []),
    VHostArg = list_to_binary(proplists:get_value(?VHOST_OPT, Opts)),
    ArgAtoms = default_if_empty(Args, [name, type]),
    display_info_list(rpc_call(Node, rabbit_exchange, info_all,
                               [VHostArg, ArgAtoms]),
                      ArgAtoms);

action(list_bindings, Node, Args, Opts, Inform) ->
    Inform("Listing bindings", []),
    VHostArg = list_to_binary(proplists:get_value(?VHOST_OPT, Opts)),
    ArgAtoms = default_if_empty(Args, [source_name, source_kind,
                                       destination_name, destination_kind,
                                       routing_key, arguments]),
    display_info_list(rpc_call(Node, rabbit_binding, info_all,
                               [VHostArg, ArgAtoms]),
                      ArgAtoms);

action(list_connections, Node, Args, _Opts, Inform) ->
    Inform("Listing connections", []),
    ArgAtoms = default_if_empty(Args, [user, peer_address, peer_port, state]),
    display_info_list(rpc_call(Node, rabbit_networking, connection_info_all,
                               [ArgAtoms]),
                      ArgAtoms);

action(list_channels, Node, Args, _Opts, Inform) ->
    Inform("Listing channels", []),
    ArgAtoms = default_if_empty(Args, [pid, user, consumer_count,
                                       messages_unacknowledged]),
    display_info_list(rpc_call(Node, rabbit_channel, info_all, [ArgAtoms]),
                      ArgAtoms);

action(list_consumers, Node, _Args, Opts, Inform) ->
    Inform("Listing consumers", []),
    VHostArg = list_to_binary(proplists:get_value(?VHOST_OPT, Opts)),
    display_info_list(rpc_call(Node, rabbit_amqqueue, consumers_all, [VHostArg]),
                      rabbit_amqqueue:consumer_info_keys());

action(trace_on, Node, [], Opts, Inform) ->
    VHost = proplists:get_value(?VHOST_OPT, Opts),
    Inform("Starting tracing for vhost \"~s\"", [VHost]),
    rpc_call(Node, rabbit_trace, start, [list_to_binary(VHost)]);

action(trace_off, Node, [], Opts, Inform) ->
    VHost = proplists:get_value(?VHOST_OPT, Opts),
    Inform("Stopping tracing for vhost \"~s\"", [VHost]),
    rpc_call(Node, rabbit_trace, stop, [list_to_binary(VHost)]);

action(set_vm_memory_high_watermark, Node, [Arg], _Opts, Inform) ->
    Frac = list_to_float(case string:chr(Arg, $.) of
                             0 -> Arg ++ ".0";
                             _ -> Arg
                         end),
    Inform("Setting memory threshold on ~p to ~p", [Node, Frac]),
    rpc_call(Node, vm_memory_monitor, set_vm_memory_high_watermark, [Frac]);

action(set_permissions, Node, [Username, CPerm, WPerm, RPerm], Opts, Inform) ->
    VHost = proplists:get_value(?VHOST_OPT, Opts),
    Inform("Setting permissions for user \"~s\" in vhost \"~s\"",
           [Username, VHost]),
    call(Node, {rabbit_auth_backend_internal, set_permissions,
                [Username, VHost, CPerm, WPerm, RPerm]});

action(clear_permissions, Node, [Username], Opts, Inform) ->
    VHost = proplists:get_value(?VHOST_OPT, Opts),
    Inform("Clearing permissions for user \"~s\" in vhost \"~s\"",
           [Username, VHost]),
    call(Node, {rabbit_auth_backend_internal, clear_permissions,
                [Username, VHost]});

action(list_permissions, Node, [], Opts, Inform) ->
    VHost = proplists:get_value(?VHOST_OPT, Opts),
    Inform("Listing permissions in vhost \"~s\"", [VHost]),
    display_info_list(call(Node, {rabbit_auth_backend_internal,
                             list_vhost_permissions, [VHost]}),
                      rabbit_auth_backend_internal:vhost_perms_info_keys());

action(set_parameter, Node, [Component, Key, Value], _Opts, Inform) ->
    Inform("Setting runtime parameter ~p for component ~p to ~p",
           [Key, Component, Value]),
    rpc_call(Node, rabbit_runtime_parameters, parse_set,
             [list_to_binary(Component), list_to_binary(Key), Value]);

action(clear_parameter, Node, [Component, Key], _Opts, Inform) ->
    Inform("Clearing runtime parameter ~p for component ~p", [Key, Component]),
    rpc_call(Node, rabbit_runtime_parameters, clear, [list_to_binary(Component),
                                                      list_to_binary(Key)]);

action(list_parameters, Node, Args = [], _Opts, Inform) ->
    Inform("Listing runtime parameters", []),
    display_info_list(
      rpc_call(Node, rabbit_runtime_parameters, list_formatted, Args),
      rabbit_runtime_parameters:info_keys());

action(report, Node, _Args, _Opts, Inform) ->
    io:format("Reporting server status on ~p~n~n", [erlang:universaltime()]),
    [begin ok = action(Action, N, [], [], Inform), io:nl() end ||
        N      <- unsafe_rpc(Node, rabbit_mnesia, running_clustered_nodes, []),
        Action <- [status, cluster_status, environment]],
    VHosts = unsafe_rpc(Node, rabbit_vhost, list, []),
    [print_report(Node, Q)      || Q <- ?GLOBAL_QUERIES],
    [print_report(Node, Q, [V]) || Q <- ?VHOST_QUERIES, V <- VHosts],
    io:format("End of server status report~n"),
    ok;

action(eval, Node, [Expr], _Opts, _Inform) ->
    case erl_scan:string(Expr) of
        {ok, Scanned, _} ->
            case erl_parse:parse_exprs(Scanned) of
                {ok, Parsed} ->
                    {value, Value, _} = unsafe_rpc(
                                          Node, erl_eval, exprs, [Parsed, []]),
                    io:format("~p~n", [Value]),
                    ok;
                {error, E} ->
                    {error_string, format_parse_error(E)}
            end;
        {error, E, _} ->
            {error_string, format_parse_error(E)}
    end.

%%----------------------------------------------------------------------------

wait_for_application(Node, PidFile, Application, Inform) ->
    Pid = read_pid_file(PidFile, true),
    Inform("pid is ~s", [Pid]),
    wait_for_application(Node, Pid, Application).

wait_for_application(Node, Pid, Application) ->
    case process_up(Pid) of
        true  -> case rabbit_nodes:is_running(Node, Application) of
                     true  -> ok;
                     false -> timer:sleep(?EXTERNAL_CHECK_INTERVAL),
                              wait_for_application(Node, Pid, Application)
                 end;
        false -> {error, process_not_running}
    end.

wait_for_process_death(Pid) ->
    case process_up(Pid) of
        true  -> timer:sleep(?EXTERNAL_CHECK_INTERVAL),
                 wait_for_process_death(Pid);
        false -> ok
    end.

read_pid_file(PidFile, Wait) ->
    case {file:read_file(PidFile), Wait} of
        {{ok, Bin}, _} ->
            S = string:strip(binary_to_list(Bin), right, $\n),
            try list_to_integer(S)
            catch error:badarg ->
                    exit({error, {garbage_in_pid_file, PidFile}})
            end,
            S;
        {{error, enoent}, true} ->
            timer:sleep(?EXTERNAL_CHECK_INTERVAL),
            read_pid_file(PidFile, Wait);
        {{error, _} = E, _} ->
            exit({error, {could_not_read_pid, E}})
    end.

% Test using some OS clunkiness since we shouldn't trust
% rpc:call(os, getpid, []) at this point
process_up(Pid) ->
    with_os([{unix, fun () ->
                            system("ps -p " ++ Pid
                                   ++ " >/dev/null 2>&1") =:= 0
                    end},
             {win32, fun () ->
                             Res = os:cmd("tasklist /nh /fi \"pid eq " ++
                                          Pid ++ "\" 2>&1"),
                             case re:run(Res, "erl\\.exe", [{capture, none}]) of
                                 match -> true;
                                 _     -> false
                             end
                     end}]).

with_os(Handlers) ->
    {OsFamily, _} = os:type(),
    case proplists:get_value(OsFamily, Handlers) of
        undefined -> throw({unsupported_os, OsFamily});
        Handler   -> Handler()
    end.

% Like system(3)
system(Cmd) ->
    ShCmd = "sh -c '" ++ escape_quotes(Cmd) ++ "'",
    Port = erlang:open_port({spawn, ShCmd}, [exit_status,nouse_stdio]),
    receive {Port, {exit_status, Status}} -> Status end.

% Escape the quotes in a shell command so that it can be used in "sh -c 'cmd'"
escape_quotes(Cmd) ->
    lists:flatten(lists:map(fun ($') -> "'\\''"; (Ch) -> Ch end, Cmd)).

format_parse_error({_Line, Mod, Err}) ->
    lists:flatten(Mod:format_error(Err)).

%%----------------------------------------------------------------------------

default_if_empty(List, Default) when is_list(List) ->
    if List == [] -> Default;
       true       -> [list_to_atom(X) || X <- List]
    end.

display_info_list(Results, InfoItemKeys) when is_list(Results) ->
    lists:foreach(
      fun (Result) -> display_row(
                        [format_info_item(proplists:get_value(X, Result)) ||
                            X <- InfoItemKeys])
      end, Results),
    ok;
display_info_list(Other, _) ->
    Other.

display_row(Row) ->
    io:fwrite(string:join(Row, "\t")),
    io:nl().

-define(IS_U8(X),  (X >= 0 andalso X =< 255)).
-define(IS_U16(X), (X >= 0 andalso X =< 65535)).

format_info_item(#resource{name = Name}) ->
    escape(Name);
format_info_item({N1, N2, N3, N4} = Value) when
      ?IS_U8(N1), ?IS_U8(N2), ?IS_U8(N3), ?IS_U8(N4) ->
    rabbit_misc:ntoa(Value);
format_info_item({K1, K2, K3, K4, K5, K6, K7, K8} = Value) when
      ?IS_U16(K1), ?IS_U16(K2), ?IS_U16(K3), ?IS_U16(K4),
      ?IS_U16(K5), ?IS_U16(K6), ?IS_U16(K7), ?IS_U16(K8) ->
    rabbit_misc:ntoa(Value);
format_info_item(Value) when is_pid(Value) ->
    rabbit_misc:pid_to_string(Value);
format_info_item(Value) when is_binary(Value) ->
    escape(Value);
format_info_item(Value) when is_atom(Value) ->
    escape(atom_to_list(Value));
format_info_item([{TableEntryKey, TableEntryType, _TableEntryValue} | _] =
                     Value) when is_binary(TableEntryKey) andalso
                                 is_atom(TableEntryType) ->
    io_lib:format("~1000000000000p", [prettify_amqp_table(Value)]);
format_info_item([T | _] = Value)
  when is_tuple(T) orelse is_pid(T) orelse is_binary(T) orelse is_atom(T) orelse
       is_list(T) ->
    "[" ++
        lists:nthtail(2, lists:append(
                           [", " ++ format_info_item(E) || E <- Value])) ++ "]";
format_info_item(Value) ->
    io_lib:format("~w", [Value]).

display_call_result(Node, MFA) ->
    case call(Node, MFA) of
        {badrpc, _} = Res -> throw(Res);
        Res               -> io:format("~p~n", [Res]),
                             ok
    end.

unsafe_rpc(Node, Mod, Fun, Args) ->
    case rpc_call(Node, Mod, Fun, Args) of
        {badrpc, _} = Res -> throw(Res);
        Normal            -> Normal
    end.

call(Node, {Mod, Fun, Args}) ->
    rpc_call(Node, Mod, Fun, lists:map(fun list_to_binary/1, Args)).

rpc_call(Node, Mod, Fun, Args) ->
    rpc:call(Node, Mod, Fun, Args, ?RPC_TIMEOUT).

%% escape does C-style backslash escaping of non-printable ASCII
%% characters.  We don't escape characters above 127, since they may
%% form part of UTF-8 strings.

escape(Atom) when is_atom(Atom)  -> escape(atom_to_list(Atom));
escape(Bin)  when is_binary(Bin) -> escape(binary_to_list(Bin));
escape(L)    when is_list(L)     -> escape_char(lists:reverse(L), []).

escape_char([$\\ | T], Acc) ->
    escape_char(T, [$\\, $\\ | Acc]);
escape_char([X | T], Acc) when X >= 32, X /= 127 ->
    escape_char(T, [X | Acc]);
escape_char([X | T], Acc) ->
    escape_char(T, [$\\, $0 + (X bsr 6), $0 + (X band 8#070 bsr 3),
                    $0 + (X band 7) | Acc]);
escape_char([], Acc) ->
    Acc.

prettify_amqp_table(Table) ->
    [{escape(K), prettify_typed_amqp_value(T, V)} || {K, T, V} <- Table].

prettify_typed_amqp_value(longstr, Value) -> escape(Value);
prettify_typed_amqp_value(table,   Value) -> prettify_amqp_table(Value);
prettify_typed_amqp_value(array,   Value) -> [prettify_typed_amqp_value(T, V) ||
                                                 {T, V} <- Value];
prettify_typed_amqp_value(_Type,   Value) -> Value.
