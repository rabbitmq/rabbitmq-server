%%   The contents of this file are subject to the Mozilla Public License
%%   Version 1.1 (the "License"); you may not use this file except in
%%   compliance with the License. You may obtain a copy of the License at
%%   http://www.mozilla.org/MPL/
%%
%%   Software distributed under the License is distributed on an "AS IS"
%%   basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See the
%%   License for the specific language governing rights and limitations
%%   under the License.
%%
%%   The Original Code is RabbitMQ.
%%
%%   The Initial Developers of the Original Code are LShift Ltd,
%%   Cohesive Financial Technologies LLC, and Rabbit Technologies Ltd.
%%
%%   Portions created before 22-Nov-2008 00:00:00 GMT by LShift Ltd,
%%   Cohesive Financial Technologies LLC, or Rabbit Technologies Ltd
%%   are Copyright (C) 2007-2008 LShift Ltd, Cohesive Financial
%%   Technologies LLC, and Rabbit Technologies Ltd.
%%
%%   Portions created by LShift Ltd are Copyright (C) 2007-2010 LShift
%%   Ltd. Portions created by Cohesive Financial Technologies LLC are
%%   Copyright (C) 2007-2010 Cohesive Financial Technologies
%%   LLC. Portions created by Rabbit Technologies Ltd are Copyright
%%   (C) 2007-2010 Rabbit Technologies Ltd.
%%
%%   All Rights Reserved.
%%
%%   Contributor(s): ______________________________________.
%%

-module(rabbit_control).
-include("rabbit.hrl").

-export([start/0, stop/0, action/4]).

-record(params, {quiet, node, command, args}).

-define(RPC_TIMEOUT, infinity).

%%----------------------------------------------------------------------------

-ifdef(use_specs).

-spec(start/0 :: () -> no_return()).
-spec(stop/0 :: () -> 'ok').
-spec(action/4 :: (atom(), erlang_node(), [string()],
                   fun ((string(), [any()]) -> 'ok')) -> 'ok').

-endif.

%%----------------------------------------------------------------------------

start() ->
    {ok, [[NodeStr|_]|_]} = init:get_argument(nodename),
    FullCommand = init:get_plain_arguments(),
    #params{quiet = Quiet, node = Node, command = Command, args = Args} =
        parse_args(FullCommand, #params{quiet = false,
                                        node = rabbit_misc:makenode(NodeStr)}),
    Inform = case Quiet of
                 true  -> fun(_Format, _Args1) -> ok end;
                 false -> fun(Format, Args1) ->
                                  io:format(Format ++ " ...~n", Args1)
                         end
             end,
    %% The reason we don't use a try/catch here is that rpc:call turns
    %% thrown errors into normal return values
    case catch action(Command, Node, Args, Inform) of
        ok ->
            case Quiet of
                true  -> ok;
                false -> io:format("...done.~n")
            end,
            halt();
        {'EXIT', {function_clause, [{?MODULE, action, _} | _]}} ->
            error("invalid command '~s'",
                  [lists:flatten(
                     rabbit_misc:intersperse(
                       " ", [atom_to_list(Command) | Args]))]),
            usage();
        {error, Reason} ->
            error("~p", [Reason]),
            halt(2);
        {badrpc, Reason} ->
            error("unable to connect to node ~w: ~w", [Node, Reason]),
            print_badrpc_diagnostics(Node),
            halt(2);
        Other ->
            error("~p", [Other]),
            halt(2)
    end.

fmt_stderr(Format, Args) -> rabbit_misc:format_stderr(Format ++ "~n", Args).

error(Format, Args) -> fmt_stderr("Error: " ++ Format, Args).

print_badrpc_diagnostics(Node) ->
    fmt_stderr("diagnostics:", []),
    {_NodeName, NodeHost} = rabbit_misc:nodeparts(Node),
    case net_adm:names(NodeHost) of
        {error, EpmdReason} ->
            fmt_stderr("- unable to connect to epmd on ~s: ~w",
                       [NodeHost, EpmdReason]);
        {ok, NamePorts} ->
            fmt_stderr("- nodes and their ports on ~s: ~p",
                       [NodeHost, [{list_to_atom(Name), Port} ||
                                      {Name, Port} <- NamePorts]])
            end,
    fmt_stderr("- current node: ~w", [node()]),
    case init:get_argument(home) of
        {ok, [[Home]]} -> fmt_stderr("- current node home dir: ~s", [Home]);
        Other          -> fmt_stderr("- no current node home dir: ~p", [Other])
    end,
    fmt_stderr("- current node cookie hash: ~s", [rabbit_misc:cookie_hash()]),
    ok.

parse_args(["-n", NodeS | Args], Params) ->
    parse_args(Args, Params#params{node = rabbit_misc:makenode(NodeS)});
parse_args(["-q" | Args], Params) ->
    parse_args(Args, Params#params{quiet = true});
parse_args([Command | Args], Params) ->
    Params#params{command = list_to_atom(Command), args = Args};
parse_args([], _) ->
    usage().

stop() ->
    ok.

usage() ->
    io:format("Usage: rabbitmqctl [-q] [-n <node>] <command> [<arg> ...]

Available commands:

  stop      - stops the RabbitMQ application and halts the node
  stop_app  - stops the RabbitMQ application, leaving the node running
  start_app - starts the RabbitMQ application on an already-running node
  reset     - resets node to default configuration, deleting all data
  force_reset
  cluster <ClusterNode> ...
  status
  rotate_logs [Suffix]

  add_user        <UserName> <Password>
  delete_user     <UserName>
  change_password <UserName> <NewPassword>
  list_users

  add_vhost    <VHostPath>
  delete_vhost <VHostPath>
  list_vhosts

  set_permissions   [-p <VHostPath>] <UserName> <Regexp> <Regexp> <Regexp>
  clear_permissions [-p <VHostPath>] <UserName>
  list_permissions  [-p <VHostPath>]
  list_user_permissions <UserName>

  list_queues    [-p <VHostPath>] [<QueueInfoItem> ...]
  list_exchanges [-p <VHostPath>] [<ExchangeInfoItem> ...]
  list_bindings  [-p <VHostPath>]
  list_connections [<ConnectionInfoItem> ...]

Quiet output mode is selected with the \"-q\" flag. Informational
messages are suppressed when quiet mode is in effect.

<node> should be the name of the master node of the RabbitMQ
cluster. It defaults to the node named \"rabbit\" on the local
host. On a host named \"server.example.com\", the master node will
usually be rabbit@server (unless RABBITMQ_NODENAME has been set to
some non-default value at broker startup time). The output of hostname
-s is usually the correct suffix to use after the \"@\" sign.

The list_queues, list_exchanges and list_bindings commands accept an
optional virtual host parameter for which to display results. The
default value is \"/\".

<QueueInfoItem> must be a member of the list [name, durable,
auto_delete, arguments, pid, messages_ready, messages_unacknowledged,
messages_uncommitted, messages, acks_uncommitted, consumers,
transactions, memory]. The default is to display name and (number of)
messages.

<ExchangeInfoItem> must be a member of the list [name, type, durable,
auto_delete, arguments]. The default is to display name and type.

The output format for \"list_bindings\" is a list of rows containing
exchange name, queue name, routing key and arguments, in that order.

<ConnectionInfoItem> must be a member of the list [pid, address, port,
peer_address, peer_port, state, channels, user, vhost, timeout,
frame_max, client_properties, recv_oct, recv_cnt, send_oct, send_cnt,
send_pend].  The default is to display user, peer_address, peer_port
and state.

"),
    halt(1).

action(stop, Node, [], Inform) ->
    Inform("Stopping and halting node ~p", [Node]),
    call(Node, {rabbit, stop_and_halt, []});

action(stop_app, Node, [], Inform) ->
    Inform("Stopping node ~p", [Node]),
    call(Node, {rabbit, stop, []});

action(start_app, Node, [], Inform) ->
    Inform("Starting node ~p", [Node]),
    call(Node, {rabbit, start, []});

action(reset, Node, [], Inform) ->
    Inform("Resetting node ~p", [Node]),
    call(Node, {rabbit_mnesia, reset, []});

action(force_reset, Node, [], Inform) ->
    Inform("Forcefully resetting node ~p", [Node]),
    call(Node, {rabbit_mnesia, force_reset, []});

action(cluster, Node, ClusterNodeSs, Inform) ->
    ClusterNodes = lists:map(fun list_to_atom/1, ClusterNodeSs),
    Inform("Clustering node ~p with ~p",
              [Node, ClusterNodes]),
    rpc_call(Node, rabbit_mnesia, cluster, [ClusterNodes]);

action(status, Node, [], Inform) ->
    Inform("Status of node ~p", [Node]),
    case call(Node, {rabbit, status, []}) of
        {badrpc, _} = Res -> Res;
        Res               -> io:format("~p~n", [Res]),
                             ok
    end;

action(rotate_logs, Node, [], Inform) ->
    Inform("Reopening logs for node ~p", [Node]),
    call(Node, {rabbit, rotate_logs, [""]});
action(rotate_logs, Node, Args = [Suffix], Inform) ->
    Inform("Rotating logs to files with suffix ~p", [Suffix]),
    call(Node, {rabbit, rotate_logs, Args});

action(add_user, Node, Args = [Username, _Password], Inform) ->
    Inform("Creating user ~p", [Username]),
    call(Node, {rabbit_access_control, add_user, Args});

action(delete_user, Node, Args = [_Username], Inform) ->
    Inform("Deleting user ~p", Args),
    call(Node, {rabbit_access_control, delete_user, Args});

action(change_password, Node, Args = [Username, _Newpassword], Inform) ->
    Inform("Changing password for user ~p", [Username]),
    call(Node, {rabbit_access_control, change_password, Args});

action(list_users, Node, [], Inform) ->
    Inform("Listing users", []),
    display_list(call(Node, {rabbit_access_control, list_users, []}));

action(add_vhost, Node, Args = [_VHostPath], Inform) ->
    Inform("Creating vhost ~p", Args),
    call(Node, {rabbit_access_control, add_vhost, Args});

action(delete_vhost, Node, Args = [_VHostPath], Inform) ->
    Inform("Deleting vhost ~p", Args),
    call(Node, {rabbit_access_control, delete_vhost, Args});

action(list_vhosts, Node, [], Inform) ->
    Inform("Listing vhosts", []),
    display_list(call(Node, {rabbit_access_control, list_vhosts, []}));

action(list_user_permissions, Node, Args = [_Username], Inform) ->
    Inform("Listing permissions for user ~p", Args),
    display_list(call(Node, {rabbit_access_control, list_user_permissions,
                             Args}));

action(list_queues, Node, Args, Inform) ->
    Inform("Listing queues", []),
    {VHostArg, RemainingArgs} = parse_vhost_flag_bin(Args),
    ArgAtoms = default_if_empty(RemainingArgs, [name, messages]),
    display_info_list(rpc_call(Node, rabbit_amqqueue, info_all,
                               [VHostArg, ArgAtoms]),
                      ArgAtoms);

action(list_exchanges, Node, Args, Inform) ->
    Inform("Listing exchanges", []),
    {VHostArg, RemainingArgs} = parse_vhost_flag_bin(Args),
    ArgAtoms = default_if_empty(RemainingArgs, [name, type]),
    display_info_list(rpc_call(Node, rabbit_exchange, info_all,
                               [VHostArg, ArgAtoms]),
                      ArgAtoms);

action(list_bindings, Node, Args, Inform) ->
    Inform("Listing bindings", []),
    {VHostArg, _} = parse_vhost_flag_bin(Args),
    InfoKeys = [exchange_name, queue_name, routing_key, args],
    display_info_list(
      [lists:zip(InfoKeys, tuple_to_list(X)) ||
          X <- rpc_call(Node, rabbit_exchange, list_bindings, [VHostArg])],
      InfoKeys),
    ok;

action(list_connections, Node, Args, Inform) ->
    Inform("Listing connections", []),
    ArgAtoms = default_if_empty(Args, [user, peer_address, peer_port, state]),
    display_info_list(rpc_call(Node, rabbit_networking, connection_info_all,
                               [ArgAtoms]),
                      ArgAtoms);

action(Command, Node, Args, Inform) ->
    {VHost, RemainingArgs} = parse_vhost_flag(Args),
    action(Command, Node, VHost, RemainingArgs, Inform).

action(set_permissions, Node, VHost, [Username, CPerm, WPerm, RPerm], Inform) ->
    Inform("Setting permissions for user ~p in vhost ~p", [Username, VHost]),
    call(Node, {rabbit_access_control, set_permissions,
                [Username, VHost, CPerm, WPerm, RPerm]});

action(clear_permissions, Node, VHost, [Username], Inform) ->
    Inform("Clearing permissions for user ~p in vhost ~p", [Username, VHost]),
    call(Node, {rabbit_access_control, clear_permissions, [Username, VHost]});

action(list_permissions, Node, VHost, [], Inform) ->
    Inform("Listing permissions in vhost ~p", [VHost]),
    display_list(call(Node, {rabbit_access_control, list_vhost_permissions,
                             [VHost]})).

parse_vhost_flag(Args) when is_list(Args) ->
    case Args of
        ["-p", VHost | RemainingArgs] ->
            {VHost, RemainingArgs};
        RemainingArgs ->
            {"/", RemainingArgs}
    end.

parse_vhost_flag_bin(Args) ->
    {VHost, RemainingArgs} = parse_vhost_flag(Args),
    {list_to_binary(VHost), RemainingArgs}.

default_if_empty(List, Default) when is_list(List) ->
    if List == [] ->
        Default;
       true ->
        [list_to_atom(X) || X <- List]
    end.

display_info_list(Results, InfoItemKeys) when is_list(Results) ->
    lists:foreach(fun (Result) -> display_row([format_info_item(X, Result) ||
                                                  X <- InfoItemKeys])
                  end, Results),
    ok;
display_info_list(Other, _) ->
    Other.

display_row(Row) ->
    io:fwrite(lists:flatten(rabbit_misc:intersperse("\t", Row))),
    io:nl().

format_info_item(Key, Items) ->
    case proplists:get_value(Key, Items) of
        #resource{name = Name} ->
            escape(Name);
        Value when Key =:= address; Key =:= peer_address andalso
                   is_tuple(Value) ->
            inet_parse:ntoa(Value);
        Value when is_pid(Value) ->
            pid_to_string(Value);
        Value when is_binary(Value) ->
            escape(Value);
        Value when is_atom(Value) ->
            escape(atom_to_list(Value));
        Value = [{TableEntryKey, TableEntryType, _TableEntryValue} | _]
        when is_binary(TableEntryKey) andalso is_atom(TableEntryType) ->
            io_lib:format("~1000000000000p", [prettify_amqp_table(Value)]);
        Value ->
            io_lib:format("~w", [Value])
    end.

display_list(L) when is_list(L) ->
    lists:foreach(fun (I) when is_binary(I) ->
                          io:format("~s~n", [escape(I)]);
                      (I) when is_tuple(I) ->
                          display_row([escape(V)
                                       || V <- tuple_to_list(I)])
                  end,
                  lists:sort(L)),
    ok;
display_list(Other) -> Other.

call(Node, {Mod, Fun, Args}) ->
    rpc_call(Node, Mod, Fun, lists:map(fun list_to_binary/1, Args)).

rpc_call(Node, Mod, Fun, Args) ->
    rpc:call(Node, Mod, Fun, Args, ?RPC_TIMEOUT).

%% escape does C-style backslash escaping of non-printable ASCII
%% characters.  We don't escape characters above 127, since they may
%% form part of UTF-8 strings.

escape(Bin) when is_binary(Bin) ->
    escape(binary_to_list(Bin));
escape(L) when is_list(L) ->
    escape_char(lists:reverse(L), []).

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

prettify_typed_amqp_value(Type, Value) ->
    case Type of
        longstr -> escape(Value);
        table   -> prettify_amqp_table(Value);
        array   -> [prettify_typed_amqp_value(T, V) || {T, V} <- Value];
        _       -> Value
    end.

%% see http://erlang.org/doc/apps/erts/erl_ext_dist.html (8.10 and 8.7)
pid_to_string(Pid) ->
    <<131,103,100,NodeLen:16,NodeBin:NodeLen/binary,Id:32,Ser:32,_Cre:8>>
        = term_to_binary(Pid),
    Node = binary_to_term(<<131,100,NodeLen:16,NodeBin:NodeLen/binary>>),
    lists:flatten(io_lib:format("<~w.~B.~B>", [Node, Id, Ser])).
