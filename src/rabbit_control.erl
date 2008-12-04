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
%%   The Initial Developers of the Original Code are LShift Ltd.,
%%   Cohesive Financial Technologies LLC., and Rabbit Technologies Ltd.
%%
%%   Portions created by LShift Ltd., Cohesive Financial Technologies
%%   LLC., and Rabbit Technologies Ltd. are Copyright (C) 2007-2008
%%   LShift Ltd., Cohesive Financial Technologies LLC., and Rabbit
%%   Technologies Ltd.;
%%
%%   All Rights Reserved.
%%
%%   Contributor(s): ______________________________________.
%%

-module(rabbit_control).
-include("rabbit.hrl").

-export([start/0, stop/0, action/4]).

-record(params, {quiet, node, command, args}).

-define(RPC_TIMEOUT, 30000).

start() ->
    FullCommand = init:get_plain_arguments(),
    #params{quiet = Quiet, node = Node, command = Command, args = Args} = 
        parse_args(FullCommand, #params{quiet = false,
                                        node = rabbit_misc:localnode(rabbit)}),
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
            init:stop();
        {'EXIT', {function_clause, [{?MODULE, action, _} | _]}} ->
            error("invalid command '~s'",
                  [lists:flatten(
                     rabbit_misc:intersperse(
                       " ", [atom_to_list(Command) | Args]))]),
            usage();
        {error, Reason} ->
            error("~p", [Reason]),
            halt(2);
        Other ->
            error("~p", [Other]),
            halt(2)
    end.

error(Format, Args) ->
    rabbit_misc:format_stderr("Error: " ++ Format ++ "~n", Args).

parse_args(["-n", NodeS | Args], Params) ->
    Node = case lists:member($@, NodeS) of
               true  -> list_to_atom(NodeS);
               false -> rabbit_misc:localnode(list_to_atom(NodeS))
           end,
    parse_args(Args, Params#params{node = Node});
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

  map_user_vhost   <UserName> <VHostPath>
  unmap_user_vhost <UserName> <VHostPath>
  list_user_vhosts <UserName>
  list_vhost_users <VHostPath>

  list_queues    [-p <VHostPath>] <QueueInfoItem> [<QueueInfoItem> ...]
  list_exchanges [-p <VHostPath>] <ExchangeInfoItem> [<ExchangeInfoItem> ...]
  list_bindings  [-p <VHostPath>] 
  list_connections <ConnectionInfoItem> [<ConnectionInfoItem> ...]

Quiet output mode is selected with the \"-q\" flag. Informational messages
are suppressed when quiet mode is in effect.

<node> should be the name of the master node of the RabbitMQ cluster. It
defaults to the node named \"rabbit\" on the local host. On a host named
\"server.example.com\", the master node will usually be rabbit@server (unless
NODENAME has been set to some non-default value at broker startup time). The
output of hostname -s is usually the correct suffix to use after the \"@\" sign.

The list_queues, list_exchanges and list_bindings commands accept an optional
virtual host parameter for which to display results. The default value is \"/\".

<QueueInfoItem> must be a member of the list [name, durable, auto_delete, 
arguments, pid, messages_ready, messages_unacknowledged, messages_uncommitted, 
messages, acks_uncommitted, consumers, transactions, memory]. The default is 
 to display name and (number of) messages.

<ExchangeInfoItem> must be a member of the list [name, type, durable, 
auto_delete, arguments]. The default is to display name and type.

The output format for \"list_bindings\" is a list of rows containing 
exchange name, routing key, queue name and arguments, in that order.

<ConnectioInfoItem> must be a member of the list [pid, address, port, 
peer_address, peer_port, state, channels, user, vhost, timeout, frame_max,
recv_oct, recv_cnt, send_oct, send_cnt, send_pend]. The default is to display 
user, peer_address and peer_port.

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
    Res = call(Node, {rabbit, status, []}),
    io:format("~p~n", [Res]),
    ok;

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

action(map_user_vhost, Node, Args = [_Username, _VHostPath], Inform) ->
    Inform("Mapping user ~p to vhost ~p", Args),
    call(Node, {rabbit_access_control, map_user_vhost, Args});

action(unmap_user_vhost, Node, Args = [_Username, _VHostPath], Inform) ->
    Inform("Unmapping user ~p from vhost ~p", Args),
    call(Node, {rabbit_access_control, unmap_user_vhost, Args});

action(list_user_vhosts, Node, Args = [_Username], Inform) ->
    Inform("Listing vhosts for user ~p", Args),
    display_list(call(Node, {rabbit_access_control, list_user_vhosts, Args}));

action(list_vhost_users, Node, Args = [_VHostPath], Inform) ->
    Inform("Listing users for vhosts ~p", Args),
    display_list(call(Node, {rabbit_access_control, list_vhost_users, Args}));

action(list_queues, Node, Args, Inform) ->
    Inform("Listing queues", []),
    {VHostArg, RemainingArgs} = parse_vhost_flag(Args),
    ArgAtoms = default_if_empty(RemainingArgs, [name, messages]),
    display_info_list(rpc_call(Node, rabbit_amqqueue, info_all, [VHostArg, ArgAtoms]), ArgAtoms);

action(list_exchanges, Node, Args, Inform) ->
    Inform("Listing exchanges", []),
    {VHostArg, RemainingArgs} = parse_vhost_flag(Args),
    ArgAtoms = default_if_empty(RemainingArgs, [name, type]),
    display_info_list(rpc_call(Node, rabbit_exchange, info_all, [VHostArg, ArgAtoms]), ArgAtoms);

action(list_bindings, Node, Args, Inform) ->
    Inform("Listing bindings", []),
    {VHostArg, _} = parse_vhost_flag(Args),
    lists:map(
        fun({#resource{name = ExchangeName, virtual_host = _VirtualHost}, 
             #resource{name = QueueName, virtual_host = _VirtualHost},
             RoutingKey, 
             Arguments}) ->
            io:format("~s\t~s\t~s\t~w~n", 
                [url_encode(ExchangeName), RoutingKey, url_encode(QueueName), Arguments])
        end, 
        rpc_call(Node, rabbit_exchange, list_bindings, [VHostArg])),
    ok;

action(list_connections, Node, Args, Inform) ->
    Inform("Listing connections", []),
    ArgAtoms = default_if_empty(Args, [user, peer_address, peer_port]),
    display_info_list(rpc_call(Node, rabbit_networking, connection_info_all, [ArgAtoms]), ArgAtoms).

parse_vhost_flag(Args) when is_list(Args) ->
        case Args of 
            ["-p", VHost | RemainingArgs] -> {list_to_binary(VHost), RemainingArgs};  
            RemainingArgs                 -> {<<"/">>, RemainingArgs}
        end.

default_if_empty(List, Default) when is_list(List) ->
    if List == [] -> 
        Default; 
       true -> 
        [list_to_atom(X) || X <- List]
    end.

display_info_list(Results, InfoItemArgs) when is_list(Results) ->
    lists:map(
        fun (ResultRow) ->
            RenderInfoItem = 
                fun(InfoItemName) -> 
                    {value, Info = {InfoItemName, InfoItemValue}} = lists:keysearch(InfoItemName, 1, ResultRow),
                    case Info of
                        {_, #resource{name = Name}} ->
                            url_encode(Name);
                        {Key, IpAddress} when Key =:= address; Key =:= peer_address andalso is_tuple(IpAddress) ->
                            inet_parse:ntoa(IpAddress);
                        _ when is_binary(InfoItemValue) -> 
                            url_encode(InfoItemValue);
                        _ -> 
                            io_lib:format("~w", [InfoItemValue])
                    end 
                end,
            io:fwrite(string:join([RenderInfoItem(X) || X <- InfoItemArgs], "\t")),
            io:nl()
        end,
        Results),
    ok.

display_list(L) when is_list(L) ->
    lists:foreach(fun (I) ->
                          io:format("~s~n", [binary_to_list(I)])
                  end,
                  lists:sort(L)),
    ok;
display_list(Other) -> Other.

call(Node, {Mod, Fun, Args}) ->
    rpc_call(Node, Mod, Fun, lists:map(fun list_to_binary/1, Args)).

rpc_call(Node, Mod, Fun, Args) ->
    rpc:call(Node, Mod, Fun, Args, ?RPC_TIMEOUT).

%% url_encode is lifted from ibrowse, modified to preserve some characters
url_encode(Bin) when binary(Bin) ->
    url_encode_char(lists:reverse(binary_to_list(Bin)), []).

url_encode_char([X | T], Acc) when X >= $a, X =< $z ->
    url_encode_char(T, [X | Acc]);
url_encode_char([X | T], Acc) when X >= $A, X =< $Z ->
    url_encode_char(T, [X | Acc]);
url_encode_char([X | T], Acc) when X >= $0, X =< $9 ->
    url_encode_char(T, [X | Acc]);
url_encode_char([X | T], Acc)
  when X == $-; X == $_; X == $.; X == $~;
       X == $!; X == $*; X == $'; X == $(;
       X == $); X == $;; X == $:; X == $@;
       X == $&; X == $=; X == $+; X == $$;
       X == $,; X == $/; X == $?; X == $%;
       X == $#; X == $[; X == $] ->
    url_encode_char(T, [X | Acc]);
url_encode_char([X | T], Acc) ->
    url_encode_char(T, [$%, d2h(X bsr 4), d2h(X band 16#0f) | Acc]);
url_encode_char([], Acc) ->
    Acc.

d2h(N) when N<10 -> N+$0;
d2h(N) -> N+$a-10.

