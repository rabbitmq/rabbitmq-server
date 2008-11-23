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

-export([start/0, stop/0, action/3]).

-define(RPC_TIMEOUT, 30000).

start() ->
    case init:get_plain_arguments() of
        [] ->
            usage();
        FullCommand ->
            {Node, Command, Args} = parse_args(FullCommand),
            case catch action(Command, Node, Args) of
                ok ->
                    io:format("done.~n"),
                    init:stop();
                {'EXIT', {function_clause, [{?MODULE, action, _} | _]}} ->
                    io:format("Invalid command ~p~n", [FullCommand]),
                    usage();
                Other ->
                    io:format("~nrabbit_control action ~p failed:~n~p~n", [Command, Other]),
                    halt(2)
            end
    end.

parse_args(["-n", NodeS, Command | Args]) ->
    Node = case lists:member($@, NodeS) of
               true  -> list_to_atom(NodeS);
               false -> rabbit_misc:localnode(list_to_atom(NodeS))
           end,
    {Node, list_to_atom(Command), Args};
parse_args([Command | Args]) ->
    {rabbit_misc:localnode(rabbit), list_to_atom(Command), Args}.

stop() ->
    ok.

usage() ->
    io:format("Usage: rabbitmqctl [-n <node>] <command> [<arg> ...]

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

  list_queues <QueueInfoItem> [<QueueInfoItem> ...]
  list_exchanges <ExchangeInfoItem> [<ExchangeInfoItem> ...]
  list_bindings
  list_connections <ConnectionInfoItem> [<ConnectionInfoItem> ...]

<node> should be the name of the master node of the RabbitMQ cluster. It
defaults to the node named \"rabbit\" on the local host. On a host named
\"server.example.com\", the master node will usually be rabbit@server (unless
NODENAME has been set to some non-default value at broker startup time). The
output of hostname -s is usually the correct suffix to use after the \"@\" sign.

<QueueInfoItem> must be a member of the list [name, durable, auto_delete, 
arguments, pid, messages_ready, messages_unacknowledged, messages_uncommitted, 
messages, acks_uncommitted, consumers, transactions, memory]. The default is 
 to display name and (number of) messages.

<ExchangeInfoItem> must be a member of the list [name, type, durable, 
auto_delete, arguments]. The default is to display name and type.

The output format for \"list_bindings\" is a list of rows containing 
virtual host, exchange name, routing key, queue name and arguments, in that 
order.

<ConnectioInfoItem> must be a member of the list [pid, address, port, 
peer_address, peer_port, state, channels, user, vhost, timeout, frame_max].
The default is to display user, peer_address and peer_port.

"),
    halt(1).

action(stop, Node, []) ->
    io:format("Stopping and halting node ~p ...", [Node]),
    call(Node, {rabbit, stop_and_halt, []});

action(stop_app, Node, []) ->
    io:format("Stopping node ~p ...", [Node]),
    call(Node, {rabbit, stop, []});

action(start_app, Node, []) ->
    io:format("Starting node ~p ...", [Node]),
    call(Node, {rabbit, start, []});

action(reset, Node, []) ->
    io:format("Resetting node ~p ...", [Node]),
    call(Node, {rabbit_mnesia, reset, []});

action(force_reset, Node, []) ->
    io:format("Forcefully resetting node ~p ...", [Node]),
    call(Node, {rabbit_mnesia, force_reset, []});

action(cluster, Node, ClusterNodeSs) ->
    ClusterNodes = lists:map(fun list_to_atom/1, ClusterNodeSs),
    io:format("Clustering node ~p with ~p ...",
              [Node, ClusterNodes]),
    rpc_call(Node, rabbit_mnesia, cluster, [ClusterNodes]);

action(status, Node, []) ->
    io:format("Status of node ~p ...", [Node]),
    Res = call(Node, {rabbit, status, []}),
    io:format("~n~p~n", [Res]),
    ok;

action(rotate_logs, Node, []) ->
    io:format("Reopening logs for node ~p ...", [Node]),
    call(Node, {rabbit, rotate_logs, [""]});
action(rotate_logs, Node, Args = [Suffix]) ->
    io:format("Rotating logs to files with suffix ~p ...", [Suffix]),
    call(Node, {rabbit, rotate_logs, Args});

action(add_user, Node, Args = [Username, _Password]) ->
    io:format("Creating user ~p ...", [Username]),
    call(Node, {rabbit_access_control, add_user, Args});

action(delete_user, Node, Args = [_Username]) ->
    io:format("Deleting user ~p ...", Args),
    call(Node, {rabbit_access_control, delete_user, Args});

action(change_password, Node, Args = [Username, _Newpassword]) ->
    io:format("Changing password for user ~p ...", [Username]),
    call(Node, {rabbit_access_control, change_password, Args});

action(list_users, Node, []) ->
    io:format("Listing users ..."),
    display_list(call(Node, {rabbit_access_control, list_users, []}));

action(add_vhost, Node, Args = [_VHostPath]) ->
    io:format("Creating vhost ~p ...", Args),
    call(Node, {rabbit_access_control, add_vhost, Args});

action(delete_vhost, Node, Args = [_VHostPath]) ->
    io:format("Deleting vhost ~p ...", Args),
    call(Node, {rabbit_access_control, delete_vhost, Args});

action(list_vhosts, Node, []) ->
    io:format("Listing vhosts ..."),
    display_list(call(Node, {rabbit_access_control, list_vhosts, []}));

action(map_user_vhost, Node, Args = [_Username, _VHostPath]) ->
    io:format("Mapping user ~p to vhost ~p ...", Args),
    call(Node, {rabbit_access_control, map_user_vhost, Args});

action(unmap_user_vhost, Node, Args = [_Username, _VHostPath]) ->
    io:format("Unmapping user ~p from vhost ~p ...", Args),
    call(Node, {rabbit_access_control, unmap_user_vhost, Args});

action(list_user_vhosts, Node, Args = [_Username]) ->
    io:format("Listing vhosts for user ~p...", Args),
    display_list(call(Node, {rabbit_access_control, list_user_vhosts, Args}));

action(list_vhost_users, Node, Args = [_VHostPath]) ->
    io:format("Listing users for vhosts ~p...", Args),
    display_list(call(Node, {rabbit_access_control, list_vhost_users, Args}));

action(list_queues, Node, Args) ->
    io:format("Listing queues ...~n"),
    ArgAtoms = [list_to_atom(X) || X <- default_if_empty(Args, ["name", "messages"])],
    display_info_list(rpc_call(Node, rabbit_amqqueue, info_all, [ArgAtoms]), ArgAtoms);

action(list_exchanges, Node, Args) ->
    io:format("Listing exchanges ...~n"),
    ArgAtoms = [list_to_atom(X) || X <- default_if_empty(Args, ["name", "type"])],
    display_info_list(rpc_call(Node, rabbit_exchange, info_all, [ArgAtoms]), ArgAtoms);

action(list_bindings, Node, []) ->
    io:format("Listing bindings ...~n"),
    lists:map(
        fun({#resource{name = ExchangeName, virtual_host = VirtualHost}, 
             #resource{name = QueueName, virtual_host = VirtualHost},
             RoutingKey, Arguments}) ->
            io:format("~s ~s ~s ~s ~w~n", 
                [VirtualHost, ExchangeName, RoutingKey, QueueName, Arguments])
        end, 
        rpc_call(Node, rabbit_exchange, list_bindings, [])),
    ok;

action(list_connections, Node, Args) ->
    io:format("Listing connections ...~n"),
    ArgAtoms = [list_to_atom(X) || X <- default_if_empty(Args, ["user", "peer_address", "peer_port"])],
    display_info_list(rpc_call(Node, rabbit_networking, connection_info_all, [ArgAtoms]), ArgAtoms).

default_if_empty(List, Default) when is_list(List) ->
    case List of
        [] -> Default;
        _ -> List
    end.

display_info_list(Results, InfoItemArgs) when is_list(Results) ->
    lists:map(
        fun (ResultRow) ->
            lists:foreach(
                fun(InfoItemName) -> 
                    {value, Info = {InfoItemName, Data}} = lists:keysearch(InfoItemName, 1, ResultRow),
                    case Info of
                        {_, #resource{virtual_host = VHostPath, name = Name}} ->
                            io:format("~s@~s ", [Name, VHostPath]);
                        {address, {A,B,C,D}} when is_integer(A), is_integer(B), is_integer(C), is_integer(D) -> 
                            io:format("~w.~w.~w.~w ", [A, B, C, D]);
                        {peer_address, {A,B,C,D}} when is_integer(A), is_integer(B), is_integer(C), is_integer(D) -> 
                            io:format("~w.~w.~w.~w ", [A, B, C, D]);
                        _ when is_binary(Data) -> 
                            io:format("~s ", [Data]);
                        _ -> 
                            io:format("~w ", [Data])
                    end 
                end,
                InfoItemArgs),
            io:nl()
        end,
        Results),
    ok.

display_list(L) when is_list(L) ->
    lists:foreach(fun (I) ->
                          io:format("~n~s", [binary_to_list(I)])
                  end,
                  lists:sort(L)),
    io:nl();
display_list(Other) -> Other.

call(Node, {Mod, Fun, Args}) ->
    rpc_call(Node, Mod, Fun, lists:map(fun list_to_binary/1, Args)).

rpc_call(Node, Mod, Fun, Args) ->
    rpc:call(Node, Mod, Fun, Args, ?RPC_TIMEOUT).
