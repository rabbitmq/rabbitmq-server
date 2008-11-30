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
                true  -> fun(_Format, _Args) -> ok end;
                false -> fun(Format, Args) ->
                                 io:format(Format ++ " ...~n", Args)
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
    io:format("Error: " ++ Format ++"~n", Args).

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

Quiet output mode is selected with the \"-q\" flag. Informational messages
are suppressed when quiet mode is in effect.

<node> should be the name of the master node of the RabbitMQ cluster. It
defaults to the node named \"rabbit\" on the local host. On a host named
\"server.example.com\", the master node will usually be rabbit@server (unless
NODENAME has been set to some non-default value at broker startup time). The
output of hostname -s is usually the correct suffix to use after the \"@\" sign.

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
    display_list(call(Node, {rabbit_access_control, list_vhost_users, Args})).

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
