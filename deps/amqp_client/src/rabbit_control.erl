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
%%   Portions created by LShift Ltd., Cohesive Financial
%%   Technologies LLC., and Rabbit Technologies Ltd. are Copyright (C) 
%%   2007 LShift Ltd., Cohesive Financial Technologies LLC., and Rabbit 
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
        FullCommand = [Command | Args] ->
            {Node, Args1} = determine_node(Args),
            case catch action(list_to_atom(Command), Node, Args1) of
                ok ->
                    io:format("done.~n"),
                    halt(0);
                {'EXIT', {function_clause, [{?MODULE, action, _} | _]}} ->
                    io:format("Invalid command ~p~n", [FullCommand]),
                    usage();
                Other ->
                    io:format("~nrabbit_control action ~p failed:~n~p~n", [Command, Other]),
                    halt(2)
            end
    end.

stop() ->
    ok.

determine_node(AllArgs = [MaybeNode | Args]) ->
    case lists:member($@, MaybeNode) of
        true -> {list_to_atom(MaybeNode), Args};
        false -> {rabbit_misc:localnode(rabbit), AllArgs}
    end;
determine_node([]) ->
    {rabbit_misc:localnode(rabbit), []}.

usage() ->
    io:format("Usage: rabbitmqctl <command> [<node>] [<arg> ...]

Available commands:

  stop      - stops the RabbitMQ application and halts the node
  stop_app  - stops the RabbitMQ application, leaving the node running
  start_app - starts the RabbitMQ application on an already-running node
  status

  clear_db
  join_cluster  ram/disc <ClusterNode> ...
  leave_cluster

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

  add_realm    <VHostPath> <RealmName>
  delete_realm <VHostPath> <RealmName>
  list_realms  <VHostPath>

  set_permissions  <UserName> <VHostPath> <RealmName> [<Permission> ...]
      Permissions management. The available permissions are 'passive',
      'active', 'write' and 'read', corresponding to the permissions
      referred to in AMQP's \"access.request\" message, or 'all' as an
      abbreviation for all defined permission flags.
  list_permissions <UserName> <VHostPath>

<node> should be the name of the master node of the RabbitMQ cluster. It
defaults to the node named \"rabbit\" on the local host. On a host named
\"server.example.com\", the master node will usually be rabbit@server (unless
NODENAME has been set to some non-default value at broker startup time). The
output of hostname -s is usually the correct suffix to use after the \"@\" sign.

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

action(status, Node, []) ->
    io:format("Status of node ~p ...", [Node]),
    Res = call(Node, {rabbit, status, []}),
    io:format("~n~p~n", [Res]),
    ok;

action(clear_db, Node, []) ->
    io:format("Clearing db ...", []),
    call(Node, {rabbit_mnesia, clear_db, []});

action(join_cluster, Node, [TypeS | ClusterNodeSs = [_|_]]) ->
    Type = list_to_atom(TypeS),
    ClusterNodes = lists:map(fun list_to_atom/1, ClusterNodeSs),
    io:format("Joining cluster at nodes ~p ...", [ClusterNodes]),
    if (Type /= ram) and (Type /= disc_only) and (Type /= disc) ->
            io:format("~nError: invalid cluster type ~p~n", [Type]),
            usage();
       true -> ok
    end,
    rpc_call(Node, rabbit_mnesia, join_cluster, [Type, ClusterNodes]);

action(leave_cluster, Node, []) ->
    io:format("Leaving cluster ...", []),
    call(Node, {rabbit_mnesia, leave_cluster, []});

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

action(add_realm, Node, [VHostPath, RealmName]) ->
    io:format("Adding realm ~p to vhost ~p ...", [RealmName, VHostPath]),
    rpc_call(Node, rabbit_realm, add_realm,
             [realm_rsrc(VHostPath, RealmName)]);

action(delete_realm, Node, [VHostPath, RealmName]) ->
    io:format("Deleting realm ~p from vhost ~p ...", [RealmName, VHostPath]),
    rpc_call(Node, rabbit_realm, delete_realm,
             [realm_rsrc(VHostPath, RealmName)]);

action(list_realms, Node, Args = [_VHostPath]) ->
    io:format("Listing realms for vhost ~p ...", Args),
    display_list(call(Node, {rabbit_realm, list_vhost_realms, Args}));

action(set_permissions, Node,
       [Username, VHostPath, RealmName | Permissions]) ->
    io:format("Setting permissions for user ~p, vhost ~p, realm ~p ...",
              [Username, VHostPath, RealmName]),
    CheckedPermissions = check_permissions(Permissions),
    Ticket = #ticket{
      realm_name   = realm_rsrc(VHostPath, RealmName),
      passive_flag = lists:member(passive, CheckedPermissions),
      active_flag  = lists:member(active,  CheckedPermissions),
      write_flag   = lists:member(write,   CheckedPermissions),
      read_flag    = lists:member(read,    CheckedPermissions)},
    rpc_call(Node, rabbit_access_control, map_user_realm,
             [list_to_binary(Username), Ticket]);

action(list_permissions, Node, Args = [_Username, _VHostPath]) ->
    io:format("Listing permissions for user ~p in vhost ~p ...", Args),
    case call(Node, {rabbit_access_control, list_user_realms, Args}) of
        {ok, Perms} ->
            lists:foreach(fun ({RealmName, Pattern}) ->
                                  io:format("~n~s: ~p",
                                            [binary_to_list(RealmName),
                                             permission_list(Pattern)])
                          end,
                          lists:sort(Perms)),
            io:nl(),
            ok;
        Other -> Other
    end.

check_permissions([]) -> [];
check_permissions(["all" | R]) ->
    [passive, active, write, read | check_permissions(R)];
check_permissions([P | R]) when (P == "passive") or
                                (P == "active")  or
                                (P == "write")   or
                                (P == "read") ->
    [list_to_atom(P) | check_permissions(R)];
check_permissions([P | _R]) ->
    io:format("~nError: invalid permission flag ~p~n", [P]),
    usage().

permission_list(Ticket = #ticket{}) ->
    lists:foldr(fun ({Field, Label}, L) ->
                       case element(Field, Ticket) of
                           true -> [Label | L];
                           false -> L
                       end
                end,
                [],
                [{#ticket.passive_flag, passive},
                 {#ticket.active_flag,  active},
                 {#ticket.write_flag,   write},
                 {#ticket.read_flag,    read}]).

realm_rsrc(VHostPath, RealmName) ->
    rabbit_misc:r(list_to_binary(VHostPath),
                  realm,
                  list_to_binary(RealmName)).

display_list({ok, L}) ->
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
