%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2021 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(rabbit_connection_tracking).

%% Abstracts away how tracked connection records are stored
%% and queried.
%%
%% See also:
%%
%%  * rabbit_connection_tracking_handler
%%  * rabbit_reader
%%  * rabbit_event
-behaviour(rabbit_tracking).

-export([boot/0,
         update_tracked/1,
         handle_cast/1,
         register_tracked/1,
         unregister_tracked/1,
         count_tracked_items_in/1,
         clear_tracking_tables/0,
         shutdown_tracked_items/2]).

-export([ensure_tracked_connections_table_for_node/1,
         ensure_per_vhost_tracked_connections_table_for_node/1,
         ensure_per_user_tracked_connections_table_for_node/1,

         ensure_tracked_connections_table_for_this_node/0,
         ensure_per_vhost_tracked_connections_table_for_this_node/0,
         ensure_per_user_tracked_connections_table_for_this_node/0,

         tracked_connection_table_name_for/1,
         tracked_connection_per_vhost_table_name_for/1,
         tracked_connection_per_user_table_name_for/1,
         get_all_tracked_connection_table_names_for_node/1,

         delete_tracked_connections_table_for_node/1,
         delete_per_vhost_tracked_connections_table_for_node/1,
         delete_per_user_tracked_connections_table_for_node/1,
         delete_tracked_connection_user_entry/1,
         delete_tracked_connection_vhost_entry/1,

         clear_tracked_connection_tables_for_this_node/0,

         list/0, list/1, list_on_node/1, list_on_node/2, list_of_user/1,
         tracked_connection_from_connection_created/1,
         tracked_connection_from_connection_state/1,
         lookup/1,
         count/0]).

-include_lib("rabbit_common/include/rabbit.hrl").

-import(rabbit_misc, [pget/2]).

-export([close_connections/3]).

%%
%% API
%%

%% Behaviour callbacks

-spec boot() -> ok.

%% Sets up and resets connection tracking tables for this
%% node.
boot() ->
    ensure_tracked_connections_table_for_this_node(),
    rabbit_log:info("Setting up a table for connection tracking on this node: ~p",
                    [tracked_connection_table_name_for(node())]),
    ensure_per_vhost_tracked_connections_table_for_this_node(),
    rabbit_log:info("Setting up a table for per-vhost connection counting on this node: ~p",
                    [tracked_connection_per_vhost_table_name_for(node())]),
    ensure_per_user_tracked_connections_table_for_this_node(),
    rabbit_log:info("Setting up a table for per-user connection counting on this node: ~p",
                    [tracked_connection_per_user_table_name_for(node())]),
    clear_tracking_tables(),
    ok.

-spec update_tracked(term()) -> ok.

update_tracked(Event) ->
    spawn(?MODULE, handle_cast, [Event]),
    ok.

%% Asynchronously handle update events
-spec handle_cast(term()) -> ok.

handle_cast({connection_created, Details}) ->
    ThisNode = node(),
    case pget(node, Details) of
        ThisNode ->
            TConn = tracked_connection_from_connection_created(Details),
            ConnId = TConn#tracked_connection.id,
            try
                register_tracked(TConn)
            catch
                error:{no_exists, _} ->
                    Msg = "Could not register connection ~p for tracking, "
                          "its table is not ready yet or the connection terminated prematurely",
                    rabbit_log_connection:warning(Msg, [ConnId]),
                    ok;
                error:Err ->
                    Msg = "Could not register connection ~p for tracking: ~p",
                    rabbit_log_connection:warning(Msg, [ConnId, Err]),
                    ok
            end;
        _OtherNode ->
            %% ignore
            ok
    end;
handle_cast({connection_closed, Details}) ->
    ThisNode = node(),
    case pget(node, Details) of
      ThisNode ->
          %% [{name,<<"127.0.0.1:64078 -> 127.0.0.1:5672">>},
          %%  {pid,<0.1774.0>},
          %%  {node, rabbit@hostname}]
          unregister_tracked(
              rabbit_tracking:id(ThisNode, pget(name, Details)));
      _OtherNode ->
        %% ignore
        ok
    end;
handle_cast({vhost_deleted, Details}) ->
    VHost = pget(name, Details),
    %% Schedule vhost entry deletion, allowing time for connections to close
    _ = timer:apply_after(?TRACKING_EXECUTION_TIMEOUT, ?MODULE,
            delete_tracked_connection_vhost_entry, [VHost]),
    rabbit_log_connection:info("Closing all connections in vhost '~s' because it's being deleted", [VHost]),
    shutdown_tracked_items(
        rabbit_connection_tracking:list(VHost),
        rabbit_misc:format("vhost '~s' is deleted", [VHost]));
%% Note: under normal circumstances this will be called immediately
%% after the vhost_deleted above. Therefore we should be careful about
%% what we log and be more defensive.
handle_cast({vhost_down, Details}) ->
    VHost = pget(name, Details),
    Node = pget(node, Details),
    rabbit_log_connection:info("Closing all connections in vhost '~s' on node '~s'"
                               " because the vhost is stopping",
                               [VHost, Node]),
    shutdown_tracked_items(
        rabbit_connection_tracking:list_on_node(Node, VHost),
        rabbit_misc:format("vhost '~s' is down", [VHost]));
handle_cast({user_deleted, Details}) ->
    Username = pget(name, Details),
    %% Schedule user entry deletion, allowing time for connections to close
    _ = timer:apply_after(?TRACKING_EXECUTION_TIMEOUT, ?MODULE,
            delete_tracked_connection_user_entry, [Username]),
    rabbit_log_connection:info("Closing all connections from user '~s' because it's being deleted", [Username]),
    shutdown_tracked_items(
        rabbit_connection_tracking:list_of_user(Username),
        rabbit_misc:format("user '~s' is deleted", [Username]));
%% A node had been deleted from the cluster.
handle_cast({node_deleted, Details}) ->
    Node = pget(node, Details),
    rabbit_log_connection:info("Node '~s' was removed from the cluster, deleting its connection tracking tables...", [Node]),
    delete_tracked_connections_table_for_node(Node),
    delete_per_vhost_tracked_connections_table_for_node(Node),
    delete_per_user_tracked_connections_table_for_node(Node).

-spec register_tracked(rabbit_types:tracked_connection()) -> ok.
-dialyzer([{nowarn_function, [register_tracked/1]}, race_conditions]).

register_tracked(#tracked_connection{username = Username, vhost = VHost, id = ConnId, node = Node} = Conn) when Node =:= node() ->
    TableName = tracked_connection_table_name_for(Node),
    PerVhostTableName = tracked_connection_per_vhost_table_name_for(Node),
    PerUserConnTableName = tracked_connection_per_user_table_name_for(Node),
    %% upsert
    case mnesia:dirty_read(TableName, ConnId) of
        []    ->
            mnesia:dirty_write(TableName, Conn),
            mnesia:dirty_update_counter(PerVhostTableName, VHost, 1),
            mnesia:dirty_update_counter(PerUserConnTableName, Username, 1);
        [#tracked_connection{}] ->
            ok
    end,
    ok.

-spec unregister_tracked(rabbit_types:tracked_connection_id()) -> ok.

unregister_tracked(ConnId = {Node, _Name}) when Node =:= node() ->
    TableName = tracked_connection_table_name_for(Node),
    PerVhostTableName = tracked_connection_per_vhost_table_name_for(Node),
    PerUserConnTableName = tracked_connection_per_user_table_name_for(Node),
    case mnesia:dirty_read(TableName, ConnId) of
        []     -> ok;
        [#tracked_connection{vhost = VHost, username = Username}] ->
            mnesia:dirty_update_counter(PerUserConnTableName, Username, -1),
            mnesia:dirty_update_counter(PerVhostTableName, VHost, -1),
            mnesia:dirty_delete(TableName, ConnId)
    end.

-spec count_tracked_items_in({atom(), rabbit_types:vhost()}) -> non_neg_integer().

count_tracked_items_in({vhost, VirtualHost}) ->
    rabbit_tracking:count_tracked_items(
        fun tracked_connection_per_vhost_table_name_for/1,
        #tracked_connection_per_vhost.connection_count, VirtualHost,
        "connections in vhost");
count_tracked_items_in({user, Username}) ->
    rabbit_tracking:count_tracked_items(
        fun tracked_connection_per_user_table_name_for/1,
        #tracked_connection_per_user.connection_count, Username,
        "connections for user").

-spec clear_tracking_tables() -> ok.

clear_tracking_tables() ->
    clear_tracked_connection_tables_for_this_node().

-spec shutdown_tracked_items(list(), term()) -> ok.

shutdown_tracked_items(TrackedItems, Message) ->
    close_connections(TrackedItems, Message).

%% Extended API

-spec ensure_tracked_connections_table_for_this_node() -> ok.

ensure_tracked_connections_table_for_this_node() ->
    ensure_tracked_connections_table_for_node(node()).


-spec ensure_per_vhost_tracked_connections_table_for_this_node() -> ok.

ensure_per_vhost_tracked_connections_table_for_this_node() ->
    ensure_per_vhost_tracked_connections_table_for_node(node()).


-spec ensure_per_user_tracked_connections_table_for_this_node() -> ok.

ensure_per_user_tracked_connections_table_for_this_node() ->
    ensure_per_user_tracked_connections_table_for_node(node()).


%% Create tables
-spec ensure_tracked_connections_table_for_node(node()) -> ok.

ensure_tracked_connections_table_for_node(Node) ->
    TableName = tracked_connection_table_name_for(Node),
    case mnesia:create_table(TableName, [{record_name, tracked_connection},
                                         {attributes, record_info(fields, tracked_connection)}]) of
        {atomic, ok}                   -> ok;
        {aborted, {already_exists, _}} -> ok;
        {aborted, Error}               ->
            rabbit_log:error("Failed to create a tracked connection table for node ~p: ~p", [Node, Error]),
            ok
    end.

-spec ensure_per_vhost_tracked_connections_table_for_node(node()) -> ok.

ensure_per_vhost_tracked_connections_table_for_node(Node) ->
    TableName = tracked_connection_per_vhost_table_name_for(Node),
    case mnesia:create_table(TableName, [{record_name, tracked_connection_per_vhost},
                                         {attributes, record_info(fields, tracked_connection_per_vhost)}]) of
        {atomic, ok}                   -> ok;
        {aborted, {already_exists, _}} -> ok;
        {aborted, Error}               ->
            rabbit_log:error("Failed to create a per-vhost tracked connection table for node ~p: ~p", [Node, Error]),
            ok
    end.

-spec ensure_per_user_tracked_connections_table_for_node(node()) -> ok.

ensure_per_user_tracked_connections_table_for_node(Node) ->
    TableName = tracked_connection_per_user_table_name_for(Node),
    case mnesia:create_table(TableName, [{record_name, tracked_connection_per_user},
                                         {attributes, record_info(fields, tracked_connection_per_user)}]) of
        {atomic, ok}                   -> ok;
        {aborted, {already_exists, _}} -> ok;
        {aborted, Error}               ->
            rabbit_log:error("Failed to create a per-user tracked connection table for node ~p: ~p", [Node, Error]),
            ok
    end.

-spec clear_tracked_connection_tables_for_this_node() -> ok.

clear_tracked_connection_tables_for_this_node() ->
    [rabbit_tracking:clear_tracking_table(T)
        || T <- get_all_tracked_connection_table_names_for_node(node())],
    ok.

-spec delete_tracked_connections_table_for_node(node()) -> ok.

delete_tracked_connections_table_for_node(Node) ->
    TableName = tracked_connection_table_name_for(Node),
    rabbit_tracking:delete_tracking_table(TableName, Node, "tracked connection").

-spec delete_per_vhost_tracked_connections_table_for_node(node()) -> ok.

delete_per_vhost_tracked_connections_table_for_node(Node) ->
    TableName = tracked_connection_per_vhost_table_name_for(Node),
    rabbit_tracking:delete_tracking_table(TableName, Node,
        "per-vhost tracked connection").

-spec delete_per_user_tracked_connections_table_for_node(node()) -> ok.

delete_per_user_tracked_connections_table_for_node(Node) ->
    TableName = tracked_connection_per_user_table_name_for(Node),
    rabbit_tracking:delete_tracking_table(TableName, Node,
        "per-user tracked connection").

-spec tracked_connection_table_name_for(node()) -> atom().

tracked_connection_table_name_for(Node) ->
    list_to_atom(rabbit_misc:format("tracked_connection_on_node_~s", [Node])).

-spec tracked_connection_per_vhost_table_name_for(node()) -> atom().

tracked_connection_per_vhost_table_name_for(Node) ->
    list_to_atom(rabbit_misc:format("tracked_connection_per_vhost_on_node_~s", [Node])).

-spec tracked_connection_per_user_table_name_for(node()) -> atom().

tracked_connection_per_user_table_name_for(Node) ->
    list_to_atom(rabbit_misc:format(
        "tracked_connection_table_per_user_on_node_~s", [Node])).

-spec get_all_tracked_connection_table_names_for_node(node()) -> [atom()].

get_all_tracked_connection_table_names_for_node(Node) ->
    [tracked_connection_table_name_for(Node),
        tracked_connection_per_vhost_table_name_for(Node),
            tracked_connection_per_user_table_name_for(Node)].

-spec lookup(rabbit_types:connection_name()) -> rabbit_types:tracked_connection() | 'not_found'.

lookup(Name) ->
    Nodes = rabbit_nodes:all_running(),
    lookup(Name, Nodes).

lookup(_, []) ->
    not_found;
lookup(Name, [Node | Nodes]) ->
    TableName = tracked_connection_table_name_for(Node),
    case mnesia:dirty_read(TableName, {Node, Name}) of
        [] -> lookup(Name, Nodes);
        [Row] -> Row
    end.

-spec list() -> [rabbit_types:tracked_connection()].

list() ->
    lists:foldl(
      fun (Node, Acc) ->
              Tab = tracked_connection_table_name_for(Node),
              try
                  Acc ++
                  mnesia:dirty_match_object(Tab, #tracked_connection{_ = '_'})
              catch
                  exit:{aborted, {no_exists, [Tab, _]}} ->
                      %% The table might not exist yet (or is already gone)
                      %% between the time rabbit_nodes:all_running() runs and
                      %% returns a specific node, and
                      %% mnesia:dirty_match_object() is called for that node's
                      %% table.
                      Acc
              end
      end, [], rabbit_nodes:all_running()).

-spec count() -> non_neg_integer().

count() ->
    lists:foldl(
      fun (Node, Acc) ->
              Tab = tracked_connection_table_name_for(Node),
              %% mnesia:table_info() returns 0 if the table doesn't exist. We
              %% don't need the same kind of protection as the list() function
              %% above.
              Acc + mnesia:table_info(Tab, size)
      end, 0, rabbit_nodes:all_running()).

-spec list(rabbit_types:vhost()) -> [rabbit_types:tracked_connection()].

list(VHost) ->
    rabbit_tracking:match_tracked_items(
        fun tracked_connection_table_name_for/1,
        #tracked_connection{vhost = VHost, _ = '_'}).

-spec list_on_node(node()) -> [rabbit_types:tracked_connection()].

list_on_node(Node) ->
    try mnesia:dirty_match_object(
          tracked_connection_table_name_for(Node),
          #tracked_connection{_ = '_'})
    catch exit:{aborted, {no_exists, _}} -> []
    end.

-spec list_on_node(node(), rabbit_types:vhost()) -> [rabbit_types:tracked_connection()].

list_on_node(Node, VHost) ->
    try mnesia:dirty_match_object(
          tracked_connection_table_name_for(Node),
          #tracked_connection{vhost = VHost, _ = '_'})
    catch exit:{aborted, {no_exists, _}} -> []
    end.


-spec list_of_user(rabbit_types:username()) -> [rabbit_types:tracked_connection()].

list_of_user(Username) ->
    rabbit_tracking:match_tracked_items(
        fun tracked_connection_table_name_for/1,
        #tracked_connection{username = Username, _ = '_'}).

%% Internal, delete tracked entries

delete_tracked_connection_vhost_entry(Vhost) ->
    rabbit_tracking:delete_tracked_entry(
        {rabbit_vhost, exists, [Vhost]},
        fun tracked_connection_per_vhost_table_name_for/1,
        Vhost).

delete_tracked_connection_user_entry(Username) ->
    rabbit_tracking:delete_tracked_entry(
        {rabbit_auth_backend_internal, exists, [Username]},
        fun tracked_connection_per_user_table_name_for/1,
        Username).

%% Returns a #tracked_connection from connection_created
%% event details.
%%
%% @see rabbit_connection_tracking_handler.
tracked_connection_from_connection_created(EventDetails) ->
    %% Example event:
    %%
    %% [{type,network},
    %%  {pid,<0.329.0>},
    %%  {name,<<"127.0.0.1:60998 -> 127.0.0.1:5672">>},
    %%  {port,5672},
    %%  {peer_port,60998},
    %%  {host,{0,0,0,0,0,65535,32512,1}},
    %%  {peer_host,{0,0,0,0,0,65535,32512,1}},
    %%  {ssl,false},
    %%  {peer_cert_subject,''},
    %%  {peer_cert_issuer,''},
    %%  {peer_cert_validity,''},
    %%  {auth_mechanism,<<"PLAIN">>},
    %%  {ssl_protocol,''},
    %%  {ssl_key_exchange,''},
    %%  {ssl_cipher,''},
    %%  {ssl_hash,''},
    %%  {protocol,{0,9,1}},
    %%  {user,<<"guest">>},
    %%  {vhost,<<"/">>},
    %%  {timeout,14},
    %%  {frame_max,131072},
    %%  {channel_max,65535},
    %%  {client_properties,
    %%      [{<<"capabilities">>,table,
    %%        [{<<"publisher_confirms">>,bool,true},
    %%         {<<"consumer_cancel_notify">>,bool,true},
    %%         {<<"exchange_exchange_bindings">>,bool,true},
    %%         {<<"basic.nack">>,bool,true},
    %%         {<<"connection.blocked">>,bool,true},
    %%         {<<"authentication_failure_close">>,bool,true}]},
    %%       {<<"product">>,longstr,<<"Bunny">>},
    %%       {<<"platform">>,longstr,
    %%        <<"ruby 2.3.0p0 (2015-12-25 revision 53290) [x86_64-darwin15]">>},
    %%       {<<"version">>,longstr,<<"2.3.0.pre">>},
    %%       {<<"information">>,longstr,
    %%        <<"http://rubybunny.info">>}]},
    %%  {connected_at,1453214290847}]
    Name = pget(name, EventDetails),
    Node = pget(node, EventDetails),
    #tracked_connection{id           = rabbit_tracking:id(Node, Name),
                        name         = Name,
                        node         = Node,
                        vhost        = pget(vhost, EventDetails),
                        username     = pget(user, EventDetails),
                        connected_at = pget(connected_at, EventDetails),
                        pid          = pget(pid, EventDetails),
                        protocol     = pget(protocol, EventDetails),
                        type         = pget(type, EventDetails),
                        peer_host    = pget(peer_host, EventDetails),
                        peer_port    = pget(peer_port, EventDetails)}.

tracked_connection_from_connection_state(#connection{
                                            vhost = VHost,
                                            connected_at = Ts,
                                            peer_host = PeerHost,
                                            peer_port = PeerPort,
                                            user = Username,
                                            name = Name
                                           }) ->
    tracked_connection_from_connection_created(
      [{name, Name},
       {node, node()},
       {vhost, VHost},
       {user, Username},
       {user_who_performed_action, Username},
       {connected_at, Ts},
       {pid, self()},
       {type, network},
       {peer_port, PeerPort},
       {peer_host, PeerHost}]).

close_connections(Tracked, Message) ->
    close_connections(Tracked, Message, 0).

close_connections(Tracked, Message, Delay) ->
    [begin
         close_connection(Conn, Message),
         timer:sleep(Delay)
     end || Conn <- Tracked],
    ok.

close_connection(#tracked_connection{pid = Pid, type = network}, Message) ->
    try
        rabbit_networking:close_connection(Pid, Message)
    catch error:{not_a_connection, _} ->
            %% could has been closed concurrently, or the input
            %% is bogus. In any case, we should not terminate
            ok;
          _:Err ->
            %% ignore, don't terminate
            rabbit_log:warning("Could not close connection ~p: ~p", [Pid, Err]),
            ok
    end;
close_connection(#tracked_connection{pid = Pid, type = direct}, Message) ->
    %% Do an RPC call to the node running the direct client.
    Node = node(Pid),
    rpc:call(Node, amqp_direct_connection, server_close, [Pid, 320, Message]);
close_connection(#tracked_connection{pid = Pid}, Message) ->
    % best effort, this will work for connections to the stream plugin
    Node = node(Pid),
    rpc:call(Node, gen_server, call, [Pid, {shutdown, Message}, infinity]).
