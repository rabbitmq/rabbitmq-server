%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2023 VMware, Inc. or its affiliates.  All rights reserved.
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

-export([tracked_connection_table_name_for/1,
         tracked_connection_per_vhost_table_name_for/1,
         tracked_connection_per_user_table_name_for/1,
         get_all_tracked_connection_table_names_for_node/1,
         clear_tracked_connection_tables_for_this_node/0,

         ensure_tracked_tables_for_this_node/0,

         delete_tracked_connection_user_entry/1,
         delete_tracked_connection_vhost_entry/1,
         list/0, list/1, list_on_node/1, list_on_node/2, list_of_user/1,
         tracked_connection_from_connection_created/1,
         tracked_connection_from_connection_state/1,
         lookup/1, count/0]).

%% All nodes (that support the `tracking_records_in_ets' feature) must
%% export these functions with the same spec, as they are called via
%% RPC from other nodes. (Their implementation can differ.)
-export([count_local_tracked_items_in_vhost/1,
         count_local_tracked_items_of_user/1]).

-export([migrate_tracking_records/0]).

-include_lib("rabbit_common/include/rabbit.hrl").

-import(rabbit_misc, [pget/2]).

-export([close_connections/3]).

-define(TRACKED_CONNECTION_TABLE, tracked_connection).
-define(TRACKED_CONNECTION_TABLE_PER_USER, tracked_connection_per_user).
-define(TRACKED_CONNECTION_TABLE_PER_VHOST, tracked_connection_per_vhost).

%%
%% API
%%

%% Behaviour callbacks

-spec boot() -> ok.

%% Sets up and resets connection tracking tables for this
%% node.
boot() ->
    ensure_tracked_connections_table_for_this_node(),
    ensure_per_vhost_tracked_connections_table_for_this_node(),
    ensure_per_user_tracked_connections_table_for_this_node(),
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
                    Msg = "Could not register connection ~tp for tracking, "
                          "its table is not ready yet or the connection terminated prematurely",
                    rabbit_log_connection:warning(Msg, [ConnId]),
                    ok;
                error:Err ->
                    Msg = "Could not register connection ~tp for tracking: ~tp",
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
    rabbit_log_connection:info("Closing all connections in vhost '~ts' because it's being deleted", [VHost]),
    shutdown_tracked_items(
        rabbit_connection_tracking:list(VHost),
        rabbit_misc:format("vhost '~ts' is deleted", [VHost]));
%% Note: under normal circumstances this will be called immediately
%% after the vhost_deleted above. Therefore we should be careful about
%% what we log and be more defensive.
handle_cast({vhost_down, Details}) ->
    VHost = pget(name, Details),
    Node = pget(node, Details),
    rabbit_log_connection:info("Closing all connections in vhost '~ts' on node '~ts'"
                               " because the vhost is stopping",
                               [VHost, Node]),
    shutdown_tracked_items(
        rabbit_connection_tracking:list_on_node(Node, VHost),
        rabbit_misc:format("vhost '~ts' is down", [VHost]));
handle_cast({user_deleted, Details}) ->
    Username = pget(name, Details),
    %% Schedule user entry deletion, allowing time for connections to close
    _ = timer:apply_after(?TRACKING_EXECUTION_TIMEOUT, ?MODULE,
            delete_tracked_connection_user_entry, [Username]),
    rabbit_log_connection:info("Closing all connections from user '~ts' because it's being deleted", [Username]),
    shutdown_tracked_items(
        rabbit_connection_tracking:list_of_user(Username),
        rabbit_misc:format("user '~ts' is deleted", [Username]));
%% A node had been deleted from the cluster.
handle_cast({node_deleted, Details}) ->
    case rabbit_feature_flags:is_enabled(tracking_records_in_ets) of
        true ->
            ok;
        false ->
            Node = pget(node, Details),
            rabbit_log_connection:info("Node '~ts' was removed from the cluster, deleting its connection tracking tables...", [Node]),
            delete_tracked_connections_table_for_node(Node),
            delete_per_vhost_tracked_connections_table_for_node(Node),
            delete_per_user_tracked_connections_table_for_node(Node)
    end.

-spec register_tracked(rabbit_types:tracked_connection()) -> ok.
-dialyzer([{nowarn_function, [register_tracked/1]}]).

register_tracked(#tracked_connection{node = Node} = Conn) when Node =:= node() ->
    case rabbit_feature_flags:is_enabled(tracking_records_in_ets) of
        true -> register_tracked_ets(Conn);
        false -> register_tracked_mnesia(Conn)
    end.

register_tracked_ets(#tracked_connection{username = Username, vhost = VHost, id = ConnId} = Conn) ->
    _ = case ets:lookup(?TRACKED_CONNECTION_TABLE, ConnId) of
        []    ->
            ets:insert(?TRACKED_CONNECTION_TABLE, Conn),
            ets:update_counter(?TRACKED_CONNECTION_TABLE_PER_VHOST, VHost, 1, {VHost, 0}),
            ets:update_counter(?TRACKED_CONNECTION_TABLE_PER_USER, Username, 1, {Username, 0});
        [#tracked_connection{}] ->
            ok
    end,
    ok.

register_tracked_mnesia(#tracked_connection{username = Username, vhost = VHost, id = ConnId, node = Node} = Conn) ->
    TableName = tracked_connection_table_name_for(Node),
    PerVhostTableName = tracked_connection_per_vhost_table_name_for(Node),
    PerUserConnTableName = tracked_connection_per_user_table_name_for(Node),
    %% upsert
    case mnesia:dirty_read(TableName, ConnId) of
        []    ->
            mnesia:dirty_write(TableName, Conn),
            mnesia:dirty_update_counter(PerVhostTableName, VHost, 1),
            mnesia:dirty_update_counter(PerUserConnTableName, Username, 1),
            ok;
        [#tracked_connection{}] ->
            ok
    end,
    ok.

-spec unregister_tracked(rabbit_types:tracked_connection_id()) -> ok.
unregister_tracked(ConnId = {Node, _Name}) when Node =:= node() ->
    case rabbit_feature_flags:is_enabled(tracking_records_in_ets) of
        true -> unregister_tracked_ets(ConnId);
        false -> unregister_tracked_mnesia(ConnId)
    end.

unregister_tracked_ets(ConnId) ->
    case ets:lookup(?TRACKED_CONNECTION_TABLE, ConnId) of
        []     -> ok;
        [#tracked_connection{vhost = VHost, username = Username}] ->
            ets:update_counter(?TRACKED_CONNECTION_TABLE_PER_USER, Username, -1),
            ets:update_counter(?TRACKED_CONNECTION_TABLE_PER_VHOST, VHost, -1),
            ets:delete(?TRACKED_CONNECTION_TABLE, ConnId)
    end.

unregister_tracked_mnesia(ConnId = {Node, _Name}) ->
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
count_tracked_items_in(Type) ->
    case rabbit_feature_flags:is_enabled(tracking_records_in_ets) of
        true -> count_tracked_items_in_ets(Type);
        false -> count_tracked_items_in_mnesia(Type)
    end.

count_tracked_items_in_ets({vhost, VirtualHost}) ->
    rabbit_tracking:count_on_all_nodes(
      ?MODULE, count_local_tracked_items_in_vhost, [VirtualHost],
      ["connections in vhost ", VirtualHost]);
count_tracked_items_in_ets({user, Username}) ->
    rabbit_tracking:count_on_all_nodes(
      ?MODULE, count_local_tracked_items_of_user, [Username],
      ["connections for user ", Username]).

-spec count_local_tracked_items_in_vhost(rabbit_types:vhost()) -> non_neg_integer().
count_local_tracked_items_in_vhost(VirtualHost) ->
    rabbit_tracking:read_ets_counter(?TRACKED_CONNECTION_TABLE_PER_VHOST, VirtualHost).

-spec count_local_tracked_items_of_user(rabbit_types:username()) -> non_neg_integer().
count_local_tracked_items_of_user(Username) ->
    rabbit_tracking:read_ets_counter(?TRACKED_CONNECTION_TABLE_PER_USER, Username).

count_tracked_items_in_mnesia({vhost, VirtualHost}) ->
    rabbit_tracking:count_tracked_items_mnesia(
        fun tracked_connection_per_vhost_table_name_for/1,
        #tracked_connection_per_vhost.connection_count, VirtualHost,
        "connections in vhost");
count_tracked_items_in_mnesia({user, Username}) ->
    rabbit_tracking:count_tracked_items_mnesia(
        fun tracked_connection_per_user_table_name_for/1,
        #tracked_connection_per_user.connection_count, Username,
        "connections for user").

-spec clear_tracking_tables() -> ok.

clear_tracking_tables() ->
    case rabbit_feature_flags:is_enabled(tracking_records_in_ets) of
        true -> ok;
        false -> clear_tracked_connection_tables_for_this_node()
    end.

-spec shutdown_tracked_items(list(), term()) -> ok.

shutdown_tracked_items(TrackedItems, Message) ->
    close_connections(TrackedItems, Message).

%% Extended API

ensure_tracked_tables_for_this_node() ->
    ensure_tracked_connections_table_for_this_node_ets(),
    _ = ensure_per_vhost_tracked_connections_table_for_this_node_ets(),
    ensure_per_user_tracked_connections_table_for_this_node_ets().

-spec ensure_tracked_connections_table_for_this_node() -> ok.

ensure_tracked_connections_table_for_this_node() ->
    case rabbit_feature_flags:is_enabled(tracking_records_in_ets) of
        true ->
            ok;
        false ->
            ensure_tracked_connections_table_for_this_node_mnesia()
    end.

ensure_tracked_connections_table_for_this_node_ets() ->
    _ = ets:new(?TRACKED_CONNECTION_TABLE, [named_table, public, {write_concurrency, true},
                                        {keypos, #tracked_connection.id}]),
    rabbit_log:info("Setting up a table for connection tracking on this node: ~tp",
                    [?TRACKED_CONNECTION_TABLE]).

ensure_tracked_connections_table_for_this_node_mnesia() ->
    Node = node(),
    TableName = tracked_connection_table_name_for(Node),
    case mnesia:create_table(TableName, [{record_name, tracked_connection},
                                         {attributes, record_info(fields, tracked_connection)}]) of
        {atomic, ok}                   ->
            rabbit_log:info("Setting up a table for connection tracking on this node: ~tp",
                            [TableName]),
            ok;
        {aborted, {already_exists, _}} ->
            rabbit_log:info("Setting up a table for connection tracking on this node: ~tp",
                            [TableName]);
        {aborted, Error}               ->
            rabbit_log:error("Failed to create a tracked connection table for node ~tp: ~tp", [Node, Error]),
            ok
    end.

-spec ensure_per_vhost_tracked_connections_table_for_this_node() -> ok.

ensure_per_vhost_tracked_connections_table_for_this_node() ->
    case rabbit_feature_flags:is_enabled(tracking_records_in_ets) of
        true ->
            ok;
        false ->
            ensure_per_vhost_tracked_connections_table_for_this_node_mnesia()
    end.

ensure_per_vhost_tracked_connections_table_for_this_node_ets() ->
    rabbit_log:info("Setting up a table for per-vhost connection counting on this node: ~tp",
                    [?TRACKED_CONNECTION_TABLE_PER_VHOST]),
    ets:new(?TRACKED_CONNECTION_TABLE_PER_VHOST, [named_table, public, {write_concurrency, true}]).

ensure_per_vhost_tracked_connections_table_for_this_node_mnesia() ->
    Node = node(),
    TableName = tracked_connection_per_vhost_table_name_for(Node),
    case mnesia:create_table(TableName, [{record_name, tracked_connection_per_vhost},
                                         {attributes, record_info(fields, tracked_connection_per_vhost)}]) of
        {atomic, ok}                   ->
            rabbit_log:info("Setting up a table for per-vhost connection counting on this node: ~tp",
                            [TableName]),
            ok;
        {aborted, {already_exists, _}} ->
            rabbit_log:info("Setting up a table for per-vhost connection counting on this node: ~tp",
                            [TableName]),
            ok;
        {aborted, Error}               ->
            rabbit_log:error("Failed to create a per-vhost tracked connection table for node ~tp: ~tp", [Node, Error]),
            ok
    end.

-spec ensure_per_user_tracked_connections_table_for_this_node() -> ok.

ensure_per_user_tracked_connections_table_for_this_node() ->
    case rabbit_feature_flags:is_enabled(tracking_records_in_ets) of
        true ->
            ok;
        false ->
            ensure_per_user_tracked_connections_table_for_this_node_mnesia()
    end.

ensure_per_user_tracked_connections_table_for_this_node_ets() ->
    _ = ets:new(?TRACKED_CONNECTION_TABLE_PER_USER, [named_table, public, {write_concurrency, true}]),
    rabbit_log:info("Setting up a table for per-user connection counting on this node: ~tp",
                    [?TRACKED_CONNECTION_TABLE_PER_USER]).

ensure_per_user_tracked_connections_table_for_this_node_mnesia() ->
    Node = node(),
    TableName = tracked_connection_per_user_table_name_for(Node),
    case mnesia:create_table(TableName, [{record_name, tracked_connection_per_user},
                                         {attributes, record_info(fields, tracked_connection_per_user)}]) of
        {atomic, ok}                   ->
            rabbit_log:info("Setting up a table for per-user connection counting on this node: ~tp",
                            [TableName]),
            ok;
        {aborted, {already_exists, _}} ->
            rabbit_log:info("Setting up a table for per-user connection counting on this node: ~tp",
                            [TableName]),
            ok;
        {aborted, Error}               ->
            rabbit_log:error("Failed to create a per-user tracked connection table for node ~tp: ~tp", [Node, Error]),
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
    list_to_atom(rabbit_misc:format("tracked_connection_on_node_~ts", [Node])).

-spec tracked_connection_per_vhost_table_name_for(node()) -> atom().

tracked_connection_per_vhost_table_name_for(Node) ->
    list_to_atom(rabbit_misc:format("tracked_connection_per_vhost_on_node_~ts", [Node])).

-spec tracked_connection_per_user_table_name_for(node()) -> atom().

tracked_connection_per_user_table_name_for(Node) ->
    list_to_atom(rabbit_misc:format(
        "tracked_connection_table_per_user_on_node_~ts", [Node])).

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
lookup(Name, [Node | Nodes]) when Node == node() ->
    case lookup_internal(Name, Node) of
        [] -> lookup(Name, Nodes);
        [Row] -> Row
    end;
lookup(Name, [Node | Nodes]) ->
    case rabbit_misc:rpc_call(Node, ?MODULE, lookup, [Name, [Node]]) of
        [] -> lookup(Name, Nodes);
        [Row] -> Row
    end.

lookup_internal(Name, Node) ->
    case rabbit_feature_flags:is_enabled(tracking_records_in_ets) of
        true -> lookup_ets(Name, Node);
        false -> lookup_mnesia(Name, Node)
    end.

lookup_ets(Name, Node) ->
    ets:lookup(?TRACKED_CONNECTION_TABLE, {Node, Name}).

lookup_mnesia(Name, Node) ->
    TableName = tracked_connection_table_name_for(Node),
    mnesia:dirty_read(TableName, {Node, Name}).

-spec list() -> [rabbit_types:tracked_connection()].

list() ->
    lists:foldl(
      fun (Node, Acc) ->
              Acc ++ list_on_node(Node)
      end, [], rabbit_nodes:all_running()).

-spec count() -> non_neg_integer().

count() ->
    lists:foldl(
      fun (Node, Acc) ->
              count_on_node(Node) + Acc
      end, 0, rabbit_nodes:all_running()).

count_on_node(Node) ->
    case rabbit_feature_flags:is_enabled(tracking_records_in_ets) of
        true when Node == node() ->
            count_on_node_ets();
        true ->
            case rabbit_misc:rpc_call(Node, ?MODULE, count_on_node, [Node]) of
                Int when is_integer(Int) ->
                    Int;
                _ ->
                    0
            end;
        false ->
            count_on_node_mnesia(Node)
    end.

count_on_node_ets() ->
    case ets:info(?TRACKED_CONNECTION_TABLE, size) of
        undefined -> 0;
        Size -> Size
    end.

count_on_node_mnesia(Node) ->
    Tab = tracked_connection_table_name_for(Node),
    %% mnesia:table_info() returns 0 if the table doesn't exist. We
    %% don't need the same kind of protection as the list() function
    %% above.
    mnesia:table_info(Tab, size).

-spec list(rabbit_types:vhost()) -> [rabbit_types:tracked_connection()].

list(VHost) ->
    case rabbit_feature_flags:is_enabled(tracking_records_in_ets) of
        true -> list_ets(VHost);
        false -> list_mnesia(VHost)
    end.

list_ets(VHost) ->
    rabbit_tracking:match_tracked_items_ets(
        ?TRACKED_CONNECTION_TABLE,
        #tracked_connection{vhost = VHost, _ = '_'}).

list_mnesia(VHost) ->
    rabbit_tracking:match_tracked_items_mnesia(
        fun tracked_connection_table_name_for/1,
        #tracked_connection{vhost = VHost, _ = '_'}).

-spec list_on_node(node()) -> [rabbit_types:tracked_connection()].
list_on_node(Node) ->
    case rabbit_feature_flags:is_enabled(tracking_records_in_ets) of
        true when Node == node() ->
            list_on_node_ets();
        true ->
            case rabbit_misc:rpc_call(Node, ?MODULE, list_on_node, [Node]) of
                List when is_list(List) ->
                    List;
                _ ->
                    []
            end;
        false ->
            list_on_node_mnesia(Node)
    end.

list_on_node_ets() ->
    ets:tab2list(?TRACKED_CONNECTION_TABLE).

list_on_node_mnesia(Node) ->
    try mnesia:dirty_match_object(
          tracked_connection_table_name_for(Node),
          #tracked_connection{_ = '_'})
    catch exit:{aborted, {no_exists, _}} ->
            %% The table might not exist yet (or is already gone)
            %% between the time rabbit_nodes:all_running() runs and
            %% returns a specific node, and
            %% mnesia:dirty_match_object() is called for that node's
            %% table.
            []
    end.

-spec list_on_node(node(), rabbit_types:vhost()) -> [rabbit_types:tracked_connection()].

list_on_node(Node, VHost) ->
    case rabbit_feature_flags:is_enabled(tracking_records_in_ets) of
        true when Node == node() ->
            list_on_node_ets(VHost);
        true ->
            case rabbit_misc:rpc_call(Node, ?MODULE, list_on_node, [Node, VHost]) of
                List when is_list(List) ->
                    List;
                _ ->
                    []
            end;
        false ->
            list_on_node_mnesia(Node, VHost)
    end.

list_on_node_ets(VHost) ->
    ets:match_object(?TRACKED_CONNECTION_TABLE,
                     #tracked_connection{vhost = VHost, _ = '_'}).

list_on_node_mnesia(Node, VHost) ->
    try mnesia:dirty_match_object(
          tracked_connection_table_name_for(Node),
          #tracked_connection{vhost = VHost, _ = '_'})
    catch exit:{aborted, {no_exists, _}} -> []
    end.

-spec list_of_user(rabbit_types:username()) -> [rabbit_types:tracked_connection()].

list_of_user(Username) ->
    case rabbit_feature_flags:is_enabled(tracking_records_in_ets) of
        true -> list_of_user_ets(Username);
        false -> list_of_user_mnesia(Username)
    end.

list_of_user_ets(Username) ->
    rabbit_tracking:match_tracked_items_ets(
      ?TRACKED_CONNECTION_TABLE,
      #tracked_connection{username = Username, _ = '_'}).

list_of_user_mnesia(Username) ->
    rabbit_tracking:match_tracked_items_mnesia(
        fun tracked_connection_table_name_for/1,
        #tracked_connection{username = Username, _ = '_'}).

%% Internal, delete tracked entries

delete_tracked_connection_vhost_entry(VHost) ->
    rabbit_tracking:delete_tracked_entry(
      {rabbit_vhost, exists, [VHost]},
      ?TRACKED_CONNECTION_TABLE_PER_VHOST,
      fun tracked_connection_per_vhost_table_name_for/1,
      VHost).

delete_tracked_connection_user_entry(Username) ->
    rabbit_tracking:delete_tracked_entry(
      {rabbit_auth_backend_internal, exists, [Username]},
      ?TRACKED_CONNECTION_TABLE_PER_USER,
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
            rabbit_log:warning("Could not close connection ~tp: ~tp", [Pid, Err]),
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

migrate_tracking_records() ->
    Node = node(),
    rabbit_mnesia:execute_mnesia_transaction(
      fun () ->
              Table = tracked_connection_table_name_for(Node),
              _ = mnesia:lock({table, Table}, read),
              Connections = mnesia:select(Table, [{'$1',[],['$1']}]),
              lists:foreach(
                fun(Connection) ->
                        ets:insert(tracked_connection, Connection)
                end, Connections)
      end),
    rabbit_mnesia:execute_mnesia_transaction(
      fun () ->
              Table = tracked_connection_per_user_table_name_for(Node),
              _ = mnesia:lock({table, Table}, read),
              Connections = mnesia:select(Table, [{'$1',[],['$1']}]),
              lists:foreach(
                fun(#tracked_connection_per_user{connection_count = C,
                                                 user = Username}) ->
                        ets:update_counter(tracked_connection_per_user, Username, C, {Username, 0})
                end, Connections)
      end),
    rabbit_mnesia:execute_mnesia_transaction(
      fun () ->
              Table = tracked_connection_per_vhost_table_name_for(Node),
              _ = mnesia:lock({table, Table}, read),
              Connections = mnesia:select(Table, [{'$1',[],['$1']}]),
              lists:foreach(
                fun(#tracked_connection_per_vhost{connection_count = C,
                                                  vhost = VHost}) ->
                        ets:update_counter(tracked_connection_per_vhost, VHost, C, {VHost, 0})
                end, Connections)
      end).
