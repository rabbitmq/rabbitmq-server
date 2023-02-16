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

-export([update_tracked/1,
         handle_cast/1,
         register_tracked/1,
         unregister_tracked/1,
         count_tracked_items_in/1,
         shutdown_tracked_items/2]).

-export([tracked_connection_table_name_for/1,
         tracked_connection_per_vhost_table_name_for/1,
         tracked_connection_per_user_table_name_for/1,

         ensure_tracked_tables_for_this_node/0,

         delete_tracked_connection_user_entry/1,
         delete_tracked_connection_vhost_entry/1,
         list/0, list/1, list_on_node/1, list_on_node/2, list_of_user/1,
         tracked_connection_from_connection_created/1,
         tracked_connection_from_connection_state/1,
         lookup/1, count/0]).

-export([count_local_tracked_items_in_vhost/1,
         count_local_tracked_items_of_user/1]).

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
        rabbit_misc:format("user '~ts' is deleted", [Username])).

-spec register_tracked(rabbit_types:tracked_connection()) -> ok.
-dialyzer([{nowarn_function, [register_tracked/1]}]).

register_tracked(#tracked_connection{username = Username, vhost = VHost, id = ConnId, node = Node} = Conn) when Node =:= node() ->
    _ = case ets:lookup(?TRACKED_CONNECTION_TABLE, ConnId) of
            []    ->
            ets:insert(?TRACKED_CONNECTION_TABLE, Conn),
            ets:update_counter(?TRACKED_CONNECTION_TABLE_PER_VHOST, VHost, 1, {VHost, 0}),
            ets:update_counter(?TRACKED_CONNECTION_TABLE_PER_USER, Username, 1, {Username, 0});
        [#tracked_connection{}] ->
            ok
    end,
    ok.

-spec unregister_tracked(rabbit_types:tracked_connection_id()) -> ok.
unregister_tracked(ConnId = {Node, _Name}) when Node =:= node() ->
    case ets:lookup(?TRACKED_CONNECTION_TABLE, ConnId) of
        []     -> ok;
        [#tracked_connection{vhost = VHost, username = Username}] ->
            ets:update_counter(?TRACKED_CONNECTION_TABLE_PER_USER, Username, -1),
            ets:update_counter(?TRACKED_CONNECTION_TABLE_PER_VHOST, VHost, -1),
            ets:delete(?TRACKED_CONNECTION_TABLE, ConnId)
    end.

-spec count_tracked_items_in({atom(), rabbit_types:vhost()}) -> non_neg_integer().
count_tracked_items_in({vhost, VirtualHost}) ->
    rabbit_tracking:count_on_all_nodes(
      ?MODULE, count_local_tracked_items_in_vhost, [VirtualHost],
      ["connections in vhost ", VirtualHost]);
count_tracked_items_in({user, Username}) ->
    rabbit_tracking:count_on_all_nodes(
      ?MODULE, count_local_tracked_items_of_user, [Username],
      ["connections for user ", Username]).

-spec count_local_tracked_items_in_vhost(rabbit_types:vhost()) -> non_neg_integer().
count_local_tracked_items_in_vhost(VirtualHost) ->
    rabbit_tracking:read_ets_counter(?TRACKED_CONNECTION_TABLE_PER_VHOST, VirtualHost).

-spec count_local_tracked_items_of_user(rabbit_types:username()) -> non_neg_integer().
count_local_tracked_items_of_user(Username) ->
    rabbit_tracking:read_ets_counter(?TRACKED_CONNECTION_TABLE_PER_USER, Username).

-spec shutdown_tracked_items(list(), term()) -> ok.

shutdown_tracked_items(TrackedItems, Message) ->
    close_connections(TrackedItems, Message).

%% Extended API

ensure_tracked_tables_for_this_node() ->
    ensure_tracked_connections_table_for_this_node(),
    _ = ensure_per_vhost_tracked_connections_table_for_this_node(),
    ensure_per_user_tracked_connections_table_for_this_node().

ensure_tracked_connections_table_for_this_node() ->
    _ = ets:new(?TRACKED_CONNECTION_TABLE, [named_table, public, {write_concurrency, true},
                                        {keypos, #tracked_connection.id}]),
    rabbit_log:info("Setting up a table for connection tracking on this node: ~tp",
                    [?TRACKED_CONNECTION_TABLE]).

ensure_per_vhost_tracked_connections_table_for_this_node() ->
    rabbit_log:info("Setting up a table for per-vhost connection counting on this node: ~tp",
                    [?TRACKED_CONNECTION_TABLE_PER_VHOST]),
    ets:new(?TRACKED_CONNECTION_TABLE_PER_VHOST, [named_table, public, {write_concurrency, true}]).

ensure_per_user_tracked_connections_table_for_this_node() ->
    _ = ets:new(?TRACKED_CONNECTION_TABLE_PER_USER, [named_table, public, {write_concurrency, true}]),
    rabbit_log:info("Setting up a table for per-user connection counting on this node: ~tp",
                    [?TRACKED_CONNECTION_TABLE_PER_USER]).

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

-spec lookup(rabbit_types:connection_name()) -> rabbit_types:tracked_connection() | 'not_found'.

lookup(Name) ->
    Nodes = rabbit_nodes:list_running(),
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
    ets:lookup(?TRACKED_CONNECTION_TABLE, {Node, Name}).

-spec list() -> [rabbit_types:tracked_connection()].

list() ->
    lists:foldl(
      fun (Node, Acc) ->
              Acc ++ list_on_node(Node)
      end, [], rabbit_nodes:list_running()).

-spec count() -> non_neg_integer().

count() ->
    lists:foldl(
      fun (Node, Acc) ->
              count_on_node(Node) + Acc
      end, 0, rabbit_nodes:list_running()).

count_on_node(Node) when Node == node() ->
    case ets:info(?TRACKED_CONNECTION_TABLE, size) of
        undefined -> 0;
        Size -> Size
    end;
count_on_node(Node) ->
    case rabbit_misc:rpc_call(Node, ?MODULE, count_on_node, [Node]) of
        Int when is_integer(Int) ->
            Int;
        _ ->
            0
    end.

-spec list(rabbit_types:vhost()) -> [rabbit_types:tracked_connection()].

list(VHost) ->
    rabbit_tracking:match_tracked_items(
        ?TRACKED_CONNECTION_TABLE,
        #tracked_connection{vhost = VHost, _ = '_'}).

-spec list_on_node(node()) -> [rabbit_types:tracked_connection()].
list_on_node(Node) when Node == node() ->
    ets:tab2list(?TRACKED_CONNECTION_TABLE);
list_on_node(Node) ->
    case rabbit_misc:rpc_call(Node, ?MODULE, list_on_node, [Node]) of
        List when is_list(List) ->
            List;
        _ ->
            []
    end.

-spec list_on_node(node(), rabbit_types:vhost()) -> [rabbit_types:tracked_connection()].

list_on_node(Node, VHost) when Node == node() ->
    ets:match_object(?TRACKED_CONNECTION_TABLE,
                     #tracked_connection{vhost = VHost, _ = '_'});
list_on_node(Node, VHost) ->
    case rabbit_misc:rpc_call(Node, ?MODULE, list_on_node, [Node, VHost]) of
        List when is_list(List) ->
            List;
        _ ->
            []
    end.

-spec list_of_user(rabbit_types:username()) -> [rabbit_types:tracked_connection()].

list_of_user(Username) ->
    rabbit_tracking:match_tracked_items(
      ?TRACKED_CONNECTION_TABLE,
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
