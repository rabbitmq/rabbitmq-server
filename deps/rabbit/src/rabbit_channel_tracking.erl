%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2020 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(rabbit_channel_tracking).

%% Abstracts away how tracked connection records are stored
%% and queried.
%%
%% See also:
%%
%%  * rabbit_channel_tracking_handler
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

-export([list/0, list_of_user/1, list_on_node/1,
         tracked_channel_table_name_for/1,
         tracked_channel_per_user_table_name_for/1,
         get_all_tracked_channel_table_names_for_node/1,
         delete_tracked_channel_user_entry/1]).

-include_lib("rabbit_common/include/rabbit.hrl").

-import(rabbit_misc, [pget/2]).

%%
%% API
%%

%% Sets up and resets channel tracking tables for this node.
-spec boot() -> ok.

boot() ->
    ensure_tracked_channels_table_for_this_node(),
    _ = rabbit_log:info("Setting up a table for channel tracking on this node: ~p",
                    [tracked_channel_table_name_for(node())]),
    ensure_per_user_tracked_channels_table_for_node(),
    _ = rabbit_log:info("Setting up a table for channel tracking on this node: ~p",
                    [tracked_channel_per_user_table_name_for(node())]),
    clear_tracking_tables(),
    ok.

-spec update_tracked(term()) -> ok.

update_tracked(Event) ->
    spawn(?MODULE, handle_cast, [Event]),
    ok.

%% Asynchronously handle update events
-spec handle_cast(term()) -> ok.

handle_cast({channel_created, Details}) ->
    ThisNode = node(),
    case node(pget(pid, Details)) of
        ThisNode ->
            TrackedCh = #tracked_channel{id = TrackedChId} =
                tracked_channel_from_channel_created_event(Details),
            try
                register_tracked(TrackedCh)
            catch
                error:{no_exists, _} ->
                    Msg = "Could not register channel ~p for tracking, "
                          "its table is not ready yet or the channel terminated prematurely",
                    _ = rabbit_log_connection:warning(Msg, [TrackedChId]),
                    ok;
                error:Err ->
                    Msg = "Could not register channel ~p for tracking: ~p",
                    _ = rabbit_log_connection:warning(Msg, [TrackedChId, Err]),
                    ok
            end;
        _OtherNode ->
            %% ignore
            ok
    end;
handle_cast({channel_closed, Details}) ->
    %% channel has terminated, unregister iff local
    case get_tracked_channel_by_pid(pget(pid, Details)) of
        [#tracked_channel{name = Name}] ->
            unregister_tracked(rabbit_tracking:id(node(), Name));
        _Other -> ok
    end;
handle_cast({connection_closed, ConnDetails}) ->
    ThisNode= node(),
    ConnPid = pget(pid, ConnDetails),

    case pget(node, ConnDetails) of
        ThisNode ->
            TrackedChs = get_tracked_channels_by_connection_pid(ConnPid),
            _ = rabbit_log_channel:info(
                "Closing all channels from connection '~s' "
                "because it has been closed", [pget(name, ConnDetails)]),
            %% Shutting down channels will take care of unregistering the
            %% corresponding tracking.
            shutdown_tracked_items(TrackedChs, undefined),
            ok;
        _DifferentNode ->
            ok
    end;
handle_cast({user_deleted, Details}) ->
    Username = pget(name, Details),
    %% Schedule user entry deletion, allowing time for connections to close
    _ = timer:apply_after(?TRACKING_EXECUTION_TIMEOUT, ?MODULE,
            delete_tracked_channel_user_entry, [Username]),
    ok;
handle_cast({node_deleted, Details}) ->
    Node = pget(node, Details),
    _ = rabbit_log_connection:info(
        "Node '~s' was removed from the cluster, deleting"
        " its channel tracking tables...", [Node]),
    delete_tracked_channels_table_for_node(Node),
    delete_per_user_tracked_channels_table_for_node(Node).

-spec register_tracked(rabbit_types:tracked_channel()) -> ok.
-dialyzer([{nowarn_function, [register_tracked/1]}, race_conditions]).

register_tracked(TrackedCh =
  #tracked_channel{node = Node, name = Name, username = Username}) ->
    ChId = rabbit_tracking:id(Node, Name),
    TableName = tracked_channel_table_name_for(Node),
    PerUserChTableName = tracked_channel_per_user_table_name_for(Node),
    %% upsert
    case mnesia:dirty_read(TableName, ChId) of
      []    ->
          mnesia:dirty_write(TableName, TrackedCh),
          mnesia:dirty_update_counter(PerUserChTableName, Username, 1);
      [#tracked_channel{}] ->
          ok
    end,
    ok.

-spec unregister_tracked(rabbit_types:tracked_channel_id()) -> ok.

unregister_tracked(ChId = {Node, _Name}) when Node =:= node() ->
    TableName = tracked_channel_table_name_for(Node),
    PerUserChannelTableName = tracked_channel_per_user_table_name_for(Node),
    case mnesia:dirty_read(TableName, ChId) of
        []     -> ok;
        [#tracked_channel{username = Username}] ->
            mnesia:dirty_update_counter(PerUserChannelTableName, Username, -1),
            mnesia:dirty_delete(TableName, ChId)
    end.

-spec count_tracked_items_in({atom(), rabbit_types:username()}) -> non_neg_integer().

count_tracked_items_in({user, Username}) ->
    rabbit_tracking:count_tracked_items(
        fun tracked_channel_per_user_table_name_for/1,
        #tracked_channel_per_user.channel_count, Username,
        "channels in vhost").

-spec clear_tracking_tables() -> ok.

clear_tracking_tables() ->
    clear_tracked_channel_tables_for_this_node(),
    ok.

-spec shutdown_tracked_items(list(), term()) -> ok.

shutdown_tracked_items(TrackedItems, _Args) ->
    close_channels(TrackedItems).

%% helper functions
-spec list() -> [rabbit_types:tracked_channel()].

list() ->
    lists:foldl(
      fun (Node, Acc) ->
              Tab = tracked_channel_table_name_for(Node),
              Acc ++ mnesia:dirty_match_object(Tab, #tracked_channel{_ = '_'})
      end, [], rabbit_nodes:all_running()).

-spec list_of_user(rabbit_types:username()) -> [rabbit_types:tracked_channel()].

list_of_user(Username) ->
    rabbit_tracking:match_tracked_items(
        fun tracked_channel_table_name_for/1,
        #tracked_channel{username = Username, _ = '_'}).

-spec list_on_node(node()) -> [rabbit_types:tracked_channel()].

list_on_node(Node) ->
    try mnesia:dirty_match_object(
          tracked_channel_table_name_for(Node),
          #tracked_channel{_ = '_'})
    catch exit:{aborted, {no_exists, _}} -> []
    end.

-spec tracked_channel_table_name_for(node()) -> atom().

tracked_channel_table_name_for(Node) ->
    list_to_atom(rabbit_misc:format("tracked_channel_on_node_~s", [Node])).

-spec tracked_channel_per_user_table_name_for(node()) -> atom().

tracked_channel_per_user_table_name_for(Node) ->
    list_to_atom(rabbit_misc:format(
        "tracked_channel_table_per_user_on_node_~s", [Node])).

%% internal
ensure_tracked_channels_table_for_this_node() ->
    ensure_tracked_channels_table_for_node(node()).

ensure_per_user_tracked_channels_table_for_node() ->
    ensure_per_user_tracked_channels_table_for_node(node()).

%% Create tables
ensure_tracked_channels_table_for_node(Node) ->
    TableName = tracked_channel_table_name_for(Node),
    case mnesia:create_table(TableName, [{record_name, tracked_channel},
                                         {attributes, record_info(fields, tracked_channel)}]) of
        {atomic, ok}                   -> ok;
        {aborted, {already_exists, _}} -> ok;
        {aborted, Error}               ->
            _ = rabbit_log:error("Failed to create a tracked channel table for node ~p: ~p", [Node, Error]),
            ok
    end.

ensure_per_user_tracked_channels_table_for_node(Node) ->
    TableName = tracked_channel_per_user_table_name_for(Node),
    case mnesia:create_table(TableName, [{record_name, tracked_channel_per_user},
                                         {attributes, record_info(fields, tracked_channel_per_user)}]) of
        {atomic, ok}                   -> ok;
        {aborted, {already_exists, _}} -> ok;
        {aborted, Error}               ->
            _ = rabbit_log:error("Failed to create a per-user tracked channel table for node ~p: ~p", [Node, Error]),
            ok
    end.

clear_tracked_channel_tables_for_this_node() ->
    [rabbit_tracking:clear_tracking_table(T)
        || T <- get_all_tracked_channel_table_names_for_node(node())].

delete_tracked_channels_table_for_node(Node) ->
    TableName = tracked_channel_table_name_for(Node),
    rabbit_tracking:delete_tracking_table(TableName, Node, "tracked channel").

delete_per_user_tracked_channels_table_for_node(Node) ->
    TableName = tracked_channel_per_user_table_name_for(Node),
    rabbit_tracking:delete_tracking_table(TableName, Node,
        "per-user tracked channels").

get_all_tracked_channel_table_names_for_node(Node) ->
    [tracked_channel_table_name_for(Node),
        tracked_channel_per_user_table_name_for(Node)].

get_tracked_channels_by_connection_pid(ConnPid) ->
    rabbit_tracking:match_tracked_items(
        fun tracked_channel_table_name_for/1,
        #tracked_channel{connection = ConnPid, _ = '_'}).

get_tracked_channel_by_pid(ChPid) ->
    rabbit_tracking:match_tracked_items(
        fun tracked_channel_table_name_for/1,
        #tracked_channel{pid = ChPid, _ = '_'}).

delete_tracked_channel_user_entry(Username) ->
    rabbit_tracking:delete_tracked_entry(
        {rabbit_auth_backend_internal, exists, [Username]},
        fun tracked_channel_per_user_table_name_for/1,
        Username).

tracked_channel_from_channel_created_event(ChannelDetails) ->
    Node = node(ChPid = pget(pid, ChannelDetails)),
    Name = pget(name, ChannelDetails),
    #tracked_channel{
        id    = rabbit_tracking:id(Node, Name),
        name  = Name,
        node  = Node,
        vhost = pget(vhost, ChannelDetails),
        pid   = ChPid,
        connection = pget(connection, ChannelDetails),
        username   = pget(user, ChannelDetails)}.

close_channels(TrackedChannels = [#tracked_channel{}|_]) ->
    [rabbit_channel:shutdown(ChPid)
        || #tracked_channel{pid = ChPid} <- TrackedChannels],
    ok;
close_channels(_TrackedChannels = []) -> ok.
