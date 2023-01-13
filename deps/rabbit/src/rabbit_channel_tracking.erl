%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2020-2023 VMware, Inc. or its affiliates.  All rights reserved.
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
         ensure_tracked_tables_for_this_node/0,
         delete_tracked_channel_user_entry/1]).

%% All nodes (that support the `tracking_records_in_ets' feature) must
%% export this function with the same spec, as they are called via
%% RPC from other nodes. (Their implementation can differ.)
-export([count_local_tracked_items_of_user/1]).

-export([migrate_tracking_records/0]).

-include_lib("rabbit_common/include/rabbit.hrl").

-import(rabbit_misc, [pget/2]).

-define(TRACKED_CHANNEL_TABLE, tracked_channel).
-define(TRACKED_CHANNEL_TABLE_PER_USER, tracked_channel_per_user).

%%
%% API
%%

%% Sets up and resets channel tracking tables for this node.
-spec boot() -> ok.

boot() ->
    ensure_tracked_channels_table_for_this_node(),
    ensure_per_user_tracked_channels_table_for_node(),
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
                    Msg = "Could not register channel ~tp for tracking, "
                          "its table is not ready yet or the channel terminated prematurely",
                    rabbit_log_connection:warning(Msg, [TrackedChId]),
                    ok;
                error:Err ->
                    Msg = "Could not register channel ~tp for tracking: ~tp",
                    rabbit_log_connection:warning(Msg, [TrackedChId, Err]),
                    ok
            end;
        _OtherNode ->
            %% ignore
            ok
    end;
handle_cast({channel_closed, Details}) ->
    %% channel has terminated, unregister if local
    unregister_tracked_by_pid(pget(pid, Details));
handle_cast({connection_closed, ConnDetails}) ->
    ThisNode= node(),
    ConnPid = pget(pid, ConnDetails),

    case pget(node, ConnDetails) of
        ThisNode ->
            TrackedChs = get_tracked_channels_by_connection_pid(ConnPid),
            rabbit_log_channel:debug(
                "Closing all channels from connection '~ts' "
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
    case rabbit_feature_flags:is_enabled(tracking_records_in_ets) of
        true ->
            ok;
        false ->
            Node = pget(node, Details),
            rabbit_log_channel:info(
              "Node '~ts' was removed from the cluster, deleting"
              " its channel tracking tables...", [Node]),
            delete_tracked_channels_table_for_node(Node),
            delete_per_user_tracked_channels_table_for_node(Node)
    end.

-spec register_tracked(rabbit_types:tracked_channel()) -> ok.
-dialyzer([{nowarn_function, [register_tracked/1]}]).

register_tracked(TrackedCh = #tracked_channel{node = Node}) when Node == node() ->
    case rabbit_feature_flags:is_enabled(tracking_records_in_ets) of
        true -> register_tracked_ets(TrackedCh);
        false -> register_tracked_mnesia(TrackedCh)
    end.

register_tracked_ets(TrackedCh = #tracked_channel{pid = ChPid, username = Username}) ->
    case ets:lookup(?TRACKED_CHANNEL_TABLE, ChPid) of
        []    ->
            ets:insert(?TRACKED_CHANNEL_TABLE, TrackedCh),
            ets:update_counter(?TRACKED_CHANNEL_TABLE_PER_USER, Username, 1, {Username, 0}),
            ok;
        [#tracked_channel{}] ->
            ok
    end,
    ok.

register_tracked_mnesia(TrackedCh =
                            #tracked_channel{node = Node, name = Name, username = Username}) ->
    ChId = rabbit_tracking:id(Node, Name),
    TableName = tracked_channel_table_name_for(Node),
    PerUserChTableName = tracked_channel_per_user_table_name_for(Node),
    case mnesia:dirty_read(TableName, ChId) of
      []    ->
          mnesia:dirty_write(TableName, TrackedCh),
          mnesia:dirty_update_counter(PerUserChTableName, Username, 1),
            ok;
      [#tracked_channel{}] ->
          ok
    end,
    ok.

-spec unregister_tracked_by_pid(pid()) -> any().
unregister_tracked_by_pid(ChPid) when node(ChPid) == node() ->
    case rabbit_feature_flags:is_enabled(tracking_records_in_ets) of
        true -> unregister_tracked_by_pid_ets(ChPid);
        false -> unregister_tracked_by_pid_mnesia(ChPid)
    end.

unregister_tracked_by_pid_ets(ChPid) ->
    case ets:lookup(?TRACKED_CHANNEL_TABLE, ChPid) of
        []     -> ok;
        [#tracked_channel{username = Username}] ->
            ets:update_counter(?TRACKED_CHANNEL_TABLE_PER_USER, Username, -1),
            ets:delete(?TRACKED_CHANNEL_TABLE, ChPid)
    end.

unregister_tracked_by_pid_mnesia(ChPid) ->
    case get_tracked_channel_by_pid_mnesia(ChPid) of
        []     -> ok;
        [#tracked_channel{id = ChId, node = Node, username = Username}] ->
            TableName = tracked_channel_table_name_for(Node),
            PerUserChannelTableName = tracked_channel_per_user_table_name_for(Node),

            mnesia:dirty_update_counter(PerUserChannelTableName, Username, -1),
            mnesia:dirty_delete(TableName, ChId)
    end.

%% @doc This function is exported and implements a rabbit_tracking
%% callback, however it is not used in rabbitmq-server any more. It is
%% only kept for backwards compatibility if 3rd-party code would rely
%% on it.
-spec unregister_tracked(rabbit_types:tracked_channel_id()) -> ok.
unregister_tracked(ChId = {Node, _Name}) when Node == node() ->
    case rabbit_feature_flags:is_enabled(tracking_records_in_ets) of
        true -> unregister_tracked_ets(ChId);
        false -> unregister_tracked_mnesia(ChId)
    end.

unregister_tracked_ets(ChId) ->
    case get_tracked_channel_by_id_ets(ChId) of
        []     -> ok;
        [#tracked_channel{pid = ChPid, username = Username}] ->
            ets:update_counter(?TRACKED_CHANNEL_TABLE_PER_USER, Username, -1),
            ets:delete(?TRACKED_CHANNEL_TABLE, ChPid)
    end.

unregister_tracked_mnesia(ChId = {Node, _Name}) when Node =:= node() ->
    TableName = tracked_channel_table_name_for(Node),
    PerUserChannelTableName = tracked_channel_per_user_table_name_for(Node),
    case mnesia:dirty_read(TableName, ChId) of
        []     -> ok;
        [#tracked_channel{username = Username}] ->
            mnesia:dirty_update_counter(PerUserChannelTableName, Username, -1),
            mnesia:dirty_delete(TableName, ChId)
    end.

-spec count_tracked_items_in({atom(), rabbit_types:username()}) -> non_neg_integer().

count_tracked_items_in(Type) ->
    case rabbit_feature_flags:is_enabled(tracking_records_in_ets) of
        true -> count_tracked_items_in_ets(Type);
        false -> count_tracked_items_in_mnesia(Type)
    end.

count_tracked_items_in_ets({user, Username}) ->
    rabbit_tracking:count_on_all_nodes(
      ?MODULE, count_local_tracked_items_of_user, [Username],
      ["channels of user ", Username]).

-spec count_local_tracked_items_of_user(rabbit_types:username()) -> non_neg_integer().
count_local_tracked_items_of_user(Username) ->
    rabbit_tracking:read_ets_counter(?TRACKED_CHANNEL_TABLE_PER_USER, Username).

count_tracked_items_in_mnesia({user, Username}) ->
    rabbit_tracking:count_tracked_items_mnesia(
        fun tracked_channel_per_user_table_name_for/1,
        #tracked_channel_per_user.channel_count, Username,
        "channels of user").

-spec clear_tracking_tables() -> ok.

clear_tracking_tables() ->
    case rabbit_feature_flags:is_enabled(tracking_records_in_ets) of
        true -> ok;
        false -> clear_tracked_channel_tables_for_this_node()
    end.

-spec shutdown_tracked_items(list(), term()) -> ok.

shutdown_tracked_items(TrackedItems, _Args) ->
    close_channels(TrackedItems).

%% helper functions
-spec list() -> [rabbit_types:tracked_channel()].

list() ->
    lists:foldl(
      fun (Node, Acc) ->
              Acc ++ list_on_node(Node)
      end, [], rabbit_nodes:all_running()).

-spec list_of_user(rabbit_types:username()) -> [rabbit_types:tracked_channel()].

list_of_user(Username) ->
    case rabbit_feature_flags:is_enabled(tracking_records_in_ets) of
        true -> list_of_user_ets(Username);
        false -> list_of_user_mnesia(Username)
    end.

list_of_user_ets(Username) ->
    rabbit_tracking:match_tracked_items_ets(
      ?TRACKED_CHANNEL_TABLE,
      #tracked_channel{username = Username, _ = '_'}).

list_of_user_mnesia(Username) ->
    rabbit_tracking:match_tracked_items_mnesia(
        fun tracked_channel_table_name_for/1,
        #tracked_channel{username = Username, _ = '_'}).

-spec list_on_node(node()) -> [rabbit_types:tracked_channel()].
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
    ets:tab2list(?TRACKED_CHANNEL_TABLE).

list_on_node_mnesia(Node) ->
    try mnesia:dirty_match_object(
          tracked_channel_table_name_for(Node),
          #tracked_channel{_ = '_'})
    catch exit:{aborted, {no_exists, _}} ->
            %% The table might not exist yet (or is already gone)
            %% between the time rabbit_nodes:all_running() runs and
            %% returns a specific node, and
            %% mnesia:dirty_match_object() is called for that node's
            %% table.
            []
    end.

-spec tracked_channel_table_name_for(node()) -> atom().

tracked_channel_table_name_for(Node) ->
    list_to_atom(rabbit_misc:format("tracked_channel_on_node_~ts", [Node])).

-spec tracked_channel_per_user_table_name_for(node()) -> atom().

tracked_channel_per_user_table_name_for(Node) ->
    list_to_atom(rabbit_misc:format(
        "tracked_channel_table_per_user_on_node_~ts", [Node])).

ensure_tracked_tables_for_this_node() ->
    _ = ensure_tracked_channels_table_for_this_node_ets(),
    _ = ensure_per_user_tracked_channels_table_for_this_node_ets(),
    ok.

%% internal
ensure_tracked_channels_table_for_this_node() ->
    case rabbit_feature_flags:is_enabled(tracking_records_in_ets) of
        true ->
            ok;
        false ->
            ensure_tracked_channels_table_for_this_node_mnesia()
    end.

ensure_per_user_tracked_channels_table_for_node() ->
    case rabbit_feature_flags:is_enabled(tracking_records_in_ets) of
        true ->
            ok;
        false ->
            ensure_per_user_tracked_channels_table_for_this_node_mnesia()
    end.

%% Create tables
ensure_tracked_channels_table_for_this_node_ets() ->
    rabbit_log:info("Setting up a table for channel tracking on this node: ~tp",
                    [?TRACKED_CHANNEL_TABLE]),
    ets:new(?TRACKED_CHANNEL_TABLE, [named_table, public, {write_concurrency, true},
                                     {keypos, #tracked_channel.pid}]).

ensure_tracked_channels_table_for_this_node_mnesia() ->
    Node = node(),
    TableName = tracked_channel_table_name_for(Node),
    case mnesia:create_table(TableName, [{record_name, tracked_channel},
                                         {attributes, record_info(fields, tracked_channel)}]) of
        {atomic, ok}                   ->
            rabbit_log:info("Setting up a table for channel tracking on this node: ~tp",
                            [TableName]),
            ok;
        {aborted, {already_exists, _}} ->
            rabbit_log:info("Setting up a table for channel tracking on this node: ~tp",
                            [TableName]),
            ok;
        {aborted, Error}               ->
            rabbit_log:error("Failed to create a tracked channel table for node ~tp: ~tp", [Node, Error]),
            ok
    end.

ensure_per_user_tracked_channels_table_for_this_node_ets() ->
    rabbit_log:info("Setting up a table for channel tracking on this node: ~tp",
                    [?TRACKED_CHANNEL_TABLE_PER_USER]),
    ets:new(?TRACKED_CHANNEL_TABLE_PER_USER, [named_table, public, {write_concurrency, true}]).

ensure_per_user_tracked_channels_table_for_this_node_mnesia() ->
    Node = node(),
    TableName = tracked_channel_per_user_table_name_for(Node),
    case mnesia:create_table(TableName, [{record_name, tracked_channel_per_user},
                                         {attributes, record_info(fields, tracked_channel_per_user)}]) of
        {atomic, ok}                   ->
            rabbit_log:info("Setting up a table for channel tracking on this node: ~tp",
                            [TableName]),
            ok;
        {aborted, {already_exists, _}} ->
            rabbit_log:info("Setting up a table for channel tracking on this node: ~tp",
                            [TableName]),
            ok;
        {aborted, Error}               ->
            rabbit_log:error("Failed to create a per-user tracked channel table for node ~tp: ~tp", [Node, Error]),
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
    case rabbit_feature_flags:is_enabled(tracking_records_in_ets) of
        true -> get_tracked_channels_by_connection_pid_ets(ConnPid);
        false -> get_tracked_channels_by_connection_pid_mnesia(ConnPid)
    end.

get_tracked_channels_by_connection_pid_ets(ConnPid) ->
    rabbit_tracking:match_tracked_items_local(
        ?TRACKED_CHANNEL_TABLE,
        #tracked_channel{connection = ConnPid, _ = '_'}).

get_tracked_channels_by_connection_pid_mnesia(ConnPid) ->
    rabbit_tracking:match_tracked_items_mnesia(
        fun tracked_channel_table_name_for/1,
        #tracked_channel{connection = ConnPid, _ = '_'}).

get_tracked_channel_by_id_ets(ChId) ->
    rabbit_tracking:match_tracked_items_ets(
        ?TRACKED_CHANNEL_TABLE,
        #tracked_channel{id = ChId, _ = '_'}).

get_tracked_channel_by_pid_mnesia(ChPid) ->
    rabbit_tracking:match_tracked_items_mnesia(
        fun tracked_channel_table_name_for/1,
        #tracked_channel{pid = ChPid, _ = '_'}).

delete_tracked_channel_user_entry(Username) ->
    rabbit_tracking:delete_tracked_entry(
      {rabbit_auth_backend_internal, exists, [Username]},
      ?TRACKED_CHANNEL_TABLE_PER_USER,
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

migrate_tracking_records() ->
    Node = node(),
    rabbit_mnesia:execute_mnesia_transaction(
      fun () ->
              Table = tracked_channel_table_name_for(Node),
              _ = mnesia:lock({table, Table}, read),
              Channels = mnesia:select(Table, [{'$1',[],['$1']}]),
              lists:foreach(
                fun(Channel) ->
                        ets:insert(tracked_channel, Channel)
                end, Channels)
      end),
    rabbit_mnesia:execute_mnesia_transaction(
      fun () ->
              Table = tracked_channel_per_user_table_name_for(Node),
              _ = mnesia:lock({table, Table}, read),
              Channels = mnesia:select(Table, [{'$1',[],['$1']}]),
              lists:foreach(
                fun(#tracked_channel_per_user{channel_count = C,
                                              user = Username}) ->
                        ets:update_counter(tracked_channel_per_user, Username, C, {Username, 0})
                end, Channels)
      end).
