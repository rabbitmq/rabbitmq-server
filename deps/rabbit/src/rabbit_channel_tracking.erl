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

-export([update_tracked/1,
         handle_cast/1,
         register_tracked/1,
         unregister_tracked/1,
         count_tracked_items_in/1,
         shutdown_tracked_items/2]).

-export([list/0, list_of_user/1, list_on_node/1,
         tracked_channel_table_name_for/1,
         tracked_channel_per_user_table_name_for/1,
         ensure_tracked_tables_for_this_node/0,
         delete_tracked_channel_user_entry/1]).

-export([count_local_tracked_items_of_user/1]).

-include_lib("rabbit_common/include/rabbit.hrl").

-import(rabbit_misc, [pget/2]).

-define(TRACKED_CHANNEL_TABLE, tracked_channel).
-define(TRACKED_CHANNEL_TABLE_PER_USER, tracked_channel_per_user).

%%
%% API
%%

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
    ok.

-spec register_tracked(rabbit_types:tracked_channel()) -> ok.
-dialyzer([{nowarn_function, [register_tracked/1]}]).

register_tracked(TrackedCh = #tracked_channel{pid = ChPid, username = Username,
                                              node = Node}) when Node == node() ->
    case ets:lookup(?TRACKED_CHANNEL_TABLE, ChPid) of
        []    ->
            ets:insert(?TRACKED_CHANNEL_TABLE, TrackedCh),
            ets:update_counter(?TRACKED_CHANNEL_TABLE_PER_USER, Username, 1, {Username, 0}),
            ok;
        [#tracked_channel{}] ->
            ok
    end,
    ok.

-spec unregister_tracked_by_pid(pid()) -> any().
unregister_tracked_by_pid(ChPid) when node(ChPid) == node() ->
    case ets:lookup(?TRACKED_CHANNEL_TABLE, ChPid) of
        []     -> ok;
        [#tracked_channel{username = Username}] ->
            ets:update_counter(?TRACKED_CHANNEL_TABLE_PER_USER, Username, -1),
            ets:delete(?TRACKED_CHANNEL_TABLE, ChPid)
    end.

%% @doc This function is exported and implements a rabbit_tracking
%% callback, however it is not used in rabbitmq-server any more. It is
%% only kept for backwards compatibility if 3rd-party code would rely
%% on it.
-spec unregister_tracked(rabbit_types:tracked_channel_id()) -> ok.
unregister_tracked(ChId = {Node, _Name}) when Node == node() ->
    case get_tracked_channel_by_id(ChId) of
        []     -> ok;
        [#tracked_channel{pid = ChPid, username = Username}] ->
            ets:update_counter(?TRACKED_CHANNEL_TABLE_PER_USER, Username, -1),
            ets:delete(?TRACKED_CHANNEL_TABLE, ChPid)
    end.

-spec count_tracked_items_in({atom(), rabbit_types:username()}) -> non_neg_integer().

count_tracked_items_in({user, Username}) ->
    rabbit_tracking:count_on_all_nodes(
      ?MODULE, count_local_tracked_items_of_user, [Username],
      ["channels of user ", Username]).

-spec count_local_tracked_items_of_user(rabbit_types:username()) -> non_neg_integer().
count_local_tracked_items_of_user(Username) ->
    rabbit_tracking:read_ets_counter(?TRACKED_CHANNEL_TABLE_PER_USER, Username).

-spec shutdown_tracked_items(list(), term()) -> ok.

shutdown_tracked_items(TrackedItems, _Args) ->
    close_channels(TrackedItems).

%% helper functions
-spec list() -> [rabbit_types:tracked_channel()].

list() ->
    lists:foldl(
      fun (Node, Acc) ->
              Acc ++ list_on_node(Node)
      end, [], rabbit_nodes:list_running()).

-spec list_of_user(rabbit_types:username()) -> [rabbit_types:tracked_channel()].

list_of_user(Username) ->
    rabbit_tracking:match_tracked_items(
      ?TRACKED_CHANNEL_TABLE,
      #tracked_channel{username = Username, _ = '_'}).

-spec list_on_node(node()) -> [rabbit_types:tracked_channel()].
list_on_node(Node) when Node == node() ->
    ets:tab2list(?TRACKED_CHANNEL_TABLE);
list_on_node(Node) ->
    case rabbit_misc:rpc_call(Node, ?MODULE, list_on_node, [Node]) of
        List when is_list(List) ->
            List;
        _ ->
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
    _ = ensure_tracked_channels_table_for_this_node(),
    _ = ensure_per_user_tracked_channels_table_for_this_node(),
    ok.

%% Create tables
ensure_tracked_channels_table_for_this_node() ->
    rabbit_log:info("Setting up a table for channel tracking on this node: ~tp",
                    [?TRACKED_CHANNEL_TABLE]),
    ets:new(?TRACKED_CHANNEL_TABLE, [named_table, public, {write_concurrency, true},
                                     {keypos, #tracked_channel.pid}]).

ensure_per_user_tracked_channels_table_for_this_node() ->
    rabbit_log:info("Setting up a table for channel tracking on this node: ~tp",
                    [?TRACKED_CHANNEL_TABLE_PER_USER]),
    ets:new(?TRACKED_CHANNEL_TABLE_PER_USER, [named_table, public, {write_concurrency, true}]).

get_tracked_channels_by_connection_pid(ConnPid) ->
    rabbit_tracking:match_tracked_items_local(
      ?TRACKED_CHANNEL_TABLE,
      #tracked_channel{connection = ConnPid, _ = '_'}).

get_tracked_channel_by_id(ChId) ->
    rabbit_tracking:match_tracked_items(
        ?TRACKED_CHANNEL_TABLE,
        #tracked_channel{id = ChId, _ = '_'}).

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
