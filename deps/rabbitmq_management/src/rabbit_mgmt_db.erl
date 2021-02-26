%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2021 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(rabbit_mgmt_db).

-include_lib("rabbitmq_management_agent/include/rabbit_mgmt_records.hrl").
-include_lib("rabbitmq_management_agent/include/rabbit_mgmt_metrics.hrl").
-include_lib("rabbit_common/include/rabbit.hrl").
-include_lib("rabbit_common/include/rabbit_core_metrics.hrl").

-include("rabbit_mgmt.hrl").

-behaviour(gen_server2).

-export([start_link/0]).

-export([augment_exchanges/3, augment_queues/3,
         augment_nodes/2, augment_vhosts/2,
         get_channel/2, get_connection/2,
         get_all_channels/1, get_all_connections/1,
         get_all_consumers/0, get_all_consumers/1,
         get_overview/2, get_overview/1]).

-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2,
         code_change/3, handle_pre_hibernate/1,
         format_message_queue/2]).

-import(rabbit_misc, [pget/3]).

-type maybe_slide() :: exometer_slide:slide() | not_found.
-type slide_data() :: #{atom() => {maybe_slide(), maybe_slide()}}.
-type maybe_range() :: rabbit_mgmt_stats:maybe_range().
-type ranges() :: {maybe_range(), maybe_range(), maybe_range(), maybe_range()}.
-type mfargs() :: {module(), atom(), [any()]}.
-type lookup_key() :: atom() | {atom(), any()}.

-define(NO_RANGES, {no_range, no_range, no_range, no_range}).

-define(DEFAULT_TIMEOUT, 30000).

%% The management database responds to queries from the various
%% rabbit_mgmt_wm_* modules. It calls out to all rabbit nodes to fetch
%% node-local data and aggregates it before returning it. It uses a worker-
%% pool to provide a degree of parallelism.
%%
%% The management database reads metrics and stats written by the
%% rabbit_mgmt_metrics_collector(s).
%%
%% The metrics collectors (there is one for each stats table - see ?TABLES
%% in rabbit_mgmt_metrics.hrl) periodically read their corresponding core
%% metrics ETS tables and aggregate the data into the management specific ETS
%% tables.
%%
%% The metric collectors consume two types of core metrics: created (when an
%% object is created, containing immutable facts about it) and stats (emitted on
%% a timer, with mutable facts about the object). Deleted events are handled
%% by the rabbit_mgmt_metrics_gc process.
%% In this context "objects" means connections, channels, exchanges, queues,
%% consumers, vhosts and nodes. Note that we do not care about users,
%% permissions, bindings, parameters or policies.
%%
%% Connections and channels are identified by pids. Queues and
%% exchanges are identified by names (which are #resource{}s). VHosts
%% and nodes are identified by names which are binaries. And consumers
%% are identified by {ChPid, QName, CTag}.
%%
%% The management collectors records the "created" metrics for
%% connections, channels and consumers, and can thus be authoritative
%% about those objects. For queues, exchanges and nodes we go to
%% Mnesia to find out the immutable details of the objects.
%%
%% For everything other than consumers, the collectors can then augment
%% these immutable details with stats, as the object changes. (We
%% never emit anything very interesting about consumers).
%%
%% Stats on the inbound side are referred to as coarse and
%% fine-grained. Fine grained statistics are the message rates
%% maintained by channels and associated with tuples: {publishing
%% channel, exchange}, {publishing channel, exchange, queue} and
%% {queue, consuming channel}. Coarse grained stats are everything
%% else and are associated with only one object, not a tuple.
%%
%% Within the management database though we rearrange things a bit: we
%% refer to basic stats, simple stats and detail stats.
%%
%% Basic stats are those coarse grained stats for which we do not
%% retain a history and do not perform any calculations -
%% e.g. connection.state or channel.prefetch_count.
%%
%% Simple stats are those for which we do history / calculations which
%% are associated with one object *after aggregation* - so these might
%% originate with coarse grained stats - e.g. connection.send_oct or
%% queue.messages_ready. But they might also originate from fine
%% grained stats which have been aggregated - e.g. the message rates
%% for a vhost or queue.
%%
%% Finally, detailed stats are those for which we do history /
%% calculations which are associated with two objects. These
%% have to have originated as fine grained stats, but can still have
%% been aggregated.
%%
%% Created metrics and basic stats are stored in ETS tables by object.
%% Simple and detailed stats (which only differ depending on how
%% they're keyed) are stored in aggregated stats tables
%% (see rabbit_mgmt_stats.erl and include/rabbit_mgmt_metrics.hrl)
%%
%% Keys from simple and detailed stats are aggregated in several
%% records, stored in different ETS tables. We store a base counter
%% for everything that happened before the samples we have kept,
%% and a series of records which add the timestamp as part of the key.
%%
%% There is also a GC process to handle the deleted/closed
%% rabbit events to remove the corresponding objects from the aggregated
%% stats ETS tables.
%%
%% We also have an old_aggr_stats table to let us calculate instantaneous
%% rates, in order to apportion simple / detailed stats into time
%% slices as they come in. These instantaneous rates are not returned
%% in response to any query, the rates shown in the API are calculated
%% at query time. old_aggr_stats contains both coarse and fine
%% entries. Coarse entries are pruned when the corresponding object is
%% deleted, and fine entries are pruned when the emitting channel is
%% closed, and whenever we receive new fine stats from a channel. So
%% it's quite close to being a cache of "the previous stats we
%% received".
%%
%% Overall the object is to do some aggregation when metrics are read
%% and only aggregate metrics between nodes at query time.

%%----------------------------------------------------------------------------
%% API
%%----------------------------------------------------------------------------

start_link() ->
    gen_server2:start_link({local, ?MODULE}, ?MODULE, [], []).

augment_exchanges(Xs, Ranges, basic)    ->
   submit(fun(Interval) -> list_exchange_stats(Ranges, Xs, Interval) end);
augment_exchanges(Xs, Ranges, _)    ->
   submit(fun(Interval) -> detail_exchange_stats(Ranges, Xs, Interval) end).

%% we can only cache if no ranges are requested.
%% The mgmt ui doesn't use ranges for queue listings
-spec augment_queues([proplists:proplist()], ranges(), basic | full) -> any().
augment_queues(Qs, ?NO_RANGES = Ranges, basic)    ->
   submit_cached(queues,
                 fun(Interval, Queues) ->
                         list_queue_stats(Ranges, Queues, Interval)
                 end, Qs, max(60000, length(Qs) * 2));
augment_queues(Qs, Ranges, basic)    ->
   submit(fun(Interval) -> list_queue_stats(Ranges, Qs, Interval) end);
augment_queues(Qs, Ranges, _)    ->
   submit(fun(Interval) -> detail_queue_stats(Ranges, Qs, Interval) end).

augment_vhosts(VHosts, Ranges)   ->
    submit(fun(Interval) -> vhost_stats(Ranges, VHosts, Interval) end).

augment_nodes(Nodes, Ranges) ->
    submit(fun(Interval) -> node_stats(Ranges, Nodes, Interval) end).

get_channel(Name, Ranges) ->
    submit(fun(Interval) ->
                   case created_stats_delegated(Name, channel_created_stats) of
                        not_found -> not_found;
                        Ch -> [Result] =
                              detail_channel_stats(Ranges, [Ch], Interval),
                              Result
                    end
           end).

get_connection(Name, Ranges) ->
    submit(fun(Interval) ->
                   case created_stats_delegated(Name, connection_created_stats) of
                        not_found -> not_found;
                        C ->
                            [Result] = connection_stats(Ranges, [C], Interval),
                            Result
                   end
           end).

get_all_channels(?NO_RANGES = Ranges) ->
    submit_cached(channels,
                  fun(Interval) ->
                           Chans = created_stats_delegated(channel_created_stats),
                           list_channel_stats(Ranges, Chans, Interval)
                  end);
get_all_channels(Ranges) ->
    submit(fun(Interval) ->
                   Chans = created_stats_delegated(channel_created_stats),
                   list_channel_stats(Ranges, Chans, Interval)
           end).

get_all_connections(?NO_RANGES = Ranges) ->
    submit_cached(connections,
                  fun(Interval) ->
                          Chans = created_stats_delegated(connection_created_stats),
                          connection_stats(Ranges, Chans, Interval)
                  end);
get_all_connections(Ranges) ->
    submit(fun(Interval) ->
                   Chans = created_stats_delegated(connection_created_stats),
                   connection_stats(Ranges, Chans, Interval)
           end).

get_all_consumers() -> get_all_consumers(all).
get_all_consumers(VHosts) ->
    submit(fun(_Interval) -> consumers_stats(VHosts) end).

get_overview(Ranges) -> get_overview(all, Ranges).
get_overview(User, Ranges) ->
    submit(fun(Interval) -> overview(User, Ranges, Interval) end).

%%----------------------------------------------------------------------------
%% Internal, gen_server2 callbacks
%%----------------------------------------------------------------------------

-record(state, {interval}).

init([]) ->
    {ok, Interval} = application:get_env(rabbit, collect_statistics_interval),
    rabbit_log:info("Statistics database started."),
    {ok, #state{interval = Interval}, hibernate,
     {backoff, ?HIBERNATE_AFTER_MIN, ?HIBERNATE_AFTER_MIN, ?DESIRED_HIBERNATE}}.

handle_call(_Request, _From, State) ->
    reply(not_understood, State).

handle_cast(_Request, State) ->
    noreply(State).

handle_info(_Info, State) ->
    noreply(State).

terminate(_Arg, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

reply(Reply, NewState) -> {reply, Reply, NewState, hibernate}.
noreply(NewState) -> {noreply, NewState, hibernate}.

handle_pre_hibernate(State) ->
    %% rabbit_event can end up holding on to some memory after a busy
    %% workout, but it's not a gen_server so we can't make it
    %% hibernate. The best we can do is forcibly GC it here (if
    %% rabbit_mgmt_db is hibernating the odds are rabbit_event is
    %% quiescing in some way too).
    _ = rpc:multicall(
      rabbit_nodes:all_running(), rabbit_mgmt_db_handler, gc, []),
    {hibernate, State}.

format_message_queue(Opt, MQ) -> rabbit_misc:format_message_queue(Opt, MQ).

%%----------------------------------------------------------------------------
%% Internal, utilities
%%----------------------------------------------------------------------------

pget(Key, List) -> pget(Key, List, unknown).

-type id_name() :: 'name' | 'route' | 'pid'.
-spec id_name(atom()) -> id_name().

%% id_name() and id() are for use when handling events, id_lookup()
%% for when augmenting. The difference is that when handling events a
%% queue name will be a resource, but when augmenting we will be
%% passed a queue proplist that will already have been formatted -
%% i.e. it will have name and vhost keys.
id_name(node_stats)       -> name;
id_name(node_node_stats)  -> route;
id_name(vhost_stats)      -> name;
id_name(queue_stats)      -> name;
id_name(exchange_stats)   -> name;
id_name(channel_stats)    -> pid;
id_name(connection_stats) -> pid.

id(Type, List) -> pget(id_name(Type), List).

id_lookup(queue_stats, List) ->
    rabbit_misc:r(pget(vhost, List), queue, pget(name, List));
id_lookup(exchange_stats, List) ->
    rabbit_misc:r(pget(vhost, List), exchange, pget(name, List));
id_lookup(Type, List) ->
    id(Type, List).


%%----------------------------------------------------------------------------
%% Internal, querying side api
%%----------------------------------------------------------------------------

overview(User, Ranges, Interval) ->
    VHosts = case User of
                 all -> rabbit_vhost:list_names();
                 _   -> rabbit_mgmt_util:list_visible_vhosts_names(User)
             end,
    DataLookup = get_data_from_nodes({rabbit_mgmt_data, overview_data,
                                      [User, Ranges, VHosts]}),
    MessageStats = lists:append(
             [format_range(DataLookup, vhost_stats_fine_stats,
                           pick_range(fine_stats, Ranges), Interval),
              format_range(DataLookup, vhost_msg_rates,
                           pick_range(queue_msg_rates, Ranges), Interval),
              format_range(DataLookup, vhost_stats_deliver_stats,
                           pick_range(deliver_get, Ranges), Interval)]),

    ChurnRates = format_range(DataLookup, connection_churn_rates,
                              pick_range(queue_msg_counts, Ranges), Interval),
    QueueStats = format_range(DataLookup, vhost_msg_stats,
                              pick_range(queue_msg_counts, Ranges), Interval),
    %% Filtering out the user's consumers would be rather expensive so let's
    %% just not show it
    Consumers = case User of
                    all -> [{consumers, maps:get(consumers_count, DataLookup)}];
                    _   -> []
                end,
    ObjectTotals = Consumers ++
        [{queues, length([Q || V <- VHosts, Q <- rabbit_amqqueue:list(V)])},
         {exchanges, length([X || V <- VHosts, X <- rabbit_exchange:list(V)])},
         {connections, maps:get(connections_count, DataLookup)},
         {channels, maps:get(channels_count, DataLookup)}],

    [{message_stats, MessageStats},
     {churn_rates, ChurnRates},
     {queue_totals,  QueueStats},
     {object_totals, ObjectTotals},
     {statistics_db_event_queue, event_queue()}]. % TODO: event queue?

event_queue() ->
    lists:foldl(fun ({T, _}, Sum) ->
                    case whereis(rabbit_mgmt_metrics_collector:name(T)) of
                        P when is_pid(P) ->
                            {message_queue_len, Len} =
                                erlang:process_info(P, message_queue_len),
                                Sum + Len;
                        _ -> Sum
                    end
                end, 0, ?CORE_TABLES).

consumers_stats(VHost) ->
    Data =  get_data_from_nodes({rabbit_mgmt_data, consumer_data, [VHost]}),
    Consumers = rabbit_mgmt_data_compat:fill_consumer_active_fields(
                  [V || {_,V} <- maps:to_list(Data)]),
    ChPids = [ pget(channel_pid, Con)
               || Con <- Consumers, [] =:= pget(channel_details, Con)],
    ChDets = get_channel_detail_lookup(ChPids),
    [merge_channel_into_obj(Con, ChDets) || Con <- Consumers].

-spec list_queue_stats(ranges(), [proplists:proplist()], integer()) ->
    [proplists:proplist()].
list_queue_stats(Ranges, Objs, Interval) ->
    Ids = [id_lookup(queue_stats, Obj) || Obj <- Objs],
    DataLookup = get_data_from_nodes({rabbit_mgmt_data, all_list_queue_data, [Ids, Ranges]}),
    adjust_hibernated_memory_use(
      [begin
       Id = id_lookup(queue_stats, Obj),
       Pid = pget(pid, Obj),
       QueueData = maps:get(Id, DataLookup),
       Props = maps:get(queue_stats, QueueData),
       Stats = queue_stats(QueueData, Ranges, Interval),
       {Pid, combine(Props, Obj) ++ Stats}
       end || Obj <- Objs]).

detail_queue_stats(Ranges, Objs, Interval) ->
    Ids = [id_lookup(queue_stats, Obj) || Obj <- Objs],
    DataLookup = get_data_from_nodes({rabbit_mgmt_data, all_detail_queue_data,
                                      [Ids, Ranges]}),

    QueueStats = adjust_hibernated_memory_use(
      [begin
       Id = id_lookup(queue_stats, Obj),
       Pid = pget(pid, Obj),
       QueueData = maps:get(Id, DataLookup),
       Props = maps:get(queue_stats, QueueData),
       Stats = queue_stats(QueueData, Ranges, Interval),
       ConsumerStats = rabbit_mgmt_data_compat:fill_consumer_active_fields(
                         maps:get(consumer_stats, QueueData)),
       Consumers = [{consumer_details, ConsumerStats}],
       StatsD = [{deliveries,
                  detail_stats(QueueData, channel_queue_stats_deliver_stats,
                               deliver_get, second(Id), Ranges, Interval)},
                 {incoming,
                  detail_stats(QueueData, queue_exchange_stats_publish,
                               fine_stats, first(Id), Ranges, Interval)}],
       Details = augment_details(Obj, []),
       {Pid, combine(Props, Obj) ++ Stats ++ StatsD ++ Consumers ++ Details}
       end || Obj <- Objs]),

   % patch up missing channel details
   ChPids = lists:usort(get_pids_for_missing_channel_details(QueueStats)),
   ChDets = get_channel_detail_lookup(ChPids),
   Merged = merge_channel_details(QueueStats, ChDets),
   Merged.

node_node_stats(Lookup, Node, Ranges, Interval) ->
    LocalNodeNodeMetrics = maps:from_list(ets:tab2list(node_node_metrics)),
    RemoteNodeNodeMetrics = maps:get(node_node_metrics, Lookup),
    NodeNodeMetrics = maps:merge(LocalNodeNodeMetrics, RemoteNodeNodeMetrics),
    node_node_stats(Lookup, Node, Ranges, Interval, NodeNodeMetrics).

node_node_stats(Lookup, Node, Ranges, Interval, NodeNodeMetrics) ->
    Table = node_node_coarse_stats,
    Type = coarse_node_node_stats,
    [begin
        {Stats, DetailId} = get_detail_stats(Key, Lookup, Table, Type,
                                             first(Node), Ranges, Interval),
        NodeMetrics = maybe_fetch_value(Key, NodeNodeMetrics),
        lists:flatten([{stats, Stats}, DetailId, NodeMetrics])
     end || {{T, Key}, _} <- maps:to_list(Lookup), T =:= Table].

detail_stats(Lookup, Table, Type, Id, Ranges, Interval) ->
    [begin
        {Stats, DetailId} = get_detail_stats(Key, Lookup, Table, Type,
                                             Id, Ranges, Interval),
        [{stats, Stats}|DetailId] %TODO: not actually delegated
     end || {{T, Key}, _} <- maps:to_list(Lookup), T =:= Table].

get_detail_stats(Key, Lookup, Table, Type, Id, Ranges, Interval) ->
    Range = pick_range(Type, Ranges),
    Stats = format_range(Lookup, {Table, Key}, Range, Interval),
    DetailId = format_detail_id(revert(Id, Key)),
    {Stats, DetailId}.

queue_stats(QueueData, Ranges, Interval) ->
   message_stats(format_range(QueueData, queue_stats_publish,
                              pick_range(fine_stats, Ranges), Interval) ++
                 format_range(QueueData, queue_stats_deliver_stats,
                              pick_range(deliver_get, Ranges), Interval)) ++
   format_range(QueueData, queue_process_stats,
                pick_range(process_stats, Ranges), Interval) ++
   format_range(QueueData, queue_msg_stats,
                pick_range(queue_msg_counts, Ranges), Interval).

channel_stats(ChannelData, Ranges, Interval) ->
   message_stats(format_range(ChannelData, channel_stats_fine_stats,
                              pick_range(fine_stats, Ranges), Interval) ++
                 format_range(ChannelData, channel_stats_deliver_stats,
                              pick_range(deliver_get, Ranges), Interval)) ++
   format_range(ChannelData, channel_process_stats,
                pick_range(process_stats, Ranges), Interval).

-spec format_range(slide_data(), lookup_key(), maybe_range(), non_neg_integer()) ->
    proplists:proplist().
format_range(Data, Key, Range0, Interval) ->
   Table = case Key of
               {T, _} -> T;
               T -> T
           end,
   InstantRateFun = fun() -> fetch_slides(1, Key, Data) end,
   SamplesFun = fun() -> fetch_slides(2, Key, Data) end,
   Now = exometer_slide:timestamp(),
   rabbit_mgmt_stats:format_range(Range0, Now, Table, Interval, InstantRateFun,
                                  SamplesFun).

%% basic.get-empty metric
fetch_slides(Ele, Key, Data)
  when Key =:= channel_queue_stats_deliver_stats orelse
       Key =:= channel_stats_deliver_stats orelse
       Key =:= queue_stats_deliver_stats orelse
       Key =:= vhost_stats_deliver_stats orelse
       (is_tuple(Key) andalso
        (element(1, Key) =:= channel_queue_stats_deliver_stats orelse
         element(1, Key) =:= channel_stats_deliver_stats orelse
         element(1, Key) =:= queue_stats_deliver_stats orelse
         element(1, Key) =:= vhost_stats_deliver_stats)) ->
    case element(Ele, maps:get(Key, Data)) of
        not_found -> [];
        Slides when is_list(Slides) ->
            [rabbit_mgmt_data_compat:fill_get_empty_queue_metric(S)
             || S <- Slides, not_found =/= S];
        Slide ->
            [rabbit_mgmt_data_compat:fill_get_empty_queue_metric(Slide)]
    end;
%% drop_unroutable metric
fetch_slides(Ele, Key, Data)
  when Key =:= channel_stats_fine_stats orelse
       Key =:= channel_exchange_stats_fine_stats orelse
       Key =:= vhost_stats_fine_stats orelse
       (is_tuple(Key) andalso
        (element(1, Key) =:= channel_stats_fine_stats orelse
         element(1, Key) =:= channel_exchange_stats_fine_stats orelse
         element(1, Key) =:= vhost_stats_fine_stats)) ->
    case element(Ele, maps:get(Key, Data)) of
        not_found -> [];
        Slides when is_list(Slides) ->
            [rabbit_mgmt_data_compat:fill_drop_unroutable_metric(S)
             || S <- Slides, not_found =/= S];
        Slide ->
            [rabbit_mgmt_data_compat:fill_drop_unroutable_metric(Slide)]
    end;
fetch_slides(Ele, Key, Data) ->
    case element(Ele, maps:get(Key, Data)) of
        not_found -> [];
        Slides when is_list(Slides) ->
            [S || S <- Slides, not_found =/= S];
        Slide ->
            [Slide]
    end.

get_channel_detail_lookup(ChPids) ->
   ChDets = delegate_invoke({rabbit_mgmt_data, augment_channel_pids, [ChPids]}),
   maps:from_list([{pget(pid, C), C} || [_|_] = C <- lists:append(ChDets)]).

merge_channel_details(QueueStats, Lookup) ->
    [begin
          Cons = pget(consumer_details, QueueStat),
          Cons1 = [merge_channel_into_obj(Con, Lookup) || Con <- Cons],
          rabbit_misc:pset(consumer_details, Cons1, QueueStat)
     end || QueueStat <- QueueStats].

merge_channel_into_obj(Obj, ChDet) ->
    case pget(channel_details, Obj) of
        [] -> case maps:find(pget(channel_pid, Obj), ChDet) of
                  {ok, CHd} ->
                      rabbit_misc:pset(channel_details, CHd, Obj);
                  error ->
                      Obj
              end;
        _ -> Obj
    end.

get_pids_for_missing_channel_details(QueueStats) ->
   CDs = lists:append([pget(consumer_details, QueueStat) || QueueStat <- QueueStats]),
   [ pget(channel_pid, CD) || CD <- CDs, [] =:= pget(channel_details, CD)].


list_exchange_stats(Ranges, Objs, Interval) ->
    Ids = [id_lookup(exchange_stats, Obj) || Obj <- Objs],
    DataLookup = get_data_from_nodes({rabbit_mgmt_data, all_exchange_data, [Ids, Ranges]}),
    [begin
     Id = id_lookup(exchange_stats, Obj),
     ExData = maps:get(Id, DataLookup),
     Stats = message_stats(format_range(ExData, exchange_stats_publish_out,
                                        pick_range(fine_stats, Ranges), Interval) ++
                           format_range(ExData,  exchange_stats_publish_in,
                                        pick_range(deliver_get, Ranges), Interval)),
     Obj ++ Stats
     end || Obj <- Objs].

detail_exchange_stats(Ranges, Objs, Interval) ->
    Ids = [id_lookup(exchange_stats, Obj) || Obj <- Objs],
    DataLookup = get_data_from_nodes({rabbit_mgmt_data, all_exchange_data, [Ids, Ranges]}),
    [begin
     Id = id_lookup(exchange_stats, Obj),
     ExData = maps:get(Id, DataLookup),
     Stats = message_stats(format_range(ExData, exchange_stats_publish_out,
                                        pick_range(fine_stats, Ranges), Interval) ++
                           format_range(ExData,  exchange_stats_publish_in,
                                        pick_range(deliver_get, Ranges), Interval)),
     StatsD = [{incoming,
                detail_stats(ExData, channel_exchange_stats_fine_stats,
                                     fine_stats, second(Id), Ranges, Interval)},
               {outgoing,
                detail_stats(ExData, queue_exchange_stats_publish,
                                     fine_stats, second(Id), Ranges, Interval)}],
     %% remove live state? not sure it has!
     Obj ++ StatsD ++ Stats
     end || Obj <- Objs].

connection_stats(Ranges, Objs, Interval) ->
    Ids = [id_lookup(connection_stats, Obj) || Obj <- Objs],
    DataLookup = get_data_from_nodes({rabbit_mgmt_data, all_connection_data, [Ids, Ranges]}),
    [begin
     Id = id_lookup(connection_stats, Obj),
     ConnData = maps:get(Id, DataLookup),
     Props = maps:get(connection_stats, ConnData),
     Stats = format_range(ConnData, connection_stats_coarse_conn_stats,
                          pick_range(coarse_conn_stats, Ranges), Interval),
     Details = augment_details(Obj, []), % TODO: not delegated
     combine(Props, Obj) ++ Details ++ Stats
     end || Obj <- Objs].

list_channel_stats(Ranges, Objs, Interval) ->
    Ids = [id_lookup(channel_stats, Obj) || Obj <- Objs],
    DataLookup = get_data_from_nodes({rabbit_mgmt_data, all_list_channel_data, [Ids, Ranges]}),
    ChannelStats =
        [begin
         Id = id_lookup(channel_stats, Obj),
         ChannelData = maps:get(Id, DataLookup),
         Props = maps:get(channel_stats, ChannelData),
         Stats = channel_stats(ChannelData, Ranges, Interval),
         combine(Props, Obj) ++ Stats
         end || Obj <- Objs],
    ChannelStats.

detail_channel_stats(Ranges, Objs, Interval) ->
    Ids = [id_lookup(channel_stats, Obj) || Obj <- Objs],
    DataLookup = get_data_from_nodes({rabbit_mgmt_data, all_detail_channel_data,
                                      [Ids, Ranges]}),
    ChannelStats =
        [begin
         Id = id_lookup(channel_stats, Obj),
         ChannelData = maps:get(Id, DataLookup),
         Props = maps:get(channel_stats, ChannelData),
         Stats = channel_stats(ChannelData, Ranges, Interval),
         ConsumerStats = rabbit_mgmt_data_compat:fill_consumer_active_fields(
                           maps:get(consumer_stats, ChannelData)),
         Consumers = [{consumer_details, ConsumerStats}],
         StatsD = [{publishes,
                    detail_stats(ChannelData, channel_exchange_stats_fine_stats,
                                 fine_stats, first(Id), Ranges, Interval)},
                   {deliveries,
                    detail_stats(ChannelData, channel_queue_stats_deliver_stats,
                                 fine_stats, first(Id), Ranges, Interval)}],
         combine(Props, Obj) ++ Consumers ++ Stats ++ StatsD
         end || Obj <- Objs],
     rabbit_mgmt_format:strip_pids(ChannelStats).

vhost_stats(Ranges, Objs, Interval) ->
    Ids = [id_lookup(vhost_stats, Obj) || Obj <- Objs],
    DataLookup = get_data_from_nodes({rabbit_mgmt_data,  all_vhost_data, [Ids, Ranges]}),
    [begin
     Id = id_lookup(vhost_stats, Obj),
     VData = maps:get(Id, DataLookup),
     Stats = format_range(VData, vhost_stats_coarse_conn_stats,
                          pick_range(coarse_conn_stats, Ranges), Interval) ++
             format_range(VData, vhost_msg_stats,
                         pick_range(queue_msg_rates, Ranges), Interval),
     StatsD = message_stats(format_range(VData, vhost_stats_fine_stats,
                                         pick_range(fine_stats, Ranges), Interval) ++
                            format_range(VData, vhost_stats_deliver_stats,
                                         pick_range(deliver_get, Ranges), Interval)),
     Details = augment_details(Obj, []),
     Obj ++ Details ++ Stats ++ StatsD
     end || Obj <- Objs].

node_stats(Ranges, Objs, Interval) ->
    Ids = [id_lookup(node_stats, Obj) || Obj <- Objs],
    DataLookup = get_data_from_nodes({rabbit_mgmt_data, all_node_data, [Ids, Ranges]}),
    [begin
     Id = id_lookup(node_stats, Obj),
     NData = maps:get(Id, DataLookup),
     Props = maps:get(node_stats, NData),
     Stats = format_range(NData, node_coarse_stats,
                          pick_range(coarse_node_stats, Ranges), Interval) ++
             format_range(NData, node_persister_stats,
                          pick_range(coarse_node_stats, Ranges), Interval) ++
             format_range(NData, connection_churn_rates,
                          pick_range(churn_rates, Ranges), Interval),
     NodeNodeStats = node_node_stats(NData, Id, Ranges, Interval),
     StatsD = [{cluster_links, NodeNodeStats}],
     MgmtStats = maps:get(mgmt_stats, NData),
     Details = augment_details(Obj, []), % augmentation needs to be node local
     combine(Props, Obj) ++ Details ++ Stats ++ StatsD ++ MgmtStats
     end || Obj <- Objs].

combine(New, Old) ->
    case pget(state, Old) of
        unknown -> New ++ Old;
        live    -> New ++ lists:keydelete(state, 1, Old);
        _       -> lists:keydelete(state, 1, New) ++ Old
    end.

revert({'_', _}, {Id, _}) ->
    Id;
revert({_, '_'}, {_, Id}) ->
    Id.

%%----------------------------------------------------------------------------
%% Internal, delegated operations
%%----------------------------------------------------------------------------

-spec get_data_from_nodes(mfargs()) -> #{atom() => any()}.
get_data_from_nodes(MFA) ->
    Data = delegate_invoke(MFA),
    lists:foldl(fun(D, Agg) ->
                        maps_merge(fun merge_data/3, D, Agg)
                end, #{}, Data).

maps_merge(Fun, M1, M2) ->
    JustM2 = maps:without(maps:keys(M1), M2),
    maps:merge(JustM2,
               maps:map(fun(K, V1) ->
                            case maps:find(K, M2) of
                                {ok, V2} -> Fun(K, V1, V2);
                                error    -> V1
                            end
                        end,
                        M1)).

-spec merge_data(atom(), any(), any()) -> any().
merge_data(_, A, B) when is_integer(A), is_integer(B) -> A + B;
merge_data(_, A, B) when is_list(A), is_list(B) ->
    A ++ B;
merge_data(_, {A1, B1}, {[_|_] = A2, [_|_] = B2}) ->
    {[A1 | A2], [B1 | B2]};
merge_data(_, {A1, B1}, {A2, B2}) -> % first slide
    {[A1, A2], [B1, B2]};
merge_data(_, D1, D2) -> % we assume if we get here both values a maps
   try
       maps_merge(fun merge_data/3, D1, D2)
   catch
       error:Err ->
           rabbit_log:debug("merge_data err ~p got: ~p ~p ~n", [Err, D1, D2]),
           case is_map(D1) of
               true -> D1;
               false -> D2
           end
   end.

%% We do this when retrieving the queue record rather than when
%% storing it since the memory use will drop *after* we find out about
%% hibernation, so to do it when we receive a queue stats event would
%% be fiddly and racy. This should be quite cheap though.
adjust_hibernated_memory_use(Qs) ->
    Pids = [Pid || {Pid, Q} <- Qs, pget(idle_since, Q, not_idle) =/= not_idle],
    %% We use delegate here not for ordering reasons but because we
    %% want to get the right amount of parallelism and minimise
    %% cross-cluster communication.
    {Mem, _BadNodes} = delegate:invoke(Pids, {erlang, process_info, [memory]}),
    MemDict = maps:from_list([{P, M} || {P, M = {memory, _}} <- Mem]),
    [case maps:find(Pid, MemDict) of
         error        -> Q;
         {ok, Memory} -> [Memory | proplists:delete(memory, Q)]
     end || {Pid, Q} <- Qs].

-spec created_stats_delegated(any(), fun((any()) -> any()) | atom()) -> not_found | any().
created_stats_delegated(Key, Type) ->
    Data = delegate_invoke({rabbit_mgmt_data, augmented_created_stats, [Key, Type]}),
    case [X || X <- Data, X =/= not_found] of
        [] -> not_found;
        [X] -> X
    end.

created_stats_delegated(Type) ->
    lists:append(
      delegate_invoke({rabbit_mgmt_data, augmented_created_stats, [Type]})).

-spec delegate_invoke(mfargs()) -> [any()].
delegate_invoke(FunOrMFA) ->
    MemberPids = [P || P <- pg:get_members(?MANAGEMENT_PG_SCOPE, management_db)],
    {Results, Errors} = delegate:invoke(MemberPids, ?DELEGATE_PREFIX, FunOrMFA),
    case Errors of
        [] -> ok;
        _ -> rabbit_log:warning("Management delegate query returned errors:~n~p", [Errors])
    end,
    [R || {_, R} <- Results].

submit(Fun) ->
    {ok, Interval} = application:get_env(rabbit, collect_statistics_interval),
    worker_pool:submit(management_worker_pool, fun() -> Fun(Interval) end, reuse).

submit_cached(Key, Fun) ->
    {ok, Interval} = application:get_env(rabbit, collect_statistics_interval),
    {ok, Res} = rabbit_mgmt_db_cache:fetch(Key, fun() -> Fun(Interval) end),
    Res.

submit_cached(Key, Fun, Arg, Timeout) ->
    {ok, Interval} = application:get_env(rabbit, collect_statistics_interval),
    {ok, Res} = rabbit_mgmt_db_cache:fetch(Key,
                                           fun(A) -> Fun(Interval, A) end,
                                           [Arg], Timeout),
    Res.

%% Note: Assumes Key is a two-tuple.
%% If not found at first, Key is reversed and tried again.
%% Seems to be necessary based on node_node_metrics table keys
%% and Key values in Lookup
maybe_fetch_value(Key, Dict) ->
    maybe_fetch_value(maps:is_key(Key, Dict), go, Key, Dict).

maybe_fetch_value(true, _Cont, Key, Dict) ->
    maps:get(Key, Dict);
maybe_fetch_value(false, stop, _Key, _Dict) ->
    [];
maybe_fetch_value(false, go, Key, Dict) ->
    Key2 = reverse_key(Key),
    maybe_fetch_value(maps:is_key(Key2, Dict), stop, Key2, Dict).

message_stats([]) ->
    [];
message_stats(Stats) ->
    [{message_stats, Stats}].

pick_range(Table, Ranges) ->
    rabbit_mgmt_data:pick_range(Table, Ranges).

first(Id)  ->
    {Id, '_'}.

second(Id) ->
    {'_', Id}.

reverse_key({K1, K2}) ->
    {K2, K1}.

augment_details(Obj, Acc) ->
    rabbit_mgmt_data:augment_details(Obj, Acc).

format_detail_id(ChPid) when is_pid(ChPid) ->
    augment_details([{channel, ChPid}], []);
format_detail_id(#resource{name = Name, virtual_host = Vhost, kind = Kind}) ->
    [{Kind, [{name, Name}, {vhost, Vhost}]}];
format_detail_id(Node) when is_atom(Node) ->
    [{name, Node}].
