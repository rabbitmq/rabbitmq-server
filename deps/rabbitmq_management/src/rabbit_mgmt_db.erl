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
%%   The Original Code is RabbitMQ Management Plugin.
%%
%%   The Initial Developer of the Original Code is GoPivotal, Inc.
%%   Copyright (c) 2010-2013 GoPivotal, Inc.  All rights reserved.
%%

-module(rabbit_mgmt_db).

-include("rabbit_mgmt.hrl").
-include_lib("rabbit_common/include/rabbit.hrl").

-behaviour(gen_server2).

-export([start_link/0]).

-export([augment_exchanges/3, augment_queues/3,
         augment_nodes/1, augment_vhosts/2,
         get_channel/2, get_connection/2,
         get_all_channels/1, get_all_connections/1,
         get_overview/2, get_overview/1]).

-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2,
         code_change/3, handle_pre_hibernate/1, prioritise_cast/3,
         format_message_queue/2]).

%% For testing
-export([override_lookups/1, reset_lookups/0]).

-import(rabbit_misc, [pget/3, pset/3]).

%% The management database listens to events broadcast via the
%% rabbit_event mechanism, and responds to queries from the various
%% rabbit_mgmt_wm_* modules. It handles several kinds of events, and
%% slices and dices them in various ways.
%%
%% There are three types of events coming in: created (when an object
%% is created, containing immutable facts about it), stats (emitted on
%% a timer, with mutable facts about the object), and deleted (just
%% containing the object's ID). In this context "objects" means
%% connections, channels, exchanges, queues, consumers, vhosts and
%% nodes. Note that we do not care about users, permissions, bindings,
%% parameters or policies.
%%
%% Connections and channels are identified by pids. Queues and
%% exchanges are identified by names (which are #resource{}s). VHosts
%% and nodes are identified by names which are binaries. And consumers
%% are identified by {ChPid, QName, CTag}.
%%
%% The management database records the "created" events for
%% connections, channels and consumers, and can thus be authoritative
%% about those objects. For queues, exchanges and nodes we go to
%% Mnesia to find out the immutable details of the objects.
%%
%% For everything other than consumers, the database can then augment
%% these immutable details with stats, as the object changes. (We
%% never emit anything very interesting about consumers).
%%
%% Stats on the inbound side are refered to as coarse- and
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
%% Created events and basic stats are stored in ETS tables by object,
%% looked up in an orddict in #state.tables. Simple and detailed stats
%% (which only differ depending on how they're keyed) are stored in
%% #state.aggregated_stats.
%%
%% For detailed stats we also store an index for each object referencing
%% all the other objects that form a detailed stats key with it. This is
%% so that we can always avoid table scanning while deleting stats and
%% thus make sure that handling deleted events is O(n)-ish.
%%
%% For each key for simple and detailed stats we maintain a #stats{}
%% record, essentially a base counter for everything that happened
%% before the samples we have kept, and a gb_tree of {timestamp,
%% sample} values.
%%
%% We also have #state.old_stats to let us calculate instantaneous
%% rates, in order to apportion simple / detailed stats into time
%% slices as they come in. These instantaneous rates are not returned
%% in response to any query, the rates shown in the API are calculated
%% at query time. old_stats contains both coarse and fine
%% entries. Coarse entries are pruned when the corresponding object is
%% deleted, and fine entries are pruned when the emitting channel is
%% closed, and whenever we receive new fine stats from a channel. So
%% it's quite close to being a cache of "the previous stats we
%% received".
%%
%% We also keep a timer going, in order to prune old samples from
%% #state.aggregated_stats.
%%
%% Overall the object is to do all the aggregation when events come
%% in, and make queries be simple lookups as much as possible. One
%% area where this does not happen is the global overview - which is
%% aggregated from vhost stats at query time since we do not want to
%% reveal anything about other vhosts to unprivileged users.

-record(state, {
          %% "stats" for which no calculations are required
          tables,
          %% database of aggregated samples
          aggregated_stats,
          %% index for detailed aggregated_stats that have 2-tuple keys
          aggregated_stats_index,
          %% What the previous info item was for any given
          %% {queue/channel/connection}
          old_stats,
          gc_timer,
          gc_continuation,
          lookups,
          interval}).

-define(FINE_STATS_TYPES, [channel_queue_stats, channel_exchange_stats,
                           channel_queue_exchange_stats]).
-define(TABLES, [queue_stats, connection_stats, channel_stats,
                 consumers_by_queue, consumers_by_channel,
                 node_stats]).

-define(DELIVER_GET, [deliver, deliver_no_ack, get, get_no_ack]).
-define(FINE_STATS, [publish, publish_in, publish_out,
                     ack, deliver_get, confirm, return_unroutable, redeliver] ++
            ?DELIVER_GET).

-define(COARSE_QUEUE_STATS,
        [messages, messages_ready, messages_unacknowledged]).

-define(COARSE_CONN_STATS, [recv_oct, send_oct]).

-define(GC_INTERVAL, 5000).
-define(GC_MIN_ROWS, 100).
-define(GC_MIN_RATIO, 0.01).

-define(DROP_LENGTH, 1000).

prioritise_cast({event, #event{type  = Type,
                               props = Props}}, Len, _State)
  when (Type =:= channel_stats orelse
        Type =:= queue_stats) andalso Len > ?DROP_LENGTH ->
    case pget(idle_since, Props) of
        unknown -> drop;
        _       -> 0
    end;
prioritise_cast(_Msg, _Len, _State) ->
    0.

%%----------------------------------------------------------------------------
%% API
%%----------------------------------------------------------------------------

start_link() ->
    %% When failing over it is possible that the mirrored_supervisor
    %% might hear of the death of the old DB, and start a new one,
    %% before the global name server notices. Therefore rather than
    %% telling gen_server:start_link/4 to register it for us, we
    %% invoke global:re_register_name/2 ourselves, and just steal the
    %% name if it existed before. We therefore rely on
    %% mirrored_supervisor to maintain the uniqueness of this process.
    case gen_server2:start_link(?MODULE, [], []) of
        {ok, Pid} -> yes = global:re_register_name(?MODULE, Pid),
                     rabbit:force_event_refresh(),
                     {ok, Pid};
        Else      -> Else
    end.

%% R = Ranges, M = Mode
augment_exchanges(Xs, R, M) -> safe_call({augment_exchanges, Xs, R, M}, Xs).
augment_queues(Qs, R, M)    -> safe_call({augment_queues, Qs, R, M}, Qs).
augment_vhosts(VHosts, R)   -> safe_call({augment_vhosts, VHosts, R}, VHosts).
augment_nodes(Nodes)        -> safe_call({augment_nodes, Nodes}, Nodes).

get_channel(Name, R)        -> safe_call({get_channel, Name, R}, not_found).
get_connection(Name, R)     -> safe_call({get_connection, Name, R}, not_found).

get_all_channels(R)         -> safe_call({get_all_channels, R}).
get_all_connections(R)      -> safe_call({get_all_connections, R}).

get_overview(User, R)       -> safe_call({get_overview, User, R}).
get_overview(R)             -> safe_call({get_overview, all, R}).

override_lookups(Lookups)   -> safe_call({override_lookups, Lookups}).
reset_lookups()             -> safe_call(reset_lookups).

safe_call(Term) -> safe_call(Term, []).

safe_call(Term, Item) ->
    try
        gen_server2:call({global, ?MODULE}, Term, infinity)
    catch exit:{noproc, _} -> Item
    end.

%%----------------------------------------------------------------------------
%% Internal, gen_server2 callbacks
%%----------------------------------------------------------------------------

init([]) ->
    %% When Rabbit is overloaded, it's usually especially important
    %% that the management plugin work.
    process_flag(priority, high),
    {ok, Interval} = application:get_env(rabbit, collect_statistics_interval),
    rabbit_node_monitor:subscribe(self()),
    rabbit_log:info("Statistics database started.~n"),
    Table = fun () -> ets:new(rabbit_mgmt_db, [ordered_set]) end,
    Tables = orddict:from_list([{Key, Table()} || Key <- ?TABLES]),
    {ok, set_gc_timer(
           reset_lookups(
             #state{interval               = Interval,
                    tables                 = Tables,
                    old_stats              = Table(),
                    aggregated_stats       = Table(),
                    aggregated_stats_index = Table()})), hibernate,
     {backoff, ?HIBERNATE_AFTER_MIN, ?HIBERNATE_AFTER_MIN, ?DESIRED_HIBERNATE}}.

handle_call({augment_exchanges, Xs, Ranges, basic}, _From, State) ->
    reply(list_exchange_stats(Ranges, Xs, State), State);

handle_call({augment_exchanges, Xs, Ranges, full}, _From, State) ->
    reply(detail_exchange_stats(Ranges, Xs, State), State);

handle_call({augment_queues, Qs, Ranges, basic}, _From, State) ->
    reply(list_queue_stats(Ranges, Qs, State), State);

handle_call({augment_queues, Qs, Ranges, full}, _From, State) ->
    reply(detail_queue_stats(Ranges, Qs, State), State);

handle_call({augment_vhosts, VHosts, Ranges}, _From, State) ->
    reply(vhost_stats(Ranges, VHosts, State), State);

handle_call({augment_nodes, Nodes}, _From, State) ->
    {reply, node_stats(Nodes, State), State};

handle_call({get_channel, Name, Ranges}, _From,
            State = #state{tables = Tables}) ->
    case created_event(Name, channel_stats, Tables) of
        not_found -> reply(not_found, State);
        Ch        -> [Result] = detail_channel_stats(Ranges, [Ch], State),
                     reply(Result, State)
    end;

handle_call({get_connection, Name, Ranges}, _From,
            State = #state{tables = Tables}) ->
    case created_event(Name, connection_stats, Tables) of
        not_found -> reply(not_found, State);
        Conn      -> [Result] = connection_stats(Ranges, [Conn], State),
                     reply(Result, State)
    end;

handle_call({get_all_channels, Ranges}, _From,
            State = #state{tables = Tables}) ->
    Chans = created_events(channel_stats, Tables),
    reply(list_channel_stats(Ranges, Chans, State), State);

handle_call({get_all_connections, Ranges}, _From,
            State = #state{tables = Tables}) ->
    Conns = created_events(connection_stats, Tables),
    reply(connection_stats(Ranges, Conns, State), State);

handle_call({get_overview, User, Ranges}, _From,
            State = #state{tables = Tables}) ->
    VHosts = case User of
                 all -> rabbit_vhost:list();
                 _   -> rabbit_mgmt_util:list_visible_vhosts(User)
             end,
    %% TODO: there's no reason we can't do an overview of send_oct and
    %% recv_oct now!
    VStats = [read_simple_stats(vhost_stats, VHost, State) ||
                 VHost <- VHosts],
    MessageStats = [overview_sum(Type, VStats) || Type <- ?FINE_STATS],
    QueueStats = [overview_sum(Type, VStats) || Type <- ?COARSE_QUEUE_STATS],
    F = case User of
            all -> fun (L) -> length(L) end;
            _   -> fun (L) -> length(rabbit_mgmt_util:filter_user(L, User)) end
        end,
    %% Filtering out the user's consumers would be rather expensive so let's
    %% just not show it
    Consumers = case User of
                    all -> Table = orddict:fetch(consumers_by_queue, Tables),
                           [{consumers, ets:info(Table, size)}];
                    _   -> []
                end,
    ObjectTotals = Consumers ++
        [{queues,      length([Q || V <- VHosts,
                                    Q <- rabbit_amqqueue:list(V)])},
         {exchanges,   length([X || V <- VHosts,
                                    X <- rabbit_exchange:list(V)])},
         {connections, F(created_events(connection_stats, Tables))},
         {channels,    F(created_events(channel_stats, Tables))}],
    reply([{message_stats, format_samples(Ranges, MessageStats, State)},
           {queue_totals,  format_samples(Ranges, QueueStats, State)},
           {object_totals, ObjectTotals}], State);

handle_call({override_lookups, Lookups}, _From, State) ->
    reply(ok, State#state{lookups = Lookups});

handle_call(reset_lookups, _From, State) ->
    reply(ok, reset_lookups(State));

handle_call(_Request, _From, State) ->
    reply(not_understood, State).

handle_cast({event, Event}, State) ->
    handle_event(Event, State),
    noreply(State);

handle_cast(_Request, State) ->
    noreply(State).

handle_info(gc, State) ->
    noreply(set_gc_timer(gc_batch(State)));

handle_info({node_down, Node}, State = #state{tables = Tables}) ->
    Conns = created_events(connection_stats, Tables),
    Chs = created_events(channel_stats, Tables),
    delete_all_from_node(connection_closed, Node, Conns, State),
    delete_all_from_node(channel_closed, Node, Chs, State),
    noreply(State);

handle_info(_Info, State) ->
    noreply(State).

terminate(_Arg, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

reply(Reply, NewState) -> {reply, Reply, NewState, hibernate}.
noreply(NewState) -> {noreply, NewState, hibernate}.

set_gc_timer(State) ->
    TRef = erlang:send_after(?GC_INTERVAL, self(), gc),
    State#state{gc_timer = TRef}.

reset_lookups(State) ->
    State#state{lookups = [{exchange, fun rabbit_exchange:lookup/1},
                           {queue,    fun rabbit_amqqueue:lookup/1}]}.

handle_pre_hibernate(State) ->
    %% rabbit_event can end up holding on to some memory after a busy
    %% workout, but it's not a gen_server so we can't make it
    %% hibernate. The best we can do is forcibly GC it here (if
    %% rabbit_mgmt_db is hibernating the odds are rabbit_event is
    %% quiescing in some way too).
    rpc:multicall(
      rabbit_mnesia:cluster_nodes(running), rabbit_mgmt_db_handler, gc, []),
    {hibernate, State}.

format_message_queue(Opt, MQ) -> rabbit_misc:format_message_queue(Opt, MQ).

delete_all_from_node(Type, Node, Items, State) ->
    [case node(Pid) of
         Node -> handle_event(#event{type = Type, props = [{pid, Pid}]}, State);
         _    -> ok
     end || Item <- Items, Pid <- [pget(pid, Item)]].

%%----------------------------------------------------------------------------
%% Internal, utilities
%%----------------------------------------------------------------------------

pget(Key, List) -> pget(Key, List, unknown).

%% id_name() and id() are for use when handling events, id_lookup()
%% for when augmenting. The difference is that when handling events a
%% queue name will be a resource, but when augmenting we will be
%% passed a queue proplist that will already have been formatted -
%% i.e. it will have name and vhost keys.
id_name(node_stats)       -> name;
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

lookup_element(Table, Key) -> lookup_element(Table, Key, 2).

lookup_element(Table, Key, Pos) ->
    try ets:lookup_element(Table, Key, Pos)
    catch error:badarg -> []
    end.

fine_stats_id(ChPid, {Q, X}) -> {ChPid, Q, X};
fine_stats_id(ChPid, QorX)   -> {ChPid, QorX}.

floor(TS, #state{interval = Interval}) ->
    rabbit_mgmt_util:floor(rabbit_mgmt_format:timestamp_ms(TS), Interval).
ceil(TS, #state{interval = Interval}) ->
    rabbit_mgmt_util:ceil (rabbit_mgmt_format:timestamp_ms(TS), Interval).

details_key(Key) -> list_to_atom(atom_to_list(Key) ++ "_details").

%%----------------------------------------------------------------------------
%% Internal, event-receiving side
%%----------------------------------------------------------------------------

handle_event(#event{type = queue_stats, props = Stats, timestamp = Timestamp},
             State) ->
    handle_stats(queue_stats, Stats, Timestamp,
                 [{fun rabbit_mgmt_format:properties/1,[backing_queue_status]},
                  {fun rabbit_mgmt_format:timestamp/1, [idle_since]},
                  {fun rabbit_mgmt_format:queue_status/1, [status]}],
                 [messages, messages_ready, messages_unacknowledged], State);

handle_event(Event = #event{type = queue_deleted,
                            props = [{name, Name}],
                            timestamp = Timestamp},
             State = #state{old_stats = OldTable}) ->
    delete_consumers(Name, consumers_by_queue, consumers_by_channel, State),
    %% This is fiddly. Unlike for connections and channels, we need to
    %% decrease any amalgamated coarse stats for [messages,
    %% messages_ready, messages_unacknowledged] for this queue - since
    %% the queue's deletion means we have really got rid of messages!
    Id = {coarse, {queue_stats, Name}},
    %% This ceil must correspond to the ceil in append_samples/5
    TS = ceil(Timestamp, State),
    OldStats = lookup_element(OldTable, Id),
    [record_sample(Id, {Key, -pget(Key, OldStats, 0), TS, State}, State)
     || Key <- ?COARSE_QUEUE_STATS],
    delete_samples(channel_queue_stats,  {'_', Name}, State),
    delete_samples(queue_exchange_stats, {Name, '_'}, State),
    delete_samples(queue_stats,          Name,        State),
    handle_deleted(queue_stats, Event, State);

handle_event(Event = #event{type = exchange_deleted,
                            props = [{name, Name}]}, State) ->
    delete_samples(channel_exchange_stats,  {'_', Name}, State),
    delete_samples(queue_exchange_stats,    {'_', Name}, State),
    delete_samples(exchange_stats,          Name,        State),
    handle_deleted(exchange_stats, Event, State);

handle_event(#event{type = vhost_deleted,
                    props = [{name, Name}]}, State) ->
    delete_samples(vhost_stats, Name, State),
    {ok, State};

handle_event(#event{type = connection_created, props = Stats}, State) ->
    handle_created(
      connection_stats, Stats,
      [{fun rabbit_mgmt_format:addr/1,         [host, peer_host]},
       {fun rabbit_mgmt_format:port/1,         [port, peer_port]},
       {fun rabbit_mgmt_format:protocol/1,     [protocol]},
       {fun rabbit_mgmt_format:amqp_table/1,   [client_properties]}], State);

handle_event(#event{type = connection_stats, props = Stats,
                    timestamp = Timestamp},
             State) ->
    handle_stats(connection_stats, Stats, Timestamp, [], ?COARSE_CONN_STATS,
                 State);

handle_event(Event = #event{type  = connection_closed,
                            props = [{pid, Pid}]}, State) ->
    delete_samples(connection_stats, Pid, State),
    handle_deleted(connection_stats, Event, State);

handle_event(#event{type = channel_created, props = Stats}, State) ->
    handle_created(channel_stats, Stats, [], State);

handle_event(#event{type = channel_stats, props = Stats, timestamp = Timestamp},
             State = #state{old_stats = OldTable}) ->
    handle_stats(channel_stats, Stats, Timestamp,
                 [{fun rabbit_mgmt_format:timestamp/1, [idle_since]}],
                 [], State),
    ChPid = id(channel_stats, Stats),
    AllStats = [old_fine_stats(Type, Stats, State)
                || Type <- ?FINE_STATS_TYPES],
    ets:match_delete(OldTable, {{fine, {ChPid, '_'}},      '_'}),
    ets:match_delete(OldTable, {{fine, {ChPid, '_', '_'}}, '_'}),
    [handle_fine_stats(Timestamp, AllStatsElem, State)
     || AllStatsElem <- AllStats],
    {ok, State};

handle_event(Event = #event{type = channel_closed,
                            props = [{pid, Pid}]},
             State = #state{old_stats = Old}) ->
    delete_consumers(Pid, consumers_by_channel, consumers_by_queue, State),
    delete_samples(channel_queue_stats,    {Pid, '_'}, State),
    delete_samples(channel_exchange_stats, {Pid, '_'}, State),
    delete_samples(channel_stats,          Pid,        State),
    handle_deleted(channel_stats, Event, State),
    ets:match_delete(Old, {{fine, {Pid, '_'}},      '_'}),
    ets:match_delete(Old, {{fine, {Pid, '_', '_'}}, '_'});

handle_event(#event{type = consumer_created, props = Props}, State) ->
    Fmt = [{fun rabbit_mgmt_format:amqp_table/1, [arguments]}],
    handle_consumer(fun(Table, Id, P0) ->
                            P = rabbit_mgmt_format:format(P0, Fmt),
                            ets:insert(Table, {Id, P})
                    end,
                    Props, State);

handle_event(#event{type = consumer_deleted, props = Props}, State) ->
    handle_consumer(fun(Table, Id, _P) -> ets:delete(Table, Id) end,
                    Props, State);

%% TODO: we don't clear up after dead nodes here - this is a very tiny
%% leak every time a node is permanently removed from the cluster. Do
%% we care?
handle_event(#event{type = node_stats, props = Stats, timestamp = Timestamp},
             State = #state{tables = Tables}) ->
    Table = orddict:fetch(node_stats, Tables),
    ets:insert(Table, {{pget(name, Stats), stats},
                       proplists:delete(name, Stats), Timestamp}),
    {ok, State};

handle_event(_Event, State) ->
    {ok, State}.

handle_created(TName, Stats, Funs, State = #state{tables = Tables}) ->
    Formatted = rabbit_mgmt_format:format(Stats, Funs),
    ets:insert(orddict:fetch(TName, Tables), {{id(TName, Stats), create},
                                              Formatted,
                                              pget(name, Stats)}),
    {ok, State}.

handle_stats(TName, Stats, Timestamp, Funs, RatesKeys,
             State = #state{tables = Tables, old_stats = OldTable}) ->
    Id = id(TName, Stats),
    IdSamples = {coarse, {TName, Id}},
    OldStats = lookup_element(OldTable, IdSamples),
    append_samples(Stats, Timestamp, OldStats, IdSamples, RatesKeys, State),
    StripKeys = [id_name(TName)] ++ RatesKeys ++ ?FINE_STATS_TYPES,
    Stats1 = [{K, V} || {K, V} <- Stats, not lists:member(K, StripKeys)],
    Stats2 = rabbit_mgmt_format:format(Stats1, Funs),
    ets:insert(orddict:fetch(TName, Tables), {{Id, stats}, Stats2, Timestamp}),
    {ok, State}.

handle_deleted(TName, #event{props = Props}, State = #state{tables    = Tables,
                                                            old_stats = Old}) ->
    Id = id(TName, Props),
    case orddict:find(TName, Tables) of
        {ok, Table} -> ets:delete(Table, {Id, create}),
                       ets:delete(Table, {Id, stats});
        error       -> ok
    end,
    ets:delete(Old, {coarse, {TName, Id}}),
    {ok, State}.

handle_consumer(Fun, Props, State = #state{tables = Tables}) ->
    P = rabbit_mgmt_format:format(Props, []),
    CTag = pget(consumer_tag, P),
    Q    = pget(queue,        P),
    Ch   = pget(channel,      P),
    QTable  = orddict:fetch(consumers_by_queue,   Tables),
    ChTable = orddict:fetch(consumers_by_channel, Tables),
    Fun(QTable,  {Q, Ch, CTag}, P),
    Fun(ChTable, {Ch, Q, CTag}, P),
    {ok, State}.

%% The consumer_deleted event is emitted by queues themselves -
%% therefore in the event that a queue dies suddenly we may not get
%% it. The best way to handle this is to make sure we also clean up
%% consumers when we hear about any queue going down.
delete_consumers(PrimId, PrimTableName, SecTableName,
                 #state{tables = Tables}) ->
    Table1 = orddict:fetch(PrimTableName, Tables),
    Table2 = orddict:fetch(SecTableName, Tables),
    SecIdCTags = ets:match(Table1, {{PrimId, '$1', '$2'}, '_'}),
    ets:match_delete(Table1, {{PrimId, '_', '_'}, '_'}),
    [ets:delete(Table2, {SecId, PrimId, CTag}) || [SecId, CTag] <- SecIdCTags].

old_fine_stats(Type, Props, #state{old_stats = Old}) ->
    case pget(Type, Props) of
        unknown       -> ignore;
        AllFineStats0 -> ChPid = id(channel_stats, Props),
                         [begin
                              Id = fine_stats_id(ChPid, Ids),
                              {Id, Stats, lookup_element(Old, {fine, Id})}
                          end || {Ids, Stats} <- AllFineStats0]
    end.

handle_fine_stats(_Timestamp, ignore, _State) ->
    ok;

handle_fine_stats(Timestamp, AllStats, State) ->
    [handle_fine_stat(Id, Stats, Timestamp, OldStats, State) ||
        {Id, Stats, OldStats} <- AllStats].

handle_fine_stat(Id, Stats, Timestamp, OldStats, State) ->
    Total = lists:sum([V || {K, V} <- Stats, lists:member(K, ?DELIVER_GET)]),
    Stats1 = case Total of
                 0 -> Stats;
                 _ -> [{deliver_get, Total}|Stats]
             end,
    append_samples(Stats1, Timestamp, OldStats, {fine, Id}, all, State).

delete_samples(Type, {Id, '_'}, State) ->
    delete_samples_with_index(Type, Id, fun forward/2, State);
delete_samples(Type, {'_', Id}, State) ->
    delete_samples_with_index(Type, Id, fun reverse/2, State);
delete_samples(Type, Id, #state{aggregated_stats = ETS}) ->
    ets:match_delete(ETS, delete_match(Type, Id)).

delete_samples_with_index(Type, Id, Order,
                          #state{aggregated_stats       = ETS,
                                 aggregated_stats_index = ETSi}) ->
    Ids2 = lists:append(ets:match(ETSi, {{Type, Id, '$1'}})),
    ets:match_delete(ETSi, {{Type, Id, '_'}}),
    [begin
         ets:match_delete(ETS, delete_match(Type, Order(Id, Id2))),
         ets:match_delete(ETSi, {{Type, Id2, Id}})
     end || Id2 <- Ids2].

forward(A, B) -> {A, B}.
reverse(A, B) -> {B, A}.

delete_match(Type, Id) -> {{{Type, Id}, '_'}, '_'}.

append_samples(Stats, TS, OldStats, Id, Keys,
               State = #state{old_stats = OldTable}) ->
    case ignore_coarse_sample(Id, State) of
        false ->
            %% This ceil must correspond to the ceil in handle_event
            %% queue_deleted
            NewMS = ceil(TS, State),
            case Keys of
                all -> [append_sample(Key, Value, NewMS, OldStats, Id, State)
                        || {Key, Value} <- Stats];
                _   -> [append_sample(
                          Key, pget(Key, Stats), NewMS, OldStats, Id, State)
                        || Key <- Keys]
            end,
            ets:insert(OldTable, {Id, Stats});
        true ->
            ok
    end.

append_sample(Key, Value, NewMS, OldStats, Id, State) when is_number(Value) ->
    record_sample(
      Id, {Key, Value - pget(Key, OldStats, 0), NewMS, State}, State);

append_sample(_Key, _Value, _NewMS, _OldStats, _Id, _State) ->
    ok.

ignore_coarse_sample({coarse, {queue_stats, Q}}, State) ->
    not object_exists(Q, State);
ignore_coarse_sample(_, _) ->
    false.

record_sample({coarse, Id}, Args, State) ->
    record_sample0(Id, Args),
    record_sample0({vhost_stats, vhost(Id, State)}, Args);

%% Deliveries / acks (Q -> Ch)
record_sample({fine, {Ch, Q = #resource{kind = queue}}}, Args, State) ->
    case object_exists(Q, State) of
        true  -> record_sample0({channel_queue_stats, {Ch, Q}}, Args),
                 record_sample0({queue_stats,         Q},       Args);
        false -> ok
    end,
    record_sample0({channel_stats, Ch},       Args),
    record_sample0({vhost_stats,   vhost(Q)}, Args);

%% Publishes / confirms (Ch -> X)
record_sample({fine, {Ch, X = #resource{kind = exchange}}}, Args, State) ->
    case object_exists(X, State) of
        true  -> record_sample0({channel_exchange_stats, {Ch, X}}, Args),
                 record_sampleX(publish_in,              X,        Args);
        false -> ok
    end,
    record_sample0({channel_stats, Ch},       Args),
    record_sample0({vhost_stats,   vhost(X)}, Args);

%% Publishes (but not confirms) (Ch -> X -> Q)
record_sample({fine, {_Ch,
                      Q = #resource{kind = queue},
                      X = #resource{kind = exchange}}}, Args, State) ->
    %% TODO This one logically feels like it should be here. It would
    %% correspond to "publishing channel message rates to queue" -
    %% which would be nice to handle - except we don't. And just
    %% uncommenting this means it gets merged in with "consuming
    %% channel delivery from queue" - which is not very helpful.
    %% record_sample0({channel_queue_stats, {Ch, Q}}, Args),
    QExists = object_exists(Q, State),
    XExists = object_exists(X, State),
    case QExists of
        true  -> record_sample0({queue_stats,          Q},       Args);
        false -> ok
    end,
    case QExists andalso XExists of
        true  -> record_sample0({queue_exchange_stats, {Q,  X}}, Args);
        false -> ok
    end,
    case XExists of
        true  -> record_sampleX(publish_out,           X,        Args);
        false -> ok
    end.

%% We have to check the queue and exchange objects still exist since
%% their deleted event could be overtaken by a channel stats event
%% which contains fine stats referencing them. That's also why we
%% don't need to check the channels exist - their deleted event can't
%% be overtaken by their own last stats event.
%%
%% Also, sometimes the queue_deleted event is not emitted by the queue
%% (in the nodedown case) - so it can overtake the final queue_stats
%% event (which is not *guaranteed* to be lost). So we make a similar
%% check for coarse queue stats.
%%
%% We can be sure that mnesia will be up to date by the time we receive
%% the event (even though we dirty read) since the deletions are
%% synchronous and we do not emit the deleted event until after the
%% deletion has occurred.
object_exists(Name = #resource{kind = Kind}, #state{lookups = Lookups}) ->
    case (pget(Kind, Lookups))(Name) of
        {ok, _} -> true;
        _       -> false
    end.

vhost(#resource{virtual_host = VHost}) -> VHost.

vhost({queue_stats, #resource{virtual_host = VHost}}, _State) ->
    VHost;
vhost({TName, Pid}, #state{tables = Tables}) ->
    Table = orddict:fetch(TName, Tables),
    pget(vhost, lookup_element(Table, {Pid, create})).

%% exchanges have two sets of "publish" stats, so rearrange things a touch
record_sampleX(RenamePublishTo, X, {publish, Diff, TS, State}) ->
    record_sample0({exchange_stats, X}, {RenamePublishTo, Diff, TS, State});
record_sampleX(_RenamePublishTo, X, {Type, Diff, TS, State}) ->
    record_sample0({exchange_stats, X}, {Type, Diff, TS, State}).

record_sample0(Id0, {Key, Diff, TS, #state{aggregated_stats       = ETS,
                                           aggregated_stats_index = ETSi}}) ->
    Id = {Id0, Key},
    Old = case lookup_element(ETS, Id) of
              [] -> case Id0 of
                        {Type, {Id1, Id2}} ->
                            ets:insert(ETSi, {{Type, Id2, Id1}}),
                            ets:insert(ETSi, {{Type, Id1, Id2}});
                        _ ->
                            ok
                    end,
                    rabbit_mgmt_stats:blank();
              E  -> E
          end,
    ets:insert(ETS, {Id, rabbit_mgmt_stats:record(TS, Diff, Old)}).

%%----------------------------------------------------------------------------
%% Internal, querying side
%%----------------------------------------------------------------------------

-define(QUEUE_DETAILS,
        {queue_stats, [{incoming,   queue_exchange_stats, fun first/1},
                       {deliveries, channel_queue_stats,  fun second/1}]}).

-define(EXCHANGE_DETAILS,
        {exchange_stats, [{incoming, channel_exchange_stats, fun second/1},
                          {outgoing, queue_exchange_stats,   fun second/1}]}).

-define(CHANNEL_DETAILS,
        {channel_stats, [{publishes,  channel_exchange_stats, fun first/1},
                         {deliveries, channel_queue_stats,    fun first/1}]}).

first(Id)  -> {Id, '$1'}.
second(Id) -> {'$1', Id}.

list_queue_stats(Ranges, Objs, State) ->
    adjust_hibernated_memory_use(
      merge_stats(Objs, queue_funs(Ranges, State))).

detail_queue_stats(Ranges, Objs, State) ->
    adjust_hibernated_memory_use(
      merge_stats(Objs, [consumer_details_fun(
                           fun (Props) -> id_lookup(queue_stats, Props) end,
                           consumers_by_queue, State),
                         detail_stats_fun(Ranges, ?QUEUE_DETAILS, State)
                         | queue_funs(Ranges, State)])).

queue_funs(Ranges, State) ->
    [basic_stats_fun(queue_stats, State),
     simple_stats_fun(Ranges, queue_stats, State),
     augment_msg_stats_fun(State)].

list_exchange_stats(Ranges, Objs, State) ->
    merge_stats(Objs, [simple_stats_fun(Ranges, exchange_stats, State),
                       augment_msg_stats_fun(State)]).

detail_exchange_stats(Ranges, Objs, State) ->
    merge_stats(Objs, [simple_stats_fun(Ranges, exchange_stats, State),
                       detail_stats_fun(Ranges, ?EXCHANGE_DETAILS, State),
                       augment_msg_stats_fun(State)]).

connection_stats(Ranges, Objs, State) ->
    merge_stats(Objs, [basic_stats_fun(connection_stats, State),
                       simple_stats_fun(Ranges, connection_stats, State),
                       augment_msg_stats_fun(State)]).

list_channel_stats(Ranges, Objs, State) ->
    merge_stats(Objs, [basic_stats_fun(channel_stats, State),
                       simple_stats_fun(Ranges, channel_stats, State),
                       augment_msg_stats_fun(State)]).

detail_channel_stats(Ranges, Objs, State) ->
    merge_stats(Objs, [basic_stats_fun(channel_stats, State),
                       simple_stats_fun(Ranges, channel_stats, State),
                       consumer_details_fun(
                         fun (Props) -> pget(pid, Props) end,
                         consumers_by_channel, State),
                       detail_stats_fun(Ranges, ?CHANNEL_DETAILS, State),
                       augment_msg_stats_fun(State)]).

vhost_stats(Ranges, Objs, State) ->
    merge_stats(Objs, [simple_stats_fun(Ranges, vhost_stats, State)]).

node_stats(Objs, State) ->
    merge_stats(Objs, [basic_stats_fun(node_stats, State)]).

merge_stats(Objs, Funs) ->
    [lists:foldl(fun (Fun, Props) -> Fun(Props) ++ Props end, Obj, Funs)
     || Obj <- Objs].

%% i.e. the non-calculated stats
basic_stats_fun(Type, #state{tables = Tables}) ->
    Table = orddict:fetch(Type, Tables),
    fun (Props) ->
            Id = id_lookup(Type, Props),
            lookup_element(Table, {Id, stats})
    end.

%% i.e. coarse stats, and fine stats aggregated up to a single number per thing
simple_stats_fun(Ranges, Type, State) ->
    fun (Props) ->
            Id = id_lookup(Type, Props),
            extract_msg_stats(
              format_samples(Ranges, read_simple_stats(Type, Id, State), State))
    end.

%% i.e. fine stats that are broken out per sub-thing
detail_stats_fun(Ranges, {IdType, FineSpecs}, State) ->
    fun (Props) ->
            Id = id_lookup(IdType, Props),
            [detail_stats(Ranges, Name, AggregatedStatsType, IdFun(Id), State)
             || {Name, AggregatedStatsType, IdFun} <- FineSpecs]
    end.

read_simple_stats(Type, Id, #state{aggregated_stats = ETS}) ->
    FromETS = ets:match(ETS, {{{Type, Id}, '$1'}, '$2'}),
    [{K, V} || [K, V] <- FromETS].

read_detail_stats(Type, Id, #state{aggregated_stats = ETS}) ->
    %% Id must contain '$1'
    FromETS = ets:match(ETS, {{{Type, Id}, '$2'}, '$3'}),
    %% [[G, K, V]] -> [{G, [{K, V}]}] where G is Q/X/Ch, K is from
    %% ?FINE_STATS and V is a stats tree
    %% TODO does this need to be optimised?
    lists:foldl(
      fun ([G, K, V], L) ->
              case lists:keyfind(G, 1, L) of
                  false    -> [{G, [{K, V}]} | L];
                  {G, KVs} -> lists:keyreplace(G, 1, L, {G, [{K, V} | KVs]})
              end
      end, [], FromETS).

extract_msg_stats(Stats) ->
    FineStats = lists:append([[K, details_key(K)] || K <- ?FINE_STATS]),
    {MsgStats, Other} =
        lists:partition(fun({K, _}) -> lists:member(K, FineStats) end, Stats),
    case MsgStats of
        [] -> Other;
        _  -> [{message_stats, MsgStats} | Other]
    end.

detail_stats(Ranges, Name, AggregatedStatsType, Id, State) ->
    {Name,
     [[{stats, format_samples(Ranges, KVs, State)} | format_detail_id(G, State)]
      || {G, KVs} <- read_detail_stats(AggregatedStatsType, Id, State)]}.

format_detail_id(ChPid, State) when is_pid(ChPid) ->
    augment_msg_stats([{channel, ChPid}], State);
format_detail_id(#resource{name = Name, virtual_host = Vhost, kind = Kind},
                 _State) ->
    [{Kind, [{name, Name}, {vhost, Vhost}]}].

format_samples(Ranges, ManyStats, #state{interval = Interval}) ->
    lists:append(
      [case rabbit_mgmt_stats:is_blank(Stats) of
           true  -> [];
           false -> {Details, Counter} = rabbit_mgmt_stats:format(
                                           pick_range(K, Ranges),
                                           Stats, Interval),
                    [{K,              Counter},
                     {details_key(K), Details}]
       end || {K, Stats} <- ManyStats]).

pick_range(K, {RangeL, RangeM, RangeD}) ->
    case {lists:member(K, ?COARSE_QUEUE_STATS),
          lists:member(K, ?FINE_STATS),
          lists:member(K, ?COARSE_CONN_STATS)} of
        {true, false, false} -> RangeL;
        {false, true, false} -> RangeM;
        {false, false, true} -> RangeD
    end.

%% We do this when retrieving the queue record rather than when
%% storing it since the memory use will drop *after* we find out about
%% hibernation, so to do it when we receive a queue stats event would
%% be fiddly and racy. This should be quite cheap though.
adjust_hibernated_memory_use(Qs) ->
    Pids = [pget(pid, Q) ||
               Q <- Qs, pget(idle_since, Q, not_idle) =/= not_idle],
    %% We use delegate here not for ordering reasons but because we
    %% want to get the right amount of parallelism and minimise
    %% cross-cluster communication.
    {Mem, _BadNodes} = delegate:invoke(Pids, {erlang, process_info, [memory]}),
    MemDict = dict:from_list([{P, M} || {P, M = {memory, _}} <- Mem]),
    [case dict:find(pget(pid, Q), MemDict) of
         error        -> Q;
         {ok, Memory} -> [Memory|proplists:delete(memory, Q)]
     end || Q <- Qs].

created_event(Name, Type, Tables) ->
    Table = orddict:fetch(Type, Tables),
    case ets:match(Table, {{'$1', create}, '_', Name}) of
        []     -> not_found;
        [[Id]] -> lookup_element(Table, {Id, create})
    end.

created_events(Type, Tables) ->
    [Facts || {{_, create}, Facts, _Name}
                  <- ets:tab2list(orddict:fetch(Type, Tables))].

consumer_details_fun(KeyFun, TableName, State = #state{tables = Tables}) ->
    Table = orddict:fetch(TableName, Tables),
    fun ([])    -> [];
        (Props) -> Pattern = {KeyFun(Props), '_', '_'},
                   [{consumer_details,
                     [augment_msg_stats(augment_consumer(Obj), State)
                      || Obj <- lists:append(
                                  ets:match(Table, {Pattern, '$1'}))]}]
    end.

augment_consumer(Obj) ->
    [{queue, rabbit_mgmt_format:resource(pget(queue, Obj))} |
     proplists:delete(queue, Obj)].

%%----------------------------------------------------------------------------
%% Internal, query-time summing for overview
%%----------------------------------------------------------------------------

overview_sum(Type, VHostStats) ->
    Stats = [pget(Type, VHost, rabbit_mgmt_stats:blank())
             || VHost <- VHostStats],
    {Type, rabbit_mgmt_stats:sum(Stats)}.

%%----------------------------------------------------------------------------
%% Internal, query-time augmentation
%%----------------------------------------------------------------------------

augment_msg_stats(Props, State) ->
    rabbit_mgmt_format:strip_pids(
      (augment_msg_stats_fun(State))(Props) ++ Props).

augment_msg_stats_fun(State) ->
    Funs = [{connection, fun augment_connection_pid/2},
            {channel,    fun augment_channel_pid/2},
            {owner_pid,  fun augment_connection_pid/2}],
    fun (Props) -> augment(Props, Funs, State) end.

augment(Items, Funs, State) ->
    Augmented = [augment(K, Items, Fun, State) || {K, Fun} <- Funs],
    [{K, V} || {K, V} <- Augmented, V =/= unknown].

augment(K, Items, Fun, State) ->
    Key = details_key(K),
    case pget(K, Items) of
        none    -> {Key, unknown};
        unknown -> {Key, unknown};
        Id      -> {Key, Fun(Id, State)}
    end.

augment_channel_pid(Pid, #state{tables = Tables}) ->
    Ch = lookup_element(orddict:fetch(channel_stats, Tables),
                        {Pid, create}),
    Conn = lookup_element(orddict:fetch(connection_stats, Tables),
                          {pget(connection, Ch), create}),
    [{name,            pget(name,   Ch)},
     {number,          pget(number, Ch)},
     {connection_name, pget(name,         Conn)},
     {peer_port,       pget(peer_port,    Conn)},
     {peer_host,       pget(peer_host,    Conn)}].

augment_connection_pid(Pid, #state{tables = Tables}) ->
    Conn = lookup_element(orddict:fetch(connection_stats, Tables),
                          {Pid, create}),
    [{name,         pget(name,         Conn)},
     {peer_port,    pget(peer_port,    Conn)},
     {peer_host,    pget(peer_host,    Conn)}].

%%----------------------------------------------------------------------------
%% Internal, event-GCing
%%----------------------------------------------------------------------------

gc_batch(State = #state{aggregated_stats = ETS,
                        gc_continuation = Continuation}) ->
    Match = case Continuation of
                undefined -> Size = ets:info(ETS, size),
                             Rows = lists:max([?GC_MIN_ROWS,
                                               round(?GC_MIN_RATIO * Size)]),
                             ets:match(ETS, '$1', Rows);
                _         -> ets:match(Continuation)
            end,
    case Match of
        {Matches, Continuation1} ->
            {ok, Policies} = application:get_env(
                               rabbitmq_management, sample_retention_policies),
            Now = floor(erlang:now(), State),
            [gc(Key, Stats, Policies, Now, ETS) || [{Key, Stats}] <- Matches],
            State#state{gc_continuation = Continuation1};
        '$end_of_table' ->
            State#state{gc_continuation = undefined}
    end.

gc({{Type, Id}, Key}, Stats, Policies, Now, ETS) ->
    Policy = pget(retention_policy(Type), Policies),
    case rabbit_mgmt_stats:gc({Policy, Now}, Stats) of
        Stats  -> ok;
        Stats2 -> ets:insert(ETS, {{{Type, Id}, Key}, Stats2})
    end.

retention_policy(vhost_stats)            -> global;
retention_policy(queue_stats)            -> basic;
retention_policy(exchange_stats)         -> basic;
retention_policy(connection_stats)       -> basic;
retention_policy(channel_stats)          -> basic;
retention_policy(queue_exchange_stats)   -> detailed;
retention_policy(channel_exchange_stats) -> detailed;
retention_policy(channel_queue_stats)    -> detailed.
