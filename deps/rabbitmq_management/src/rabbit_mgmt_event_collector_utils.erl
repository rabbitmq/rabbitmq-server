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
%%   The Initial Developer of the Original Code is Pivotal Software, Inc.
%%   Copyright (c) 2010-2016 Pivotal Software, Inc.  All rights reserved.
%%

-module(rabbit_mgmt_event_collector_utils).

-include("rabbit_mgmt_metrics.hrl").
-include("rabbit_mgmt_event_collector.hrl").
-include_lib("rabbit_common/include/rabbit.hrl").

-export([handle_event/2]).

-import(rabbit_misc, [pget/3]).
-import(rabbit_mgmt_db, [pget/2, id_name/1, id/2, lookup_element/2]).

%%----------------------------------------------------------------------------
%% External functions
%%----------------------------------------------------------------------------

%%------------------------------------------------------------------------------        %% @doc Handles events from any collector.
%%
%% All the gen_server of the collectors have the same internal state record,
%% which contains the interval, lookups and rate_mode required
%% by this function. Apart from the lookups that can be modified by the
%% tests, the rest of the state doesn't change after startup.
%%
%% Ideally, the gen_server should pass only the required parameters and not the
%% full state. However, this simplified the refactor and avoided changing all
%% internal functions.
%%
%% @end
%%------------------------------------------------------------------------------ 
-spec handle_event(#event{}, #state{}) -> ok.
handle_event(#event{type = queue_stats, props = Stats, timestamp = Timestamp},
             State) ->
    handle_stats(queue_stats, Stats, Timestamp,
                 {fun rabbit_mgmt_format:format_queue_stats/1, false},
                 ?QUEUE_MSG_COUNTS, ?QUEUE_MSG_RATES ++ ?PROCESS_STATS, State);

handle_event(Event = #event{type = queue_deleted,
                            props = [{name, Name}],
                            timestamp = Timestamp},
             State) ->
    delete_consumers(Name, consumers_by_queue, consumers_by_channel),
    %% This is fiddly. Unlike for connections and channels, we need to
    %% decrease any amalgamated coarse stats for [messages,
    %% messages_ready, messages_unacknowledged] for this queue - since
    %% the queue's deletion means we have really got rid of messages!
    Id = {coarse, {queue_stats, Name}},
    %% This ceil must correspond to the ceil in append_samples/5
    TS = ceil(Timestamp, State),
    OldStats = lookup_element(old_stats, Id),
    record_sample_list(Id, OldStats, TS, State, ?QUEUE_MSG_COUNTS),
    delete_samples(channel_queue_stats,  {'_', Name}),
    delete_samples(queue_exchange_stats, {Name, '_'}),
    delete_samples(queue_stats,          Name),
    handle_deleted(queue_stats, Event);

handle_event(Event = #event{type = exchange_deleted,
                            props = [{name, Name}]}, _State) ->
    delete_samples(channel_exchange_stats,  {'_', Name}),
    delete_samples(queue_exchange_stats,    {'_', Name}),
    delete_samples(exchange_stats,          Name),
    handle_deleted(exchange_stats, Event);

handle_event(#event{type = vhost_deleted,
                    props = [{name, Name}]}, _State) ->
    delete_samples(vhost_stats, Name);

handle_event(#event{type = connection_created, props = Stats}, _State) ->
    handle_created(
      connection_stats, Stats,
      {fun rabbit_mgmt_format:format_connection_created/1, true});

handle_event(#event{type = connection_stats, props = Stats,
                    timestamp = Timestamp},
             State) ->
    handle_stats(connection_stats, Stats, Timestamp, {[], false},
                 ?COARSE_CONN_STATS, ?PROCESS_STATS, State);

handle_event(Event = #event{type  = connection_closed,
                            props = [{pid, Pid}]}, _State) ->
    delete_samples(connection_stats, Pid),
    handle_deleted(connection_stats, Event);

handle_event(#event{type = channel_created, props = Stats}, _State) ->
    handle_created(channel_stats, Stats, {[], false});

handle_event(#event{type = channel_stats, props = Stats, timestamp = Timestamp},
             State) ->
    handle_stats(channel_stats, Stats, Timestamp,
                 {fun rabbit_mgmt_format:format_channel_stats/1, true},
                 [], ?PROCESS_STATS, State),
    ChPid = id(channel_stats, Stats),
    AllStats = [old_fine_stats(ChPid, Type, Stats)
                || Type <- ?FINE_STATS_TYPES],
    Objs = ets:lookup(old_stats_fine_index, ChPid),
    ets:delete(old_stats_fine_index, ChPid),
    [ets:delete(old_stats, Key) || {_, Key} <- Objs],
    %% This ceil must correspond to the ceil in handle_event
    %% queue_deleted
    handle_fine_stats_list(ChPid, ceil(Timestamp, State), State, AllStats);

handle_event(Event = #event{type = channel_closed,
                            props = [{pid, Pid}]},
             _State) ->
    delete_consumers(Pid, consumers_by_channel, consumers_by_queue),
    delete_samples(channel_queue_stats,    {Pid, '_'}),
    delete_samples(channel_exchange_stats, {Pid, '_'}),
    delete_samples(channel_stats,          Pid),
    handle_deleted(channel_stats, Event),
    Objs = ets:lookup(old_stats_fine_index, Pid),
    ets:delete(old_stats_fine_index, Pid),
    [ets:delete(old_stats, Key) || {_, Key} <- Objs];

handle_event(#event{type = consumer_created, props = Props}, _State) ->
    Fmt = {fun rabbit_mgmt_format:format_arguments/1, true},
    handle_consumer(fun(Table, Id, P0) ->
                            P = rabbit_mgmt_format:format(P0, Fmt),
                            ets:insert(Table, {Id, P})
                    end,
                    Props);

handle_event(#event{type = consumer_deleted, props = Props}, _State) ->
    handle_consumer(fun(Table, Id, _P) -> ets:delete(Table, Id) end,
                    Props);

%% TODO: we don't clear up after dead nodes here - this is a very tiny
%% leak every time a node is permanently removed from the cluster. Do
%% we care?
handle_event(#event{type = node_stats, props = Stats0, timestamp = Timestamp},
             State) ->
    Stats = proplists:delete(persister_stats, Stats0) ++
        pget(persister_stats, Stats0),
    handle_stats(node_stats, Stats, Timestamp, {[], false}, ?COARSE_NODE_STATS, State);

handle_event(#event{type = node_node_stats, props = Stats,
                    timestamp = Timestamp}, State) ->
    handle_stats(node_node_stats, Stats, Timestamp, {[], false}, ?COARSE_NODE_NODE_STATS,
                 State);

handle_event(Event = #event{type  = node_node_deleted,
                            props = [{route, Route}]}, _State) ->
    delete_samples(node_node_stats, Route),
    handle_deleted(node_node_stats, Event);

handle_event(_Event, _State) ->
    ok.

%%----------------------------------------------------------------------------
%% Internal functions
%%----------------------------------------------------------------------------
handle_stats(TName, Stats, Timestamp, Funs, RatesKeys, State) ->
    handle_stats(TName, Stats, Timestamp, Funs, RatesKeys, [], State).

handle_stats(TName, Stats, Timestamp, Funs, RatesKeys, NoAggRatesKeys,
             State) ->
    Id = id(TName, Stats),
    IdSamples = {coarse, {TName, Id}},
    OldStats = lookup_element(old_stats, IdSamples),
    append_set_of_samples(
      Stats, Timestamp, OldStats, IdSamples, RatesKeys, NoAggRatesKeys, State),
    StripKeys = [id_name(TName)] ++ RatesKeys ++ ?FINE_STATS_TYPES,
    Stats1 = [{K, V} || {K, V} <- Stats, not lists:member(K, StripKeys),
                        V =/= unknown],
    Stats2 = rabbit_mgmt_format:format(Stats1, Funs),
    ets:insert(TName, {{Id, stats}, Stats2, Timestamp}),
    ok.

fine_stats_id(ChPid, {Q, X}) -> {ChPid, Q, X};
fine_stats_id(ChPid, QorX)   -> {ChPid, QorX}.

ceil(TS, #state{interval = Interval}) ->
    rabbit_mgmt_util:ceil(TS, Interval).

handle_created(TName, Stats, Funs) ->
    Formatted = rabbit_mgmt_format:format(Stats, Funs),
    Id = id(TName, Stats),
    ets:insert(TName, {{Id, create}, Formatted, pget(name, Stats)}),
    case lists:member(TName, ?PROC_STATS_TABLES) of
        true  -> ets:insert(rabbit_mgmt_stats_tables:key_index(TName), {Id});
        false -> true
    end.

handle_deleted(TName, #event{props = Props}) ->
    Id = id(TName, Props),
    case lists:member(TName, ?TABLES) of
        true  -> ets:delete(TName, {Id, create}),
                 ets:delete(TName, {Id, stats});
        false -> ok
    end,
    ets:delete(old_stats, {coarse, {TName, Id}}),
    case lists:member(TName, ?PROC_STATS_TABLES) of
        true  -> ets:delete(rabbit_mgmt_stats_tables:key_index(TName), Id);
        false -> true
    end.

handle_consumer(Fun, Props) ->
    P = rabbit_mgmt_format:format(Props, {[], false}),
    CTag = pget(consumer_tag, P),
    Q    = pget(queue,        P),
    Ch   = pget(channel,      P),
    Fun(consumers_by_queue,  {Q, Ch, CTag}, P),
    Fun(consumers_by_channel, {Ch, Q, CTag}, P).

%% The consumer_deleted event is emitted by queues themselves -
%% therefore in the event that a queue dies suddenly we may not get
%% it. The best way to handle this is to make sure we also clean up
%% consumers when we hear about any queue going down.
delete_consumers(PrimId, PrimTableName, SecTableName) ->
    SecIdCTags = ets:match(PrimTableName, {{PrimId, '$1', '$2'}, '_'}),
    ets:match_delete(PrimTableName, {{PrimId, '_', '_'}, '_'}),
    delete_consumers_entry(PrimId, SecTableName, SecIdCTags).

delete_consumers_entry(PrimId, SecTableName, [[SecId, CTag] | SecIdTags]) ->
    ets:delete(SecTableName, {SecId, PrimId, CTag}),
    delete_consumers_entry(PrimId, SecTableName, SecIdTags);
delete_consumers_entry(_PrimId, _SecTableName, []) ->
    ok.

old_fine_stats(ChPid, Type, Props) ->
    case pget(Type, Props) of
        unknown       -> ignore;
        AllFineStats0 -> [begin
                              Id = fine_stats_id(ChPid, Ids),
                              {{fine, Id}, Stats, lookup_element(old_stats, {fine, Id})}
                          end || {Ids, Stats} <- AllFineStats0]
    end.

handle_fine_stats_list(ChPid, Timestamp, State, [AllStatsElem | AllStats]) ->
    handle_fine_stats(ChPid, Timestamp, AllStatsElem, State),
    handle_fine_stats_list(ChPid, Timestamp, State, AllStats);
handle_fine_stats_list(_ChPid, _Timestamp, _State, []) ->
    ok.

handle_fine_stats(_ChPid, _Timestamp, ignore, _State) ->
    ok;
handle_fine_stats(ChPid, Timestamp, [{Id, Stats, OldStats} | AllStats], State) ->
    Total = lists:sum([V || {K, V} <- Stats, lists:member(K, ?DELIVER_GET)]),
    Stats1 = case Total of
                 0 -> Stats;
                 _ -> [{deliver_get, Total}|Stats]
             end,
    append_all_samples(Timestamp, OldStats, Id, true, State, Stats1),
    ets:insert(old_stats, {Id, Stats1}),
    ets:insert(old_stats_fine_index, {ChPid, Id}),
    handle_fine_stats(ChPid, Timestamp, AllStats, State);
handle_fine_stats(_ChPid, _Timestamp, [], _State) ->
    ok.

delete_samples(Type, Id0) ->
    [rabbit_mgmt_stats:delete_stats(Table, Id0)
     || {Table, _} <- rabbit_mgmt_stats_tables:aggr_tables(Type)].

append_set_of_samples(Stats, TS, OldStats, Id, Keys, NoAggKeys, State) ->
    %% Refactored to avoid duplicated calls to ignore_coarse_sample, ceil and
    %% ets:insert(old_stats ...)
    case ignore_coarse_sample(Id, State) of
        false ->
            %% This ceil must correspond to the ceil in handle_event
            %% queue_deleted
            NewMS = ceil(TS, State),
            append_samples_by_keys(
              Stats, NewMS, OldStats, Id, Keys, true, State),
            append_samples_by_keys(
              Stats, NewMS, OldStats, Id, NoAggKeys, false, State),
            ets:insert(old_stats, {Id, Stats});
        true ->
            ok
    end.

append_samples_by_keys(Stats, TS, OldStats, Id, Keys, Agg, State) ->
    case Keys of
        all ->
            append_all_samples(TS, OldStats, Id, Agg, State, Stats);
        _   ->
            append_some_samples(TS, OldStats, Id, Agg, State, Stats, Keys)
    end.

append_some_samples(NewMS, OldStats, Id, Agg, State, Stats, [K | Keys]) ->
    V = pget(K, Stats),
    case V =/= 0 orelse lists:member(K, ?ALWAYS_REPORT_STATS) of
        true ->
            append_sample(K, V, NewMS, OldStats, Id, Agg, State);
        false ->
            ok
    end,
    append_some_samples(NewMS, OldStats, Id, Agg, State, Stats, Keys);
append_some_samples(_NewMS, _OldStats, _Id, _Agg, _State, _Stats, []) ->
    ok.

append_all_samples(NewMS, OldStats, Id, Agg, State, [{K, 0} | Stats]) ->
    case lists:member(K, ?ALWAYS_REPORT_STATS) of
        true ->
            append_sample(K, 0, NewMS, OldStats, Id, Agg, State);
        false ->
            ok
    end,
    append_all_samples(NewMS, OldStats, Id, Agg, State, Stats);
append_all_samples(NewMS, OldStats, Id, Agg, State, [{K, V} | Stats]) ->
    append_sample(K, V, NewMS, OldStats, Id, Agg, State),
    append_all_samples(NewMS, OldStats, Id, Agg, State, Stats);
append_all_samples(_NewMS, _OldStats, _Id, _Agg, _State, []) ->
    ok.

append_sample(Key, Val, NewMS, OldStats, Id, Agg, State) when is_number(Val) ->
    OldVal = case pget(Key, OldStats, 0) of
        N when is_number(N) -> N;
        _                   -> 0
    end,
    record_sample(Id, {Key, Val - OldVal, NewMS, State}, Agg, State),
    ok;
append_sample(_Key, _Value, _NewMS, _OldStats, _Id, _Agg, _State) ->
    ok.

ignore_coarse_sample({coarse, {queue_stats, Q}}, State) ->
    not object_exists(Q, State);
ignore_coarse_sample(_, _) ->
    false.


record_sample_list(Id, OldStats, TS, State, [Key | Keys]) ->
    record_sample(Id, {Key, -pget(Key, OldStats, 0), TS, State}, true, State),
    record_sample_list(Id, OldStats, TS, State, Keys);
record_sample_list(_Id, _OldStats, _TS, _State, []) ->
    ok.

%% Node stats do not have a vhost of course
record_sample({coarse, {node_stats, _Node} = Id}, Args, true, _State) ->
    record_sample0(Id, Args);

record_sample({coarse, {node_node_stats, _Names} = Id}, Args, true, _State) ->
    record_sample0(Id, Args);

record_sample({coarse, Id}, Args, false, _State) ->
    record_sample0(Id, Args);

record_sample({coarse, Id}, Args, true, _State) ->
    record_sample0(Id, Args),
    record_sample0({vhost_stats, vhost(Id)}, Args);

%% Deliveries / acks (Q -> Ch)
record_sample({fine, {Ch, Q = #resource{kind = queue}}}, Args, true, State) ->
    case object_exists(Q, State) of
        true  -> record_sample0({channel_queue_stats, {Ch, Q}}, Args),
                 record_sample0({queue_stats,         Q},       Args);
        false -> ok
    end,
    record_sample0({channel_stats, Ch},       Args),
    record_sample0({vhost_stats,   vhost(Q)}, Args);

%% Publishes / confirms (Ch -> X)
record_sample({fine, {Ch, X = #resource{kind = exchange}}}, Args, true,State) ->
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
                      X = #resource{kind = exchange}}}, Args, true, State) ->
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

vhost(#resource{virtual_host = VHost}) ->
    VHost;
vhost({queue_stats, #resource{virtual_host = VHost}}) ->
    VHost;
vhost({TName, Pid}) ->
    pget(vhost, lookup_element(TName, {Pid, create})).

%% exchanges have two sets of "publish" stats, so rearrange things a touch
record_sampleX(RenamePublishTo, X, {publish, Diff, TS, State}) ->
    record_sample0({exchange_stats, X}, {RenamePublishTo, Diff, TS, State});
record_sampleX(_RenamePublishTo, X, {Type, Diff, TS, State}) ->
    record_sample0({exchange_stats, X}, {Type, Diff, TS, State}).

%% Ignore case where ID1 and ID2 are in a tuple, i.e. detailed stats,
%% when in basic mode
record_sample0({Type, {_ID1, _ID2}}, {_, _, _, #state{rates_mode = basic}})
  when Type =/= node_node_stats ->
    ok;
record_sample0({Type, Id0}, {Key0, Diff, TS, #state{}}) ->
    {Key, Pos} = stat_type(Key0),
    Id = {Id0, TS},
    rabbit_mgmt_stats:record(Id, Pos, Diff, Key,
                             rabbit_mgmt_stats_tables:aggr_table(Type, Key)).

%%------------------------------------------------------------------------------
%% @hidden
%% @doc Returns the type of the stat and the position in the tuple
%%
%% Uses the record definitions for simplicity, keeping track of the positions in
%% the tuple.
%% @end
%%------------------------------------------------------------------------------
stat_type(deliver) ->
    {deliver_get, #deliver_get.deliver};
stat_type(deliver_no_ack) ->
    {deliver_get, #deliver_get.deliver_no_ack};
stat_type(get) ->
    {deliver_get, #deliver_get.get};
stat_type(get_no_ack) ->
    {deliver_get, #deliver_get.get_no_ack};
stat_type(publish) ->
    {fine_stats, #fine_stats.publish};
stat_type(publish_in) ->
    {fine_stats, #fine_stats.publish_in};
stat_type(publish_out) ->
    {fine_stats, #fine_stats.publish_out};
stat_type(ack) ->
    {fine_stats, #fine_stats.ack};
stat_type(deliver_get) ->
    {fine_stats, #fine_stats.deliver_get};
stat_type(confirm) ->
    {fine_stats, #fine_stats.confirm};
stat_type(return_unroutable) ->
    {fine_stats, #fine_stats.return_unroutable};
stat_type(redeliver) ->
    {fine_stats, #fine_stats.redeliver};
stat_type(disk_reads) ->
    {queue_msg_rates, #queue_msg_rates.disk_reads};
stat_type(disk_writes) ->
    {queue_msg_rates, #queue_msg_rates.disk_writes};
stat_type(messages) ->
    {queue_msg_counts, #queue_msg_counts.messages};
stat_type(messages_ready) ->
    {queue_msg_counts, #queue_msg_counts.messages_ready};
stat_type(messages_unacknowledged) ->
    {queue_msg_counts, #queue_msg_counts.messages_unacknowledged};
stat_type(mem_used) ->
    {coarse_node_stats, #coarse_node_stats.mem_used};
stat_type(fd_used) ->
    {coarse_node_stats, #coarse_node_stats.fd_used};
stat_type(sockets_used) ->
    {coarse_node_stats, #coarse_node_stats.sockets_used};
stat_type(proc_used) ->
    {coarse_node_stats, #coarse_node_stats.proc_used};
stat_type(disk_free) ->
    {coarse_node_stats, #coarse_node_stats.disk_free};
stat_type(io_read_count) ->
    {coarse_node_stats, #coarse_node_stats.io_read_count};
stat_type(io_read_bytes) ->
    {coarse_node_stats, #coarse_node_stats.io_read_bytes};
stat_type(io_read_time) ->
    {coarse_node_stats, #coarse_node_stats.io_read_time};
stat_type(io_write_count) ->
    {coarse_node_stats, #coarse_node_stats.io_write_count};
stat_type(io_write_bytes) ->
    {coarse_node_stats, #coarse_node_stats.io_write_bytes};
stat_type(io_write_time) ->
    {coarse_node_stats, #coarse_node_stats.io_write_time};
stat_type(io_sync_count) ->
    {coarse_node_stats, #coarse_node_stats.io_sync_count};
stat_type(io_sync_time) ->
    {coarse_node_stats, #coarse_node_stats.io_sync_time};
stat_type(io_seek_count) ->
    {coarse_node_stats, #coarse_node_stats.io_seek_count};
stat_type(io_seek_time) ->
    {coarse_node_stats, #coarse_node_stats.io_seek_time};
stat_type(io_reopen_count) ->
    {coarse_node_stats, #coarse_node_stats.io_reopen_count};
stat_type(mnesia_ram_tx_count) ->
    {coarse_node_stats, #coarse_node_stats.mnesia_ram_tx_count};
stat_type(mnesia_disk_tx_count) ->
    {coarse_node_stats, #coarse_node_stats.mnesia_disk_tx_count};
stat_type(msg_store_read_count) ->
    {coarse_node_stats, #coarse_node_stats.msg_store_read_count};
stat_type(msg_store_write_count) ->
    {coarse_node_stats, #coarse_node_stats.msg_store_write_count};
stat_type(queue_index_journal_write_count) ->
    {coarse_node_stats, #coarse_node_stats.queue_index_journal_write_count};
stat_type(queue_index_write_count) ->
    {coarse_node_stats, #coarse_node_stats.queue_index_write_count};
stat_type(queue_index_read_count) ->
    {coarse_node_stats, #coarse_node_stats.queue_index_read_count};
stat_type(gc_num) ->
    {coarse_node_stats, #coarse_node_stats.gc_num};
stat_type(gc_bytes_reclaimed) ->
    {coarse_node_stats, #coarse_node_stats.gc_bytes_reclaimed};
stat_type(context_switches) ->
    {coarse_node_stats, #coarse_node_stats.context_switches};
stat_type(send_bytes) ->
    {coarse_node_node_stats, #coarse_node_node_stats.send_bytes};
stat_type(recv_bytes) ->
    {coarse_node_node_stats, #coarse_node_node_stats.recv_bytes};
stat_type(recv_oct) ->
    {coarse_conn_stats, #coarse_conn_stats.recv_oct};
stat_type(send_oct) ->
    {coarse_conn_stats, #coarse_conn_stats.send_oct};
stat_type(reductions) ->
    {process_stats, #process_stats.reductions};
stat_type(io_file_handle_open_attempt_count) ->
    {coarse_node_stats, #coarse_node_stats.io_file_handle_open_attempt_count};
stat_type(io_file_handle_open_attempt_time) ->
    {coarse_node_stats, #coarse_node_stats.io_file_handle_open_attempt_time}.
