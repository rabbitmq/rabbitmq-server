%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2021 VMware, Inc. or its affiliates.  All rights reserved.
%%
-module(rabbit_mgmt_metrics_collector).

-include_lib("rabbit_common/include/rabbit.hrl").
-include_lib("rabbit_common/include/rabbit_core_metrics.hrl").
-include("rabbit_mgmt_metrics.hrl").

-behaviour(gen_server).
-compile({no_auto_import, [ceil/1]}).

-spec start_link(atom()) -> rabbit_types:ok_pid_or_error().

-export([name/1]).
-export([start_link/1]).
-export([override_lookups/2, reset_lookups/1]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2,
         code_change/3]).
-export([index_table/2]).
-export([reset_all/0]).

-import(rabbit_mgmt_data, [lookup_element/3]).

-record(state, {table, interval, policies, rates_mode, lookup_queue,
                lookup_exchange, old_aggr_stats}).

%% Data is stored in ETS tables:
%% * One ETS table per metric (queue_stats, channel_stats_deliver_stats...)
%%   (see ?TABLES in rabbit_mgmt_metrics.hrl)
%% * Stats are stored as key value pairs where the key is a tuple of
%%  some value (such as a channel pid) and the retention interval.
%%  The value is an instance of an exometer_slide providing a sliding window
%%  of samples for some {Object, Interval}.
%% * Each slide can store multiple stats. See stats_per_table in
%%  rabbit_mgmt_metrics.hrl for a map of which stats are recorded in which
%%  table.

reset_all() ->
    [reset(Table) || {Table, _} <- ?CORE_TABLES].

reset(Table) ->
    gen_server:cast(name(Table), reset).

name(Table) ->
    list_to_atom((atom_to_list(Table) ++ "_metrics_collector")).


start_link(Table) ->
    gen_server:start_link({local, name(Table)}, ?MODULE, [Table], []).

override_lookups(Table, Lookups) ->
    gen_server:call(name(Table), {override_lookups, Lookups}, infinity).

reset_lookups(Table) ->
    gen_server:call(name(Table), reset_lookups, infinity).

init([Table]) ->
    {RatesMode, Policies} = load_config(),
    Policy = retention_policy(Table),
    Interval = take_smaller(proplists:get_value(Policy, Policies, [])) * 1000,
    erlang:send_after(Interval, self(), collect_metrics),
    {ok, #state{table = Table, interval = Interval,
                policies = {proplists:get_value(basic, Policies),
                            proplists:get_value(detailed, Policies),
                            proplists:get_value(global, Policies)},
                rates_mode = RatesMode,
                old_aggr_stats = #{},
                lookup_queue = fun queue_exists/1,
                lookup_exchange = fun exchange_exists/1}}.

handle_call(reset_lookups, _From, State) ->
    {reply, ok, State#state{lookup_queue = fun queue_exists/1,
                            lookup_exchange = fun exchange_exists/1}};
handle_call({override_lookups, Lookups}, _From, State) ->
    {reply, ok, State#state{lookup_queue = pget(queue, Lookups),
                            lookup_exchange = pget(exchange, Lookups)}};
handle_call({submit, Fun}, _From, State) ->
    {reply, Fun(), State};
handle_call(_Request, _From, State) ->
    {noreply, State}.

handle_cast(reset, State) ->
    {noreply, State#state{old_aggr_stats = #{}}};
handle_cast(_Msg, State) ->
    {noreply, State}.


handle_info(collect_metrics, #state{interval = Interval} = State0) ->
    Timestamp = exometer_slide:timestamp(),
    State = aggregate_metrics(Timestamp, State0),
    erlang:send_after(Interval, self(), collect_metrics),
    {noreply, State};
handle_info(purge_old_stats, State) ->
    {noreply, State#state{old_aggr_stats = #{}}};
handle_info(_Msg, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

retention_policy(connection_created) -> basic; %% really nothing
retention_policy(connection_metrics) -> basic;
retention_policy(connection_coarse_metrics) -> basic;
retention_policy(channel_created) -> basic;
retention_policy(channel_metrics) -> basic;
retention_policy(channel_queue_exchange_metrics) -> detailed;
retention_policy(channel_exchange_metrics) -> detailed;
retention_policy(channel_queue_metrics) -> detailed;
retention_policy(channel_process_metrics) -> basic;
retention_policy(consumer_created) -> basic;
retention_policy(queue_metrics) -> basic;
retention_policy(queue_coarse_metrics) -> basic;
retention_policy(node_persister_metrics) -> global;
retention_policy(node_coarse_metrics) -> global;
retention_policy(node_metrics) -> basic;
retention_policy(node_node_metrics) -> global;
retention_policy(connection_churn_metrics) -> basic.

take_smaller(Policies) ->
    Intervals = [I || {_, I} <- Policies],
    case Intervals of
        [] -> throw(missing_sample_retention_policies);
        _ -> lists:min(Intervals)
    end.

insert_old_aggr_stats(NextStats, Id, Stat) ->
    NextStats#{Id => Stat}.

handle_deleted_queues(queue_coarse_metrics, Remainders,
                      #state{policies = {BPolicies, _, GPolicies}}) ->
    TS = exometer_slide:timestamp(),
    lists:foreach(fun ({Queue, {R, U, M}}) ->
                            NegStats = ?vhost_msg_stats(-R, -U, -M),
                            [insert_entry(vhost_msg_stats, vhost(Queue), TS,
                                          NegStats, Size, Interval, true)
                             || {Size, Interval} <- GPolicies],
                            % zero out msg stats to avoid duplicating msg
                            % stats when a master queue is migrated
                            QNegStats = ?queue_msg_stats(0, 0, 0),
                            [insert_entry(queue_msg_stats, Queue, TS,
                                          QNegStats, Size, Interval, false)
                             || {Size, Interval} <- BPolicies],
                            ets:delete(queue_stats, Queue),
                            ets:delete(queue_process_stats, Queue)
                  end, maps:to_list(Remainders));
handle_deleted_queues(_T, _R, _P) -> ok.

aggregate_metrics(Timestamp, #state{table = Table,
                                    policies = {_, _, _GPolicies}} = State0) ->
    Table = State0#state.table,
    {Next, Ops, #state{old_aggr_stats = Remainders}} =
        ets:foldl(fun(R, {NextStats, O, State}) ->
                          aggregate_entry(R, NextStats, O, State)
                  end, {#{}, #{}, State0}, Table),
    maps:fold(fun(Tbl, TblOps, Acc) ->
                      _ = exec_table_ops(Tbl, Timestamp, TblOps),
                      Acc
              end, no_acc, Ops),

    handle_deleted_queues(Table, Remainders, State0),
    State0#state{old_aggr_stats = Next}.

exec_table_ops(Table, Timestamp, TableOps) ->
    maps:fold(fun(_Id, {insert, Entry}, A) ->
                      ets:insert(Table, Entry),
                      A;
                 (Id, {insert_with_index, Entry}, A) ->
                      insert_with_index(Table, Id, Entry),
                      A;
                 ({Id, Size, Interval, Incremental},
                  {insert_entry, Entry}, A) ->
                      insert_entry(Table, Id, Timestamp, Entry, Size,
                                   Interval, Incremental),
                      A
              end, no_acc, TableOps).


aggregate_entry({Id, Metrics}, NextStats, Ops0,
                #state{table = connection_created} = State) ->
    case ets:lookup(connection_created_stats, Id) of
        [] ->
            Ftd = rabbit_mgmt_format:format(
                    Metrics,
                    {fun rabbit_mgmt_format:format_connection_created/1, true}),
            Entry = ?connection_created_stats(Id, pget(name, Ftd, unknown), Ftd),
            Ops = insert_op(connection_created_stats, Id, Entry, Ops0),
            {NextStats, Ops, State};
        _ ->
            {NextStats, Ops0, State}
    end;
aggregate_entry({Id, Metrics}, NextStats, Ops0,
                #state{table = connection_metrics} = State) ->
    Entry = ?connection_stats(Id, Metrics),
    Ops = insert_op(connection_stats, Id, Entry, Ops0),
    {NextStats, Ops, State};
aggregate_entry({Id, RecvOct, SendOct, Reductions, 0}, NextStats, Ops0,
                #state{table = connection_coarse_metrics,
                       policies = {BPolicies, _, GPolicies}} = State) ->
    Stats = ?vhost_stats_coarse_conn_stats(RecvOct, SendOct),
    Diff = get_difference(Id, Stats, State),

    Ops1 = insert_entry_ops(vhost_stats_coarse_conn_stats,
                            vhost({connection_created, Id}), true, Diff, Ops0,
                            GPolicies),

    Entry = ?connection_stats_coarse_conn_stats(RecvOct, SendOct, Reductions),
    Ops2 = insert_entry_ops(connection_stats_coarse_conn_stats, Id, false, Entry,
                            Ops1, BPolicies),
    {insert_old_aggr_stats(NextStats, Id, Stats), Ops2, State};
aggregate_entry({Id, RecvOct, SendOct, _Reductions, 1}, NextStats, Ops0,
                #state{table = connection_coarse_metrics,
                       policies = {_BPolicies, _, GPolicies}} = State) ->
    Stats = ?vhost_stats_coarse_conn_stats(RecvOct, SendOct),
    Diff = get_difference(Id, Stats, State),
    Ops1 = insert_entry_ops(vhost_stats_coarse_conn_stats,
                            vhost({connection_created, Id}), true, Diff, Ops0,
                            GPolicies),
    rabbit_core_metrics:delete(connection_coarse_metrics, Id),
    {NextStats, Ops1, State};
aggregate_entry({Id, Metrics}, NextStats, Ops0,
                #state{table = channel_created} = State) ->
    case ets:lookup(channel_created_stats, Id) of
        [] ->
            Ftd = rabbit_mgmt_format:format(Metrics, {[], false}),
            Entry = ?channel_created_stats(Id, pget(name, Ftd, unknown), Ftd),
            Ops = insert_op(channel_created_stats, Id, Entry, Ops0),
            {NextStats, Ops, State};
        _ ->
            {NextStats, Ops0, State}
    end;
aggregate_entry({Id, Metrics}, NextStats, Ops0,
                #state{table = channel_metrics} = State) ->
    %% First metric must be `idle_since` (if available), as expected by
    %% `rabbit_mgmt_format:format_channel_stats`. This is a performance
    %% optimisation that avoids traversing the whole list when only
    %% one element has to be formatted.
    Ftd = rabbit_mgmt_format:format_channel_stats(Metrics),
    Entry = ?channel_stats(Id, Ftd),
    Ops = insert_op(channel_stats, Id, Entry, Ops0),
    {NextStats, Ops, State};
aggregate_entry({{Ch, X} = Id, Publish0, Confirm, ReturnUnroutable, DropUnroutable, 0},
                NextStats, Ops0,
                #state{table = channel_exchange_metrics,
                       policies = {BPolicies, DPolicies, GPolicies},
                       rates_mode = RatesMode,
                       lookup_exchange = ExchangeFun} = State) ->
    Stats = ?channel_stats_fine_stats(Publish0, Confirm, ReturnUnroutable, DropUnroutable),
    {Publish, _, _, _} = Diff = get_difference(Id, Stats, State),

    Ops1 = insert_entry_ops(channel_stats_fine_stats, Ch, true, Diff, Ops0,
                            BPolicies),
    Ops2 = insert_entry_ops(vhost_stats_fine_stats, vhost(X), true, Diff, Ops1,
                            GPolicies),
    Ops3 = case {ExchangeFun(X), RatesMode} of
               {true, basic} ->
                   Entry = ?exchange_stats_publish_in(Publish),
                   insert_entry_ops(exchange_stats_publish_in, X, true, Entry,
                                    Ops2, DPolicies);
               {true, _} ->
                   Entry = ?exchange_stats_publish_in(Publish),
                   O = insert_entry_ops(exchange_stats_publish_in, X, true,
                                        Entry, Ops2, DPolicies),
                   insert_entry_ops(channel_exchange_stats_fine_stats, Id,
                                    false, Stats, O, DPolicies);
               _ ->
                   Ops2
           end,
    {insert_old_aggr_stats(NextStats, Id, Stats), Ops3, State};
aggregate_entry({{_Ch, X} = Id, Publish0, Confirm, ReturnUnroutable, DropUnroutable, 1},
                NextStats, Ops0,
                #state{table = channel_exchange_metrics,
                       policies = {_BPolicies, DPolicies, GPolicies},
                       lookup_exchange = ExchangeFun} = State) ->
    Stats = ?channel_stats_fine_stats(Publish0, Confirm, ReturnUnroutable, DropUnroutable),
    {Publish, _, _, _} = Diff = get_difference(Id, Stats, State),
    Ops1 = insert_entry_ops(vhost_stats_fine_stats, vhost(X), true, Diff, Ops0,
                            GPolicies),
    Ops2 = case ExchangeFun(X) of
               true ->
                   Entry = ?exchange_stats_publish_in(Publish),
                   insert_entry_ops(exchange_stats_publish_in, X, true, Entry,
                                    Ops1, DPolicies);
               _ ->
                   Ops1
           end,
    rabbit_core_metrics:delete(channel_exchange_metrics, Id),
    {NextStats, Ops2, State};
aggregate_entry({{Ch, Q} = Id, Get, GetNoAck, Deliver, DeliverNoAck,
                     Redeliver, Ack, GetEmpty, 0}, NextStats, Ops0,
                #state{table = channel_queue_metrics,
                       policies = {BPolicies, DPolicies, GPolicies},
                       rates_mode = RatesMode,
                       lookup_queue = QueueFun} = State) ->
    Stats = ?vhost_stats_deliver_stats(Get, GetNoAck, Deliver, DeliverNoAck,
                                       Redeliver, Ack,
                                       Deliver + DeliverNoAck + Get + GetNoAck,
                                       GetEmpty),
    Diff = get_difference(Id, Stats, State),

    Ops1 = insert_entry_ops(vhost_stats_deliver_stats, vhost(Q), true, Diff,
                            Ops0, GPolicies),

    Ops2 = insert_entry_ops(channel_stats_deliver_stats, Ch, true, Diff, Ops1,
                            BPolicies),

    Ops3 = case {QueueFun(Q), RatesMode} of
               {true, basic} ->
                   insert_entry_ops(queue_stats_deliver_stats, Q, true, Diff,
                                    Ops2, BPolicies);
               {true, _} ->
                   O = insert_entry_ops(queue_stats_deliver_stats, Q, true,
                                        Diff, Ops2, BPolicies),
                   insert_entry_ops(channel_queue_stats_deliver_stats, Id,
                                    false, Stats, O, DPolicies);
               _ ->
                   Ops2
           end,
     {insert_old_aggr_stats(NextStats, Id, Stats), Ops3, State};
aggregate_entry({{_, Q} = Id, Get, GetNoAck, Deliver, DeliverNoAck,
                     Redeliver, Ack, GetEmpty, 1}, NextStats, Ops0,
                #state{table = channel_queue_metrics,
                       policies = {BPolicies, _, GPolicies},
                       lookup_queue = QueueFun} = State) ->
    Stats = ?vhost_stats_deliver_stats(Get, GetNoAck, Deliver, DeliverNoAck,
                                       Redeliver, Ack,
                                       Deliver + DeliverNoAck + Get + GetNoAck,
                                       GetEmpty),
    Diff = get_difference(Id, Stats, State),

    Ops1 = insert_entry_ops(vhost_stats_deliver_stats, vhost(Q), true, Diff,
                            Ops0, GPolicies),
    Ops2 = case QueueFun(Q) of
               true ->
                   insert_entry_ops(queue_stats_deliver_stats, Q, true, Diff,
                                    Ops1, BPolicies);
               _ ->
                   Ops1
           end,
    rabbit_core_metrics:delete(channel_queue_metrics, Id),
    {NextStats, Ops2, State};
aggregate_entry({{_Ch, {Q, X}} = Id, Publish, ToDelete}, NextStats, Ops0,
                #state{table = channel_queue_exchange_metrics,
                       policies = {BPolicies, _, _},
                       rates_mode = RatesMode,
                       lookup_queue = QueueFun,
                       lookup_exchange = ExchangeFun} = State) ->
    Stats = ?queue_stats_publish(Publish),
    Diff = get_difference(Id, Stats, State),
    Ops1 = case {QueueFun(Q), ExchangeFun(X), RatesMode, ToDelete} of
               {true, false, _, _} ->
                   insert_entry_ops(queue_stats_publish, Q, true, Diff,
                                    Ops0, BPolicies);
               {false, true, _, _} ->
                   insert_entry_ops(exchange_stats_publish_out, X, true, Diff,
                                    Ops0, BPolicies);
               {true, true, basic, _} ->
                   O = insert_entry_ops(queue_stats_publish, Q, true, Diff,
                                        Ops0, BPolicies),
                   insert_entry_ops(exchange_stats_publish_out, X, true, Diff,
                                    O, BPolicies);
               {true, true, _, 0} ->
                   O1 = insert_entry_ops(queue_stats_publish, Q, true, Diff,
                                         Ops0, BPolicies),
                   O2 = insert_entry_ops(exchange_stats_publish_out, X, true,
                                         Diff, O1, BPolicies),
                   insert_entry_ops(queue_exchange_stats_publish, {Q, X},
                                         true, Diff, O2, BPolicies);
               {true, true, _, 1} ->
                   O = insert_entry_ops(queue_stats_publish, Q, true, Diff,
                                        Ops0, BPolicies),
                   insert_entry_ops(exchange_stats_publish_out, X, true,
                                    Diff, O, BPolicies);
               _ ->
                   Ops0
           end,
    case ToDelete of
        0 ->
            {insert_old_aggr_stats(NextStats, Id, Stats), Ops1, State};
        1 ->
            rabbit_core_metrics:delete(channel_queue_exchange_metrics, Id),
            {NextStats, Ops1, State}
    end;
aggregate_entry({Id, Reductions}, NextStats, Ops0,
                #state{table = channel_process_metrics,
                       policies = {BPolicies, _, _}} = State) ->
    Entry = ?channel_process_stats(Reductions),
    Ops = insert_entry_ops(channel_process_stats, Id, false,
                           Entry, Ops0, BPolicies),
    {NextStats, Ops, State};
aggregate_entry({Id, Exclusive, AckRequired, PrefetchCount,
                 Active, ActivityStatus, Args},
                NextStats, Ops0,
                #state{table = consumer_created} = State) ->
    case ets:lookup(consumer_stats, Id) of
        [] ->
            Fmt = rabbit_mgmt_format:format([{exclusive, Exclusive},
                                             {ack_required, AckRequired},
                                             {prefetch_count, PrefetchCount},
                                             {active, Active},
                                             {activity_status, ActivityStatus},
                                             {arguments, Args}], {[], false}),
            Entry = ?consumer_stats(Id, Fmt),
            Ops = insert_with_index_op(consumer_stats, Id, Entry, Ops0),
            {NextStats, Ops , State};
        [{_K, V}] ->
            CurrentActive = proplists:get_value(active, V, undefined),
            case Active =:= CurrentActive of
                false ->
                    Fmt = rabbit_mgmt_format:format([{exclusive, Exclusive},
                                                     {ack_required, AckRequired},
                                                     {prefetch_count, PrefetchCount},
                                                     {active, Active},
                                                     {activity_status, ActivityStatus},
                                                     {arguments, Args}], {[], false}),
                    Entry = ?consumer_stats(Id, Fmt),
                    Ops = insert_with_index_op(consumer_stats, Id, Entry, Ops0),
                    {NextStats, Ops , State};
                _ ->
                    {NextStats, Ops0, State}
            end;
        _ ->
            {NextStats, Ops0, State}
    end;
aggregate_entry({Id, Metrics, 0}, NextStats, Ops0,
                #state{table = queue_metrics,
                       policies = {BPolicies, _, GPolicies},
                       lookup_queue = QueueFun} = State) ->
    Stats = ?queue_msg_rates(pget(disk_reads, Metrics, 0),
                             pget(disk_writes, Metrics, 0)),
    Diff = get_difference(Id, Stats, State),
    Ops1 = insert_entry_ops(vhost_msg_rates, vhost(Id), true, Diff, Ops0,
                            GPolicies),
    Ops2 = case QueueFun(Id) of
               true ->
                   O = insert_entry_ops(queue_msg_rates, Id, false, Stats, Ops1,
                                        BPolicies),
                   Fmt = rabbit_mgmt_format:format(
                           Metrics,
                           {fun rabbit_mgmt_format:format_queue_stats/1, false}),
                   insert_op(queue_stats, Id, ?queue_stats(Id, Fmt), O);
               false ->
                   Ops1
           end,
    {insert_old_aggr_stats(NextStats, Id, Stats), Ops2, State};
aggregate_entry({Id, Metrics, 1}, NextStats, Ops0,
                #state{table = queue_metrics,
                       policies = {_, _, GPolicies}} = State) ->
    Stats = ?queue_msg_rates(pget(disk_reads, Metrics, 0),
                             pget(disk_writes, Metrics, 0)),
    Diff = get_difference(Id, Stats, State),
    Ops = insert_entry_ops(vhost_msg_rates, vhost(Id), true, Diff, Ops0,
                           GPolicies),
    rabbit_core_metrics:delete(queue_metrics, Id),
    {NextStats, Ops, State};
aggregate_entry({Name, Ready, Unack, Msgs, Red}, NextStats, Ops0,
                #state{table = queue_coarse_metrics,
                       old_aggr_stats = Old,
                       policies = {BPolicies, _, GPolicies},
                       lookup_queue = QueueFun} = State) ->
    Stats = ?vhost_msg_stats(Ready, Unack, Msgs),
    Diff = get_difference(Name, Stats, State),
    Ops1 = insert_entry_ops(vhost_msg_stats, vhost(Name), true, Diff, Ops0,
                            GPolicies),
    Ops2 = case QueueFun(Name) of
               true ->
                   QPS =?queue_process_stats(Red),
                   O1 = insert_entry_ops(queue_process_stats, Name, false, QPS,
                                         Ops1, BPolicies),
                   QMS = ?queue_msg_stats(Ready, Unack, Msgs),
                   insert_entry_ops(queue_msg_stats, Name, false, QMS,
                                    O1, BPolicies);
               _ ->
                   Ops1
           end,
    State1 = State#state{old_aggr_stats = maps:remove(Name, Old)},
    {insert_old_aggr_stats(NextStats, Name, Stats), Ops2, State1};
aggregate_entry({Id, Metrics}, NextStats, Ops0,
                #state{table = node_metrics} = State) ->
    Ops = insert_op(node_stats, Id, {Id, Metrics}, Ops0),
    {NextStats, Ops, State};
aggregate_entry({Id, Metrics}, NextStats, Ops0,
                #state{table = node_coarse_metrics,
                       policies = {_, _, GPolicies}} = State) ->
    Stats = ?node_coarse_stats(
               pget(fd_used, Metrics, 0), pget(sockets_used, Metrics, 0),
               pget(mem_used, Metrics, 0), pget(disk_free, Metrics, 0),
               pget(proc_used, Metrics, 0), pget(gc_num, Metrics, 0),
               pget(gc_bytes_reclaimed, Metrics, 0),
               pget(context_switches, Metrics, 0)),
    Ops = insert_entry_ops(node_coarse_stats, Id, false, Stats, Ops0,
                           GPolicies),
    {NextStats, Ops, State};
aggregate_entry({Id, Metrics}, NextStats, Ops0,
                #state{table = node_persister_metrics,
                       policies = {_, _, GPolicies}} = State) ->
    Stats = ?node_persister_stats(
               pget(io_read_count, Metrics, 0), pget(io_read_bytes, Metrics, 0),
               pget(io_read_time, Metrics, 0), pget(io_write_count, Metrics, 0),
               pget(io_write_bytes, Metrics, 0), pget(io_write_time, Metrics, 0),
               pget(io_sync_count, Metrics, 0), pget(io_sync_time, Metrics, 0),
               pget(io_seek_count, Metrics, 0), pget(io_seek_time, Metrics, 0),
               pget(io_reopen_count, Metrics, 0), pget(mnesia_ram_tx_count, Metrics, 0),
               pget(mnesia_disk_tx_count, Metrics, 0), pget(msg_store_read_count, Metrics, 0),
               pget(msg_store_write_count, Metrics, 0),
               pget(queue_index_journal_write_count, Metrics, 0),
               pget(queue_index_write_count, Metrics, 0), pget(queue_index_read_count, Metrics, 0),
               pget(io_file_handle_open_attempt_count, Metrics, 0),
               pget(io_file_handle_open_attempt_time, Metrics, 0)),
    Ops = insert_entry_ops(node_persister_stats, Id, false, Stats, Ops0,
                           GPolicies),
    {NextStats, Ops, State};
aggregate_entry({Id, Metrics}, NextStats, Ops0,
                #state{table = node_node_metrics,
                       policies = {_, _, GPolicies}} = State) ->
    Stats = ?node_node_coarse_stats(pget(send_bytes, Metrics, 0),
                                    pget(recv_bytes, Metrics, 0)),
    CleanMetrics = lists:keydelete(recv_bytes, 1,
                                   lists:keydelete(send_bytes, 1, Metrics)),
    Ops1 = insert_op(node_node_stats, Id, ?node_node_stats(Id, CleanMetrics),
                     Ops0),
    Ops = insert_entry_ops(node_node_coarse_stats, Id, false, Stats, Ops1,
                           GPolicies),
    {NextStats, Ops, State};
aggregate_entry({Id, ConnCreated, ConnClosed, ChCreated, ChClosed,
                 QueueDeclared, QueueCreated, QueueDeleted}, NextStats, Ops0,
                #state{table = connection_churn_metrics,
                       policies = {_, _, GPolicies}} = State) ->
    %% Id is the local node. There is only one entry on every ETS table.
    Stats = ?connection_churn_rates(ConnCreated, ConnClosed, ChCreated, ChClosed,
                                    QueueDeclared, QueueCreated, QueueDeleted),
    Diff = get_difference(Id, Stats, State),
    Ops = insert_entry_ops(connection_churn_rates, Id, true, Diff, Ops0,
                           GPolicies),
    {insert_old_aggr_stats(NextStats, Id, Stats), Ops, State}.

insert_entry(Table, Id, TS, Entry, Size, Interval0, Incremental) ->
    Key = {Id, Interval0},
    Slide =
        case ets:lookup(Table, Key) of
            [{Key, S}] ->
                S;
            [] ->
                IntervalMs = Interval0 * 1000,
                % add some margin to Size and max_n to reduce chances of off-by-one errors
                exometer_slide:new(TS - IntervalMs, (Size + Interval0) * 1000,
                                   [{interval, IntervalMs},
                                    {max_n, ceil(Size / Interval0) + 1},
                                    {incremental, Incremental}])
        end,
    insert_with_index(Table, Key, {Key, exometer_slide:add_element(TS, Entry,
                                                                   Slide)}).

update_op(Table, Key, Op, Ops) ->
    TableOps = case maps:find(Table, Ops) of
                   {ok, Inner} ->
                       maps:put(Key, Op, Inner);
                   error ->
                       Inner = #{},
                       maps:put(Key, Op, Inner)
               end,
    maps:put(Table, TableOps, Ops).

insert_with_index_op(Table, Key, Entry, Ops) ->
    update_op(Table, Key, {insert_with_index, Entry}, Ops).

insert_op(Table, Key, Entry, Ops) ->
    update_op(Table, Key, {insert, Entry}, Ops).

insert_entry_op(Table, Key, Entry, Ops) ->
    TableOps0 = case maps:find(Table, Ops) of
                    {ok, Inner} -> Inner;
                    error -> #{}
                end,
    TableOps = maps:update_with(Key, fun({insert_entry, Entry0}) ->
                                             {insert_entry, sum_entry(Entry0, Entry)}
                                     end, {insert_entry, Entry}, TableOps0),
    maps:put(Table, TableOps, Ops).

insert_entry_ops(Table, Id, Incr, Entry, Ops, Policies) ->
    lists:foldl(fun({Size, Interval}, Acc) ->
                        Key = {Id, Size, Interval, Incr},
                        insert_entry_op(Table, Key, Entry, Acc)
                end, Ops, Policies).

get_difference(Id, Stats, #state{old_aggr_stats = OldStats}) ->
    case maps:find(Id, OldStats) of
        error ->
            Stats;
        {ok, OldStat} ->
            difference(OldStat, Stats)
    end.

sum_entry({A0}, {B0}) ->
    {B0 + A0};
sum_entry({A0, A1}, {B0, B1}) ->
    {B0 + A0, B1 + A1};
sum_entry({A0, A1, A2}, {B0, B1, B2}) ->
    {B0 + A0, B1 + A1, B2 + A2};
sum_entry({A0, A1, A2, A3}, {B0, B1, B2, B3}) ->
    {B0 + A0, B1 + A1, B2 + A2, B3 + A3};
sum_entry({A0, A1, A2, A3, A4}, {B0, B1, B2, B3, B4}) ->
    {B0 + A0, B1 + A1, B2 + A2, B3 + A3, B4 + A4};
sum_entry({A0, A1, A2, A3, A4, A5}, {B0, B1, B2, B3, B4, B5}) ->
    {B0 + A0, B1 + A1, B2 + A2, B3 + A3, B4 + A4, B5 + A5};
sum_entry({A0, A1, A2, A3, A4, A5, A6}, {B0, B1, B2, B3, B4, B5, B6}) ->
    {B0 + A0, B1 + A1, B2 + A2, B3 + A3, B4 + A4, B5 + A5, B6 + A6};
sum_entry({A0, A1, A2, A3, A4, A5, A6, A7}, {B0, B1, B2, B3, B4, B5, B6, B7}) ->
    {B0 + A0, B1 + A1, B2 + A2, B3 + A3, B4 + A4, B5 + A5, B6 + A6, B7 + A7}.

difference({A0}, {B0}) ->
    {B0 - A0};
difference({A0, A1}, {B0, B1}) ->
    {B0 - A0, B1 - A1};
difference({A0, A1, A2}, {B0, B1, B2}) ->
    {B0 - A0, B1 - A1, B2 - A2};
difference({A0, A1, A2, A3}, {B0, B1, B2, B3}) ->
    {B0 - A0, B1 - A1, B2 - A2, B3 - A3};
difference({A0, A1, A2, A3, A4}, {B0, B1, B2, B3, B4}) ->
    {B0 - A0, B1 - A1, B2 - A2, B3 - A3, B4 - A4};
difference({A0, A1, A2, A3, A4, A5}, {B0, B1, B2, B3, B4, B5}) ->
    {B0 - A0, B1 - A1, B2 - A2, B3 - A3, B4 - A4, B5 - A5};
difference({A0, A1, A2, A3, A4, A5, A6}, {B0, B1, B2, B3, B4, B5, B6}) ->
    {B0 - A0, B1 - A1, B2 - A2, B3 - A3, B4 - A4, B5 - A5, B6 - A6};
difference({A0, A1, A2, A3, A4, A5, A6, A7}, {B0, B1, B2, B3, B4, B5, B6, B7}) ->
    {B0 - A0, B1 - A1, B2 - A2, B3 - A3, B4 - A4, B5 - A5, B6 - A6, B7 - A7}.

vhost(#resource{virtual_host = VHost}) ->
    VHost;
vhost({queue_stats, #resource{virtual_host = VHost}}) ->
    VHost;
vhost({TName, Pid}) ->
    pget(vhost, lookup_element(TName, Pid, 2)).

exchange_exists(Name) ->
    case rabbit_exchange:lookup(Name) of
        {ok, _} ->
            true;
        _ ->
            false
    end.

queue_exists(Name) ->
    case rabbit_amqqueue:lookup(Name) of
        {ok, _} ->
            true;
        _ ->
            false
    end.

insert_with_index(Table, Key, Tuple) ->
    Insert = ets:insert(Table, Tuple),
    insert_index(Table, Key),
    Insert.

insert_index(consumer_stats, {Q, Ch, _} = Key) ->
    ets:insert(index_table(consumer_stats, queue), {Q, Key}),
    ets:insert(index_table(consumer_stats, channel), {Ch, Key});
insert_index(channel_exchange_stats_fine_stats, {{Ch, Ex}, _} = Key) ->
    ets:insert(index_table(channel_exchange_stats_fine_stats, exchange), {Ex, Key}),
    ets:insert(index_table(channel_exchange_stats_fine_stats, channel),  {Ch, Key});
insert_index(channel_queue_stats_deliver_stats, {{Ch, Q}, _} = Key) ->
    ets:insert(index_table(channel_queue_stats_deliver_stats, queue),   {Q, Key}),
    ets:insert(index_table(channel_queue_stats_deliver_stats, channel), {Ch, Key});
insert_index(queue_exchange_stats_publish, {{Q, Ex}, _} = Key) ->
    ets:insert(index_table(queue_exchange_stats_publish, queue),    {Q, Key}),
    ets:insert(index_table(queue_exchange_stats_publish, exchange), {Ex, Key});
insert_index(node_node_coarse_stats, {{_, Node}, _} = Key) ->
    ets:insert(index_table(node_node_coarse_stats, node), {Node, Key});
insert_index(_, _) -> ok.

index_table(consumer_stats, queue) -> consumer_stats_queue_index;
index_table(consumer_stats, channel) -> consumer_stats_channel_index;
index_table(channel_exchange_stats_fine_stats, exchange) -> channel_exchange_stats_fine_stats_exchange_index;
index_table(channel_exchange_stats_fine_stats, channel) -> channel_exchange_stats_fine_stats_channel_index;
index_table(channel_queue_stats_deliver_stats, queue) -> channel_queue_stats_deliver_stats_queue_index;
index_table(channel_queue_stats_deliver_stats, channel) -> channel_queue_stats_deliver_stats_channel_index;
index_table(queue_exchange_stats_publish, queue) -> queue_exchange_stats_publish_queue_index;
index_table(queue_exchange_stats_publish, exchange) -> queue_exchange_stats_publish_exchange_index;
index_table(node_node_coarse_stats, node) -> node_node_coarse_stats_node_index.

load_config() ->
    RatesMode = rabbit_mgmt_agent_config:get_env(rates_mode),
    Policies = rabbit_mgmt_agent_config:get_env(sample_retention_policies, []),
    {RatesMode, Policies}.

ceil(X) when X < 0 ->
    trunc(X);
ceil(X) ->
    T = trunc(X),
    case X - T == 0 of
        true -> T;
        false -> T + 1
    end.

pget(Key, List) -> pget(Key, List, unknown).

pget(Key, List, Default) when is_number(Default) ->
    case rabbit_misc:pget(Key, List) of
        Number when is_number(Number) ->
            Number;
        _Other ->
            Default
    end;
pget(Key, List, Default) ->
    rabbit_misc:pget(Key, List, Default).
