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
%%   Copyright (c) 2010-2012 GoPivotal, Inc.  All rights reserved.
%%

-module(rabbit_mgmt_stats).

-include("rabbit_mgmt.hrl").
-include("rabbit_mgmt_metrics.hrl").

-export([blank/1, is_blank/3, record/5, format/6, sum/1, gc/3,
         free/1, delete_stats/2, get_keys/2]).

-import(rabbit_misc, [pget/2]).

-define(ALWAYS_REPORT, [queue_msg_counts, coarse_node_stats]).

%% Data is stored in ETS tables:
%% * one set of ETS tables per event (queue_stats, queue_exchange_stats...)
%% * each set contains one table per group of events (queue_msg_rates,
%%   deliver_get, fine_stats...) such as aggr_queue_stats_deliver_get
%%   (see ?AGGR_TABLES in rabbit_mgmt_metrics.hrl)
%% * data is then stored as a tuple (not a record) to take advantage of the
%%   atomic call ets:update_counter/3. The equivalent records are noted in
%%   rabbit_mgmt_metrics.hrl to get the position and as reference for developers
%% * Records are of the shape:
%%    {{Id, base}, Field1, Field2, ....} 
%%    {{Id, total}, Field1, Field2, ....} 
%%    {{Id, Timestamp}, Field1, Field2, ....} 
%%    where Id can be a simple key or a tuple {Id0, Id1} 
%%
%% This module is not generic any longer, any new event or field needs to be
%% manually added, but it increases the performance and allows concurrent
%% GC, event collection and querying
%%

%%----------------------------------------------------------------------------
%% External functions
%%----------------------------------------------------------------------------

blank(Name) ->
    ets:new(rabbit_mgmt_stats_tables:index(Name),
            [bag, public, named_table]),
    ets:new(rabbit_mgmt_stats_tables:key_index(Name),
            [set, public, named_table]),
    ets:new(Name, [set, public, named_table]).

is_blank(Table, Id, Record) ->
    case ets:lookup(Table, {Id, total}) of
        [] ->
            true;
        [Total] ->
            case lists:member(Record, ?ALWAYS_REPORT) of
                true -> false;
                false -> is_blank(Total)
            end
    end.

%%----------------------------------------------------------------------------
free(Table) ->
    ets:delete(Table),
    ets:delete(rabbit_mgmt_stats_tables:index(Table)),
    ets:delete(rabbit_mgmt_stats_tables:key_index(Table)).

delete_stats(Table, Id) ->
    ets:select_delete(rabbit_mgmt_stats_tables:index(Table),
                      match_spec_delete_id(Id)),
    ets:select_delete(rabbit_mgmt_stats_tables:key_index(Table),
                      match_spec_key_index_delete_id(Id)),
    ets:select_delete(Table, match_spec_delete_id(Table, Id)).

%%----------------------------------------------------------------------------
get_keys(Table, Id0) ->
    ets:select(rabbit_mgmt_stats_tables:key_index(Table), match_spec_keys(Id0)).

%%----------------------------------------------------------------------------
%% Event-time
%%----------------------------------------------------------------------------

record({Id, _TS} = Key, Pos, Diff, Record, Table) ->
    ets_update(Table, Key, Record, Pos, Diff),
    ets_update(Table, {Id, total}, Record, Pos, Diff).

%%----------------------------------------------------------------------------
%% Query-time
%%----------------------------------------------------------------------------

format(no_range, Table, Id, Interval, Type, Now) ->
    Counts = get_value(Table, Id, total, Type),
    RangePoint = ((Now div Interval) * Interval) - Interval,
    {Record, Factor} = format_rate_with(
                         Table, Id, RangePoint, Interval, Interval, Type),
    format_rate(Type, Record, Counts, Factor);

format(Range, Table, Id, Interval, Type, _Now) ->
    Base = get_value(Table, Id, base, Type),
    RangePoint = Range#range.last - Interval,
    {Samples, Counts} = extract_samples(Range, Base, Table, Id, Type),
    {Record, Factor} = format_rate_with(
                         Table, Id, RangePoint, Range#range.incr, Interval, Type),
    format_rate(Type, Record, Counts, Samples, Factor).

sum([]) -> blank();

sum([{T1, Id} | StatsN]) ->
    Table = blank(),
    AllIds = full_indexes(T1, Id),
    lists:foreach(fun(Index) ->
                          case ets:lookup(T1, Index) of
                              [V] ->
                                  {_, TS} = element(1, V),
                                  ets:insert(Table, setelement(1, V, {all, TS})),
                                  insert_index(Table, {all, TS});
                              [] -> %% base
                                  ok
                          end
                  end, AllIds),
    sum(StatsN, Table).

sum(StatsN, Table) ->
    lists:foreach(
      fun ({T1, Id}) ->
              AllIds = full_indexes(T1, Id),
              lists:foreach(fun(Index) ->
                                    case ets:lookup(T1, Index) of
                                        [V] ->
                                            {_, TS} = element(1, V),
                                            ets_update(Table, {all, TS}, V);
                                        [] -> %% base
                                            ok
                                    end
                            end, AllIds)
      end, StatsN),
    Table.

gc(Cutoff, Table, Id) ->
    gc(Cutoff, lists:reverse(indexes(Table, Id)), Table, undefined).

%%----------------------------------------------------------------------------
%% Internal functions
%%----------------------------------------------------------------------------
format_rate_with(Table, Id, RangePoint, Incr, Interval, Type) ->
    case second_largest(Table, Id) of
        [S] ->
            {_, TS} = element(1, S),
            case TS - RangePoint of %% [0]
                D when D =< Incr andalso D >= 0 -> {S, Interval};
                _                               -> {S, 0.0}
            end;
        _ ->
            {empty(Id, Type), 0.0}
    end.

%% [0] Only display the rate if it's live - i.e. ((the end of the
%% range) - interval) corresponds to the second to last data point we
%% have. If the end of the range is earlier we have gone silent, if
%% it's later we have been asked for a range back in time (in which
%% case showing the correct instantaneous rate would be quite a faff,
%% and probably unwanted). Why the second to last? Because data is
%% still arriving for the last...
second_largest(Table, Id) ->
    case ets:lookup(rabbit_mgmt_stats_tables:index(Table), Id) of
        [_, _ | _] = List ->
            ets:lookup(Table, sl(List, 0, 0));
        _ ->
            unknown
    end.

sl([{_, TS} = H | T], L1, L2) when TS > L1 ->
    sl(T, H, L2);
sl([{_, TS} = H | T], L1, L2) when TS > L2 ->
    sl(T, L1, H);
sl([_ | T], L1, L2) ->
    sl(T, L1, L2);
sl([], _L1, L2) ->
    L2.

%% What we want to do here is: given the #range{}, provide a set of
%% samples such that we definitely provide a set of samples which
%% covers the exact range requested, despite the fact that we might
%% not have it. We need to spin up over the entire range of the
%% samples we *do* have since they are diff-based (and we convert to
%% absolute values here).
extract_samples(Range, Base, Table, Id, Type) ->
    extract_samples0(Range, Base, indexes(Table, Id), Table, Type, empty_list(Type)).

extract_samples0(Range = #range{first = Next}, Base, [], Table, Type, Samples) ->
    %% [3] Empty or finished table
    extract_samples1(Range, Base, empty({unused_id, Next}, Type), [], Table, Type,
                     Samples);
extract_samples0(Range, Base, [Index | List], Table, Type, Samples) ->
    case ets:lookup(Table, Index) of
        [S] ->
            extract_samples1(Range, Base, S, List, Table, Type, Samples);
        [] ->
            extract_samples0(Range, Base, List, Table, Type, Samples)
    end.

extract_samples1(Range = #range{first = Next, last = Last, incr = Incr},
                 Base, S, List, Table, Type, Samples) ->
    {_, TS} = element(1, S),
    if
        %% We've gone over the range. Terminate.
        Next > Last ->
            {Samples, Base};
        %% We've hit bang on a sample. Record it and move to the next.
        Next =:= TS ->
            extract_samples0(Range#range{first = Next + Incr}, add_record(Base, S),
                             List, Table, Type,
                             append(add_record(Base, S), Samples, Next));
        %% We haven't yet hit the beginning of our range.
        Next > TS ->
            extract_samples0(Range, add_record(Base, S), List, Table, Type, Samples);
        %% We have a valid sample, but we haven't used it up
        %% yet. Append it and loop around.
        Next < TS ->
            extract_samples1(Range#range{first = Next + Incr}, Base, S,
                             List, Table, Type, append(Base, Samples, Next))
    end.

append({_Key, V1, V2}, {samples, V1s, V2s}, TiS) ->
    {samples, append_sample(V1, TiS, V1s), append_sample(V2, TiS, V2s)};
append({_Key, V1, V2, V3}, {samples, V1s, V2s, V3s}, TiS) ->
    {samples, append_sample(V1, TiS, V1s), append_sample(V2, TiS, V2s),
     append_sample(V3, TiS, V3s)};
append({_Key, V1, V2, V3, V4}, {samples, V1s, V2s, V3s, V4s}, TiS) ->
    {samples, append_sample(V1, TiS, V1s), append_sample(V2, TiS, V2s),
     append_sample(V3, TiS, V3s), append_sample(V4, TiS, V4s)};
append({_Key, V1, V2, V3, V4, V5, V6, V7, V8},
       {samples, V1s, V2s, V3s, V4s, V5s, V6s, V7s, V8s}, TiS) ->
    {samples, append_sample(V1, TiS, V1s), append_sample(V2, TiS, V2s),
     append_sample(V3, TiS, V3s), append_sample(V4, TiS, V4s),
     append_sample(V5, TiS, V5s), append_sample(V6, TiS, V6s),
     append_sample(V7, TiS, V7s), append_sample(V8, TiS, V8s)};
append({_Key, V1, V2, V3, V4, V5, V6, V7, V8, V9, V10, V11, V12, V13, V14, V15,
        V16, V17, V18, V19, V20, V21, V22, V23},
       {samples, V1s, V2s, V3s, V4s, V5s, V6s, V7s, V8s, V9s, V10s, V11s, V12s,
        V13s, V14s, V15s, V16s, V17s, V18s, V19s, V20s, V21s, V22s, V23s},
       TiS) ->
    {samples, append_sample(V1, TiS, V1s), append_sample(V2, TiS, V2s),
     append_sample(V3, TiS, V3s), append_sample(V4, TiS, V4s),
     append_sample(V5, TiS, V5s), append_sample(V6, TiS, V6s),
     append_sample(V7, TiS, V7s), append_sample(V8, TiS, V8s),
     append_sample(V9, TiS, V9s), append_sample(V10, TiS, V10s),
     append_sample(V11, TiS, V11s), append_sample(V12, TiS, V12s),
     append_sample(V13, TiS, V13s), append_sample(V14, TiS, V14s),
     append_sample(V15, TiS, V15s), append_sample(V16, TiS, V16s),
     append_sample(V17, TiS, V17s), append_sample(V18, TiS, V18s),
     append_sample(V19, TiS, V19s), append_sample(V20, TiS, V20s),
     append_sample(V21, TiS, V21s), append_sample(V22, TiS, V22s),
     append_sample(V23, TiS, V23s)}.

append_sample(S, TS, List) ->
    [[{sample, S}, {timestamp, TS}] | List].

blank() ->
    Table = ets:new(rabbit_mgmt_stats, [ordered_set, public]),
    ets:new(rabbit_mgmt_stats_tables:index(Table),
            [bag, public, named_table]),
    ets:new(rabbit_mgmt_stats_tables:key_index(Table),
            [ordered_set, public, named_table]),
    Table.

%%----------------------------------------------------------------------------
%% Event-GCing
%%----------------------------------------------------------------------------
%% Go through the list, amalgamating all too-old samples with the next
%% newest keepable one [0] (we move samples forward in time since the
%% semantics of a sample is "we had this many x by this time"). If the
%% sample is too old, but would not be too old if moved to a rounder
%% timestamp which does not exist then invent one and move it there
%% [1]. But if it's just outright too old, move it to the base [2].
gc(_Cutoff, [], _Table, _Keep) ->
    ok;
gc(Cutoff, [Index | T], Table, Keep) ->
    %% TODO: review why the first case is needed!
    case ets:lookup(Table, Index) of
        [S] ->
            case element(1, S) of
                {Id, TS} = Key ->
                    Keep1 = case keep(Cutoff, TS) of
                                keep ->
                                    TS;
                                drop -> %% [2]
                                    ets_update(Table, {Id, base}, S),
                                    ets_delete_value(Table, Key, S),
                                    Keep;
                                {move, D} when Keep =:= undefined -> %% [1]
                                    ets_update(Table, {Id, TS + D}, S),
                                    ets_delete_value(Table, Key, S),
                                    TS + D;
                                {move, _} -> %% [0]
                                    ets_update(Table, {Id, Keep}, S),
                                    ets_delete_value(Table, Key, S),
                                    Keep
                            end,
                    gc(Cutoff, T, Table, Keep1)
            end;
        _ ->
            gc(Cutoff, T, Table, Keep)
    end.

keep({Policy, Now}, TS) ->
    lists:foldl(fun ({AgeSec, DivisorSec}, Action) ->
                        prefer_action(
                          Action,
                          case (Now - TS) =< (AgeSec * 1000) of
                              true  -> DivisorMillis = DivisorSec * 1000,
                                       case TS rem DivisorMillis of
                                           0   -> keep;
                                           Rem -> {move, DivisorMillis - Rem}
                                       end;
                              false -> drop
                          end)
                end, drop, Policy).

prefer_action(keep,              _) -> keep;
prefer_action(_,              keep) -> keep;
prefer_action({move, A}, {move, B}) -> {move, lists:min([A, B])};
prefer_action({move, A},      drop) -> {move, A};
prefer_action(drop,      {move, A}) -> {move, A};
prefer_action(drop,           drop) -> drop.

%%----------------------------------------------------------------------------
%% ETS update
%%----------------------------------------------------------------------------
ets_update(Table, K, R, P, V) ->
    try
        ets:update_counter(Table, K, {P, V})
    catch
        _:_ ->
            ets:insert(Table, new_record(K, R, P, V)),
            insert_index(Table, K)
    end.

insert_index(_, {_, V}) when is_atom(V) ->
    ok;
insert_index(Table, {Id, _TS} = Key) ->
    ets:insert(rabbit_mgmt_stats_tables:index(Table), Key),
    ets:insert(rabbit_mgmt_stats_tables:key_index(Table), {Id}).

ets_update(Table, Key, Record) ->
    try
        ets:update_counter(Table, Key, record_to_list(Record))
    catch
        _:_ ->
            ets:insert(Table, setelement(1, Record, Key))
    end.

new_record(K, deliver_get, P, V) ->
    setelement(P, {K, 0, 0, 0, 0}, V);
new_record(K, fine_stats, P, V) ->
    setelement(P, {K, 0, 0, 0, 0, 0, 0, 0, 0}, V);
new_record(K, queue_msg_rates, P, V) ->
    setelement(P, {K, 0, 0}, V);
new_record(K, queue_msg_counts, P, V) ->
    setelement(P, {K, 0, 0, 0}, V);
new_record(K, coarse_node_stats, P, V) ->
    setelement(P, {K, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
                   0, 0, 0, 0}, V);
new_record(K, coarse_node_node_stats, P, V) ->
    setelement(P, {K, 0, 0}, V);
new_record(K, coarse_conn_stats, P, V) ->
    setelement(P, {K, 0, 0}, V).

%% Returns a list of {Position, Increment} to update the current record
record_to_list({_Key, V1, V2}) ->
    [{2, V1}, {3, V2}];
record_to_list({_Key, V1, V2, V3}) ->
    [{2, V1}, {3, V2}, {4, V3}];
record_to_list({_Key, V1, V2, V3, V4}) ->
    [{2, V1}, {3, V2}, {4, V3}, {5, V4}];
record_to_list({_Key, V1, V2, V3, V4, V5, V6, V7, V8}) ->
    [{2, V1}, {3, V2}, {4, V3}, {5, V4}, {6, V5}, {7, V6}, {8, V7}, {9, V8}];
record_to_list({_Key, V1, V2, V3, V4, V5, V6, V7, V8, V9, V10, V11, V12,
                V13, V14, V15, V16, V17, V18, V19, V20, V21, V22, V23}) ->
    [{2, V1}, {3, V2}, {4, V3}, {5, V4}, {6, V5}, {7, V6}, {8, V7}, {9, V8},
     {10, V9}, {11, V10}, {12, V11}, {13, V12}, {14, V13}, {15, V14},
     {16, V15}, {17, V16}, {18, V17}, {19, V18}, {20, V19}, {21, V20},
     {22, V21}, {23, V22}, {24, V23}].

%%----------------------------------------------------------------------------

get_value(Table, Id, Tag, Type) ->
    Key = {Id, Tag},
    case ets:lookup(Table, Key) of
        [] -> empty(Key, Type);
        [Elem] -> Elem
    end.

ets_delete_value(Table, Key, S) ->
    delete_index_entry(Table, Key),
    case ets:select_delete(Table, match_spec_delete_value(Key, S)) of
        1 -> ok;
        0 -> ets:update_counter(Table, Key, -S)
    end.

delete_index_entry(Table, Key) ->
    ets:delete(rabbit_mgmt_stats_tables:index(Table), Key).

indexes(Table, Id) ->
    lists:sort(ets:lookup(rabbit_mgmt_stats_tables:index(Table), Id)).

full_indexes(Table, Id) ->
    case ets:lookup(rabbit_mgmt_stats_tables:index(Table), Id) of
        [] ->
            [];
        Indexes ->
            [{Id, base}, {Id, total} | Indexes]
    end.

%%----------------------------------------------------------------------------
%% Match specs to select or delete from the ETS tables
%%----------------------------------------------------------------------------
match_spec_delete_id(Table, Id) ->
    MatchHead = match_head({partial_match(Id), '_'}, Table),
    Id0 = to_simple_match_spec(Id),
    [{MatchHead, [{'==', Id0, '$1'}],[true]}].

match_spec_delete_id(Id) ->
    MatchHead = {partial_match(Id), '_'},
    Id0 = to_simple_match_spec(Id),
    [{MatchHead, [{'==', Id0, '$1'}],[true]}].

match_spec_key_index_delete_id(Id) ->
    MatchHead = {partial_match(Id)},
    Id0 = to_simple_match_spec(Id),
    [{MatchHead, [{'==', Id0, '$1'}],[true]}].

match_head(Key, Table) ->
    to_match_head(Key, rabbit_mgmt_stats_tables:type_from_table(Table)).

to_match_head(Key, Type) when Type == queue_msg_rates;
                              Type == coarse_node_node_stats;
                              Type == coarse_conn_stats ->
    {Key, '_', '_'};
to_match_head(Key, queue_msg_counts) ->
    {Key, '_', '_', '_'};
to_match_head(Key, deliver_get) ->
    {Key, '_', '_', '_', '_'};
to_match_head(Key, fine_stats) ->
    {Key, '_', '_', '_', '_', '_', '_', '_', '_'}; 
to_match_head(Key, coarse_node_stats) ->
    {Key, '_', '_', '_', '_', '_', '_', '_', '_', '_', '_', '_', '_', '_', '_',
     '_', '_', '_', '_', '_', '_', '_', '_', '_'}.

partial_match({_Id0, '_'}) ->
    {'$1', '_'};
partial_match({'_', _Id1}) ->
    {'_', '$1'};
partial_match(_Id) ->
    '$1'.

to_simple_match_spec({Id0, '_'}) when is_tuple(Id0) ->
    {Id0};
to_simple_match_spec({'_', Id1}) when is_tuple(Id1) ->
    {Id1};
to_simple_match_spec({Id0, '_'}) ->
    Id0;
to_simple_match_spec({'_', Id1}) ->
    Id1;
to_simple_match_spec(Id) when is_tuple(Id) ->
    {Id};
to_simple_match_spec(Id) ->
    Id.

match_spec_delete_value(Key, S) ->
    [{'$1', [{'==', {setelement(1, S, escape(Key))}, '$1'}], [true]}].    

escape({Id, TS}) when is_tuple(Id) ->
    {{{Id}, TS}};
escape({Id, TS}) ->
    {{Id, TS}}.

to_match_condition({'_', Id1}) when is_tuple(Id1) ->
    {'==', {Id1}, '$2'};
to_match_condition({'_', Id1}) ->
    {'==', Id1, '$2'};
to_match_condition({Id0, '_'}) when is_tuple(Id0) ->
    {'==', {Id0}, '$1'};
to_match_condition({Id0, '_'}) ->
    {'==', Id0, '$1'}.

match_spec_keys(Id) ->
    MatchCondition = to_match_condition(Id),
    MatchHead = {{'$1', '$2'}},
    [{MatchHead, [MatchCondition], [{{'$1', '$2'}}]}].

%%----------------------------------------------------------------------------
%% Format output
%%----------------------------------------------------------------------------
format_rate(deliver_get, {_, D, DN, G, GN}, {_, TD, TDN, TG, TGN}, Factor) ->
    [
     {deliver, TD}, {deliver_details, [{rate, apply_factor(D, Factor)}]},
     {deliver_no_ack, TDN},
     {deliver_no_ack_details, [{rate, apply_factor(DN, Factor)}]},
     {get, TG}, {get_details, [{rate, apply_factor(G, Factor)}]},
     {get_no_ack, TGN},
     {get_no_ack_details, [{rate, apply_factor(GN, Factor)}]}
    ];
format_rate(fine_stats, {_, P, PI, PO, A, D, C, RU, R},
            {_, TP, TPI, TPO, TA, TD, TC, TRU, TR}, Factor) ->
    [
     {publish, TP}, {publish_details, [{rate, apply_factor(P, Factor)}]},
     {publish_in, TPI},
      {publish_in_details, [{rate, apply_factor(PI, Factor)}]},
     {publish_out, TPO},
     {publish_out_details, [{rate, apply_factor(PO, Factor)}]},
     {ack, TA}, {ack_details, [{rate, apply_factor(A, Factor)}]},
     {deliver_get, TD}, {deliver_get_details, [{rate, apply_factor(D, Factor)}]},
     {confirm, TC}, {confirm_details, [{rate, apply_factor(C, Factor)}]},
     {return_unroutable, TRU},
     {return_unroutable_details, [{rate, apply_factor(RU, Factor)}]},
     {redeliver, TR}, {redeliver_details, [{rate, apply_factor(R, Factor)}]}
    ];
format_rate(queue_msg_rates, {_, R, W}, {_, TR, TW}, Factor) ->
    [
     {disk_reads, TR}, {disk_reads_details, [{rate, apply_factor(R, Factor)}]},
     {disk_writes, TW}, {disk_writes_details, [{rate, apply_factor(W, Factor)}]}
    ];
format_rate(queue_msg_counts, {_, M, MR, MU}, {_, TM, TMR, TMU}, Factor) ->
    [
     {messages, TM},
     {messages_details, [{rate, apply_factor(M, Factor)}]},
     {messages_ready, TMR},
     {messages_ready_details, [{rate, apply_factor(MR, Factor)}]},
     {messages_unacknowledged, TMU},
     {messages_unacknowledged_details, [{rate, apply_factor(MU, Factor)}]}
    ];
format_rate(coarse_node_stats,
            {_, M, F, S, P, D, IR, IB, IA, IWC, IWB, IWAT, IS, ISAT, ISC,
             ISEAT, IRC, MRTC, MDTC, MSRC, MSWC, QIJWC, QIWC, QIRC},
            {_, TM, TF, TS, TP, TD, TIR, TIB, TIA, TIWC, TIWB, TIWAT, TIS,
             TISAT, TISC, TISEAT, TIRC, TMRTC, TMDTC, TMSRC, TMSWC, TQIJWC,
             TQIWC, TQIRC}, Factor) ->
    [
     {mem_used, TM},
     {mem_used_details, [{rate, apply_factor(M, Factor)}]},
     {fd_used, TF},
     {fd_used_details, [{rate, apply_factor(F, Factor)}]},
     {sockets_used, TS},
     {sockets_used_details, [{rate, apply_factor(S, Factor)}]},
     {proc_used, TP},
     {proc_used_details, [{rate, apply_factor(P, Factor)}]},
     {disk_free, TD},
     {disk_free_details, [{rate, apply_factor(D, Factor)}]},
     {io_read_count, TIR},
     {io_read_count_details, [{rate, apply_factor(IR, Factor)}]},
     {io_read_bytes, TIB},
     {io_read_bytes_details, [{rate, apply_factor(IB, Factor)}]},
     {io_read_avg_time, TIA},
     {io_read_avg_time_details, [{rate, apply_factor(IA, Factor)}]},
     {io_write_count, TIWC},
     {io_write_count_details, [{rate, apply_factor(IWC, Factor)}]},
     {io_write_bytes, TIWB},
     {io_write_bytes_details, [{rate, apply_factor(IWB, Factor)}]},
     {io_write_avg_time, TIWAT},
     {io_write_avg_time_details, [{rate, apply_factor(IWAT, Factor)}]},
     {io_sync_count, TIS},
     {io_sync_count_details, [{rate, apply_factor(IS, Factor)}]},
     {io_sync_avg_time, TISAT},
     {io_sync_avg_time_details, [{rate, apply_factor(ISAT, Factor)}]},
     {io_seek_count, TISC},
     {io_seek_count_details, [{rate, apply_factor(ISC, Factor)}]},
     {io_seek_avg_time, TISEAT},
     {io_seek_avg_time_details, [{rate, apply_factor(ISEAT, Factor)}]},
     {io_reopen_count, TIRC},
     {io_reopen_count_details, [{rate, apply_factor(IRC, Factor)}]},
     {mnesia_ram_tx_count, TMRTC},
     {mnesia_ram_tx_count_details, [{rate, apply_factor(MRTC, Factor)}]},
     {mnesia_disk_tx_count, TMDTC},
     {mnesia_disk_tx_count_details, [{rate, apply_factor(MDTC, Factor)}]},
     {msg_store_read_count, TMSRC},
     {msg_store_read_count_details, [{rate, apply_factor(MSRC, Factor)}]},
     {msg_store_write_count, TMSWC},
     {msg_store_write_count_details, [{rate, apply_factor(MSWC, Factor)}]},
     {queue_index_journal_write_count, TQIJWC},
     {queue_index_journal_write_count_details, [{rate, apply_factor(QIJWC, Factor)}]},
     {queue_index_write_count, TQIWC},
     {queue_index_write_count_details, [{rate, apply_factor(QIWC, Factor)}]},
     {queue_index_read_count, TQIRC},
     {queue_index_read_count_details, [{rate, apply_factor(QIRC, Factor)}]}
    ];
format_rate(coarse_node_node_stats, {_, S, R}, {_, TS, TR}, Factor) ->
    [
     {send_bytes, TS},
     {send_bytes_details, [{rate, apply_factor(S, Factor)}]},
     {send_bytes, TR},
     {send_bytes_details, [{rate, apply_factor(R, Factor)}]}
    ];
format_rate(coarse_conn_stats, {_, R, S}, {_, TR, TS}, Factor) ->
    [
     {send_oct, TS},
     {send_oct_details, [{rate, apply_factor(S, Factor)}]},
     {recv_oct, TR},
     {recv_oct_details, [{rate, apply_factor(R, Factor)}]}
    ].

format_rate(deliver_get, {_, D, DN, G, GN}, {_, TD, TDN, TG, TGN},
            {_, SD, SDN, SG, SGN}, Factor) ->
    Length = length(SD),
    [
     {deliver, TD}, {deliver_details, [{rate, apply_factor(D, Factor)},
                                       {samples, SD}] ++ average(SD, Length)},
     {deliver_no_ack, TDN},
     {deliver_no_ack_details, [{rate, apply_factor(DN, Factor)},
                               {samples, SDN}] ++ average(SDN, Length)},
     {get, TG}, {get_details, [{rate, apply_factor(G, Factor)},
                               {samples, SG}] ++ average(SG, Length)},
     {get_no_ack, TGN},
     {get_no_ack_details, [{rate, apply_factor(GN, Factor)},
                           {samples, SGN}] ++ average(SGN, Length)}
    ];
format_rate(fine_stats, {_, P, PI, PO, A, D, C, RU, R},
            {_, TP, TPI, TPO, TA, TD, TC, TRU, TR},
            {_, SP, SPI, SPO, SA, SD, SC, SRU, SR}, Factor) ->
    Length = length(SP),
    [
     {publish, TP},
     {publish_details, [{rate, apply_factor(P, Factor)},
                        {samples, SP}] ++ average(SP, Length)},
     {publish_in, TPI},
     {publish_in_details, [{rate, apply_factor(PI, Factor)},
                           {samples, SPI}] ++ average(SPI, Length)},
     {publish_out, TPO},
     {publish_out_details, [{rate, apply_factor(PO, Factor)},
                            {samples, SPO}] ++ average(SPO, Length)},
     {ack, TA}, {ack_details, [{rate, apply_factor(A, Factor)},
                               {samples, SA}] ++ average(SA, Length)},
     {deliver_get, TD},
     {deliver_get_details, [{rate, apply_factor(D, Factor)},
                            {samples, SD}] ++ average(SD, Length)},
     {confirm, TC},
     {confirm_details, [{rate, apply_factor(C, Factor)},
                        {samples, SC}] ++ average(SC, Length)},
     {return_unroutable, TRU},
     {return_unroutable_details, [{rate, apply_factor(RU, Factor)},
                                  {samples, SRU}] ++ average(SRU, Length)},
     {redeliver, TR},
     {redeliver_details, [{rate, apply_factor(R, Factor)},
                          {samples, SR}] ++ average(SR, Length)}
    ];
format_rate(queue_msg_rates, {_, R, W}, {_, TR, TW}, {_, SR, SW}, Factor) ->
    Length = length(SR),
    [
     {disk_reads, TR},
     {disk_reads_details, [{rate, apply_factor(R, Factor)},
                           {samples, SR}] ++ average(SR, Length)},
     {disk_writes, TW},
     {disk_writes_details, [{rate, apply_factor(W, Factor)},
                            {samples, SW}] ++ average(SW, Length)}
    ];
format_rate(queue_msg_counts, {_, M, MR, MU}, {_, TM, TMR, TMU},
            {_, SM, SMR, SMU}, Factor) ->
    Length = length(SM),
    [
     {messages, TM},
     {messages_details, [{rate, apply_factor(M, Factor)},
                         {samples, SM}] ++ average(SM, Length)},
     {messages_ready, TMR},
     {messages_ready_details, [{rate, apply_factor(MR, Factor)},
                               {samples, SMR}] ++ average(SMR, Length)},
     {messages_unacknowledged, TMU},
     {messages_unacknowledged_details, [{rate, apply_factor(MU, Factor)},
                                        {samples, SMU}] ++ average(SMU, Length)}
    ];
format_rate(coarse_node_stats,
            {_, M, F, S, P, D, IR, IB, IA, IWC, IWB, IWAT, IS, ISAT, ISC,
             ISEAT, IRC, MRTC, MDTC, MSRC, MSWC, QIJWC, QIWC, QIRC},
            {_, TM, TF, TS, TP, TD, TIR, TIB, TIA, TIWC, TIWB, TIWAT, TIS,
             TISAT, TISC, TISEAT, TIRC, TMRTC, TMDTC, TMSRC, TMSWC, TQIJWC,
             TQIWC, TQIRC},
            {_, SM, SF, SS, SP, SD, SIR, SIB, SIA, SIWC, SIWB, SIWAT, SIS,
             SISAT, SISC, SISEAT, SIRC, SMRTC, SMDTC, SMSRC, SMSWC, SQIJWC,
             SQIWC, SQIRC}, Factor) ->
    Length = length(SM),
    [
     {mem_used, TM},
     {mem_used_details, [{rate, apply_factor(M, Factor)},
                         {samples, SM}] ++ average(SM, Length)},
     {fd_used, TF},
     {fd_used_details, [{rate, apply_factor(F, Factor)},
                        {samples, SF}] ++ average(SF, Length)},
     {sockets_used, TS},
     {sockets_used_details, [{rate, apply_factor(S, Factor)},
                             {samples, SS}] ++ average(SS, Length)},
     {proc_used, TP},
     {proc_used_details, [{rate, apply_factor(P, Factor)},
                          {samples, SP}] ++ average(SP, Length)},
     {disk_free, TD},
     {disk_free_details, [{rate, apply_factor(D, Factor)},
                          {samples, SD}] ++ average(SD, Length)},
     {io_read_count, TIR},
     {io_read_count_details, [{rate, apply_factor(IR, Factor)},
                              {samples, SIR}] ++ average(SIR, Length)},
     {io_read_bytes, TIB},
     {io_read_bytes_details, [{rate, apply_factor(IB, Factor)},
                              {samples, SIB}] ++ average(SIB, Length)},
     {io_read_avg_time, TIA},
     {io_read_avg_time_details, [{rate, apply_factor(IA, Factor)},
                                 {samples, SIA}] ++ average(SIA, Length)},
     {io_write_count, TIWC},
     {io_write_count_details, [{rate, apply_factor(IWC, Factor)},
                               {samples, SIWC}] ++ average(SIWC, Length)},
     {io_write_bytes, TIWB},
     {io_write_bytes_details, [{rate, apply_factor(IWB, Factor)},
                               {samples, SIWB}] ++ average(SIWB, Length)},
     {io_write_avg_time, TIWAT},
     {io_write_avg_time_details, [{rate, apply_factor(IWAT, Factor)},
                                  {samples, SIWAT}] ++ average(SIWAT, Length)},
     {io_sync_count, TIS},
     {io_sync_count_details, [{rate, apply_factor(IS, Factor)},
                              {samples, SIS}] ++ average(SIS, Length)},
     {io_sync_avg_time, TISAT},
     {io_sync_avg_time_details, [{rate, apply_factor(ISAT, Factor)},
                                 {samples, SISAT}] ++ average(SISAT, Length)},
     {io_seek_count, TISC},
     {io_seek_count_details, [{rate, apply_factor(ISC, Factor)},
                              {samples, SISC}] ++ average(SISC, Length)},
     {io_seek_avg_time, TISEAT},
     {io_seek_avg_time_details, [{rate, apply_factor(ISEAT, Factor)},
                                 {samples, SISEAT}] ++ average(SISEAT, Length)},
     {io_reopen_count, TIRC},
     {io_reopen_count_details, [{rate, apply_factor(IRC, Factor)},
                                {samples, SIRC}] ++ average(SIRC, Length)},
     {mnesia_ram_tx_count, TMRTC},
     {mnesia_ram_tx_count_details, [{rate, apply_factor(MRTC, Factor)},
                                    {samples, SMRTC}] ++ average(SMRTC, Length)},
     {mnesia_disk_tx_count, TMDTC},
     {mnesia_disk_tx_count_details, [{rate, apply_factor(MDTC, Factor)},
                                     {samples, SMDTC}] ++ average(SMDTC, Length)},
     {msg_store_read_count, TMSRC},
     {msg_store_read_count_details, [{rate, apply_factor(MSRC, Factor)},
                                     {samples, SMSRC}] ++ average(SMSRC, Length)},
     {msg_store_write_count, TMSWC},
     {msg_store_write_count_details, [{rate, apply_factor(MSWC, Factor)},
                                      {samples, SMSWC}] ++ average(SMSWC, Length)},
     {queue_index_journal_write_count, TQIJWC},
     {queue_index_journal_write_count_details,
      [{rate, apply_factor(QIJWC, Factor)},
       {samples, SQIJWC}] ++ average(SQIJWC, Length)},
     {queue_index_write_count, TQIWC},
     {queue_index_write_count_details, [{rate, apply_factor(QIWC, Factor)},
                                        {samples, SQIWC}] ++ average(SQIWC, Length)},
     {queue_index_read_count, TQIRC},
     {queue_index_read_count_details, [{rate, apply_factor(QIRC, Factor)},
                                       {samples, SQIRC}] ++ average(SQIRC, Length)}
    ];
format_rate(coarse_node_node_stats, {_, S, R}, {_, TS, TR}, {_, SS, SR},
            Factor) ->
    Length = length(SS),
    [
     {send_bytes, TS},
     {send_bytes_details, [{rate, apply_factor(S, Factor)},
                           {samples, SS}] ++ average(SS, Length)},
     {send_bytes, TR},
     {send_bytes_details, [{rate, apply_factor(R, Factor)},
                           {samples, SR}] ++ average(SR, Length)}
    ];
format_rate(coarse_conn_stats, {_, R, S}, {_, TR, TS}, {_, SR, SS}, Factor) ->
    Length = length(SS),
    [
     {send_oct, TS},
     {send_oct_details, [{rate, apply_factor(S, Factor)},
                         {samples, SS}] ++ average(SS, Length)},
     {recv_oct, TR},
     {recv_oct_details, [{rate, apply_factor(R, Factor)},
                         {samples, SR}] ++ average(SR, Length)}
    ].

apply_factor(_, 0.0) ->
    0.0;
apply_factor(S, Factor) ->
    S * 1000 / Factor.

average(_Samples, Length) when Length =< 1 ->
    [];
average(Samples, Length) ->
    [{sample, S2}, {timestamp, T2}] = hd(Samples),
    [{sample, S1}, {timestamp, T1}] = lists:last(Samples),
    Total = lists:sum([pget(sample, I) || I <- Samples]),
    [{avg_rate, (S2 - S1) * 1000 / (T2 - T1)},
     {avg,      Total / Length}].
%%----------------------------------------------------------------------------

add_record({Base, V1, V2}, {_, V11, V21}) ->
    {Base, V1 + V11, V2 + V21};
add_record({Base, V1, V2, V3}, {_, V1a, V2a, V3a}) ->
    {Base, V1 + V1a, V2 + V2a, V3 + V3a};
add_record({Base, V1, V2, V3, V4}, {_, V1a, V2a, V3a, V4a}) ->
    {Base, V1 + V1a, V2 + V2a, V3 + V3a, V4 + V4a};
add_record({Base, V1, V2, V3, V4, V5, V6, V7, V8},
           {_, V1a, V2a, V3a, V4a, V5a, V6a, V7a, V8a}) ->
    {Base, V1 + V1a, V2 + V2a, V3 + V3a, V4 + V4a, V5 + V5a, V6 + V6a, V7 + V7a,
     V8 + V8a};
add_record({Base, V1, V2, V3, V4, V5, V6, V7, V8, V9, V10, V11, V12, V13, V14,
            V15, V16, V17, V18, V19, V20, V21, V22, V23},
           {_, V1a, V2a, V3a, V4a, V5a, V6a, V7a, V8a, V9a, V10a, V11a, V12a,
            V13a, V14a, V15a, V16a, V17a, V18a, V19a, V20a, V21a, V22a, V23a}) ->
    {Base, V1 + V1a, V2 + V2a, V3 + V3a, V4 + V4a, V5 + V5a, V6 + V6a, V7 + V7a,
     V8 + V8a, V9 + V9a, V10 + V10a, V11 + V11a, V12 + V12a, V13 + V13a,
     V14 + V14a, V15 + V15a, V16 + V16a, V17 + V17a, V18 + V18a, V19 + V19a,
     V20 + V20a, V21 + V21a, V22 + V22a, V23 + V23a}.

empty(Key, Type) when Type == queue_msg_rates;
                      Type == coarse_node_node_stats;
                      Type == coarse_conn_stats ->
    {Key, 0, 0};
empty(Key, queue_msg_counts) ->
    {Key, 0, 0, 0};
empty(Key, deliver_get) ->
    {Key, 0, 0, 0, 0};
empty(Key, fine_stats) ->
    {Key, 0, 0, 0, 0, 0, 0, 0, 0}; 
empty(Key, coarse_node_stats) ->
    {Key, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0}.

empty_list(Type) when Type == queue_msg_rates;
                      Type == coarse_node_node_stats;
                      Type == coarse_conn_stats ->
    {samples, [], []};
empty_list(queue_msg_counts) ->
    {samples, [], [], []};
empty_list(deliver_get) ->
    {samples, [], [], [], []};
empty_list(fine_stats) ->
    {samples, [], [], [], [], [], [], [], []};
empty_list(coarse_node_stats) ->
    {samples, [], [], [], [], [], [], [], [], [], [], [], [], [], [], [], [],
     [], [], [], [], [], [], []}.


is_blank({_Key, 0, 0}) ->
    true;
is_blank({_Key, 0, 0, 0}) ->
    true;
is_blank({_Key, 0, 0, 0, 0}) ->
    true;
is_blank({_Key, 0, 0, 0, 0, 0, 0, 0, 0}) ->
    true;
is_blank({_Key, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
          0, 0}) ->
    true;
is_blank(_) ->
    false.
