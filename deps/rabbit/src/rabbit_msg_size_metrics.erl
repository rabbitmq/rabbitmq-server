%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2026 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
%%

%% This module tracks received message size distribution as histogram.
%% (A histogram is represented by a set of counters, one for each bucket.)
-module(rabbit_msg_size_metrics).

-export([init/1,
         observe/2,
         prometheus_format/0,
         local_summary/0,
         cluster_summary/0,
         cluster_summary_for_cli/0 ]).

%% Integration tests.
-export([raw_buckets/1,
         diff_raw_buckets/2]).

-ifdef(TEST).
-export([cleanup/1]).
-endif.

-define(BUCKET_1, 100).
-define(BUCKET_2, 1_000).
-define(BUCKET_3, 10_000).
-define(BUCKET_4, 100_000).
-define(BUCKET_5, 1_000_000).
-define(BUCKET_6, 10_000_000).
%% rabbit.max_message_size up to RabbitMQ 3.13 was 128 MiB.
%% rabbit.max_message_size since RabbitMQ 4.0 is 16 MiB.
%% To help finding an appropriate rabbit.max_message_size we also add a bucket for 50 MB.
-define(BUCKET_7, 50_000_000).
-define(BUCKET_8, 100_000_000).
%% 'infinity' means practically 512 MiB as hard limited in
%% https://github.com/rabbitmq/rabbitmq-server/blob/v4.0.2/deps/rabbit_common/include/rabbit.hrl#L254-L257
-define(BUCKET_9, 'infinity').

-define(MSG_SIZE_BUCKETS,
        [{1, ?BUCKET_1},
         {2, ?BUCKET_2},
         {3, ?BUCKET_3},
         {4, ?BUCKET_4},
         {5, ?BUCKET_5},
         {6, ?BUCKET_6},
         {7, ?BUCKET_7},
         {8, ?BUCKET_8},
         {9, ?BUCKET_9}]).

-define(POS_MSG_SIZE_SUM, 10).

-type raw_buckets() :: [{BucketUpperBound :: non_neg_integer(),
                         NumObservations :: non_neg_integer()}].

-type summary_entry() :: {{non_neg_integer(), non_neg_integer() | infinity}, {non_neg_integer(), float()}}.
-type summary() :: [summary_entry()].

-spec init(atom()) -> ok.
init(Protocol) ->
    Size = ?POS_MSG_SIZE_SUM,
    Counters = counters:new(Size, [write_concurrency]),
    put_counters(Protocol, Counters).

-spec observe(atom(), non_neg_integer()) -> ok.
observe(Protocol, MessageSize) ->
    BucketPos = find_bucket_pos(MessageSize),
    Counters = get_counters(Protocol),
    counters:add(Counters, BucketPos, 1),
    counters:add(Counters, ?POS_MSG_SIZE_SUM, MessageSize).

-spec prometheus_format() -> #{atom() => map()}.
prometheus_format() ->
    Values = [prometheus_values(Counters) || Counters <- get_labels_counters()],
    #{message_size_bytes => #{type => histogram,
                              help => "Size of messages received from publishers",
                              values => Values}}.

find_bucket_pos(Size) when Size =< ?BUCKET_1 -> 1;
find_bucket_pos(Size) when Size =< ?BUCKET_2 -> 2;
find_bucket_pos(Size) when Size =< ?BUCKET_3 -> 3;
find_bucket_pos(Size) when Size =< ?BUCKET_4 -> 4;
find_bucket_pos(Size) when Size =< ?BUCKET_5 -> 5;
find_bucket_pos(Size) when Size =< ?BUCKET_6 -> 6;
find_bucket_pos(Size) when Size =< ?BUCKET_7 -> 7;
find_bucket_pos(Size) when Size =< ?BUCKET_8 -> 8;
find_bucket_pos(_Size) -> 9.

raw_buckets(Protocol)
  when is_atom(Protocol) ->
    Counters = get_counters(Protocol),
    raw_buckets(Counters);
raw_buckets(Counters) ->
    [{UpperBound, counters:get(Counters, Pos)}
     || {Pos, UpperBound} <- ?MSG_SIZE_BUCKETS].

-spec diff_raw_buckets(raw_buckets(), raw_buckets()) -> raw_buckets().
diff_raw_buckets(After, Before) ->
    diff_raw_buckets(After, Before, []).

diff_raw_buckets([], [], Acc) ->
    lists:reverse(Acc);
diff_raw_buckets([{UpperBound, CounterAfter} | After],
                 [{UpperBound, CounterBefore} | Before],
                 Acc) ->
    case CounterAfter - CounterBefore of
        0 ->
            diff_raw_buckets(After, Before, Acc);
        Diff ->
            diff_raw_buckets(After, Before, [{UpperBound, Diff} | Acc])
    end.

%% "If you have looked at a /metrics for a histogram, you probably noticed that the buckets
%% aren’t just a count of events that fall into them. The buckets also include a count of
%% events in all the smaller buckets, all the way up to the +Inf, bucket which is the total
%% number of events. This is known as a cumulative histogram, and why the bucket label
%% is called le, standing for less than or equal to.
%% This is in addition to buckets being counters, so Prometheus histograms are cumula‐
%% tive in two different ways."
%% [Prometheus: Up & Running]
prometheus_values({Labels, Counters}) ->
    {Buckets, Count} = lists:mapfoldl(
                         fun({UpperBound, NumObservations}, Acc0) ->
                                 Acc = Acc0 + NumObservations,
                                 {{UpperBound, Acc}, Acc}
                         end, 0, raw_buckets(Counters)),
    Sum = counters:get(Counters, ?POS_MSG_SIZE_SUM),
    {Labels, Buckets, Count, Sum}.

put_counters(Protocol, Counters) ->
    persistent_term:put({?MODULE, Protocol}, Counters).

get_counters(Protocol) ->
    persistent_term:get({?MODULE, Protocol}).

get_labels_counters() ->
    [{[{protocol, Protocol}], Counters}
     || {{?MODULE, Protocol}, Counters} <- persistent_term:get()].

get_protocols() ->
    [Protocol
     || {{?MODULE, Protocol}, _} <- persistent_term:get()].

%% Aggregates data for all protocols on the local node
-spec local_summary() -> summary().
local_summary() ->
    PerProtocolBuckets = lists:map(fun(Protocol) ->
                                           raw_buckets(Protocol)
                                   end, get_protocols()),

    %% Sum buckets for all protocols
    Buckets0 = [{?BUCKET_1, 0}, {?BUCKET_2, 0}, {?BUCKET_3, 0}, {?BUCKET_4, 0},
                {?BUCKET_5, 0}, {?BUCKET_6, 0}, {?BUCKET_7, 0}, {?BUCKET_8, 0}, {?BUCKET_9, 0}],
    Buckets = lists:foldl(fun sum_protocol_buckets/2,
                          Buckets0,
                          PerProtocolBuckets),

    Total = lists:sum([Count || {_UpperBound, Count} <- Buckets]),

    Ranges = lists:map(fun({UpperBound, Count}) ->
                               Percentage = case Total of
                                                0 -> 0.0;
                                                _ -> (Count / Total) * 100
                                            end,
                               {bucket_range(UpperBound), {Count, Percentage}}
                       end, Buckets),

    Ranges.

sum_protocol_buckets(ProtocolBuckets, Acc) ->
    lists:map(fun({UpperBound, AccCount}) ->
                      ProtocolCount = proplists:get_value(UpperBound, ProtocolBuckets, 0),
                      {UpperBound, AccCount + ProtocolCount}
              end, Acc).

%% Aggregates sumamries from all nodes
-spec cluster_summary() -> summary().
cluster_summary() ->
    RemoteNodes = [Node || Node <- rabbit_nodes:list_running(), Node =/= node()],
    RemoteSummaries = [ Summary || {ok, Summary} <- erpc:multicall(RemoteNodes,
                                                                   ?MODULE,
                                                                   local_summary,
                                                                   [],
                                                                   5000)], 
    lists:foldl(fun merge_summaries/2, local_summary(), RemoteSummaries).

bucket_name({_, ?BUCKET_1}) -> <<"below 100B">>;
bucket_name({_, ?BUCKET_2}) -> <<"between 100B and 1KB">>;
bucket_name({_, ?BUCKET_3}) -> <<"between 1KB and 10KB">>;
bucket_name({_, ?BUCKET_4}) -> <<"between 10KB and 100KB">>;
bucket_name({_, ?BUCKET_5}) -> <<"between 100KB and 1MB">>;
bucket_name({_, ?BUCKET_6}) -> <<"between 1MB and 10MB">>;
bucket_name({_, ?BUCKET_7}) -> <<"between 10MB and 50MB">>;
bucket_name({_, ?BUCKET_8}) -> <<"between 50MB and 100MB">>;
bucket_name({_, ?BUCKET_9}) -> <<"above 100MB">>.

cluster_summary_for_cli() ->
    [[{<<"Message Size">>, bucket_name(Range)},
               {<<"Count">>, Count},
               {<<"Percentage">>, iolist_to_binary(io_lib:format("~.2f", [Percentage]))}]
              || {Range, {Count, Percentage}} <- cluster_summary()].

get_count_for_range(Range, SummaryList) ->
    case proplists:get_value(Range, SummaryList) of
        {Count, _} -> Count;
        undefined -> 0
    end.

%% Merges two summary lists by adding their counts and recalculating percentages
merge_summaries(Summary1, Summary2) ->
    %% Get all bucket ranges
    AllRanges = lists:usort([Range || {Range, _} <- Summary1] ++ [Range || {Range, _} <- Summary2]),

    MergedRanges = lists:map(fun(Range) ->
                                     Count1 = get_count_for_range(Range, Summary1),
                                     Count2 = get_count_for_range(Range, Summary2),
                                     NewCount = Count1 + Count2,
                                     {Range, NewCount}
                             end, AllRanges),

    %% Calculate total and percentages
    NewTotal = lists:sum([Count || {_, Count} <- MergedRanges]),
    FinalRanges = lists:map(fun({Range, Count}) ->
                                    NewPercentage = case NewTotal of
                                                        0 -> 0.0;
                                                        _ -> (Count / NewTotal) * 100
                                                    end,
                                    {Range, {Count, NewPercentage}}
                            end, MergedRanges),

    FinalRanges.

bucket_range(?BUCKET_1) -> {0, 100};
bucket_range(?BUCKET_2) -> {101, 1000};
bucket_range(?BUCKET_3) -> {1001, 10000};
bucket_range(?BUCKET_4) -> {10001, 100000};
bucket_range(?BUCKET_5) -> {100001, 1000000};
bucket_range(?BUCKET_6) -> {1000001, 10000000};
bucket_range(?BUCKET_7) -> {10000001, 50000000};
bucket_range(?BUCKET_8) -> {50000001, 100000000};
bucket_range(?BUCKET_9) -> {100000001, infinity}.

-ifdef(TEST).
%% "Counters are not tied to the current process and are automatically
%% garbage collected when they are no longer referenced."
-spec cleanup(atom()) -> ok.
cleanup(Protocol) ->
    persistent_term:erase({?MODULE, Protocol}),
    ok.
-endif.
