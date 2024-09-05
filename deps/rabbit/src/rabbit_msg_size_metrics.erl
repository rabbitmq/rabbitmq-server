%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2024 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
%%

%% This module tracks received message size distribution as histograms.
%% (One histogram is represented by a set of counters, one for each
%%  bucket.)
-module(rabbit_msg_size_metrics).

-export([init/1,
         overview/0,
         prometheus_format/0,
         update/2]).

%% Useful for testing
-export([overview/1,
         changed_buckets/2,
         cleanup/1]).

-define(MSG_SIZE_BUCKETS,
        [{1, 64},
         {2, 256},
         {3, 1024},
         {4, 4 * 1024},
         {5, 16 * 1024},
         {6, 64 * 1024},
         {7, 256 * 1024},
         {8, 1024 * 1024},
         {9, 4 * 1024 * 1024},
         {10, 16 * 1024 * 1024},
         {11, 64 * 1024 * 1024},
         {12, 256 * 1024 * 1024},
         {13, infinity}]).
-define(MSG_SIZE_SUM_POS, 14).

-type labels() :: [{protocol, atom()}].
-type hist_values() :: #{BucketUpperBound :: integer() => integer(), sum => integer()}.

-spec init(labels()) -> any().
init([{protocol, Protocol}]) ->
    Size = ?MSG_SIZE_SUM_POS,
    Counters = counters:new(Size, [write_concurrency]),
    put_counters(Protocol, Counters).

-spec cleanup(labels()) -> any().
cleanup([{protocol, Protocol}]) ->
    delete_counters(Protocol).

-spec overview() -> #{labels() => #{atom() => hist_values()}}.
overview() ->
    LabelsList = fetch_labels(),
    maps:from_list([{Labels, #{message_size_bytes => overview(Labels)}} || Labels <- LabelsList]).

-spec overview(labels()) -> hist_values().
overview(Labels) ->
    {BucketValues, Sum} = values(Labels),
    BucketMap = maps:from_list(BucketValues),
    BucketMap#{sum => Sum}.

-spec prometheus_format() -> #{atom() => map()}.
prometheus_format() ->
    LabelsList = fetch_labels(),
    Values = [prometheus_values(Labels) || Labels <- LabelsList],

    #{message_size_bytes => #{type => histogram,
                              help => "Size of messages received from publishers",
                              values => Values}}.

-spec update(atom(), integer()) -> any().
update(Protocol, MessageSize) ->
    BucketPos = find_hist_bucket(?MSG_SIZE_BUCKETS, MessageSize),
    Counters = fetch_counters(Protocol),
    counters:add(Counters, BucketPos, 1),
    counters:add(Counters, ?MSG_SIZE_SUM_POS, MessageSize).

-spec changed_buckets(hist_values(), hist_values()) -> hist_values().
changed_buckets(After, Before) ->
    maps:filtermap(
      fun(Key, ValueAfter) ->
              case ValueAfter - maps:get(Key, Before) of
                  0 -> false;
                  Diff -> {true, Diff}
              end
      end, After).

%%
%% Helper functions
%%

find_hist_bucket([{BucketPos, UpperBound}|_], MessageSize) when MessageSize =< UpperBound ->
    BucketPos;
find_hist_bucket([{BucketPos, _Infinity}], _) ->
    BucketPos;
find_hist_bucket([_|T], MessageSize) ->
    find_hist_bucket(T, MessageSize).

%% Returned bucket values are count in the range (UpperBound[N-1]-UpperBound[N]]
values(_Labels = [{protocol, Protocol}]) ->
    Counters = fetch_counters(Protocol),
    Sum = counters:get(Counters, ?MSG_SIZE_SUM_POS),
    BucketValues =
        [{UpperBound, counters:get(Counters, Pos)}
         || {Pos, UpperBound} <- ?MSG_SIZE_BUCKETS],
    {BucketValues, Sum}.

%% Returned bucket values are cumulative counts, ie in the range 0-UpperBound[N],
%% as defined by Prometheus classic histogram format
prometheus_values(Labels) ->
    {BucketValues, Sum} = values(Labels),
    {CumulatedValues, Count} =
        lists:mapfoldl(
          fun({UpperBound, V}, Count0) ->
                  CumulatedValue = Count0 + V,
                  {{UpperBound, CumulatedValue}, CumulatedValue}
          end, 0, BucketValues),
    {Labels, CumulatedValues, Count, Sum}.

put_counters(Protocol, Counters) ->
    persistent_term:put({?MODULE, Protocol}, Counters).

fetch_counters(Protocol) ->
    persistent_term:get({?MODULE, Protocol}).

fetch_labels() ->
    [[{protocol, Protocol}] || {{?MODULE, Protocol}, _} <- persistent_term:get()].

delete_counters(Protocol) ->
    persistent_term:erase({?MODULE, Protocol}).
