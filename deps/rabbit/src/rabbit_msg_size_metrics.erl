%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2024 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
%%

%% This module tracks received message size distribution as histograms.
%% (One histogram is represented by a set of seshat counters, one for
%%  each bucket.)
-module(rabbit_msg_size_metrics).

-export([init/1,
         overview/0,
         prometheus_format/0,
         update/2]).

%% Useful for testing
-export([overview/1,
         changed_buckets/2]).

-define(MSG_SIZE_BUCKETS,
        [{1, 64, "64B"},
         {2, 256, "256B"},
         {3, 1024, "1KiB"},
         {4, 4 * 1024, "4KiB"},
         {5, 16 * 1024, "16KiB"},
         {6, 64 * 1024, "64KiB"},
         {7, 256 * 1024, "256KiB"},
         {8, 1024 * 1024, "1MiB"},
         {9, 4 * 1024 * 1024, "4MiB"},
         {10, 16 * 1024 * 1024, "16MiB"},
         {11, 64 * 1024 * 1024, "64MiB"},
         {12, 256 * 1024 * 1024, "256MiB"},
         {13, infinity, "512MiB"}]).

-type labels() :: [{protocol, atom()}].
-type buckets() :: #{atom() => integer()}.

-spec init(labels()) -> any().
init(Labels = [{protocol, Protocol}]) ->
    _ = seshat:new_group(?MODULE),
    Counters = seshat:new(?MODULE, Labels, message_size_counters()),
    put_counters(Protocol, Counters).

-spec overview() -> #{labels() => buckets()}.
overview() ->
    seshat:overview(?MODULE).

-spec overview([{protocol, atom()}]) -> buckets().
overview(Labels) ->
    maps:get(Labels, overview(), undefined).

-spec prometheus_format() -> #{atom() => map()}.
prometheus_format() ->
    seshat:format(?MODULE).

-spec update(atom(), integer()) -> any().
update(Protocol, MessageSize) ->
    BucketPos = find_hist_bucket(?MSG_SIZE_BUCKETS, MessageSize),
    counters:add(fetch_counters(Protocol), BucketPos, 1).

-spec changed_buckets(buckets(), buckets()) -> buckets().
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

message_size_counters() ->
    [{list_to_atom("message_size_" ++ UpperBoundReadable),
      BucketPos,
      counter,
     "Total number of messages received from publishers with a size =< " ++ UpperBoundReadable}
     || {BucketPos, _, UpperBoundReadable} <- ?MSG_SIZE_BUCKETS].

find_hist_bucket([{BucketPos, UpperBound, _}|_], MessageSize) when MessageSize =< UpperBound ->
    BucketPos;
find_hist_bucket([{BucketPos, _Infinity, _}], _) ->
    BucketPos;
find_hist_bucket([_|T], MessageSize) ->
    find_hist_bucket(T, MessageSize).

put_counters(Protocol, Counters) ->
    persistent_term:put({?MODULE, Protocol}, Counters).

fetch_counters(Protocol) ->
    persistent_term:get({?MODULE, Protocol}).

