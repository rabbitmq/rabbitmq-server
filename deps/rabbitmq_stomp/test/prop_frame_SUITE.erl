%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2026 Broadcom. All Rights Reserved. The term "Broadcom" refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
%%

-module(prop_frame_SUITE).

-include_lib("proper/include/proper.hrl").
-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").
-compile([export_all, nowarn_export_all]).

-import(rabbit_ct_proper_helpers, [run_proper/3]).

all() ->
    [{group, properties}].

groups() ->
    [{properties, [parallel],
      [
       prop_within_limit_succeeds,
       prop_over_limit_fails,
       prop_duplicates_do_not_exhaust_limit
      ]}].

%% Any frame with up to 100 unique header names parses successfully.
prop_within_limit_succeeds(_Config) ->
    run_proper(
      fun() ->
          ?FORALL(N, range(0, 100),
              begin
                  {ok, _, _} = parse(make_frame(unique_headers(N))),
                  true
              end)
      end, [], 1000).

%% Any frame with more than 100 unique header names is rejected.
prop_over_limit_fails(_Config) ->
    run_proper(
      fun() ->
          ?FORALL(N, range(101, 300),
              begin
                  {error, too_many_headers} = parse(make_frame(unique_headers(N))),
                  true
              end)
      end, [], 1000).

%% Duplicate entries for a header name already seen never trigger the limit.
prop_duplicates_do_not_exhaust_limit(_Config) ->
    run_proper(
      fun() ->
          ?FORALL({Unique, Dups}, {range(1, 50), range(0, 500)},
              begin
                  Headers = unique_headers(Unique) ++
                            [{"h1", "dup"} || _ <- lists:seq(1, Dups)],
                  {ok, _, _} = parse(make_frame(Headers)),
                  true
              end)
      end, [], 1000).

%%-------------------------------------------------------------------

unique_headers(N) ->
    [{lists:flatten(io_lib:format("h~b", [I])), "v"} || I <- lists:seq(1, N)].

make_frame(Headers) ->
    HdrStr = lists:flatten([K ++ ":" ++ V ++ "\n" || {K, V} <- Headers]),
    iolist_to_binary(["CONNECT\n", HdrStr, "\n\0"]).

parse(Bin) ->
    rabbit_stomp_frame:parse(Bin, rabbit_stomp_frame:initial_state()).
