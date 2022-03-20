%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2011-2022 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(unit_collections_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").

-compile(export_all).

all() ->
    [
      {group, parallel_tests}
    ].

groups() ->
    [
      {parallel_tests, [parallel], [
          pmerge,
          plmerge,
          unfold
        ]}
    ].

%% -------------------------------------------------------------------
%% Test Cases
%% -------------------------------------------------------------------

pmerge(_Config) ->
    P = [{a, 1}, {b, 2}],
    P = rabbit_misc:pmerge(a, 3, P),
    [{c, 3} | P] = rabbit_misc:pmerge(c, 3, P),
    passed.

plmerge(_Config) ->
    P1 = [{a, 1}, {b, 2}, {c, 3}],
    P2 = [{a, 2}, {d, 4}],
    [{a, 1}, {b, 2}, {c, 3}, {d, 4}] = rabbit_misc:plmerge(P1, P2),
    passed.

unfold(_Config) ->
    {[], test} = rabbit_misc:unfold(fun (_V) -> false end, test),
    List = lists:seq(2,20,2),
    {List, 0} = rabbit_misc:unfold(fun (0) -> false;
                                       (N) -> {true, N*2, N-1}
                                   end, 10),
    passed.
