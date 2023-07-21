%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2023 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(serial_number_SUITE).
-include_lib("eunit/include/eunit.hrl").

-compile([export_all,
          nowarn_export_all]).

-import(serial_number, [add/2,
                        compare/2,
                        usort/1,
                        ranges/1,
                        diff/2,
                        foldl/4]).

all() -> [test_add,
          test_compare,
          test_usort,
          test_ranges,
          test_diff,
          test_foldl].

test_add(_Config) ->
    ?assertEqual(1, add(0, 1)),
    %% "Addition of a value outside the range
    %% [0 .. (2^(SERIAL_BITS - 1) - 1)] is undefined."
    MaxAddend = round(math:pow(2, 32 - 1) - 1),
    MinAddend = 0,
    ?assertEqual(MaxAddend, add(0, MaxAddend)),
    ?assertEqual(MinAddend, add(0, MinAddend)),
    ?assertEqual(0, add(16#ffffffff, 1)),
    ?assertEqual(1, add(16#ffffffff, 2)),
    AddendTooLarge = MaxAddend + 1,
    ?assertExit({undefined_serial_addition, 0, AddendTooLarge},
                add(0, AddendTooLarge)),
    AddendTooSmall = MinAddend - 1,
    ?assertExit({undefined_serial_addition, 0, AddendTooSmall},
                add(0, AddendTooSmall)).

test_compare(_Config) ->
    ?assertEqual(equal, compare(0, 0)),
    ?assertEqual(equal, compare(16#ffffffff, 16#ffffffff)),
    ?assertEqual(less, compare(0, 1)),
    ?assertEqual(greater, compare(1, 0)),
    ?assertEqual(less, compare(0, 2)),
    ?assertEqual(less, compare(0, round(math:pow(2, 32 - 1)) - 1)),
    ?assertExit({undefined_serial_comparison, 0, _},
                compare(0, round(math:pow(2, 32 - 1)))),
    ?assertEqual(less, compare(16#ffffffff - 5, 30_000)),
    ?assertEqual(greater, compare(1, 0)),
    ?assertEqual(greater, compare(2147483647, 0)),
    ?assertExit({undefined_serial_comparison, 2147483648, 0},
                compare(2147483648, 0)).

test_usort(_Config) ->
    ?assertEqual([],
                 usort([])),
    ?assertEqual([3],
                 usort([3])),
    ?assertEqual([0],
                 usort([0, 0])),
    ?assertEqual([4294967000, 4294967293, 4294967294, 4294967295, 0, 3, 4],
                 usort([3, 4294967295, 4294967295, 4294967293, 4294967000, 4294967294, 0, 4])).

test_ranges(_Config) ->
    ?assertEqual([],
                 ranges([])),
    ?assertEqual([{0, 0}],
                 ranges([0])),
    ?assertEqual([{0, 1}],
                 ranges([0, 1])),
    ?assertEqual([{0, 1}],
                 ranges([1, 0])),
    ?assertEqual([{0, 0}, {2, 2}],
                 ranges([0, 2])),
    ?assertEqual([{0, 0}, {2, 2}],
                 ranges([2, 0])),
    %% 2 ^ 32 - 1 = 4294967295
    ?assertEqual([{4294967290, 4294967290}, {4294967295, 4294967295}],
                 ranges([4294967290, 4294967295])),
    ?assertEqual([{4294967290, 4294967290}, {4294967295, 4294967295}],
                 ranges([4294967295, 4294967290])),
    ?assertEqual([{4294967294, 4294967294}, {0, 0}],
                 ranges([4294967294, 0])),
    ?assertEqual([{4294967294, 4294967294}, {0, 0}],
                 ranges([0, 4294967294])),
    ?assertEqual([{4294967295, 0}],
                 ranges([4294967295, 0])),
    ?assertEqual([{4294967294, 1}, {3, 5}, {10, 10}, {18, 19}],
                 ranges([4294967294, 4294967295, 0, 1, 3, 4, 5, 10, 18, 19])),
    ?assertEqual([{4294967294, 1}, {3, 5}, {10, 10}, {18, 19}],
                 ranges([1, 10, 4294967294, 0, 3, 4, 5, 19, 18, 4294967295])).

test_diff(_Config) ->
    ?assertEqual(0, diff(0, 0)),
    ?assertEqual(0, diff(1, 1)),
    ?assertEqual(0, diff(16#ffffffff, 16#ffffffff)),
    ?assertEqual(1, diff(1, 0)),
    ?assertEqual(2, diff(1, 16#ffffffff)),
    ?assertEqual(6, diff(0, 16#fffffffa)),
    ?assertEqual(206, diff(200, 16#fffffffa)),
    ?assertEqual(-2, diff(16#ffffffff, 1)),
    ?assertExit({undefined_serial_diff, _, _},
                diff(0, 16#80000000)),
    ?assertExit({undefined_serial_diff, _, _},
                diff(16#ffffffff, 16#7fffffff)).

test_foldl(_Config) ->
    ?assertEqual(
       [16#ffffffff - 1, 16#ffffffff, 0, 1],
       foldl(fun(S, Acc) ->
                     Acc ++ [S]
             end, [], 16#ffffffff - 1, 1)),

    ?assertEqual(
       [0],
       foldl(fun(S, Acc) ->
                     Acc ++ [S]
             end, [], 0, 0)).
