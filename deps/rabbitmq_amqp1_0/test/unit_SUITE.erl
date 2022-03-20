%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2022 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(unit_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").

-include("rabbit_amqp1_0.hrl").

-import(rabbit_amqp1_0_util, [serial_add/2, serial_diff/2, serial_compare/2]).

-compile(export_all).

all() ->
    [
      serial_arithmetic
    ].

-include_lib("eunit/include/eunit.hrl").

serial_arithmetic(_Config) ->
    ?assertEqual(1, serial_add(0, 1)),
    ?assertEqual(16#7fffffff, serial_add(0, 16#7fffffff)),
    ?assertEqual(0, serial_add(16#ffffffff, 1)),
    %% Cannot add more than 2 ^ 31 - 1
    ?assertExit({out_of_bound_serial_addition, _, _},
                serial_add(200, 16#80000000)),
    ?assertEqual(1, serial_diff(1, 0)),
    ?assertEqual(2, serial_diff(1, 16#ffffffff)),
    ?assertEqual(-2, serial_diff(16#ffffffff, 1)),
    ?assertExit({indeterminate_serial_diff, _, _},
                serial_diff(0, 16#80000000)),
    ?assertExit({indeterminate_serial_diff, _, _},
                serial_diff(16#ffffffff, 16#7fffffff)).
