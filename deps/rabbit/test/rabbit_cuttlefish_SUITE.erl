%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2023-2023 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(rabbit_cuttlefish_SUITE).

-include_lib("eunit/include/eunit.hrl").

-compile(export_all).

all() ->
    [
        aggregate_props_empty,
        aggregate_props_one_group,
        aggregate_props_two_groups,
        aggregate_props_with_prefix
    ].

groups() ->
    [
        {parallel_tests, [parallel], all()}
    ].

aggregate_props_empty(_) ->
    ?assertEqual(rabbit_cuttlefish:aggregate_props([], []), []).


aggregate_props_one_group(_) ->
    Have = rabbit_cuttlefish:aggregate_props([
         {["1", "banana"], "apple"},
         {["1", "orange"], "carrot"}
        ], []),
    ?assertEqual(Have,
                 [{<<"1">>, [{<<"banana">>, "apple"},
                             {<<"orange">>, "carrot"}]}]).

aggregate_props_two_groups(_) ->
    Have = rabbit_cuttlefish:aggregate_props([
         {["1", "banana"], "apple"},
         {["2", "orange"], "carrot"}
        ], []),
    ?assertEqual(Have,
                 [{<<"1">>, [{<<"banana">>, "apple"}]},
                  {<<"2">>, [{<<"orange">>, "carrot"}]}]).


aggregate_props_with_prefix(_) ->
    Have = rabbit_cuttlefish:aggregate_props([
            {["pre", "fix", "1", "banana"], "apple"},
            {["other"], "settings"}
        ], ["pre", "fix"]),
    ?assertEqual(Have, [{<<"1">>, [{<<"banana">>, "apple"}]}]).
