%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2018-2021 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(unit_priority_queue_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").

-compile(export_all).

all() ->
    [
        member,
        member_priority_queue
    ].

member(_Config) ->
    Q = lists:foldl(fun(V, Acc) -> priority_queue:in(V, Acc) end, priority_queue:new(), lists:seq(1, 10)),
    ?assert(priority_queue:member(1, Q)),
    ?assert(priority_queue:member(2, Q)),
    ?assertNot(priority_queue:member(100, Q)),
    ?assertNot(priority_queue:member(1, priority_queue:new())),
    ok.

member_priority_queue(_Config) ->
    Q = lists:foldl(fun(V, Acc) -> priority_queue:in(V, V rem 4, Acc) end, priority_queue:new(),
        lists:seq(1, 100)),
    ?assert(priority_queue:member(1, Q)),
    ?assert(priority_queue:member(50, Q)),
    ?assertNot(priority_queue:member(200, Q)),
    ok.
