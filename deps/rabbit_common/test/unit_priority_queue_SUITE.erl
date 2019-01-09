%% The contents of this file are subject to the Mozilla Public License
%% Version 1.1 (the "License"); you may not use this file except in
%% compliance with the License. You may obtain a copy of the License at
%% http://www.mozilla.org/MPL/
%%
%% Software distributed under the License is distributed on an "AS IS"
%% basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See the
%% License for the specific language governing rights and limitations
%% under the License.
%%
%% The Original Code is RabbitMQ.
%%
%% The Initial Developer of the Original Code is GoPivotal, Inc.
%% Copyright (c) 2018 Pivotal Software, Inc.  All rights reserved.
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