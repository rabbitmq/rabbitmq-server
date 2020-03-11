%% The contents of this file are subject to the Mozilla Public License
%% Version 1.1 (the "License"); you may not use this file except in
%% compliance with the License. You may obtain a copy of the License
%% at https://www.mozilla.org/MPL/
%%
%% Software distributed under the License is distributed on an "AS IS"
%% basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See
%% the License for the specific language governing rights and
%% limitations under the License.
%%
%% The Original Code is RabbitMQ.
%%
%% Copyright (c) 2020 Pivotal Software, Inc.  All rights reserved.
%%

-module(gatherer_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").

-export([all/0,
         groups/0,
         init_per_group/2,
         end_per_group/2,
         init_per_testcase/2,
         end_per_testcase/2,

         enqueue_and_dequeue/1,
         enqueue_and_dequeue_multiple/1,
         enqueue_and_dequeue_before_finish/1,
         producer_is_blocked_until_exists_consumer/1,
         producer_cannot_outpace_consumer/1
        ]).

all() ->
    [
     {group, synchronous_tests},
     {group, asynchronous_tests}
    ].

groups() ->
    [
     {synchronous_tests, [],
      [enqueue_and_dequeue,
       enqueue_and_dequeue_multiple,
       enqueue_and_dequeue_before_finish]},
     {asynchronous_tests, [],
      [producer_is_blocked_until_exists_consumer,
       producer_cannot_outpace_consumer]}
    ].

%% -------------------------------------------------------------------
%% Testsuite setup/teardown.
%% -------------------------------------------------------------------

init_per_group(_, Config) ->
    Config.

end_per_group(_, Config) ->
    Config.

init_per_testcase(_Testcase, Config) ->
    Config.

end_per_testcase(_Testcase, _Config) ->
    ok.

%% -------------------------------------------------------------------
%% Testcases.
%% -------------------------------------------------------------------

enqueue_and_dequeue(_Config) ->
    {ok, Gatherer} = gatherer:start_link(),
    ok = gatherer:fork(Gatherer),
    ok = gatherer:in(Gatherer, test_value),
    ok = gatherer:finish(Gatherer),
    ?assertEqual({value, test_value}, gatherer:out(Gatherer)),
    ?assertEqual(empty, gatherer:out(Gatherer)),
    ok = gatherer:stop(Gatherer).

enqueue_and_dequeue_multiple(_Config) ->
    {ok, Gatherer} = gatherer:start_link(),
    ok = gatherer:fork(Gatherer),
    ok = gatherer:in(Gatherer, test_value),
    ok = gatherer:in(Gatherer, test_value2),
    ok = gatherer:finish(Gatherer),
    ?assertEqual({value, test_value}, gatherer:out(Gatherer)),
    ?assertEqual({value, test_value2}, gatherer:out(Gatherer)),
    ?assertEqual(empty, gatherer:out(Gatherer)),
    ok = gatherer:stop(Gatherer).

enqueue_and_dequeue_before_finish(_Config) ->
    {ok, Gatherer} = gatherer:start_link(),
    ok = gatherer:fork(Gatherer),
    ok = gatherer:in(Gatherer, test_value),
    ?assertEqual({value, test_value}, gatherer:out(Gatherer)),
    ok = gatherer:finish(Gatherer),
    empty = gatherer:out(Gatherer),
    ok = gatherer:stop(Gatherer).

producer_is_blocked_until_exists_consumer(_Config) ->
    {ok, Gatherer} = gatherer:start_link(),
    Consumer = self(),
    Producer = spawn_link(fun () ->
                                  ok = gatherer:fork(Gatherer),
                                  ok = gatherer:sync_in(Gatherer, test_value),
                                  Consumer ! finished,
                                  ok = gatherer:finish(Gatherer)
                          end),
    receive
        _ -> 
            ?assert(false, "Producer should not have finished yet")
    after
        500 ->
            ok
    end,
    ?assertEqual({value, test_value}, gatherer:out(Gatherer)),
    receive
        finished ->
            ok
    after
        500 ->
            ?assert(false, "Producer failed to finish")
    end,
    empty = gatherer:out(Gatherer),
    ok = gatherer:stop(Gatherer).

producer_cannot_outpace_consumer(_Config) ->
    {ok, Gatherer} = gatherer:start_link(),
    Consumer = self(),
    Producer = spawn_link(fun () ->
                                  ok = gatherer:fork(Gatherer),
                                  ok = gatherer:sync_in(Gatherer, test_value),
                                  ok = gatherer:sync_in(Gatherer, test_value2),
                                  Consumer ! finished,
                                  ok = gatherer:finish(Gatherer)
                          end),
    receive
        _ -> 
            ?assert(false, "Producer should not have finished yet")
    after
        500 ->
            ok
    end,
    ?assertEqual({value, test_value}, gatherer:out(Gatherer)),
    receive
        _ -> 
            ?assert(false, "Producer should not have finished yet")
    after
        500 ->
            ok
    end,
    ?assertEqual({value, test_value2}, gatherer:out(Gatherer)),
    receive
        finished ->
            ok
    after
        500 ->
            ?assert(false, "Producer failed to finish")
    end,
    empty = gatherer:out(Gatherer),
    ok = gatherer:stop(Gatherer).
