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
%% The Initial Developer of the Original Code is VMware, Inc.
%% Copyright (c) 2007-2012 VMware, Inc.  All rights reserved.
%%

-module(gm_soak_test).

-export([test/0]).
-export([joined/2, members_changed/3, handle_msg/3, terminate/2]).

-behaviour(gm).

-include("gm_specs.hrl").

%% ---------------------------------------------------------------------------
%% Soak test
%% ---------------------------------------------------------------------------

get_state() ->
    get(state).

with_state(Fun) ->
    put(state, Fun(get_state())).

inc() ->
    case 1 + get(count) of
        100000 -> Now = now(),
                  Start = put(ts, Now),
                  Diff = timer:now_diff(Now, Start),
                  Rate = 100000 / (Diff / 1000000),
                  io:format("~p seeing ~p msgs/sec~n", [self(), Rate]),
                  put(count, 0);
        N      -> put(count, N)
    end.

joined([], Members) ->
    io:format("Joined ~p (~p members)~n", [self(), length(Members)]),
    put(state, dict:from_list([{Member, empty} || Member <- Members])),
    put(count, 0),
    put(ts, now()),
    ok.

members_changed([], Births, Deaths) ->
    with_state(
      fun (State) ->
              State1 =
                  lists:foldl(
                    fun (Born, StateN) ->
                            false = dict:is_key(Born, StateN),
                            dict:store(Born, empty, StateN)
                    end, State, Births),
              lists:foldl(
                fun (Died, StateN) ->
                        true = dict:is_key(Died, StateN),
                        dict:store(Died, died, StateN)
                end, State1, Deaths)
      end),
    ok.

handle_msg([], From, {test_msg, Num}) ->
    inc(),
    with_state(
      fun (State) ->
              ok = case dict:find(From, State) of
                       {ok, died} ->
                           exit({{from, From},
                                 {received_posthumous_delivery, Num}});
                       {ok, empty} -> ok;
                       {ok, Num}   -> ok;
                       {ok, Num1} when Num < Num1 ->
                           exit({{from, From},
                                 {duplicate_delivery_of, Num},
                                 {expecting, Num1}});
                       {ok, Num1} ->
                           exit({{from, From},
                                 {received_early, Num},
                                 {expecting, Num1}});
                       error ->
                           exit({{from, From},
                                 {received_premature_delivery, Num}})
                   end,
              dict:store(From, Num + 1, State)
      end),
    ok.

terminate([], Reason) ->
    io:format("Left ~p (~p)~n", [self(), Reason]),
    ok.

spawn_member() ->
    spawn_link(
      fun () ->
              {MegaSecs, Secs, MicroSecs} = now(),
              random:seed(MegaSecs, Secs, MicroSecs),
              %% start up delay of no more than 10 seconds
              timer:sleep(random:uniform(10000)),
              {ok, Pid} = gm:start_link(?MODULE, ?MODULE, []),
              Start = random:uniform(10000),
              send_loop(Pid, Start, Start + random:uniform(10000)),
              gm:leave(Pid),
              spawn_more()
      end).

spawn_more() ->
    [spawn_member() || _ <- lists:seq(1, 4 - random:uniform(4))].

send_loop(_Pid, Target, Target) ->
    ok;
send_loop(Pid, Count, Target) when Target > Count ->
    case random:uniform(3) of
        3 -> gm:confirmed_broadcast(Pid, {test_msg, Count});
        _ -> gm:broadcast(Pid, {test_msg, Count})
    end,
    timer:sleep(random:uniform(5) - 1), %% sleep up to 4 ms
    send_loop(Pid, Count + 1, Target).

test() ->
    ok = gm:create_tables(),
    spawn_member(),
    spawn_member().
