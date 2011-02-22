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
%% Copyright (c) 2007-2011 VMware, Inc.  All rights reserved.
%%

-module(gm_tests).

-export([test_join_leave/0,
         test_broadcast/0,
         test_confirmed_broadcast/0,
         test_member_death/0,
         all_tests/0]).
-export([joined/2, members_changed/3, handle_msg/3, terminate/2]).

-behaviour(gm).

-include("gm_specs.hrl").

-define(RECEIVE_AFTER(Body, Bool, Error),
        receive Body ->
                true = Bool,
                passed
        after 1000 ->
                throw(Error)
        end).

joined(Pid, Members) ->
    Pid ! {joined, self(), Members},
    ok.

members_changed(Pid, Births, Deaths) ->
    Pid ! {members_changed, self(), Births, Deaths},
    ok.

handle_msg(Pid, From, Msg) ->
    Pid ! {msg, self(), From, Msg},
    ok.

terminate(Pid, Reason) ->
    Pid ! {termination, self(), Reason},
    ok.

%% ---------------------------------------------------------------------------
%% Functional tests
%% ---------------------------------------------------------------------------

all_tests() ->
    passed = test_join_leave(),
    passed = test_broadcast(),
    passed = test_confirmed_broadcast(),
    passed = test_member_death(),
    passed.

test_join_leave() ->
    with_two_members(fun (_Pid, _Pid2) -> passed end).

test_broadcast() ->
    test_broadcast(fun gm:broadcast/2).

test_confirmed_broadcast() ->
    test_broadcast(fun gm:confirmed_broadcast/2).

test_member_death() ->
    with_two_members(
      fun (Pid, Pid2) ->
              {ok, Pid3} = gm:start_link(?MODULE, ?MODULE, self()),
              passed = receive_joined(Pid3, [Pid, Pid2, Pid3],
                                      timeout_joining_gm_group_3),
              passed = receive_birth(Pid, Pid3, timeout_waiting_for_birth_3_1),
              passed = receive_birth(Pid2, Pid3, timeout_waiting_for_birth_3_2),

              unlink(Pid3),
              exit(Pid3, kill),

              passed = (test_broadcast_fun(fun gm:confirmed_broadcast/2))(
                         Pid, Pid2),

              passed = receive_death(Pid, Pid3, timeout_waiting_for_death_3_1),
              passed = receive_death(Pid2, Pid3, timeout_waiting_for_death_3_2),

              passed
      end).

test_broadcast(Fun) ->
    with_two_members(test_broadcast_fun(Fun)).

test_broadcast_fun(Fun) ->
      fun (Pid, Pid2) ->
              ok = Fun(Pid, magic_message),
              passed = receive_or_throw({msg, Pid, Pid, magic_message},
                                        timeout_waiting_for_msg),
              passed = receive_or_throw({msg, Pid2, Pid, magic_message},
                                        timeout_waiting_for_msg)
      end.

with_two_members(Fun) ->
    ok = gm:create_tables(),

    {ok, Pid} = gm:start_link(?MODULE, ?MODULE, self()),
    passed = receive_joined(Pid, [Pid], timeout_joining_gm_group_1),

    {ok, Pid2} = gm:start_link(?MODULE, ?MODULE, self()),
    passed = receive_joined(Pid2, [Pid, Pid2], timeout_joining_gm_group_2),

    passed = receive_birth(Pid, Pid2, timeout_waiting_for_birth_2),

    passed = Fun(Pid, Pid2),

    ok = gm:leave(Pid),
    passed = receive_death(Pid2, Pid, timeout_waiting_for_death_1),
    passed =
        receive_termination(Pid, normal, timeout_waiting_for_termination_1),

    ok = gm:leave(Pid2),
    passed =
        receive_termination(Pid2, normal, timeout_waiting_for_termination_2),

    receive X -> throw({unexpected_message, X})
    after 0 -> passed
    end.

receive_or_throw(Pattern, Error) ->
    ?RECEIVE_AFTER(Pattern, true, Error).

receive_birth(From, Born, Error) ->
    ?RECEIVE_AFTER({members_changed, From, Birth, Death},
                   ([Born] == Birth) andalso ([] == Death),
                   Error).

receive_death(From, Died, Error) ->
    ?RECEIVE_AFTER({members_changed, From, Birth, Death},
                   ([] == Birth) andalso ([Died] == Death),
                   Error).

receive_joined(From, Members, Error) ->
    ?RECEIVE_AFTER({joined, From, Members2},
                   lists:usort(Members) == lists:usort(Members2),
                   Error).

receive_termination(From, Reason, Error) ->
    ?RECEIVE_AFTER({termination, From, Reason1},
                   Reason == Reason1,
                   Error).
