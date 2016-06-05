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
%% Copyright (c) 2007-2015 Pivotal Software, Inc.  All rights reserved.
%%

-module(gm_SUITE).

-behaviour(gm).

-include_lib("common_test/include/ct.hrl").

-include("gm_specs.hrl").

-compile(export_all).

-define(RECEIVE_OR_THROW(Body, Bool, Error),
        receive Body ->
                true = Bool,
                passed
        after 1000 ->
                throw(Error)
        end).

all() ->
    [
      join_leave,
      broadcast,
      confirmed_broadcast,
      member_death,
      receive_in_order
    ].

init_per_suite(Config) ->
    ok = application:set_env(mnesia, dir, ?config(priv_dir, Config)),
    ok = application:start(mnesia),
    {ok, FHC} = file_handle_cache:start_link(),
    unlink(FHC),
    {ok, WPS} = worker_pool_sup:start_link(),
    unlink(WPS),
    rabbit_ct_helpers:set_config(Config, [
        {file_handle_cache_pid, FHC},
        {worker_pool_sup_pid, WPS}
      ]).

end_per_suite(Config) ->
    exit(?config(worker_pool_sup_pid, Config), shutdown),
    exit(?config(file_handle_cache_pid, Config), shutdown),
    ok = application:stop(mnesia),
    Config.

%% ---------------------------------------------------------------------------
%% Functional tests
%% ---------------------------------------------------------------------------

join_leave(_Config) ->
    passed = with_two_members(fun (_Pid, _Pid2) -> passed end).

broadcast(_Config) ->
    passed = do_broadcast(fun gm:broadcast/2).

confirmed_broadcast(_Config) ->
    passed = do_broadcast(fun gm:confirmed_broadcast/2).

member_death(_Config) ->
    passed = with_two_members(
      fun (Pid, Pid2) ->
              {ok, Pid3} = gm:start_link(
                             ?MODULE, ?MODULE, self(),
                             fun rabbit_misc:execute_mnesia_transaction/1),
              passed = receive_joined(Pid3, [Pid, Pid2, Pid3],
                                      timeout_joining_gm_group_3),
              passed = receive_birth(Pid, Pid3, timeout_waiting_for_birth_3_1),
              passed = receive_birth(Pid2, Pid3, timeout_waiting_for_birth_3_2),

              unlink(Pid3),
              exit(Pid3, kill),

              %% Have to do some broadcasts to ensure that all members
              %% find out about the death.
              passed = (broadcast_fun(fun gm:confirmed_broadcast/2))(
                         Pid, Pid2),

              passed = receive_death(Pid, Pid3, timeout_waiting_for_death_3_1),
              passed = receive_death(Pid2, Pid3, timeout_waiting_for_death_3_2),

              passed
      end).

receive_in_order(_Config) ->
    passed = with_two_members(
      fun (Pid, Pid2) ->
              Numbers = lists:seq(1,1000),
              [begin ok = gm:broadcast(Pid, N), ok = gm:broadcast(Pid2, N) end
               || N <- Numbers],
              passed = receive_numbers(
                         Pid, Pid, {timeout_for_msgs, Pid, Pid}, Numbers),
              passed = receive_numbers(
                         Pid, Pid2, {timeout_for_msgs, Pid, Pid2}, Numbers),
              passed = receive_numbers(
                         Pid2, Pid, {timeout_for_msgs, Pid2, Pid}, Numbers),
              passed = receive_numbers(
                         Pid2, Pid2, {timeout_for_msgs, Pid2, Pid2}, Numbers),
              passed
      end).

do_broadcast(Fun) ->
    with_two_members(broadcast_fun(Fun)).

broadcast_fun(Fun) ->
    fun (Pid, Pid2) ->
            ok = Fun(Pid, magic_message),
            passed = receive_or_throw({msg, Pid, Pid, magic_message},
                                      timeout_waiting_for_msg),
            passed = receive_or_throw({msg, Pid2, Pid, magic_message},
                                      timeout_waiting_for_msg)
    end.

with_two_members(Fun) ->
    ok = gm:create_tables(),

    {ok, Pid} = gm:start_link(?MODULE, ?MODULE, self(),
                              fun rabbit_misc:execute_mnesia_transaction/1),
    passed = receive_joined(Pid, [Pid], timeout_joining_gm_group_1),

    {ok, Pid2} = gm:start_link(?MODULE, ?MODULE, self(),
                               fun rabbit_misc:execute_mnesia_transaction/1),
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
    ?RECEIVE_OR_THROW(Pattern, true, Error).

receive_birth(From, Born, Error) ->
    ?RECEIVE_OR_THROW({members_changed, From, Birth, Death},
                      ([Born] == Birth) andalso ([] == Death),
                      Error).

receive_death(From, Died, Error) ->
    ?RECEIVE_OR_THROW({members_changed, From, Birth, Death},
                      ([] == Birth) andalso ([Died] == Death),
                      Error).

receive_joined(From, Members, Error) ->
    ?RECEIVE_OR_THROW({joined, From, Members1},
                      lists:usort(Members) == lists:usort(Members1),
                      Error).

receive_termination(From, Reason, Error) ->
    ?RECEIVE_OR_THROW({termination, From, Reason1},
                      Reason == Reason1,
                      Error).

receive_numbers(_Pid, _Sender, _Error, []) ->
    passed;
receive_numbers(Pid, Sender, Error, [N | Numbers]) ->
    ?RECEIVE_OR_THROW({msg, Pid, Sender, M},
                      M == N,
                      Error),
    receive_numbers(Pid, Sender, Error, Numbers).

%% -------------------------------------------------------------------
%% gm behavior callbacks.
%% -------------------------------------------------------------------

joined(Pid, Members) ->
    Pid ! {joined, self(), Members},
    ok.

members_changed(Pid, Births, Deaths) ->
    Pid ! {members_changed, self(), Births, Deaths},
    ok.

handle_msg(Pid, From, Msg) ->
    Pid ! {msg, self(), From, Msg},
    ok.

handle_terminate(Pid, Reason) ->
    Pid ! {termination, self(), Reason},
    ok.
