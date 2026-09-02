%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2026 Broadcom. All Rights Reserved. The term "Broadcom" refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.

-module(unit_fifo_client_SUITE).

-compile(nowarn_export_all).
-compile(export_all).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").

all() ->
    [
     return_accumulation_is_not_quadratic,
     discard_accumulation_is_not_quadratic,
     return_and_discard_accumulation_preserves_order
    ].

%% While a consumer is over the soft limit, return/3 and discard/3 stash
%% message ids into the client's own unsent_commands map instead of
%% sending them immediately. Each call must cost O(length(MsgIds)), not
%% O(length(already-accumulated ids)): a client that nacks one message at
%% a time for the whole session must not turn its own client-side state
%% into a quadratic-time structure.
return_accumulation_is_not_quadratic(_Config) ->
    accumulation_is_not_quadratic(return).

discard_accumulation_is_not_quadratic(_Config) ->
    accumulation_is_not_quadratic(discard).

accumulation_is_not_quadratic(Fun) ->
    ServerId = {unit_fifo_client_SUITE_fake_server, node()},
    State0 = rabbit_fifo_client:init([ServerId], 0),
    %% SoftLimit = 0 means the very next command already exceeds it,
    %% flipping the client into the "slow" accumulation path.
    {State1, []} = rabbit_fifo_client:settle(<<"tag">>, [0], State0),

    N = 150_000,
    {Time, _State2} =
        timer:tc(
          fun () ->
                  lists:foldl(
                    fun (I, State) ->
                            {S, []} = rabbit_fifo_client:Fun(<<"tag">>, [I], State),
                            S
                    end, State1, lists:seq(1, N))
          end),
    ct:pal("~b calls to ~p/3 while slow took ~b us", [N, Fun, Time]),
    %% O(n) accumulation comfortably finishes within a second; the
    %% quadratic bug did not for the same N on ordinary hardware.
    ?assert(Time < 1_000_000).

%% rabbit_fifo relies on a #return{}/#discard{} command's msg_ids order to
%% redeliver and dead-letter messages in the order they were returned or
%% discarded (see rabbit_fifo:return_multiple/8, discard_or_dead_letter/4).
%% Accumulating cheaply while slow must not scramble that order across
%% separate return/3 or discard/3 calls.
return_and_discard_accumulation_preserves_order(_Config) ->
    %% #return{} and #discard{} are {Tag, ConsumerKey, MsgIds} tuples;
    %% neither record is exported via a shared header, so msg_ids is read
    %% positionally here.
    order_is_preserved(return, 3),
    order_is_preserved(discard, 3).

order_is_preserved(Fun, MsgIdsFieldPos) ->
    Name = list_to_atom(
             "unit_fifo_client_SUITE_order_test_" ++ atom_to_list(Fun)),
    true = register(Name, self()),
    ServerId = {Name, node()},
    State0 = rabbit_fifo_client:init([ServerId], 0),
    {State1, []} = rabbit_fifo_client:settle(<<"tag">>, [0], State0),
    flush_gen_casts(),

    {State2, []} = rabbit_fifo_client:Fun(<<"tag">>, [1, 2], State1),
    {State3, []} = rabbit_fifo_client:Fun(<<"tag">>, [3, 4], State2),
    %% modify/6 always flushes pending settles/returns/discards first.
    {_State4, []} = rabbit_fifo_client:modify(<<"tag">>, [5], false, false,
                                               #{}, State3),

    MsgIds = receive
                 {'$gen_cast', {command, _Priority, {'$usr', Cmd, _Mode}}} ->
                     element(MsgIdsFieldPos, Cmd)
             after 1000 ->
                     exit({command_not_sent, Fun})
             end,
    true = unregister(Name),
    ?assertEqual([1, 2, 3, 4], MsgIds).

flush_gen_casts() ->
    receive
        {'$gen_cast', _} -> flush_gen_casts()
    after 0 ->
            ok
    end.
