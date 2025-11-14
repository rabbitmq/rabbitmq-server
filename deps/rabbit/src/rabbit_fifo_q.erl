%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2025 Broadcom. All Rights Reserved.
%% The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries.
%% All rights reserved.
-module(rabbit_fifo_q).

-include("rabbit_fifo.hrl").
-export([
         new/0,
         in/3,
         out/1,
         get/1,
         len/1,
         to_queues/1,
         from_lqueue/1,
         indexes/1,
         get_lowest_index/1,
         overview/1
        ]).

-define(WEIGHT, 2).
-define(NON_EMPTY, {_, [_|_]}).
-define(EMPTY, {[], []}).

%% a weighted priority queue with only two priorities

-type queue() :: {list(msg()), list(msg())}.

-record(?MODULE, {hi = ?EMPTY :: queue(), %% high
                  no = ?EMPTY :: queue(), %% normal
                  len = 0 :: non_neg_integer(),
                  dequeue_counter = 0 :: non_neg_integer()}).

-opaque state() :: #?MODULE{}.

-export_type([state/0]).

-spec new() -> state().
new() ->
    #?MODULE{}.

-spec in(hi | no, msg(), state()) -> state().
in(hi, Item, #?MODULE{hi = Hi, len = Len} = State) ->
    State#?MODULE{hi = in(Item, Hi),
                  len = Len + 1};
in(no, Item, #?MODULE{no = No, len = Len} = State) ->
    State#?MODULE{no = in(Item, No),
                  len = Len + 1}.

-spec out(state()) ->
    empty | {msg(), state()}.
out(#?MODULE{len = 0}) ->
    empty;
out(#?MODULE{hi = Hi0,
             no = No0,
             len = Len,
             dequeue_counter = C0} = State) ->
    C = case C0 of
            ?WEIGHT ->
                0;
            _ ->
                C0 + 1
        end,
    case next(State) of
        {hi, Msg} ->
            {Msg, State#?MODULE{hi = drop(Hi0),
                                dequeue_counter = C,
                                len = Len - 1}};
        {no, Msg} ->
            {Msg, State#?MODULE{no = drop(No0),
                                dequeue_counter = C,
                                len = Len - 1}}
    end.

-spec get(state()) -> empty | msg().
get(#?MODULE{len = 0}) ->
    empty;
get(#?MODULE{} = State) ->
    {_, Msg} = next(State),
    Msg.

-spec len(state()) -> non_neg_integer().
len(#?MODULE{len = Len}) ->
    Len.

-spec to_queues(state()) -> {High :: queue(),
                             Normal :: queue()}.
to_queues(#?MODULE{hi = Hi,
                   no = No}) ->
    {Hi, No}.


-spec from_lqueue(lqueue:lqueue(msg())) -> state().
from_lqueue(LQ) ->
    lqueue:fold(fun (Item, Acc) ->
                        in(no, Item, Acc)
                end, new(), LQ).

-spec indexes(state()) -> [ra:index()].
indexes(#?MODULE{hi = {Hi1, Hi2},
                 no = {No1, No2}}) ->
    A = lists:map(fun msg_idx/1, Hi1),
    B = lists:foldl(fun msg_idx_fld/2, A, Hi2),
    C = lists:foldl(fun msg_idx_fld/2, B, No1),
    lists:foldl(fun msg_idx_fld/2, C, No2).

-spec get_lowest_index(state()) -> undefined | ra:index().
get_lowest_index(#?MODULE{len = 0}) ->
    undefined;
get_lowest_index(#?MODULE{hi = Hi, no = No}) ->
    case peek(Hi) of
        empty ->
            msg_idx(peek(No));
        HiMsg ->
            HiIdx = msg_idx(HiMsg),
            case peek(No) of
                empty ->
                    HiIdx;
                NoMsg ->
                    min(HiIdx, msg_idx(NoMsg))
            end
    end.

-spec overview(state()) ->
    #{len := non_neg_integer(),
      num_hi := non_neg_integer(),
      num_no := non_neg_integer(),
      lowest_index := ra:index()}.
overview(#?MODULE{len = Len,
                  hi = {Hi1, Hi2},
                  no = _} = State) ->
    %% TODO: this could be very slow with large backlogs,
    %% consider keeping a separate counter for 'hi', 'no' messages
    NumHi = length(Hi1) + length(Hi2),
    #{len => Len,
      num_hi => NumHi,
      num_no => Len - NumHi,
      lowest_index => get_lowest_index(State)}.

%% internals

next(#?MODULE{hi = ?NON_EMPTY = Hi,
              no = ?NON_EMPTY = No,
              dequeue_counter = ?WEIGHT}) ->
    HiMsg = peek(Hi),
    NoMsg = peek(No),
    HiIdx = msg_idx(HiMsg),
    NoIdx = msg_idx(NoMsg),
    %% always favour hi priority messages when it is safe to do so,
    %% i.e. the index is lower than the next index for the 'no' queue
    case HiIdx < NoIdx of
        true ->
            {hi, HiMsg};
        false ->
            {no, NoMsg}
    end;
next(#?MODULE{hi = ?NON_EMPTY = Hi}) ->
    {hi, peek(Hi)};
next(#?MODULE{no = No}) ->
    {no, peek(No)}.

%% invariant, if the queue is non empty so is the Out (right) list.
in(X, ?EMPTY) ->
    {[], [X]};
in(X, {In, Out}) ->
    {[X | In], Out}.

peek(?EMPTY) ->
    empty;
peek({_, [H | _]}) ->
    H.

drop({In, [_]}) ->
    %% the last Out one
    {[], lists:reverse(In)};
drop({In, [_ | Out]}) ->
    {In, Out}.

msg_idx_fld(Msg, Acc) when is_list(Acc) ->
    [msg_idx(Msg) | Acc].

msg_idx(?MSG(Idx, _Header)) ->
    Idx;
msg_idx(Packed) when ?IS_PACKED(Packed) ->
    ?PACKED_IDX(Packed).
