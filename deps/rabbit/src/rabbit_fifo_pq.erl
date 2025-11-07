%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2025 Broadcom. All Rights Reserved.
%% The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries.
%% All rights reserved.
-module(rabbit_fifo_pq).

-include("rabbit_fifo.hrl").
-export([
         new/0,
         in/3,
         out/1,
         get/1,
         len/1,
         to_list/1,
         from_list/1,
         from_lqueue/1,
         indexes/1,
         get_lowest_index/1,
         overview/1
        ]).

% -define(NON_EMPTY, {_, [_|_]}).
-define(EMPTY, {[], []}).

%% a weighted priority queue with only two priorities
-type priority() :: 0..31.
-type queue() :: {list(msg()), list(msg())}.

-record(?MODULE, {buckets = #{} :: #{priority() => queue()},
                  len = 0 :: non_neg_integer(),
                  bitmap = 0 :: integer()}).

-opaque state() :: #?MODULE{}.

-export_type([state/0,
              priority/0]).

-spec new() -> state().
new() ->
    #?MODULE{}.

-spec in(priority(), msg(), state()) -> state().
in(Priority0, Item, #?MODULE{buckets = Buckets0,
                             bitmap = Bitmap0,
                             len = Len} = State)
  when Priority0 >= 0 andalso
       Priority0 =< 31 ->
    %% invert priority
    Priority = 31 - Priority0,
    case Buckets0 of
        #{Priority := Queue0} ->
            %% there are messages for the priority already
            State#?MODULE{buckets = Buckets0#{Priority => in(Item, Queue0)},
                          len = Len + 1};
        _ ->
            Bitmap = Bitmap0 bor (1 bsl Priority),
            %% there are no messages for the priority
            State#?MODULE{buckets = Buckets0#{Priority => in(Item, ?EMPTY)},
                          bitmap = Bitmap,
                          len = Len + 1}
    end.

first_set_bit(0) ->
    32;
first_set_bit(Bitmap) ->
    count_trailing(Bitmap band -Bitmap).

-define(DEBRUIJN_SEQ, 16#077CB531).
-define(DEBRUIJN_LOOKUP,
        {0, 1, 28, 2, 29, 14, 24, 3, 30, 22, 20, 15, 25, 17, 4, 8,
         31, 27, 13, 23, 21, 19, 16, 7, 26, 12, 18, 6, 11, 5, 10, 9}).

count_trailing(N) ->
    Lookup = ((N * ?DEBRUIJN_SEQ) bsr 27) band 31,
    element(Lookup + 1, ?DEBRUIJN_LOOKUP).

-spec out(state()) ->
    empty | {msg(), state()}.
out(#?MODULE{len = 0}) ->
    empty;
out(#?MODULE{buckets = Buckets,
             len = Len,
             bitmap = Bitmap0} = State) ->
    Priority = first_set_bit(Bitmap0),
    #{Priority := Q0} = Buckets,
    Msg = peek(Q0),
    case drop(Q0) of
        ?EMPTY ->
            %% zero bit in bitmap
            %% as we know the bit is set we just need to xor rather than
            %% create a mask then xor
            Bitmap = Bitmap0 bxor (1 bsl Priority),
            {Msg, State#?MODULE{buckets = maps:remove(Priority, Buckets),
                                len = Len - 1,
                                bitmap = Bitmap}};
        Q ->
            {Msg, State#?MODULE{buckets = maps:put(Priority, Q, Buckets),
                                len = Len - 1}}
    end.

-spec get(state()) -> empty | msg().
get(#?MODULE{len = 0}) ->
    empty;
get(#?MODULE{buckets = Buckets,
             bitmap = Bitmap}) ->
    Priority = first_set_bit(Bitmap),
    #{Priority := Q0} = Buckets,
    peek(Q0).

-spec len(state()) -> non_neg_integer().
len(#?MODULE{len = Len}) ->
    Len.

-spec from_list([{priority(), term()}]) -> state().
from_list(Items) when is_list(Items) ->
    lists:foldl(fun ({P, Item}, Acc) ->
                        in(P, Item, Acc)
                end, new(), Items).

-spec to_list(state()) -> [msg()].
to_list(State) ->
    to_list(out(State), []).

-spec from_lqueue(lqueue:lqueue(msg())) -> state().
from_lqueue(LQ) ->
    lqueue:fold(fun (Item, Acc) ->
                        in(4, Item, Acc)
                end, new(), LQ).

-spec indexes(state()) -> [ra:index()].
indexes(#?MODULE{buckets = Buckets}) ->
    maps:fold(
      fun (_P, {L1, L2}, Acc0) ->
              Acc = lists:foldl(fun msg_idx_fld/2, Acc0, L1),
              lists:foldl(fun msg_idx_fld/2, Acc, L2)
      end, [], Buckets).

-spec get_lowest_index(state()) -> undefined | ra:index().
get_lowest_index(#?MODULE{len = 0}) ->
    undefined;
get_lowest_index(#?MODULE{buckets = Buckets}) ->
    lists:min(
      maps:fold(fun (_, Q, Acc) ->
                        case peek(Q) of
                            empty ->
                                Acc;
                            Msg ->
                                [msg_idx(Msg) | Acc]
                        end
                end, [], Buckets)).

-spec overview(state()) ->
    #{len := non_neg_integer(),
      num_active_priorities := 0..32,
      lowest_index := ra:index()}.
overview(#?MODULE{len = Len,
                  buckets = Buckets} = State) ->
    #{len => Len,
      num_active_priorities => map_size(Buckets),
      lowest_index => get_lowest_index(State)}.

%% internals

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

to_list(empty, Acc) ->
    lists:reverse(Acc);
to_list({Item, State}, Acc) ->
    to_list(out(State), [Item | Acc]).

