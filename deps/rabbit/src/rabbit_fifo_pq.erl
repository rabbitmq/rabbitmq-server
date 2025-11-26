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
         overview/1,
         take_while/2,
         any_priority_next/2,
         fold_priorities_next/3
        ]).

-define(STATE, pq).
-define(EMPTY, {0, [], []}).

%% supports 32 priorities, needs to be a power of 2 to support the De Bruijn
%% lookup method. 64 would push the bitmap into an erlang big number so we
%% have to settle for 32
-type priority() :: 0..31.
-type queue() :: {non_neg_integer(), list(msg()), list(msg())}.

-record(?STATE, {buckets = #{} :: #{priority() => queue()},
                 len = 0 :: non_neg_integer(),
                 bitmap = 0 :: integer()}).

-opaque state() :: #?STATE{}.

-export_type([state/0,
              priority/0]).

-spec new() -> state().
new() ->
    #?STATE{}.

-spec in(priority(), msg(), state()) -> state().
in(Priority0, Item, #?STATE{buckets = Buckets0,
                            bitmap = Bitmap0,
                            len = Len} = State)
  when Priority0 >= 0 andalso
       Priority0 =< 31 ->
    %% invert priority
    Priority = 31 - Priority0,
    case Buckets0 of
        #{Priority := Queue0} ->
            %% there are messages for the priority already
            State#?STATE{buckets = Buckets0#{Priority => in(Item, Queue0)},
                         len = Len + 1};
        _ ->
            Bitmap = Bitmap0 bor (1 bsl Priority),
            %% there are no messages for the priority
            State#?STATE{buckets = Buckets0#{Priority => in(Item, ?EMPTY)},
                         bitmap = Bitmap,
                         len = Len + 1}
    end.

-spec out(state()) ->
    empty | {msg(), state()}.
out(#?STATE{len = 0}) ->
    empty;
out(#?STATE{buckets = Buckets,
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
            {Msg, State#?STATE{buckets = maps:remove(Priority, Buckets),
                               len = Len - 1,
                               bitmap = Bitmap}};
        Q ->
            {Msg, State#?STATE{buckets = maps:put(Priority, Q, Buckets),
                               len = Len - 1}}
    end.

-spec get(state()) -> empty | msg().
get(#?STATE{len = 0}) ->
    empty;
get(#?STATE{buckets = Buckets,
            bitmap = Bitmap}) ->
    Priority = first_set_bit(Bitmap),
    #{Priority := Q0} = Buckets,
    peek(Q0).

-spec len(state()) -> non_neg_integer().
len(#?STATE{len = Len}) ->
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
indexes(#?STATE{buckets = Buckets}) ->
    maps:fold(
      fun (_P, {_, L1, L2}, Acc0) ->
              Acc = lists:foldl(fun msg_idx_fld/2, Acc0, L1),
              lists:foldl(fun msg_idx_fld/2, Acc, L2)
      end, [], Buckets).

-spec get_lowest_index(state()) -> undefined | ra:index().
get_lowest_index(#?STATE{len = 0}) ->
    undefined;
get_lowest_index(#?STATE{buckets = Buckets}) ->
    maps:fold(fun (_, Q, Acc) ->
                      case peek(Q) of
                          empty ->
                              Acc;
                          Msg ->
                              min(msg_idx(Msg), Acc)
                      end
              end, undefined, Buckets).

-spec overview(state()) ->
    #{len := non_neg_integer(),
      detail := #{priority() => pos_integer()},
      num_active_priorities := 0..32,
      lowest_index := ra:index()}.
overview(#?STATE{len = Len,
                 buckets = Buckets} = State) ->
    Detail  = maps:fold(fun (P0, {C, _, _}, Acc) ->
                                P = 31-P0,
                                Acc#{P => C}
                        end, #{}, Buckets),
    #{len => Len,
      detail => Detail,
      num_active_priorities => map_size(Buckets),
      lowest_index => get_lowest_index(State)}.

-spec take_while(fun ((msg()) -> boolean()), state()) ->
    {[msg()], state()}.
take_while(Fun, #?STATE{len = Len,
                        buckets = Buckets0} = State)
  when is_function(Fun) ->
    {Buckets, Acc} = maps:fold(
                       fun (P, Q0, {B0, Items0}) ->
                               case take_while(Q0, Fun, Items0) of
                                   {?EMPTY, Items} ->
                                       {maps:remove(P, B0), Items};
                                   {Q, Items} ->
                                       {B0#{P => Q}, Items}
                               end
                       end,
                       {Buckets0, []},
                       maps:iterator(Buckets0, ordered)),

    %% TODO: optimise updates
    %% update bitmap
    Bitmap = maps:fold(fun (P, _Q, B) -> B bor (1 bsl P) end, 0, Buckets),

    {lists:reverse(Acc),
     State#?STATE{len = Len - length(Acc),
                  buckets = Buckets,
                  bitmap = Bitmap}}.

-spec any_priority_next(fun ((msg()) -> boolean()), state()) ->
    boolean().
any_priority_next(Fun, #?STATE{buckets = Buckets0})
  when is_function(Fun) ->
    maps_any(Fun, maps:next(maps:iterator(Buckets0))).

-spec fold_priorities_next(fun ((msg(), Acc) -> Acc), Acc, state()) ->
    Acc when Acc :: term().
fold_priorities_next(Fun, Acc, #?STATE{buckets = Buckets0})
  when is_function(Fun) ->
    maps:fold(fun (_P, Q, A) ->
                      Fun(peek(Q), A)
              end, Acc, Buckets0).

%% INTERNAL

maps_any(_Fun, none) ->
    false;
maps_any(Fun, {_, Q, I}) ->
    case Fun(peek(Q)) of
        true ->
            true;
        false ->
            maps_any(Fun, maps:next(I))
    end.

take_while(?EMPTY, _Fun, Acc) ->
    {?EMPTY, Acc};
take_while(Q, Fun, Acc) ->
    case peek(Q) of
        empty ->
            {Q, Acc};
        Msg ->
            case Fun(Msg) of
                true ->
                    take_while(drop(Q), Fun, [Msg | Acc]);
                false ->
                    {Q, Acc}
            end
    end.



%% invariant, if the queue is non empty so is the Out (right) list.
in(X, ?EMPTY) ->
    {1, [], [X]};
in(X, {C, In, Out}) ->
    {C+1, [X | In], Out}.

peek(?EMPTY) ->
    empty;
peek({_, _, [H | _]}) ->
    H.

drop({C, In, [_]}) ->
    %% the last Out one
    {C-1, [], lists:reverse(In)};
drop({C, In, [_ | Out]}) ->
    {C-1, In, Out}.

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

