%% The contents of this file are subject to the Mozilla Public License
%% Version 1.1 (the "License"); you may not use this file except in
%% compliance with the License. You may obtain a copy of the License
%% at http://www.mozilla.org/MPL/
%%
%% Software distributed under the License is distributed on an "AS IS"
%% basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See
%% the License for the specific language governing rights and
%% limitations under the License.
%%
%% The Original Code is RabbitMQ.
%%
%% The Initial Developer of the Original Code is VMware, Inc.
%% Copyright (c) 2007-2011 VMware, Inc.  All rights reserved.
%%

-module(bpqueue).

%% Block-prefixed queue. From the perspective of the queue interface
%% the datastructure acts like a regular queue where each value is
%% paired with the prefix.
%%
%% This is implemented as a queue of queues, which is more space and
%% time efficient, whilst supporting the normal queue interface. Each
%% inner queue has a prefix, which does not need to be unique, and it
%% is guaranteed that no two consecutive blocks have the same
%% prefix. len/1 returns the flattened length of the queue and is
%% O(1).

-export([new/0, is_empty/1, len/1, in/3, in_r/3, out/1, out_r/1, join/2,
         foldl/3, foldr/3, from_list/1, to_list/1, map_fold_filter_l/4,
         map_fold_filter_r/4]).

%%----------------------------------------------------------------------------

-ifdef(use_specs).

-export_type([bpqueue/0]).

-type(bpqueue() :: {non_neg_integer(), queue()}).
-type(prefix() :: any()).
-type(value() :: any()).
-type(result() :: ({'empty', bpqueue()} |
                   {{'value', prefix(), value()}, bpqueue()})).

-spec(new/0 :: () -> bpqueue()).
-spec(is_empty/1 :: (bpqueue()) -> boolean()).
-spec(len/1 :: (bpqueue()) -> non_neg_integer()).
-spec(in/3 :: (prefix(), value(), bpqueue()) -> bpqueue()).
-spec(in_r/3 :: (prefix(), value(), bpqueue()) -> bpqueue()).
-spec(out/1 :: (bpqueue()) -> result()).
-spec(out_r/1 :: (bpqueue()) -> result()).
-spec(join/2 :: (bpqueue(), bpqueue()) -> bpqueue()).
-spec(foldl/3 :: (fun ((prefix(), value(), B) -> B), B, bpqueue()) -> B).
-spec(foldr/3 :: (fun ((prefix(), value(), B) -> B), B, bpqueue()) -> B).
-spec(from_list/1 :: ([{prefix(), [value()]}]) -> bpqueue()).
-spec(to_list/1 :: (bpqueue()) -> [{prefix(), [value()]}]).
-spec(map_fold_filter_l/4 :: ((fun ((prefix()) -> boolean())),
                              (fun ((value(), B) ->
                                           ({prefix(), value(), B} | 'stop'))),
                              B,
                              bpqueue()) ->
             {bpqueue(), B}).
-spec(map_fold_filter_r/4 :: ((fun ((prefix()) -> boolean())),
                              (fun ((value(), B) ->
                                           ({prefix(), value(), B} | 'stop'))),
                              B,
                              bpqueue()) ->
             {bpqueue(), B}).

-endif.

%%----------------------------------------------------------------------------

new() -> {0, queue:new()}.

is_empty({0, _Q}) -> true;
is_empty(_BPQ)    -> false.

len({N, _Q}) -> N.

in(Prefix, Value, {0, Q}) ->
    {1, queue:in({Prefix, queue:from_list([Value])}, Q)};
in(Prefix, Value, BPQ) ->
    in1({fun queue:in/2, fun queue:out_r/1}, Prefix, Value, BPQ).

in_r(Prefix, Value, BPQ = {0, _Q}) ->
    in(Prefix, Value, BPQ);
in_r(Prefix, Value, BPQ) ->
    in1({fun queue:in_r/2, fun queue:out/1}, Prefix, Value, BPQ).

in1({In, Out}, Prefix, Value, {N, Q}) ->
    {N+1, case Out(Q) of
              {{value, {Prefix, InnerQ}}, Q1} ->
                  In({Prefix, In(Value, InnerQ)}, Q1);
              {{value, {_Prefix, _InnerQ}}, _Q1} ->
                  In({Prefix, queue:in(Value, queue:new())}, Q)
          end}.

in_q(Prefix, Queue, BPQ = {0, Q}) ->
    case queue:len(Queue) of
        0 -> BPQ;
        N -> {N, queue:in({Prefix, Queue}, Q)}
    end;
in_q(Prefix, Queue, BPQ) ->
    in_q1({fun queue:in/2, fun queue:out_r/1,
           fun queue:join/2},
          Prefix, Queue, BPQ).

in_q_r(Prefix, Queue, BPQ = {0, _Q}) ->
    in_q(Prefix, Queue, BPQ);
in_q_r(Prefix, Queue, BPQ) ->
    in_q1({fun queue:in_r/2, fun queue:out/1,
           fun (T, H) -> queue:join(H, T) end},
          Prefix, Queue, BPQ).

in_q1({In, Out, Join}, Prefix, Queue, BPQ = {N, Q}) ->
    case queue:len(Queue) of
        0 -> BPQ;
        M -> {N + M, case Out(Q) of
                         {{value, {Prefix, InnerQ}}, Q1} ->
                             In({Prefix, Join(InnerQ, Queue)}, Q1);
                         {{value, {_Prefix, _InnerQ}}, _Q1} ->
                             In({Prefix, Queue}, Q)
                     end}
    end.

out({0, _Q} = BPQ) -> {empty, BPQ};
out(BPQ)           -> out1({fun queue:in_r/2, fun queue:out/1}, BPQ).

out_r({0, _Q} = BPQ) -> {empty, BPQ};
out_r(BPQ)           -> out1({fun queue:in/2, fun queue:out_r/1}, BPQ).

out1({In, Out}, {N, Q}) ->
    {{value, {Prefix, InnerQ}}, Q1} = Out(Q),
    {{value, Value}, InnerQ1} = Out(InnerQ),
    Q2 = case queue:is_empty(InnerQ1) of
             true  -> Q1;
             false -> In({Prefix, InnerQ1}, Q1)
         end,
    {{value, Prefix, Value}, {N-1, Q2}}.

join({0, _Q}, BPQ) ->
    BPQ;
join(BPQ, {0, _Q}) ->
    BPQ;
join({NHead, QHead}, {NTail, QTail}) ->
    {{value, {Prefix, InnerQHead}}, QHead1} = queue:out_r(QHead),
    {NHead + NTail,
     case queue:out(QTail) of
         {{value, {Prefix, InnerQTail}}, QTail1} ->
             queue:join(
               queue:in({Prefix, queue:join(InnerQHead, InnerQTail)}, QHead1),
               QTail1);
         {{value, {_Prefix, _InnerQTail}}, _QTail1} ->
             queue:join(QHead, QTail)
     end}.

foldl(_Fun, Init, {0, _Q}) -> Init;
foldl( Fun, Init, {_N, Q}) -> fold1(fun queue:out/1, Fun, Init, Q).

foldr(_Fun, Init, {0, _Q}) -> Init;
foldr( Fun, Init, {_N, Q}) -> fold1(fun queue:out_r/1, Fun, Init, Q).

fold1(Out, Fun, Init, Q) ->
    case Out(Q) of
        {empty, _Q} ->
            Init;
        {{value, {Prefix, InnerQ}}, Q1} ->
            fold1(Out, Fun, fold1(Out, Fun, Prefix, Init, InnerQ), Q1)
    end.

fold1(Out, Fun, Prefix, Init, InnerQ) ->
    case Out(InnerQ) of
        {empty, _Q} ->
            Init;
        {{value, Value}, InnerQ1} ->
            fold1(Out, Fun, Prefix, Fun(Prefix, Value, Init), InnerQ1)
    end.

from_list(List) ->
    {FinalPrefix, FinalInnerQ, ListOfPQs1, Len} =
        lists:foldl(
          fun ({_Prefix, []}, Acc) ->
                  Acc;
              ({Prefix, InnerList}, {Prefix, InnerQ, ListOfPQs, LenAcc}) ->
                  {Prefix, queue:join(InnerQ, queue:from_list(InnerList)),
                   ListOfPQs, LenAcc + length(InnerList)};
              ({Prefix1, InnerList}, {Prefix, InnerQ, ListOfPQs, LenAcc}) ->
                  {Prefix1, queue:from_list(InnerList),
                   [{Prefix, InnerQ} | ListOfPQs], LenAcc + length(InnerList)}
          end, {undefined, queue:new(), [], 0}, List),
    ListOfPQs2 = [{FinalPrefix, FinalInnerQ} | ListOfPQs1],
    [{undefined, InnerQ1} | Rest] = All = lists:reverse(ListOfPQs2),
    {Len, queue:from_list(case queue:is_empty(InnerQ1) of
                              true  -> Rest;
                              false -> All
                          end)}.

to_list({0, _Q}) -> [];
to_list({_N, Q}) -> [{Prefix, queue:to_list(InnerQ)} ||
                        {Prefix, InnerQ} <- queue:to_list(Q)].

%% map_fold_filter_[lr](FilterFun, Fun, Init, BPQ) -> {BPQ, Init}
%% where FilterFun(Prefix) -> boolean()
%%       Fun(Value, Init) -> {Prefix, Value, Init} | stop
%%
%% The filter fun allows you to skip very quickly over blocks that
%% you're not interested in. Such blocks appear in the resulting bpq
%% without modification. The Fun is then used both to map the value,
%% which also allows you to change the prefix (and thus block) of the
%% value, and also to modify the Init/Acc (just like a fold).  If the
%% Fun returns 'stop' then it is not applied to any further items.
map_fold_filter_l(_PFilter, _Fun, Init, BPQ = {0, _Q}) ->
    {BPQ, Init};
map_fold_filter_l(PFilter, Fun, Init, {N, Q}) ->
    map_fold_filter1({fun queue:out/1, fun queue:in/2,
                      fun in_q/3, fun join/2},
                     N, PFilter, Fun, Init, Q, new()).

map_fold_filter_r(_PFilter, _Fun, Init, BPQ = {0, _Q}) ->
    {BPQ, Init};
map_fold_filter_r(PFilter, Fun, Init, {N, Q}) ->
    map_fold_filter1({fun queue:out_r/1, fun queue:in_r/2,
                      fun in_q_r/3, fun (T, H) -> join(H, T) end},
                     N, PFilter, Fun, Init, Q, new()).

map_fold_filter1(Funs = {Out, _In, InQ, Join}, Len, PFilter, Fun,
                 Init, Q, QNew) ->
    case Out(Q) of
        {empty, _Q} ->
            {QNew, Init};
        {{value, {Prefix, InnerQ}}, Q1} ->
            case PFilter(Prefix) of
                true ->
                    {Init1, QNew1, Cont} =
                        map_fold_filter2(Funs, Fun, Prefix, Prefix,
                                         Init, InnerQ, QNew, queue:new()),
                    case Cont of
                        false -> {Join(QNew1, {Len - len(QNew1), Q1}), Init1};
                        true  -> map_fold_filter1(Funs, Len, PFilter, Fun,
                                                  Init1, Q1, QNew1)
                    end;
                false ->
                    map_fold_filter1(Funs, Len, PFilter, Fun,
                                     Init, Q1, InQ(Prefix, InnerQ, QNew))
            end
    end.

map_fold_filter2(Funs = {Out, In, InQ, _Join}, Fun, OrigPrefix, Prefix,
                 Init, InnerQ, QNew, InnerQNew) ->
    case Out(InnerQ) of
        {empty, _Q} ->
            {Init, InQ(OrigPrefix, InnerQ,
                       InQ(Prefix, InnerQNew, QNew)), true};
        {{value, Value}, InnerQ1} ->
            case Fun(Value, Init) of
                stop ->
                    {Init, InQ(OrigPrefix, InnerQ,
                               InQ(Prefix, InnerQNew, QNew)), false};
                {Prefix1, Value1, Init1} ->
                    {Prefix2, QNew1, InnerQNew1} =
                        case Prefix1 =:= Prefix of
                            true  -> {Prefix, QNew, In(Value1, InnerQNew)};
                            false -> {Prefix1, InQ(Prefix, InnerQNew, QNew),
                                      In(Value1, queue:new())}
                        end,
                    map_fold_filter2(Funs, Fun, OrigPrefix, Prefix2,
                                     Init1, InnerQ1, QNew1, InnerQNew1)
            end
    end.
