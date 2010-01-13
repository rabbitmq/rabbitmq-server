%%   The contents of this file are subject to the Mozilla Public License
%%   Version 1.1 (the "License"); you may not use this file except in
%%   compliance with the License. You may obtain a copy of the License at
%%   http://www.mozilla.org/MPL/
%%
%%   Software distributed under the License is distributed on an "AS IS"
%%   basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See the
%%   License for the specific language governing rights and limitations
%%   under the License.
%%
%%   The Original Code is RabbitMQ.
%%
%%   The Initial Developers of the Original Code are LShift Ltd,
%%   Cohesive Financial Technologies LLC, and Rabbit Technologies Ltd.
%%
%%   Portions created before 22-Nov-2008 00:00:00 GMT by LShift Ltd,
%%   Cohesive Financial Technologies LLC, or Rabbit Technologies Ltd
%%   are Copyright (C) 2007-2008 LShift Ltd, Cohesive Financial
%%   Technologies LLC, and Rabbit Technologies Ltd.
%%
%%   Portions created by LShift Ltd are Copyright (C) 2007-2009 LShift
%%   Ltd. Portions created by Cohesive Financial Technologies LLC are
%%   Copyright (C) 2007-2009 Cohesive Financial Technologies
%%   LLC. Portions created by Rabbit Technologies Ltd are Copyright
%%   (C) 2007-2009 Rabbit Technologies Ltd.
%%
%%   All Rights Reserved.
%%
%%   Contributor(s): ______________________________________.
%%

-module(bpqueue).

%% Block-prefixed queue. This implements a queue of queues, but
%% supporting the normal queue interface. Each block has a prefix and
%% it is guaranteed that no two consecutive blocks have the same
%% prefix. len/1 returns the flattened length of the queue and is O(1)

-export([new/0, is_empty/1, len/1, in/3, in_r/3, out/1, out_r/1, join/2,
         fold/3, from_list/1, to_list/1, map_fold_filter_l/4,
         map_fold_filter_r/4]).

%%----------------------------------------------------------------------------

-ifdef(use_specs).

-type(bpqueue() :: {non_neg_integer(), queue()}).
-type(prefix() :: any()).
-type(value() :: any()).
-type(result() :: {'empty', bpqueue()} |
                  {{'value', prefix(), value()}, bpqueue()}).

-spec(new/0 :: () -> bpqueue()).
-spec(is_empty/1 :: (bpqueue()) -> boolean()).
-spec(len/1 :: (bpqueue()) -> non_neg_integer()).
-spec(in/3 :: (prefix(), value(), bpqueue()) -> bpqueue()).
-spec(in_r/3 :: (prefix(), value(), bpqueue()) -> bpqueue()).
-spec(out/1 :: (bpqueue()) -> result()).
-spec(out_r/1 :: (bpqueue()) -> result()).
-spec(join/2 :: (bpqueue(), bpqueue()) -> bpqueue()).
-spec(fold/3 :: (fun ((prefix(), value(), B) -> B), B, bpqueue()) -> B).
-spec(from_list/1 :: ([{prefix(), [value()]}]) -> bpqueue()).
-spec(to_list/1 :: (bpqueue()) -> [{prefix(), [value()]}]).
-spec(map_fold_filter_l/4 ::
        (fun ((prefix()) -> boolean()),
             fun ((value(), B) -> {prefix(), value(), B}), B, bpqueue()) ->
        {bpqueue(), B}).
-spec(map_fold_filter_r/4 ::
        (fun ((prefix()) -> boolean()),
             fun ((value(), B) -> {prefix(), value(), B}), B, bpqueue()) ->
        {bpqueue(), B}).

-endif.

%%----------------------------------------------------------------------------

new() ->
    {0, queue:new()}.

is_empty({0, _Q}) ->
    true;
is_empty(_BPQ) ->
    false.

len({N, _Q}) ->
    N.

in(Prefix, Value, {0, Q}) ->
    {1, queue:in({Prefix, queue:in(Value, Q)}, Q)};
in(Prefix, Value, {N, Q}) ->
    {N+1,
     case queue:out_r(Q) of
         {{value, {Prefix, InnerQ}}, Q1} ->
             queue:in({Prefix, queue:in(Value, InnerQ)}, Q1);
         {{value, {_Prefix, _InnerQ}}, _Q1} ->
             queue:in({Prefix, queue:in(Value, queue:new())}, Q)
     end}.

in_r(Prefix, Value, {0, Q}) ->
    {1, queue:in({Prefix, queue:in(Value, Q)}, Q)};
in_r(Prefix, Value, {N, Q}) ->
    {N+1,
     case queue:out(Q) of
         {{value, {Prefix, InnerQ}}, Q1} ->
             queue:in_r({Prefix, queue:in_r(Value, InnerQ)}, Q1);
         {{value, {_Prefix, _InnerQ}}, _Q1} ->
             queue:in_r({Prefix, queue:in(Value, queue:new())}, Q)
     end}.

out({0, _Q} = BPQ) ->
    {empty, BPQ};
out({N, Q}) ->
    {{value, {Prefix, InnerQ}}, Q1} = queue:out(Q),
    {{value, Value}, InnerQ1} = queue:out(InnerQ),
    Q2 = case queue:is_empty(InnerQ1) of
             true  -> Q1;
             false -> queue:in_r({Prefix, InnerQ1}, Q1)
         end,
    {{value, Prefix, Value}, {N-1, Q2}}.

out_r({0, _Q} = BPQ) ->
    {empty, BPQ};
out_r({N, Q}) ->
    {{value, {Prefix, InnerQ}}, Q1} = queue:out_r(Q),
    {{value, Value}, InnerQ1} = queue:out_r(InnerQ),
    Q2 = case queue:is_empty(InnerQ1) of
             true  -> Q1;
             false -> queue:in({Prefix, InnerQ1}, Q1)
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

fold(_Fun, Init, {0, _Q}) ->
    Init;
fold(Fun, Init, {_N, Q}) ->
    fold1(Fun, Init, Q).

fold1(Fun, Init, Q) ->
    case queue:out(Q) of
        {empty, _Q} ->
            Init;
        {{value, {Prefix, InnerQ}}, Q1} ->
            fold1(Fun, fold1(Fun, Prefix, Init, InnerQ), Q1)
    end.

fold1(Fun, Prefix, Init, InnerQ) ->
    case queue:out(InnerQ) of
        {empty, _Q} ->
            Init;
        {{value, Value}, InnerQ1} ->
            fold1(Fun, Prefix, Fun(Prefix, Value, Init), InnerQ1)
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

to_list({0, _Q}) ->
    [];
to_list({_N, Q}) ->
    lists:map(fun to_list1/1, queue:to_list(Q)).

to_list1({Prefix, InnerQ}) ->
    {Prefix, queue:to_list(InnerQ)}.

%% map_fold_filter_[lr](FilterFun, Fun, Init, BPQ) -> {BPQ, Init}
%% where FilterFun(Prefix) -> boolean()
%%       Fun(Value, Init) -> {Prefix, Value, Init}
%%
%% The filter fun allows you to skip very quickly over blocks that
%% you're not interested in. Such blocks appear in the resulting bpq
%% without modification. The Fun is then used both to map the value,
%% which also allows you to change the prefix (and thus block) of the
%% value, and also to modify the Init/Acc (just like a fold).
map_fold_filter_l(_PFilter, _Fun, Init, BPQ = {0, _Q}) ->
    {BPQ, Init};
map_fold_filter_l(PFilter, Fun, Init, {_N, Q}) ->
    map_fold_filter_l1(PFilter, Fun, Init, Q, new()).

map_fold_filter_l1(PFilter, Fun, Init, Q, QNew) ->
    case queue:out(Q) of
        {empty, _Q} ->
            {QNew, Init};
        {{value, {Prefix, InnerQ}}, Q1} ->
            InnerList = queue:to_list(InnerQ),
            {Init1, QNew1} =
                case PFilter(Prefix) of
                    true ->
                        lists:foldl(
                          fun (Value, {Acc, QNew2}) ->
                                  {Prefix1, Value1, Acc1} = Fun(Value, Acc),
                                  {Acc1, in(Prefix1, Value1, QNew2)}
                          end, {Init, QNew}, InnerList);
                    false ->
                        {Init, join(QNew, from_list([{Prefix, InnerList}]))}
                end,
            map_fold_filter_l1(PFilter, Fun, Init1, Q1, QNew1)
    end.

map_fold_filter_r(_PFilter, _Fun, Init, BPQ = {0, _Q}) ->
    {BPQ, Init};
map_fold_filter_r(PFilter, Fun, Init, {_N, Q}) ->
    map_fold_filter_r1(PFilter, Fun, Init, Q, new()).

map_fold_filter_r1(PFilter, Fun, Init, Q, QNew) ->
    case queue:out_r(Q) of
        {empty, _Q} ->
            {QNew, Init};
        {{value, {Prefix, InnerQ}}, Q1} ->
            InnerList = queue:to_list(InnerQ),
            {Init1, QNew1} =
                case PFilter(Prefix) of
                    true ->
                        lists:foldr(
                          fun (Value, {Acc, QNew2}) ->
                                  {Prefix1, Value1, Acc1} = Fun(Value, Acc),
                                  {Acc1, in_r(Prefix1, Value1, QNew2)}
                          end, {Init, QNew}, InnerList);
                    false ->
                        {Init, join(from_list([{Prefix, InnerList}]), QNew)}
                end,
            map_fold_filter_r1(PFilter, Fun, Init1, Q1, QNew1)
    end.
