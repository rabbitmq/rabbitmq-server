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
%%   Portions created by LShift Ltd are Copyright (C) 2007-2010 LShift
%%   Ltd. Portions created by Cohesive Financial Technologies LLC are
%%   Copyright (C) 2007-2010 Cohesive Financial Technologies
%%   LLC. Portions created by Rabbit Technologies Ltd are Copyright
%%   (C) 2007-2010 Rabbit Technologies Ltd.
%%
%%   All Rights Reserved.
%%
%%   Contributor(s): ______________________________________.
%%

%% Priority queues have essentially the same interface as ordinary
%% queues, except that a) there is an in/3 that takes a priority, and
%% b) we have only implemented the core API we need.
%%
%% Priorities should be integers - the higher the value the higher the
%% priority - but we don't actually check that.
%%
%% in/2 inserts items with priority 0.
%%
%% We optimise the case where a priority queue is being used just like
%% an ordinary queue. When that is the case we represent the priority
%% queue as an ordinary queue. We could just call into the 'queue'
%% module for that, but for efficiency we implement the relevant
%% functions directly in here, thus saving on inter-module calls and
%% eliminating a level of boxing.
%%
%% When the queue contains items with non-zero priorities, it is
%% represented as a sorted kv list with the inverted Priority as the
%% key and an ordinary queue as the value. Here again we use our own
%% ordinary queue implemention for efficiency, often making recursive
%% calls into the same function knowing that ordinary queues represent
%% a base case.


-module(priority_queue).

-export([new/0, is_queue/1, is_empty/1, len/1, to_list/1, in/2, in/3,
         out/1, pout/1, join/2]).

%%----------------------------------------------------------------------------

-ifdef(use_specs).

-type(priority() :: integer()).
-type(squeue() :: {queue, [any()], [any()]}).
-type(pqueue() ::  squeue() |
                   {pqueue, [{priority(), non_neg_integer(), squeue()}]}).

-spec(new/0 :: () -> pqueue()).
-spec(is_queue/1 :: (any()) -> boolean()).
-spec(is_empty/1 :: (pqueue()) -> boolean()).
-spec(len/1 :: (pqueue()) -> non_neg_integer()).
-spec(to_list/1 :: (pqueue()) -> [{priority(), any()}]).
-spec(in/2 :: (any(), pqueue()) -> pqueue()).
-spec(in/3 :: (any(), priority(), pqueue()) -> pqueue()).
-spec(out/1 :: (pqueue()) -> {empty | {value, any()}, pqueue()}).
-spec(pout/1 :: (pqueue()) -> {empty | {value, any()}, pqueue()}).
-spec(join/2 :: (pqueue(), pqueue()) -> pqueue()).

-endif.

%%----------------------------------------------------------------------------

new() ->
    {queue, [], []}.

is_queue({queue, R, F}) when is_list(R), is_list(F) ->
    true;
is_queue({pqueue, Queues}) when is_list(Queues) ->
    lists:all(fun ({P, Len, Q}) ->
                      is_integer(P) andalso is_queue(Q) andalso is_integer(Len)
                          andalso Len >= 0
              end, Queues);
is_queue(_) ->
    false.

is_empty({queue, [], []}) ->
    true;
is_empty(_) ->
    false.

len({queue, R, F}) when is_list(R), is_list(F) ->
    length(R) + length(F);
len({pqueue, Queues}) ->
    lists:sum([Len || {_, Len, _} <- Queues]).

to_list({queue, In, Out}) when is_list(In), is_list(Out) ->
    [{0, V} || V <- Out ++ lists:reverse(In, [])];
to_list({pqueue, Queues}) ->
    [{-P, V} || {P, _Len, Q} <- Queues, {0, V} <- to_list(Q)].

in(Item, Q) ->
    in(Item, 0, Q).

in(X, 0, {queue, [_] = In, []}) ->
    {queue, [X], In};
in(X, 0, {queue, In, Out}) when is_list(In), is_list(Out) ->
    {queue, [X|In], Out};
in(X, Priority, _Q = {queue, [], []}) ->
    in(X, Priority, {pqueue, []});
in(X, Priority, Q = {queue, _, _}) ->
    in(X, Priority, {pqueue, [{0, len(Q), Q}]});
in(X, Priority, {pqueue, Queues}) ->
    P = -Priority,
    {pqueue, case lists:keysearch(P, 1, Queues) of
                 {value, {_, Len, Q}} ->
                     lists:keyreplace(P, 1, Queues, {P, Len + 1, in(X, Q)});
                 false ->
                     lists:keysort(1, [{P, 1, {queue, [X], []}} | Queues])
             end}.

out({queue, [], []} = Q) ->
    {empty, Q};
out({queue, [V], []}) ->
    {{value, V}, {queue, [], []}};
out({queue, [Y|In], []}) ->
    [V|Out] = lists:reverse(In, []),
    {{value, V}, {queue, [Y], Out}};
out({queue, In, [V]}) when is_list(In) ->
    {{value,V}, r2f(In)};
out({queue, In,[V|Out]}) when is_list(In) ->
    {{value, V}, {queue, In, Out}};
out({pqueue, [{P, Len, Q} | Queues]}) ->
    {R, Q1} = out(Q),
    NewQ = case is_empty(Q1) of
               true  -> case Queues of
                            []                 -> {queue, [], []};
                            [{0, _Len, OnlyQ}] -> OnlyQ;
                            [_|_]              -> {pqueue, Queues}
                        end;
               false -> {pqueue, [{P, Len - 1, Q1} | Queues]}
           end,
    {R, NewQ}.

pout({queue, _, _} = Q) ->
    out(Q);
pout({pqueue, [{P, Len, Q}]}) ->
    {R, Q1} = out(Q),
    NewQ = case is_empty(Q1) of
               true  -> {queue, [], []};
               false -> {pqueue, [{P, Len - 1, Q1}]}
           end,
    {R, NewQ};
pout({pqueue, Queues}) ->
    {Total, Weights, _, _} =
        lists:foldr(fun ({P, Len, _Q}, {T, Ws, NP, OldP}) ->
                            NP1 = NP + case OldP of
                                           undefined -> 1;
                                           _         -> OldP - P
                                       end,
                            W = Len * NP1,
                            {T + W, [W | Ws], NP1, P}
                    end, {0, [], 0, undefined}, Queues),
    {LHS, [{P, Len, Q} | RHS]} =
        pout(random:uniform(Total) - 1, Weights, [], Queues),
    {R, Q1} = out(Q),
    NewQ = case {LHS, is_empty(Q1), RHS} of
               {[], true, []} ->
                   {queue, [], []};
               {[], true, [{0, _Len, OnlyQ}]} ->
                   OnlyQ;
               {[{0, _Len, OnlyQ}], true, []} ->
                   OnlyQ;
               {_, true, _} ->
                   {pqueue, LHS ++ RHS};
               {_, false, _} ->
                   {pqueue, LHS ++ [{P, Len - 1, Q1} | RHS]}
           end,
    {R, NewQ}.

pout(ToSkip, [W | _Ws], Skipped, Queues) when ToSkip < W ->
    {lists:reverse(Skipped), Queues};
pout(ToSkip, [W | Ws], Skipped, [Q | Queues]) ->
    pout(ToSkip - W, Ws, [Q | Skipped], Queues).

join(A, {queue, [], []}) ->
    A;
join({queue, [], []}, B) ->
    B;
join({queue, AIn, AOut}, {queue, BIn, BOut}) ->
    {queue, BIn, AOut ++ lists:reverse(AIn, BOut)};
join(A = {queue, _, _}, {pqueue, BPQ}) ->
    {Pre, Post} = lists:splitwith(fun ({P, _, _}) -> P < 0 end, BPQ),
    ALen = len(A),
    Post1 = case Post of
                [] ->
                    [ {0, ALen, A} ];
                [ {0, Len, ZeroQueue} | Rest ] ->
                    [ {0, ALen + Len, join(A, ZeroQueue)} | Rest ];
                _ ->
                    [ {0, ALen, A} | Post ]
            end,
    {pqueue, Pre ++ Post1};
join({pqueue, APQ}, B = {queue, _, _}) ->
    {Pre, Post} = lists:splitwith(fun ({P, _, _}) -> P < 0 end, APQ),
    BLen = len(B),
    Post1 = case Post of
                [] ->
                    [ {0, BLen, B} ];
                [{0, Len, ZeroQueue} | Rest] ->
                    [ {0, BLen + Len, join(ZeroQueue, B)} | Rest ];
                _ ->
                    [ {0, BLen, B} | Post ]
            end,
    {pqueue, Pre ++ Post1};
join({pqueue, APQ}, {pqueue, BPQ}) ->
    {pqueue, merge(APQ, BPQ, [])}.

merge([], BPQ, Acc) ->
    lists:reverse(Acc, BPQ);
merge(APQ, [], Acc) ->
    lists:reverse(Acc, APQ);
merge([{P, ALen, A}|As], [{P, BLen, B}|Bs], Acc) ->
    merge(As, Bs, [ {P, ALen + BLen, join(A, B)} | Acc ]);
merge([{PA, ALen, A}|As], Bs = [{PB, _, _}|_], Acc) when PA < PB ->
    merge(As, Bs, [ {PA, ALen, A} | Acc ]);
merge(As = [{_, _, _}|_], [{PB, BLen, B}|Bs], Acc) ->
    merge(As, Bs, [ {PB, BLen, B} | Acc ]).

r2f([])      -> {queue, [], []};
r2f([_] = R) -> {queue, [], R};
r2f([X,Y])   -> {queue, [X], [Y]};
r2f([X,Y|R]) -> {queue, [X,Y], lists:reverse(R, [])}.
