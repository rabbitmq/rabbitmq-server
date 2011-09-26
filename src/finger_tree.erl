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
%% Copyright (c) 2011-2011 VMware, Inc.  All rights reserved.
%%

-module(finger_tree).

%% This is an Erlang implementation of 2-3 finger trees, as defined by
%% Ralf Hinze and Ross Paterson in their "Finger Trees: A Simple
%% General-purpose Data Structure" paper[0].
%%
%% As usual with a queue-like thing, adding and removing from either
%% end is O(1) amortized.
%%
%% On the whole, nearly everything else is O(log_2(N)), including
%% join. Whilst there are instances of list append (++) in the code,
%% it's only lists that are bounded in length to four items.
%%
%% Because Erlang lacks type classes, it's currently forced to cache
%% sizes. This allows len to be O(1), and permits split_at based on
%% index, which still remains O(log_2(N)) which demonstrates we're
%% able to take advantage of the tree structure. However, it's not
%% difficult to imagine a callback mechanism to allow the monoid
%% implementation to vary. See the paper[0] for examples of other
%% things that could be implemented.
%%
%% [0]: http://www.soi.city.ac.uk/~ross/papers/FingerTree.html


-compile([export_all]).

-record(finger_tree_single, {elem}).
-record(finger_tree_deep, {measured, prefix, middle, suffix}).
-record(node, {measured, tuple}).
-record(digit, {list}).

%% queue interface
new() -> finger_tree_empty.

in(A, FT) -> cons_l(A, FT).
in_r(A, FT) -> cons_r(A, FT).

out(FT) -> uncons_r(FT).
out_r(FT) -> uncons_l(FT).

len(FT) -> measure(FT).

%% To/From list
cons(A, L) -> [A | L].
to_list(FT) -> reduce_r(fun cons/2, FT, []).
from_list(L) -> reduce_r_in(L, finger_tree_empty).

is_empty(finger_tree_empty)  -> true;
is_empty(#finger_tree_single {}) -> false;
is_empty(#finger_tree_deep {})   -> false.

%% Smart constructors
digit(A) -> #digit { list = A }.

node2(A, B) ->
    #node { tuple = {A, B}, measured = add(measure(A), measure(B)) }.
node3(A, B, C) ->
    #node { tuple = {A, B, C},
            measured = add(add(measure(A), measure(B)), measure(C)) }.

deep(Prefix, Middle, Suffix) ->
    #finger_tree_deep { measured = add(add(measure(Prefix),
                                           measure(Middle)),
                                       measure(Suffix)),
                        prefix = Prefix, middle = Middle, suffix = Suffix }.

deep_r(#digit { list = [] }, M, S) ->
    case uncons_r(M) of
        {empty,      _ } -> from_list(S);
        {{value, A}, M1} -> deep(digit(to_list(A)), M1, S)
    end;
deep_r(P, M, S) ->
    deep(P, M, S).

deep_l(P, M, #digit { list = [] }) ->
    case uncons_l(M) of
        {empty, _} -> from_list(P);
        {{value, A}, M1} -> deep(P, M1, digit(to_list(A)))
    end;
deep_l(P, M, S) ->
    deep(P, M, S).

%% Monoid
measure(finger_tree_empty) -> null();
measure(#finger_tree_single { elem = E }) -> measure(E);
measure(#finger_tree_deep{ measured = V }) -> V;

measure(#node { measured = V }) -> V;

measure(#digit { list = Xs }) ->
    reduce_l(fun (A, I) -> add(I, measure(A)) end, Xs, null());

%% Size implementation of monoid
measure(_) -> 1.

null() -> 0.
add(A, B) -> A + B.


%% Reduce primitives
reduce_r(_Fun, finger_tree_empty, Z) -> Z;
reduce_r(Fun, #finger_tree_single { elem = E }, Z) -> Fun(E, Z);
reduce_r(Fun, #finger_tree_deep { prefix = P, middle = M, suffix = S }, Z) ->
    R = reduce_r(fun (A, B) -> reduce_r(Fun, A, B) end, M, reduce_r(Fun, S, Z)),
    reduce_r(Fun, P, R);

reduce_r(Fun, #node { tuple = {A, B} },    Z) -> Fun(A, Fun(B, Z));
reduce_r(Fun, #node { tuple = {A, B, C} }, Z) -> Fun(A, Fun(B, Fun(C, Z)));

reduce_r(Fun, #digit { list = List }, Z) -> lists:foldr(Fun, Z, List);

reduce_r(Fun, X, Z) when is_list(X) -> lists:foldr(Fun, Z, X).

reduce_l(_Fun, finger_tree_empty, Z) -> Z;
reduce_l(Fun, #finger_tree_single { elem = E }, Z) -> Fun(Z, E);
reduce_l(Fun, #finger_tree_deep { prefix = P, middle = M, suffix = S }, Z) ->
    L = reduce_l(fun (A, B) -> reduce_l(Fun, A, B) end, reduce_l(Fun, Z, P), M),
    reduce_l(Fun, L, S);

reduce_l(Fun, #node { tuple = {A, B} },    Z) -> Fun(Fun(Z, B), A);
reduce_l(Fun, #node { tuple = {A, B, C} }, Z) -> Fun(Fun(Fun(Z, C), B), A);

reduce_l(Fun, #digit { list = List }, Z) -> lists:foldl(Fun, Z, List);

reduce_l(Fun, X, Z) when is_list(X) -> lists:foldl(Fun, Z, X).

reduce_r_in(X, FT) -> reduce_r(fun cons_r/2, X, FT).
reduce_l_in(X, FT) -> reduce_l(fun cons_l/2, X, FT).


%% Consing
cons_r(A, finger_tree_empty) ->
    #finger_tree_single { elem = A };
cons_r(A, #finger_tree_single { elem = B }) ->
    deep(digit([A]), finger_tree_empty, digit([B]));
cons_r(A, #finger_tree_deep { prefix = #digit { list = [B, C, D, E] }, middle = M, suffix = S }) ->
    deep(digit([A, B]), cons_r(node3(C, D, E), M), S);
cons_r(A, #finger_tree_deep { prefix = #digit { list = P }, middle = M, suffix = S }) ->
    deep(digit([A | P]), M, S).

cons_l(A, finger_tree_empty) ->
    #finger_tree_single { elem = A };
cons_l(A, #finger_tree_single { elem = B }) ->
    deep(digit([B]), finger_tree_empty, digit([A]));
cons_l(A, #finger_tree_deep { prefix = P, middle = M, suffix = #digit { list = [E, D, C, B] } }) ->
    deep(P, cons_l(node3(E, D, C), M), digit([B, A]));
cons_l(A, #finger_tree_deep { prefix = P, middle = M, suffix = #digit { list = S } }) ->
    deep(P, M, digit(S ++ [A])).

%% Unconsing
uncons_r(finger_tree_empty = FT) ->
    {empty, FT};
uncons_r(#finger_tree_single { elem = E }) ->
    {{value, E}, finger_tree_empty};
uncons_r(#finger_tree_deep { prefix = #digit { list = [A | P] }, middle = M, suffix = S }) ->
    {{value, A}, deep_r(digit(P), M, S)}.

uncons_l(finger_tree_empty = FT) ->
    {empty, FT};
uncons_l(#finger_tree_single { elem = E }) ->
    {{value, E}, finger_tree_empty};
uncons_l(#finger_tree_deep { prefix = P, middle = M, suffix = #digit { list = S } }) ->
    case S of
        [A] -> {{value, A}, deep_l(P, M, digit([]))};
        _   -> [A | S1] = lists:reverse(S),
               {{value, A}, deep_l(P, M, digit(lists:reverse(S1)))}
    end.

%% Joining
ft_nodes([A, B])         -> [node2(A, B)];
ft_nodes([A, B, C])      -> [node3(A, B, C)];
ft_nodes([A, B, C, D])   -> [node2(A, B), node2(C, D)];
ft_nodes([A, B, C | Xs]) -> [node3(A, B, C) | ft_nodes(Xs)].

app3(finger_tree_empty, Ts, Xs) ->
    reduce_r_in(Ts, Xs);
app3(Xs, Ts, finger_tree_empty) ->
    reduce_l_in(Ts, Xs);
app3(#finger_tree_single { elem = E }, Ts, Xs) ->
    cons_r(E, reduce_r_in(Ts, Xs));
app3(Xs, Ts, #finger_tree_single { elem = E }) ->
    cons_l(E, reduce_l_in(Ts, Xs));
app3(#finger_tree_deep { prefix = P1, middle = M1, suffix = #digit { list = S1 } }, #digit { list = Ts },
     #finger_tree_deep { prefix = #digit { list = P2 }, middle = M2, suffix = S2 }) ->
    deep(P1, app3(M1, digit(ft_nodes(S1 ++ (Ts ++ P2))), M2), S2).

join(FT1, FT2) -> app3(FT1, digit([]), FT2).

%% Splitting
split_digit(_Pred, _Init, #digit { list = [A] }) ->
    {split, digit([]), A, digit([])};
split_digit(Pred, Init, #digit { list = [A | List] }) ->
    Init1 = add(Init, measure(A)),
    case Pred(Init1) of
        true  -> {split, digit([]), A, digit(List)};
        false -> {split, #digit { list = L }, X, R} = split_digit(Pred, Init1, digit(List)),
                 {split, digit([A | L]), X, R}
    end.

split_node(Pred, Init, #node { tuple = {A, B} }) ->
    Init1 = add(Init, measure(A)),
    case Pred(Init1) of
        true  -> {split, digit([]), A, digit([B])};
        false -> {split, digit([A]), B, digit([])}
    end;
split_node(Pred, Init, #node { tuple = {A, B, C} }) ->
    Init1 = add(Init, measure(A)),
    case Pred(Init1) of
        true  -> {split, digit([]), A, digit([B, C])};
        false -> Init2 = add(Init1, measure(B)),
                 case Pred(Init2) of
                     true  -> {split, digit([A]), B, digit([C])};
                     false -> {split, digit([A, B]), C, digit([])}
                 end
    end.

split_tree(_Pred, _Init, #finger_tree_single { elem = E }) ->
    {split, finger_tree_empty, E, finger_tree_empty};
split_tree(Pred, Init, #finger_tree_deep { prefix = P, middle = M, suffix = S }) ->
    VP = add(Init, measure(P)),
    case Pred(VP) of
        true ->
            {split, #digit { list = L }, X, R} = split_digit(Pred, Init, P),
            {split, from_list(L), X, deep_r(R, M, S)};
        false ->
            VM = add(VP, measure(M)),
            case VM /= VP andalso Pred(VM) of
                true ->
                    {split, ML, Xs, MR} = split_tree(Pred, VP, M),
                    Init1 = add(VP, measure(ML)),
                    {split, L, X, R} = split_node(Pred, Init1, Xs),
                    {split, deep_l(P, ML, L), X, deep_r(R, MR, S)};
                false ->
                    {split, L, X, #digit { list = R }} = split_digit(Pred, VM, S),
                    {split, deep_l(P, M, L), X, from_list(R)}
            end
    end.

split(_Pred, finger_tree_empty) ->
    {finger_tree_empty, finger_tree_empty};
split(Pred, FT) ->
    {split, L, X, R} = split_tree(Pred, null(), FT),
    case Pred(measure(FT)) of
        true  -> {L, cons_r(X, R)};
        false -> {FT, finger_tree_empty}
    end.

split_at(N, FT) -> split(fun (E) -> N < E end, FT).
