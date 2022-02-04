%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2011-2021 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(lqueue).

-compile({no_auto_import,[get/1]}).

%% Most functions in lqueue were copied from OTP's queue module
%% and amended to maintain the length of the queue.
%% lqueue implements a subset of Erlang's queue module.
%% lqueues maintain their own length, so lqueue:len/1
%% is an O(1) operation, in contrast with queue:len/1 which is O(n).

%% Creation, inspection and conversion
-export([new/0, is_empty/1, len/1, to_list/1, from_list/1]).
%% Original style API
-export([in/2, in_r/2, out/1, out_r/1]).
%% Less garbage style API
-export([get/1, get_r/1, peek/1, peek_r/1, drop/1]).
%% Higher level API
-export([join/2, fold/3]).

-export_type([
              ?MODULE/0,
              ?MODULE/1
             ]).

%%--------------------------------------------------------------------------
%% Efficient implementation of double ended fifo queues
%%
%% Queue representation
%%
%% {Length,RearList,FrontList}
%%
%% The first element in the queue is at the head of the FrontList
%% The last element in the queue is at the head of the RearList,
%% that is; the RearList is reversed.

%% For backwards compatibility, lqueue functions accept old state, but return only new state.
%% Compared to the deprecated state, the new state creates less garbage per queue operation:
%% 4 = erts_debug:size({0, [], []}).
%% 6 = erts_debug:size({0, {[], []}}).
-define(STATE(T),{
                Length :: non_neg_integer(),
                Rear :: list(T), Front :: list(T)
               }).
-define(DEPRECATED_STATE(T),{
                           Length :: non_neg_integer(),
                           %% queue:queue(T)}
                           {Rear :: list(T), Front :: list(T)}
                          }).
-type ?MODULE() :: ?MODULE(_).
-opaque ?MODULE(T) :: ?STATE(T) | ?DEPRECATED_STATE(T).

%% Creation, inspection and conversion

%% O(1)
-spec new() -> ?MODULE().
new() -> {0, [], []}.

%% O(1)
-spec is_empty(Q :: ?MODULE()) -> boolean().
is_empty({0, [], []}) ->
    true;
is_empty({L, R, F})
  when is_integer(L), L > 0, is_list(R), is_list(F) ->
    false;
%% accept deprecated state
is_empty({L, {R, F}})
  when is_integer(L), L >= 0, is_list(R), is_list(F) ->
    is_empty({L, R, F});
is_empty(Q) ->
    erlang:error(badarg, [Q]).

%% O(1)
-spec len(Q :: ?MODULE()) -> non_neg_integer().
len({L, R, F})
  when is_integer(L), L >= 0, is_list(R), is_list(F) ->
    L;
%% accept deprecated state
len({L, _Q})
  when is_integer(L), L >= 0 ->
    L;
len(Q) ->
    erlang:error(badarg, [Q]).

%% O(len(Q))
-spec to_list(Q :: ?MODULE(Item)) -> list(Item).
to_list({L, R, F})
  when is_integer(L), L >= 0, is_list(R), is_list(F) ->
    F ++ lists:reverse(R, []);
%% accept deprecated state
to_list({L, {R, F}})
  when is_integer(L), L >= 0, is_list(R), is_list(F) ->
    to_list({L, R, F});
to_list(Q) ->
    erlang:error(badarg, [Q]).

%% Create queue from list
%%
%% O(length(L))
-spec from_list(L :: list(Item)) -> ?MODULE(Item).
from_list(L)
  when is_list(L) ->
    f2r(length(L), L);
from_list(L) ->
    erlang:error(badarg, [L]).

%%--------------------------------------------------------------------------
%% Original style API

%% Append to tail/rear
%% Put at least one element in each list, if it is cheap
%%
%% O(1)
-spec in(Item, Q1 :: ?MODULE(Item)) -> Q2 :: ?MODULE(Item).
in(X, {1, [_] = In, []}) ->
    {2, [X], In};
in(X, {L, In, Out})
  when is_integer(L), L >= 0, is_list(In), is_list(Out) ->
    {L + 1, [X|In], Out};
%% accept deprecated state
in(X, {L, {In, Out}})
  when is_integer(L), L >= 0, is_list(In), is_list(Out) ->
    in(X, {L, In, Out});
in(X, Q) ->
    erlang:error(badarg, [X, Q]).

%% Prepend to head/front
%% Put at least one element in each list, if it is cheap
%%
%% O(1)
-spec in_r(Item, Q1 :: ?MODULE(Item)) -> Q2 :: ?MODULE(Item).
in_r(X, {1, [], [_] = F}) ->
    {2, F, [X]};
in_r(X, {L, R, F})
  when is_integer(L), L >= 0, is_list(R), is_list(F) ->
    {L + 1, R, [X|F]};
%% accept deprecated state
in_r(X, {L, {R, F}})
  when is_integer(L), L >= 0, is_list(R), is_list(F) ->
    in_r(X, {L, R, F});
in_r(X, Q) ->
    erlang:error(badarg, [X, Q]).

%% Take from head/front
%%
%% O(1) amortized, O(len(Q)) worst case
-spec out(Q1 :: ?MODULE(Item)) ->
    {{value, Item}, Q2 :: ?MODULE(Item)} |
    {empty, Q1 :: ?MODULE(Item)}.
out({0, [], []} = Q) ->
    {empty, Q};
out({1, [V], []}) ->
    {{value, V}, {0, [], []}};
out({L, [Y|In], []})
  when is_integer(L), L > 0 ->
    [V|Out] = lists:reverse(In, []),
    {{value, V}, {L - 1, [Y], Out}};
out({L, In, [V]})
  when is_integer(L), L > 0, is_list(In) ->
    {{value, V}, r2f(L - 1, In)};
out({L, In, [V|Out]})
  when is_integer(L), L > 0, is_list(In) ->
    {{value, V}, {L - 1, In, Out}};
%% accept deprecated state
out({L, {In, Out}})
  when is_integer(L), L >= 0, is_list(In), is_list(Out) ->
    out({L, In, Out});
out(Q) ->
    erlang:error(badarg, [Q]).

%% Take from tail/rear
%%
%% O(1) amortized, O(len(Q)) worst case
-spec out_r(Q1 :: ?MODULE(Item)) ->
    {{value, Item}, Q2 :: ?MODULE(Item)} |
    {empty, Q1 :: ?MODULE(Item)}.
out_r({0, [], []} = Q) ->
    {empty, Q};
out_r({1, [], [V]}) ->
    {{value, V}, {0, [], []}};
out_r({L, [], [Y|Out]})
  when is_integer(L), L > 0 ->
    [V|In] = lists:reverse(Out, []),
    {{value, V}, {L - 1, In, [Y]}};
out_r({L, [V], Out})
  when is_integer(L), L > 0, is_list(Out) ->
    {{value, V}, f2r(L - 1, Out)};
out_r({L, [V|In], Out})
  when is_integer(L), L > 0, is_list(Out) ->
    {{value, V}, {L - 1, In, Out}};
%% accept deprecated state
out_r({L, {In, Out}})
  when is_integer(L), L >= 0, is_list(In), is_list(Out) ->
    out_r({L, In, Out});
out_r(Q) ->
    erlang:error(badarg, [Q]).

%%--------------------------------------------------------------------------
%% Less garbage style API.

%% Return the first element in the queue
%%
%% O(1) since the queue is supposed to be well formed
-spec get(Q :: ?MODULE(Item)) -> Item.
get({0, [], []} = Q) ->
    erlang:error(empty, [Q]);
get({L, R, F})
  when is_integer(L), L > 0, is_list(R), is_list(F) ->
    get(R, F);
%% accept deprecated state
get({L, {R, F}})
  when is_integer(L), L >= 0, is_list(R), is_list(F) ->
    get({L, R, F});
get(Q) ->
    erlang:error(badarg, [Q]).

-spec get(list(), list()) -> term().
get(R, [H|_])
  when is_list(R) ->
    H;
get([H], []) ->
    H;
get([_|R], []) -> % malformed queue -> O(len(Q))
    lists:last(R).

%% Return the last element in the queue
%%
%% O(1) since the queue is supposed to be well formed
-spec get_r(Q :: ?MODULE(Item)) -> Item.
get_r({0, [], []} = Q) ->
    erlang:error(empty, [Q]);
get_r({L, [H|_], F})
  when is_integer(L), L > 0, is_list(F) ->
    H;
get_r({1, [], [H]}) ->
    H;
get_r({L, [], [_|F]}) % malformed queue -> O(len(Q))
  when is_integer(L), L > 0 ->
    lists:last(F);
%% accept deprecated state
get_r({L, {R, F}})
  when is_integer(L), L >= 0, is_list(R), is_list(F) ->
    get_r({L, R, F});
get_r(Q) ->
    erlang:error(badarg, [Q]).

%% Return the first element in the queue
%%
%% O(1) since the queue is supposed to be well formed
-spec peek(Q :: ?MODULE(Item)) -> empty | {value, Item}.
peek({0, [], []}) ->
    empty;
peek({L, R, [H|_]})
  when is_integer(L), L > 0, is_list(R) ->
    {value, H};
peek({1, [H], []}) ->
    {value, H};
peek({L, [_|R], []}) % malformed queue -> O(len(Q))
  when is_integer(L), L > 0 ->
    {value, lists:last(R)};
%% accept deprecated state
peek({L, {R, F}})
  when is_integer(L), L >= 0, is_list(R), is_list(F) ->
    peek({L, R, F});
peek(Q) ->
    erlang:error(badarg, [Q]).

%% Return the last element in the queue
%%
%% O(1) since the queue is supposed to be well formed
-spec peek_r(Q :: ?MODULE(Item)) -> empty | {value, Item}.
peek_r({0, [], []}) ->
    empty;
peek_r({L, [H|_], F})
  when is_integer(L), L > 0, is_list(F) ->
    {value,H};
peek_r({1, [], [H]}) ->
    {value, H};
peek_r({L, [], [_|R]}) % malformed queue -> O(len(Q))
  when is_integer(L), L > 0 ->
    {value, lists:last(R)};
%% accept deprecated state
peek_r({L, {R, F}})
  when is_integer(L), L >= 0, is_list(R), is_list(F) ->
    peek_r({L, R, F});
peek_r(Q) ->
    erlang:error(badarg, [Q]).

%% Remove the first element and return resulting queue
%%
%% O(1) amortized
-spec drop(Q1 :: ?MODULE(Item)) -> Q2 :: ?MODULE(Item).
drop({0, [], []} = Q) ->
    erlang:error(empty, [Q]);
drop({1, [_], []}) ->
    {0, [], []};
drop({L, [Y|R], []})
  when is_integer(L), L > 0 ->
    [_|F] = lists:reverse(R, []),
    {L - 1, [Y], F};
drop({L, R, [_]})
  when is_integer(L), L > 0, is_list(R) ->
    r2f(L - 1, R);
drop({L, R, [_|F]})
  when is_integer(L), L > 0, is_list(R) ->
    {L - 1, R, F};
%% accept deprecated state
drop({L, {R, F}})
  when is_integer(L), L >= 0, is_list(R), is_list(F) ->
    drop({L, R, F});
drop(Q) ->
    erlang:error(badarg, [Q]).

%% Join two queues
%%
%% Q2 empty: O(1)
%% else:     O(len(Q1))
-spec join(Q1 :: ?MODULE(Item), Q2 :: ?MODULE(Item)) -> Q3 :: ?MODULE(Item).
join({L, R, F} = Q, {0, [], []})
  when is_integer(L), L >= 0, is_list(R), is_list(F) ->
    Q;
join({0, [], []}, {L, R, F} = Q)
  when is_integer(L), L >= 0, is_list(R), is_list(F) ->
    Q;
join({L1, R1, F1}, {L2, R2, F2})
  when is_integer(L1), L1 >= 0, is_list(R1), is_list(F1),
       is_integer(L2), L2 >= 0, is_list(R2), is_list(F2) ->
    {L1 + L2, R2, F1 ++ lists:reverse(R1, F2)};
%% accept deprecated state
join({L, {R, F}}, Q)
  when is_integer(L), L >= 0, is_list(R), is_list(F) ->
    join({L, R, F}, Q);
join(Q, {L, {R, F}})
  when is_integer(L), L >= 0, is_list(R), is_list(F) ->
    join(Q, {L, R, F});
join(Q1, Q2) ->
    erlang:error(badarg, [Q1,Q2]).

%% Fold a function over a queue, in queue order.
%%
%% O(len(Q))
-spec fold(Fun, Acc0, Q :: ?MODULE(Item)) -> Acc1 when
      Fun :: fun((Item, AccIn) -> AccOut),
                 Acc0 :: term(),
                 Acc1 :: term(),
                 AccIn :: term(),
                 AccOut :: term().
fold(Fun, Acc0, {L, R, F})
  when is_integer(L), L >= 0, is_function(Fun, 2), is_list(R), is_list(F) ->
    Acc1 = lists:foldl(Fun, Acc0, F),
    lists:foldr(Fun, Acc1, R);
%% accept deprecated state
fold(Fun, Acc0, {L, {R, F}})
  when is_integer(L), L >= 0, is_function(Fun, 2), is_list(R), is_list(F) ->
    fold(Fun, Acc0, {L, R, F});
fold(Fun, Acc0, Q) ->
    erlang:error(badarg, [Fun, Acc0, Q]).

%%--------------------------------------------------------------------------
%% Internal workers

-compile({inline, [r2f/2, f2r/2]}).

%% Move half of elements from R to F, if there are at least three
r2f(0, []) ->
    {0, [], []};
r2f(1, [_] = R) ->
    {1, [], R};
r2f(2, [X, Y]) ->
    {2, [X], [Y]};
r2f(Len, List) ->
    {FF, RR} = lists:split(Len div 2 + 1, List),
    {Len, FF, lists:reverse(RR, [])}.

%% Move half of elements from F to R, if there are enough
f2r(0, []) ->
    {0, [], []};
f2r(1, [_] = F) ->
    {1, F, []};
f2r(2, [X, Y]) ->
    {2, [Y], [X]};
f2r(Len, List) ->
    {FF, RR} = lists:split(Len div 2 + 1, List),
    {Len, lists:reverse(RR, []), FF}.
