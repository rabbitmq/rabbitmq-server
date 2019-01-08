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
%% The Initial Developer of the Original Code is GoPivotal, Inc.
%% Copyright (c) 2011-2015 Pivotal Software, Inc.  All rights reserved.
%%

-module(lqueue).

%% lqueue implements a subset of Erlang's queue module. lqueues
%% maintain their own length, so lqueue:len/1
%% is an O(1) operation, in contrast with queue:len/1 which is O(n).

-export([new/0, is_empty/1, len/1, in/2, in_r/2, out/1, out_r/1, join/2,
         foldl/3, foldr/3, from_list/1, drop/1, to_list/1, peek/1, peek_r/1]).

-define(QUEUE, queue).

-export_type([
              ?MODULE/0,
              ?MODULE/1
             ]).

-opaque ?MODULE() :: {non_neg_integer(), queue:queue(term())}.
-opaque ?MODULE(T) :: {non_neg_integer(), queue:queue(T)}.
-type value()     :: any().
-type result(T)    :: 'empty' | {'value', T}.

-spec new() -> ?MODULE(_).
-spec drop(?MODULE(T)) -> ?MODULE(T).
-spec is_empty(?MODULE(_)) -> boolean().
-spec len(?MODULE(_)) -> non_neg_integer().
-spec in(T, ?MODULE(T)) -> ?MODULE(T).
-spec in_r(value(), ?MODULE()) -> ?MODULE().
-spec out(?MODULE(T)) -> {result(T), ?MODULE()}.
-spec out_r(?MODULE(T)) -> {result(T), ?MODULE()}.
-spec join(?MODULE(A), ?MODULE(B)) -> ?MODULE(A | B).
-spec foldl(fun ((T, B) -> B), B, ?MODULE(T)) -> B.
-spec foldr(fun ((T, B) -> B), B, ?MODULE(T)) -> B.
-spec from_list([T]) -> ?MODULE(T).
-spec to_list(?MODULE(T)) -> [T].
% -spec peek(?MODULE()) -> result().
-spec peek(?MODULE(T)) -> result(T).
-spec peek_r(?MODULE(T)) -> result(T).

new() -> {0, ?QUEUE:new()}.

drop({L, Q}) -> {L - 1, ?QUEUE:drop(Q)}.

is_empty({0, _Q}) -> true;
is_empty(_)       -> false.

in(V, {L, Q}) -> {L+1, ?QUEUE:in(V, Q)}.

in_r(V, {L, Q}) -> {L+1, ?QUEUE:in_r(V, Q)}.

out({0, _Q} = Q) -> {empty, Q};
out({L,  Q})     -> {Result, Q1} = ?QUEUE:out(Q),
                    {Result, {L-1, Q1}}.

out_r({0, _Q} = Q) -> {empty, Q};
out_r({L,  Q})     -> {Result, Q1} = ?QUEUE:out_r(Q),
                      {Result, {L-1, Q1}}.

join({L1, Q1}, {L2, Q2}) -> {L1 + L2, ?QUEUE:join(Q1, Q2)}.

to_list({_L, Q}) -> ?QUEUE:to_list(Q).

from_list(L) -> {length(L), ?QUEUE:from_list(L)}.

foldl(Fun, Init, Q) ->
    case out(Q) of
        {empty, _Q}      -> Init;
        {{value, V}, Q1} -> foldl(Fun, Fun(V, Init), Q1)
    end.

foldr(Fun, Init, Q) ->
    case out_r(Q) of
        {empty, _Q}      -> Init;
        {{value, V}, Q1} -> foldr(Fun, Fun(V, Init), Q1)
    end.

len({L, _}) -> L.


peek({ 0, _Q}) -> empty;
peek({_L,  Q}) -> ?QUEUE:peek(Q).

peek_r({ 0, _Q}) -> empty;
peek_r({_L,  Q}) -> ?QUEUE:peek_r(Q).
