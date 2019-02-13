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
%% Copyright (c) 2011-2019 Pivotal Software, Inc.  All rights reserved.
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

-opaque ?MODULE() :: ?MODULE(_).
-opaque ?MODULE(T) :: {non_neg_integer(), queue:queue(T)}.
-type value()     :: any().
-type result(T)    :: 'empty' | {'value', T}.

-spec new() -> ?MODULE(_).

new() -> {0, ?QUEUE:new()}.

-spec drop(?MODULE(T)) -> ?MODULE(T).

drop({L, Q}) -> {L - 1, ?QUEUE:drop(Q)}.

-spec is_empty(?MODULE(_)) -> boolean().

is_empty({0, _Q}) -> true;
is_empty(_)       -> false.

-spec in(T, ?MODULE(T)) -> ?MODULE(T).

in(V, {L, Q}) -> {L+1, ?QUEUE:in(V, Q)}.

-spec in_r(value(), ?MODULE(T)) -> ?MODULE(T).

in_r(V, {L, Q}) -> {L+1, ?QUEUE:in_r(V, Q)}.

-spec out(?MODULE(T)) -> {result(T), ?MODULE(T)}.

out({0, _Q} = Q) -> {empty, Q};
out({L,  Q})     -> {Result, Q1} = ?QUEUE:out(Q),
                    {Result, {L-1, Q1}}.

-spec out_r(?MODULE(T)) -> {result(T), ?MODULE(T)}.

out_r({0, _Q} = Q) -> {empty, Q};
out_r({L,  Q})     -> {Result, Q1} = ?QUEUE:out_r(Q),
                      {Result, {L-1, Q1}}.

-spec join(?MODULE(A), ?MODULE(B)) -> ?MODULE(A | B).

join({L1, Q1}, {L2, Q2}) -> {L1 + L2, ?QUEUE:join(Q1, Q2)}.

-spec to_list(?MODULE(T)) -> [T].

to_list({_L, Q}) -> ?QUEUE:to_list(Q).

-spec from_list([T]) -> ?MODULE(T).

from_list(L) -> {length(L), ?QUEUE:from_list(L)}.

-spec foldl(fun ((T, B) -> B), B, ?MODULE(T)) -> B.

foldl(Fun, Init, Q) ->
    case out(Q) of
        {empty, _Q}      -> Init;
        {{value, V}, Q1} -> foldl(Fun, Fun(V, Init), Q1)
    end.

-spec foldr(fun ((T, B) -> B), B, ?MODULE(T)) -> B.

foldr(Fun, Init, Q) ->
    case out_r(Q) of
        {empty, _Q}      -> Init;
        {{value, V}, Q1} -> foldr(Fun, Fun(V, Init), Q1)
    end.

-spec len(?MODULE(_)) -> non_neg_integer().

len({L, _}) -> L.

-spec peek(?MODULE(T)) -> result(T).

peek({ 0, _Q}) -> empty;
peek({_L,  Q}) -> ?QUEUE:peek(Q).

-spec peek_r(?MODULE(T)) -> result(T).

peek_r({ 0, _Q}) -> empty;
peek_r({_L,  Q}) -> ?QUEUE:peek_r(Q).
