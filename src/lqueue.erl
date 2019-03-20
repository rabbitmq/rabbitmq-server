%% The contents of this file are subject to the Mozilla Public License
%% Version 1.1 (the "License"); you may not use this file except in
%% compliance with the License. You may obtain a copy of the License
%% at https://www.mozilla.org/MPL/
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
         foldl/3, foldr/3, from_list/1, to_list/1, peek/1, peek_r/1]).

-define(QUEUE, queue).

-export_type([?MODULE/0]).

-opaque ?MODULE() :: {non_neg_integer(), queue:queue()}.
-type value()     :: any().
-type result()    :: 'empty' | {'value', value()}.

-spec new() -> ?MODULE().
-spec is_empty(?MODULE()) -> boolean().
-spec len(?MODULE()) -> non_neg_integer().
-spec in(value(), ?MODULE()) -> ?MODULE().
-spec in_r(value(), ?MODULE()) -> ?MODULE().
-spec out(?MODULE()) -> {result(), ?MODULE()}.
-spec out_r(?MODULE()) -> {result(), ?MODULE()}.
-spec join(?MODULE(), ?MODULE()) -> ?MODULE().
-spec foldl(fun ((value(), B) -> B), B, ?MODULE()) -> B.
-spec foldr(fun ((value(), B) -> B), B, ?MODULE()) -> B.
-spec from_list([value()]) -> ?MODULE().
-spec to_list(?MODULE()) -> [value()].
-spec peek(?MODULE()) -> result().
-spec peek_r(?MODULE()) -> result().

new() -> {0, ?QUEUE:new()}.

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

len({L, _Q}) -> L.

peek({ 0, _Q}) -> empty;
peek({_L,  Q}) -> ?QUEUE:peek(Q).

peek_r({ 0, _Q}) -> empty;
peek_r({_L,  Q}) -> ?QUEUE:peek_r(Q).
