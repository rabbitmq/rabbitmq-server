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

-module(lqueue).

-export([new/0, is_empty/1, len/1, in/2, in_r/2, out/1, out_r/1, join/2,
         foldl/3, foldr/3, from_list/1, to_list/1, peek/1, peek_r/1]).

-define(QUEUE, queue).

-ifdef(use_specs).

-export_type([?MODULE/0]).

-opaque(?MODULE() ::
           {non_neg_integer(), ?QUEUE(), maybe_value(), maybe_value()}).
-type(value() :: any()).
-type(maybe_value() :: {'value', value()} | 'empty').
-type(result() :: {maybe_value(), ?MODULE}).

-spec(new/0 :: () -> ?MODULE()).
-spec(is_empty/1 :: (?MODULE()) -> boolean()).
-spec(len/1 :: (?MODULE()) -> non_neg_integer()).
-spec(in/2 :: (value(), ?MODULE()) -> ?MODULE()).
-spec(in_r/2 :: (value(), ?MODULE()) -> ?MODULE()).
-spec(out/1 :: (?MODULE()) -> result()).
-spec(out_r/1 :: (?MODULE()) -> result()).
-spec(join/2 :: (?MODULE(), ?MODULE()) -> ?MODULE()).
-spec(foldl/3 :: (fun ((value(), B) -> B), B, ?MODULE()) -> B).
-spec(foldr/3 :: (fun ((value(), B) -> B), B, ?MODULE()) -> B).
-spec(from_list/1 :: ([value()]) -> ?MODULE()).
-spec(to_list/1 :: (?MODULE()) -> [value()]).
-spec(peek(?MODULE) -> maybe_value()).
-spec(peek_r(?MODULE) -> maybe_value()).

-endif.

new() -> {0, ?QUEUE:new(), empty, empty}.

is_empty({0, _Q, _I, _O}) -> true;
is_empty(_              ) -> false.

in(V, {0, Q, empty,       empty}) -> {1, Q, empty, {value, V}};
in(V, {1, Q, empty,       V1   }) -> {2, Q, {value, V}, V1};
in(V, {1, Q, V1,          empty}) -> {2, Q, {value, V}, V1};
in(V, {N, Q, {value, V1}, V2   }) -> {N+1, ?QUEUE:in(V1, Q), {value, V}, V2}.

in_r(V, {0, Q, empty, empty      }) -> {1, Q, {value, V}, empty};
in_r(V, {1, Q, V1,    empty      }) -> {2, Q, V1, {value, V}};
in_r(V, {1, Q, empty, V1         }) -> {2, Q, V1, {value, V}};
in_r(V, {N, Q, V1,    {value, V2}}) -> {N+1, ?QUEUE:in_r(V2, Q), V1, {value, V}}.

out({0, _Q, _I, _O} = Q)   -> {empty, Q};
out({1,  Q, empty,     V}) -> {V,  {0, Q, empty, empty}};
out({1,  Q,     V, empty}) -> {V,  {0, Q, empty, empty}};
out({2,  Q,    V1,    V2}) -> {V2, {1, Q, empty, V1}};
out({N,  Q,    V1,    V2}) -> {V3 = {value, _V}, Q1} = ?QUEUE:out(Q),
                              {V2, {N-1, Q1, V1, V3}}.

out_r({0, _Q, _I, _O} = Q)   -> {empty, Q};
out_r({1,  Q, empty,     V}) -> {V,  {0, Q, empty, empty}};
out_r({1,  Q,     V, empty}) -> {V,  {0, Q, empty, empty}};
out_r({2,  Q,    V1,    V2}) -> {V1, {1, Q, V2,    empty}};
out_r({N,  Q,    V1,    V2}) -> {V3 = {value, _V}, Q1} = ?QUEUE:out_r(Q),
                                {V1, {N-1, Q1, V3, V2}}.

join({0, _, _, _}, Q) -> Q;
join(Q, {0, _, _, _}) -> Q;

join({1, _, empty,       {value, V} }, Q) -> in_r(V, Q);
join({1, _, {value, V},  empty      }, Q) -> in_r(V, Q);
join({2, _, {value, V1}, {value, V2}}, Q) -> in_r(V2, in_r(V1, Q));

join(Q, {1, _, empty,       {value, V} }) -> in(V, Q);
join(Q, {1, _, {value, V},  empty      }) -> in(V, Q);
join(Q, {2, _, {value, V1}, {value, V2}}) -> in(V1, in(V2, Q));

join({L1, Q1, {value, InV}, Out}, {L2, Q2, In, {value, OutV}}) ->
    {L1+L2, ?QUEUE:join(?QUEUE:in(InV, Q1), ?QUEUE:in_r(OutV, Q2)), In, Out}.

to_list({0, _, _, _}) -> [];
to_list({1, _, empty, {value, V}}) -> [V];
to_list({1, _, {value, V}, empty}) -> [V];
to_list({_, Q, {value, InV}, {value, OutV}}) ->
    [OutV | ?QUEUE:to_list(Q) ++ [InV]].

from_list([])            -> new();
from_list([V])           -> in(V, new());
from_list([V1, V2])      -> in(V2, in(V1, new()));
from_list([V1 | Vs] = L) -> Q = ?QUEUE:from_list(Vs),
                            {V2, Q1} = ?QUEUE:out_r(Q),
                            {length(L), Q1, V2, V1}.

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

len({L, _Q, _I, _O}) ->
    L.

peek({0, _, _, _}) -> empty;
peek({1, _, V, empty}) -> V;
peek({_L, _Q, _I, O}) -> O.

peek_r({0, _, _, _}) -> empty;
peek_r({1, _, empty, V}) -> V;
peek_r({_L, _Q, I, _O}) -> I.
