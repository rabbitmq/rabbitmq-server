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

-module(fqueue).

%% This is a queue module, but optimised so that join is O(1). This
%% forces the queue to go deep and then there are some neat pivotting
%% tricks on out and out_r such that we ensure that we don't
%% constantly have to dive deep into the structure. However, in a deep
%% queue case, repeated out and out_r applications will be more
%% expensive. This is the pathological case.

%% Creation, inspection and conversion
-export([new/0,is_queue/1,is_empty/1,len/1,to_list/1,from_list/1,member/2]).
%% Original style API
-export([in/2,in_r/2,out/1,out_r/1]).
%% Less garbage style API
%%-export([get/1,get_r/1,peek/1,peek_r/1,drop/1,drop_r/1]).

%% Higher level API
%%-export([reverse/1,join/2,split/2,filter/2]).
-export([join/2]).

-record(fq, {len, head, tail}).

new() -> #fq { len = 0, head = [], tail = [] }.

is_queue(#fq { len = L, head = H, tail = T })
  when is_integer(L) andalso is_list(H) andalso is_list(T) ->
    true;
is_queue(_) ->
    false.

is_empty(#fq { len = 0 }) ->
    true;
is_empty(_) ->
    false.

len(FQ) -> FQ#fq.len.

to_list(FQ) ->
    to_list(false, FQ).

to_list(true, List) when is_list(List) ->
    lists:foldl(fun (#fq{} = Q, Acc) -> to_list(false, Q) ++ Acc;
                    (V, Acc)         -> [unescape(V) | Acc]
                end, [], List);
to_list(false, List) when is_list(List) ->
    lists:foldr(fun (#fq{} = Q, Acc) -> to_list(true, Q) ++ Acc;
                    (V, Acc)         -> [unescape(V) | Acc]
                end, [], List);
to_list(Reverse, #fq { head = H, tail = T }) ->
    to_list(Reverse, H) ++ to_list(not Reverse, T).

from_list(L) -> #fq { len = length(L), head = [escape(V) || V <- L], tail = [] }.

member(X, #fq { head = H, tail = T }) ->
    member(X, H) orelse member(X, T);
member(X, List) when is_list(List) ->
    lists:any(fun (E) -> member(X, unescape(E)) end, List);
member(X, X) -> true.

in(X, #fq { len = 0 } = Q) ->
    Q #fq { len = 1, head = [escape(X)] };
in(X, #fq { len = L, tail = T } = Q) ->
    Q #fq { len = L+1, tail = [escape(X) | T] }.

in_r(X, #fq { len = 0 } = Q) ->
    Q #fq { len = 1, tail = [escape(X)] };
in_r(X, #fq { len = L, head = H } = Q) ->
    Q #fq { len = L+1, head = [escape(X) | H] }.

out(#fq { len = 0 } = Q) ->
    {empty, Q};
out(#fq { head = [#fq{} = IQ], tail = [] }) ->
    out(IQ);
out(#fq { tail = [#fq{} = IQ], head = [] }) ->
    out(IQ);
out(#fq { len = L, head = [#fq{ len = L1, tail = T1 } = IQ | H], tail = T }) ->
    %% Essentially we pivot so that the IQ becomes the outer, and we
    %% stuff ourselves at the end
    out(IQ #fq { len = L, tail = [#fq { len = L - L1, head = H, tail = T } | T1] });
out(#fq { len = L, head = [V], tail = T }) ->
    {{value, unescape(V)}, #fq { len = L-1, head = lists:reverse(T), tail = [] }};
out(#fq { len = L, head = [V | H] } = Q) ->
    {{value, unescape(V)}, Q #fq { len = L-1, head = H }};
out(#fq { head = [], tail = T } = Q) ->
    out(Q #fq { head = lists:reverse(T), tail = [] }).

out_r(#fq { len = 0 } = Q) ->
    {empty, Q};
out_r(#fq { tail = [#fq{} = IQ], head = [] }) ->
    out_r(IQ);
out_r(#fq { head = [#fq{} = IQ], tail = [] }) ->
    out_r(IQ);
out_r(#fq { len = L, tail = [#fq{ len = L1, head = H1 } = IQ | T], head = H }) ->
    %% Essentially we pivot so that the IQ becomes the outer, and we
    %% stuff ourselves at the start
    out_r(IQ #fq { len = L, head = [#fq { len = L - L1, tail = T, head = H } | H1] });
out_r(#fq { len = L, tail = [V], head = H }) ->
    {{value, unescape(V)}, #fq { len = L-1, tail = lists:reverse(H), head = [] }};
out_r(#fq { len = L, tail = [V | T] } = Q) ->
    {{value, unescape(V)}, Q #fq { len = L-1, tail = T }};
out_r(#fq { tail = [], head = H } = Q) ->
    out_r(Q #fq { tail = lists:reverse(H), head = [] }).

join(Q, #fq { len = 0 }) ->
    Q;
join(#fq { len = 0 }, Q) ->
    Q;
join(Q, #fq { len = 1 } = Q1) ->
    {{value, V}, _} = out(Q1),
    in(V, Q);
join(#fq { len = 1 } = Q, Q1) ->
    {{value, V}, _} = out(Q),
    in_r(V, Q1);
join(#fq { len = L, tail = T } = Q, #fq { len = L1 } = Q1) ->
    Q #fq { len = L+L1, tail = [Q1 | T] }.


-compile({inline, [{escape,1},{unescape,1}]}).

escape(#fq{} = V)        -> {escaped, V};
escape({escaped, _} = V) -> {escaped, V};
escape(V)                -> V.

unescape({escaped, V}) -> V;
unescape(V)            -> V.
