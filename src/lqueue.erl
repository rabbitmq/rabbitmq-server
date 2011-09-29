-module(lqueue).

-compile([export_all]).

-define(QUEUE, queue).

new() -> {0, ?QUEUE:new()}.

is_empty({0, _Q}) ->
    true;
is_empty(_) ->
    false.

in(V, {L, Q}) -> {L+1, ?QUEUE:in(V, Q)}.
in_r(V, {L, Q}) -> {L+1, ?QUEUE:in_r(V, Q)}.

out({0, _Q} = Q) ->
    {empty, Q};
out({L, Q}) ->
    {Result, Q1} = ?QUEUE:out(Q),
    {Result, {L-1, Q1}}.

out_r({0, _Q} = Q) ->
    {empty, Q};
out_r({L, Q}) ->
    {Result, Q1} = ?QUEUE:out_r(Q),
    {Result, {L-1, Q1}}.

join({L1, Q1}, {L2, Q2}) ->
    {L1 + L2, ?QUEUE:join(Q1, Q2)}.

to_list({_L, Q}) ->
    ?QUEUE:to_list(Q).

from_list(L) ->
    {length(L), ?QUEUE:from_list(L)}.

queue_fold(Fun, Init, Q) ->
    case out(Q) of
        {empty, _Q}      -> Init;
        {{value, V}, Q1} -> queue_fold(Fun, Fun(V, Init), Q1)
    end.

len({L, _Q}) ->
    L.
