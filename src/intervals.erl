-module(intervals).
-export([empty/0, full/0, half/1, single_int/1, single_string/1, range/2, ranges/1]).
-export([is_element/2]).
-export([invert/1, merge/3, intersection/2, union/2, symmetric_difference/2, difference/2]).
-export([first_fit/2]).

empty() ->
    {false, []}.

full() ->
    {true, []}.

half(N) ->
    {false, [N]}.

single_int(N) ->
    {false, [N, N+1]}.

single_string(N) ->
    {false, [N, N ++ [0]]}.

range(inf, inf) ->
    full();
range(inf, N) ->
    {true, [N]};
range(N, inf) ->
    half(N);
range(N, M)
  when N >= M ->
    empty();
range(N, M) ->
    {false, [N, M]}.

ranges([]) ->
    empty();
ranges([{N,M} | Ranges]) ->
    {Initial, Acc0} = range(N,M),
    {Initial, lists:reverse(ranges(lists:reverse(Acc0), Ranges))}.

ranges(Acc, []) ->
    Acc;
ranges(Acc, [{N, M} | Ranges])
  when is_number(N) andalso is_number(M) ->
    if
	N < M ->
	    ranges([M, N | Acc], Ranges);
	true ->
	    ranges(Acc, Ranges)
    end;
ranges(Acc, [{N, inf}]) ->
    [N | Acc].

is_element(E, {Initial, Toggles}) ->
    is_element(E, Initial, Toggles).

is_element(_E, Current, []) ->
    Current;
is_element(E, Current, [T | _])
  when E < T ->
    Current;
is_element(E, Current, [_ | Rest]) ->
    is_element(E, not Current, Rest).

invert({true, Toggles}) ->
    {false, Toggles};
invert({false, Toggles}) ->
    {true, Toggles}.

merge(Op, {S1, T1}, {S2, T2}) ->
    Initial = merge1(Op, S1, S2),
    {Initial, merge(Op, Initial, [], S1, T1, S2, T2)}.

intersection(A, B) -> merge(intersection, A, B).
union(A, B) -> merge(union, A, B).
symmetric_difference(A, B) -> merge(symmetric_difference, A, B).
difference(A, B) -> merge(difference, A, B).

merge1(intersection, A, B) -> A and B;
merge1(union, A, B) -> A or B; 
merge1(symmetric_difference, A, B) -> A xor B;
merge1(difference, A, B) -> A and not B.

merge(Op, SA, TA, S1, [T1 | R1], S2, [T2 | R2])
  when T1 == T2 ->
    update(Op, SA, TA, T1, not S1, R1, not S2, R2);
merge(Op, SA, TA, S1, [T1 | R1], S2, R2 = [T2 | _])
  when T1 < T2 ->
    update(Op, SA, TA, T1, not S1, R1, S2, R2);
merge(Op, SA, TA, S1, R1, S2, [T2 | R2]) ->
    update(Op, SA, TA, T2, S1, R1, not S2, R2);
merge(Op, _SA, TA, S1, [], _S2, R2) ->
    finalise(TA, mergeempty(Op, left, S1, R2));
merge(Op, _SA, TA, _S1, R1, S2, []) ->
    finalise(TA, mergeempty(Op, right, S2, R1)).

update(Op, SA, TA, T1, S1, R1, S2, R2) ->
    Merged = merge1(Op, S1, S2),
    if
	SA == Merged ->
	    merge(Op, SA, TA, S1, R1, S2, R2);
	true ->
	    merge(Op, Merged, [T1 | TA], S1, R1, S2, R2)
    end.

finalise(TA, Tail) ->
    lists:reverse(TA, Tail).

mergeempty(intersection, _LeftOrRight, true, TailT) ->
    TailT;
mergeempty(intersection, _LeftOrRight, false, _TailT) ->
    [];
mergeempty(union, _LeftOrRight, true, _TailT) ->
    [];
mergeempty(union, _LeftOrRight, false, TailT) ->
    TailT;
mergeempty(symmetric_difference, _LeftOrRight, _EmptyS, TailT) ->
    TailT;
mergeempty(difference, left, true, TailT) ->
    TailT;
mergeempty(difference, right, false, TailT) ->
    TailT;
mergeempty(difference, _LeftOrRight, _EmptyS, _TailT) ->
    [].

first_fit(Request, {false, Toggles}) ->
    first_fit1(Request, Toggles).

first_fit1(_Request, []) ->
    none;
first_fit1(_Request, [N]) ->
    {ok, N};
first_fit1(inf, [_N, _M | Rest]) ->
    first_fit1(inf, Rest);
first_fit1(Request, [N, M | _Rest])
  when M - N >= Request ->
    {ok, N};
first_fit1(Request, [_N, _M | Rest]) ->
    first_fit1(Request, Rest).
