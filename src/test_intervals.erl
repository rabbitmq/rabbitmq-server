-module(test_intervals).

-export([all/0]).
-compile(export_all).

all() ->
    {failing, []} = {failing, failing()},
    ok = ranges_tests(),
    ok = is_element_tests(),
    ok = first_fit_tests(),
    ok.

cases() ->
    [{intersection,false,false,{false,[2,3,6,7,9,10]}},
     {intersection,false,true,{false,[1,2,3,4,12,13]}},
     {intersection,true,false,{false,[5,6,7,8,11,12,13]}},
     {intersection,true,true,{true,[1,4,5,8,9,10,11]}},
     {union,false,false,{false,[1,4,5,8,9,10,11]}},
     {union,false,true,{true,[5,6,7,8,11,12,13]}},
     {union,true,false,{true,[1,2,3,4,12,13]}},
     {union,true,true,{true,[2,3,6,7,9,10]}},
     {symmetric_difference,false,false,{false,[1,2,3,4,5,6,7,8,11]}},
     {symmetric_difference,false,true,{true,[1,2,3,4,5,6,7,8,11]}},
     {symmetric_difference,true,false,{true,[1,2,3,4,5,6,7,8,11]}},
     {symmetric_difference,true,true,{false,[1,2,3,4,5,6,7,8,11]}},
     {difference,false,false,{false,[1,2,3,4,12,13]}},
     {difference,false,true,{false,[2,3,6,7,9,10]}},
     {difference,true,false,{true,[1,4,5,8,9,10,11]}},
     {difference,true,true,{false,[5,6,7,8,11,12,13]}}].

failing() ->
    lists:flatmap(fun run1/1, cases()).

run1({Op, A, B, Expected}) ->
    case merge(Op, A, B) of
	Expected ->
	    [];
	Actual ->
	    [{Op, A, B, Actual}]
    end.

%% 0 1 2 3 4 5 6 7 8 9 A B C D E F
%% | | | | | | | | | | | | | | | |
%%   ------    --    --    --
%%     --    ------  --  --  ----====

topline() -> [1, 4, 6, 7, 9, 10, 12, 13].
bottomline() -> [2, 3, 5, 8, 9, 10, 11, 12, 13].

merge(Op, S1, S2) ->
    intervals:merge(Op, {S1, topline()}, {S2, bottomline()}).

rendercase({Op, S1, S2, Expected}) ->
    I1 = {S1, topline()},
    I2 = {S2, bottomline()},
    Result = intervals:merge(Op, I1, I2),
    io:format("********* ~p:~n", [Op]),
    io:format("I1:       " ++ renderline(I1)),
    io:format("I2:       " ++ renderline(I2)),
    io:format("Actual:   " ++ renderline(Result)),
    io:format("Expected: " ++ renderline(Expected)),
    io:format("~n"),
    Result.

renderall() ->
    lists:foreach(fun rendercase/1, cases()).

renderline({Initial, Toggles}) ->
    lists:flatten([renderfirstlast(Initial), renderline(0, Initial, Toggles), 13,10]).

renderfirstlast(true) ->
    "====";
renderfirstlast(false) ->
    "    ".

rendercell(true) ->
    "--";
rendercell(false) ->
    "  ".

renderline(Pos, State, [])
  when Pos < 15 ->
    [rendercell(State), renderline(Pos + 1, State, [])];
renderline(_Pos, State, []) ->
    renderfirstlast(State);
renderline(Pos, State, Rest = [Toggle | _])
  when Pos < Toggle ->
    [rendercell(State), renderline(Pos + 1, State, Rest)];
renderline(Pos, State, [Toggle | Rest])
  when Pos == Toggle ->
    [rendercell(not State), renderline(Pos + 1, not State, Rest)].

ranges_tests() ->
    Empty = intervals:empty(),
    {range_empty_test, Empty} = {range_empty_test, intervals:range(22, 22)},
    BottomLine = bottomline(),
    {ranges_test1, {false, BottomLine}} = {ranges_test1, intervals:ranges([{2, 3},
									   {5, 8},
									   {9, 10},
									   {11, 12},
									   {13, inf}])},
    ok.

is_element_test(Cases, R) ->
    NR = intervals:invert(R),
    lists:foreach(fun ({E, Expected}) ->
			  {E, Expected} = {E, intervals:is_element(E, R)}
		  end, Cases),
    lists:foreach(fun ({E, Expected}) ->
			  NotExpected = not Expected,
			  {E, NotExpected} = {E, intervals:is_element(E, NR)}
		  end, Cases),
    ok.

is_element_tests() ->
    ok = is_element_test([{5, true},
			  {-5, false},
			  {15, false},
			  {0, true},
			  {10, false},
			  {10.1, false},
			  {0.9, true}],
			 intervals:range(0, 10)),
    ok = is_element_test([{"", false},
			  {"just", true},
			  {"maybe", true},
			  {"not", false},
			  {"testing", false},
			  {"zow", true}],
			 intervals:union(intervals:range("a", "n"),
					 intervals:range("z", "{"))),
    ok.

first_fit_tests() ->
    R1 = intervals:ranges([{2, 3}, {5, 10}]),
    {ok, 2} = intervals:first_fit(1, R1),
    {ok, 5} = intervals:first_fit(2, R1),
    {ok, 5} = intervals:first_fit(5, R1),
    none = intervals:first_fit(6, R1),
    none = intervals:first_fit(inf, R1),
    R2 = intervals:union(R1, intervals:range(20, inf)),
    {ok, 20} = intervals:first_fit(6, R2),
    {ok, 20} = intervals:first_fit(inf, R2),
    ok.
