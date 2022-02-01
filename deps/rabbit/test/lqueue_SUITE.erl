-module(lqueue_SUITE).

-export([suite/0, all/0, groups/0,
         init_per_suite/1, end_per_suite/1,
         init_per_group/2, end_per_group/2,
         init_per_testcase/2, end_per_testcase/2
        ]).

-export([do/1, to_list/1, io_test/1, op_test/1, error/1, oops/1,
         prop_from_list_to_list/1, prop_from_list_length/1,
         prop_in_out/1, prop_join_fold/1, prop_fifo/1, prop_r/1,
         deprecated_state/1, out_r_worst_case/1]).

-include_lib("proper/include/proper.hrl").
-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").

suite() ->
    [{timetrap,{minutes,1}}].

all() ->
    [{group, tests}].

groups() ->
    [{tests, [parallel],
      [
       %% copied from OTP's queue module
       do,
       to_list,
       io_test,
       op_test,
       error,
       oops,
       %% property tests
       prop_from_list_to_list,
       prop_from_list_length,
       prop_in_out,
       prop_join_fold,
       prop_fifo,
       prop_r,
       %% test old state to new state conversion
       deprecated_state,
       %% tests involving lists:reverse/2
       out_r_worst_case
      ]
     }].

init_per_suite(Config) ->
    Config.

end_per_suite(_Config) ->
    ok.

init_per_group(_GroupName, Config) ->
    Config.

end_per_group(_GroupName, Config) ->
    Config.

init_per_testcase(_Case, Config) ->
    Config.

end_per_testcase(_Case, _Config) ->
    ok.

do(Config) when is_list(Config) ->
    L = [{in, 1},
         {in, 2},
         {out, {value, 1}},
         {in, 3},
         {out, {value, 2}},
         {out, {value, 3}},
         {out, empty}
        ],

    E = lqueue:new(),
    [] = lqueue:to_list(E),
    Q = do_queue(E, L),
    true = lqueue:is_empty(Q),
    0 = lqueue:len(Q),
    ok.

do_queue(Q, []) ->
    Q;
do_queue(Q, [E | Rest]) ->
    do_queue(do_queue_1(E, Q), Rest).

do_queue_1({in, E}, Q) ->
    lqueue:in(E, Q);
do_queue_1({out, E}, Q) ->
    case lqueue:out(Q) of
        {E, Q1} ->
            Q1;
        Other ->
            ct:fail({"out failed", E, Q, Other})
    end.

%% OTP-2701
to_list(Config) when is_list(Config) ->
    E = lqueue:new(),
    Q = do_queue(E, [{in, 1},
                     {in, 2},
                     {in, 3},
                     {out, {value, 1}},
                     {in, 4},
                     {in, 5}]),
    4 = lqueue:len(Q),
    case lqueue:to_list(Q) of
        [2,3,4,5] ->
            ok;
        Other1 ->
            ct:fail(Other1)
    end,
    ok.


%% Test input and output.
io_test(Config) when is_list(Config) ->
    E = lqueue:new(),
    do_io_test(E),
    ok.

do_io_test(E) ->
    [4,3,5] =
	io([in,in,out,out,out,in_r,in_r,in], E, 1),
    [5,3,4] =
	io([in_r,in_r,out_r,out_r,out_r,in,in,in_r], E, 1),
    %%
    [] =
	io([in,in,out,in,in,out,out,in,out,out], E, 1),
    [] =
	io([in_r,in_r,out_r,in_r,in_r,out_r,out_r,in_r,out_r,out_r],
	   E, 1),
    %%
    [5,6] =
	io([in,in,in,out,out,in,in,in,out,out], E, 1),
    [6,5] =
	io([in_r,in_r,in_r,out_r,out_r,in_r,in_r,in_r,out_r,out_r],
	   E, 1),
    %%
    [5] =
	io([in,out,out,in,out,in,out,in,out,in], E, 1),
    [5] =
	io([in_r,out_r,out_r,in_r,out_r,in_r,out_r,in_r,out_r,in_r],
	   E, 1),
    %%
    [] =
	io([in,out,in,in,out,out,in,in,in,out,out,out],
	   E, 1),
    [] =
	io([in_r,out_r,in_r,in_r,out_r,out_r,in_r,in_r,in_r,out_r,out_r,out_r],
	   E, 1),
    %%
    [3] =	io([in,in,in,in_r,out,out,out], E, 1),
    [3] =	io([in_r,in_r,in_r,in,out_r,out_r,out_r], E, 1),
    %%
    [3] =
	io([in,peek,peek_r,drop,in_r,peek,peek_r,in,peek,peek_r,drop], E, 1),
    %% Malformed queues UGLY-GUTS-ALL-OVER-THE-PLACE
    [2,1] = io([peek], {2,[1,2],[]}, 1),
    [1,2] = io([peek_r], {2,[],[1,2]}, 1),
    %%
    ok.

%% Perform a list of operations to a queue.
%% Keep a reference queue on the side; just a list.
%% Compare the read values between the queues.
%% Return the resulting queue as a list.
%% Inserted values are increments of the previously inserted.
io(Ops, Q, X) ->
    io(Ops, Q, lqueue:to_list(Q), X).

io([out | Tail], Q, [], X) ->
    {empty, Q1} = lqueue:out(Q),
    io(Tail, Q1, [], X);
io([out | Tail], Q, [H | T], X) ->
    {{value,H}, Q1} = lqueue:out(Q),
    io(Tail, Q1, T, X);
io([out_r | Tail], Q, [], X) ->
    {empty, Q1} = lqueue:out_r(Q),
    io(Tail, Q1, [], X);
io([out_r | Tail], Q, QQ, X) ->
    {{value,H}, Q1} = lqueue:out_r(Q),
    [H | T] = lists:reverse(QQ),
    io(Tail, Q1, lists:reverse(T), X);
io([in_r | Tail], Q, QQ, X) ->
    io(Tail, lqueue:in_r(X,Q), [X|QQ], X+1);
io([in | Tail], Q, QQ, X) ->
    io(Tail, lqueue:in(X,Q), QQ++[X], X+1);
io([peek | Tail], Q, [], X) ->
    empty = lqueue:peek(Q),
    io(Tail, Q, [], X);
io([peek | Tail], Q, [H|_]=Q0, X) ->
    {value,H} = lqueue:peek(Q),
    io(Tail, Q, Q0, X);
io([peek_r | Tail], Q, [], X) ->
    empty = lqueue:peek_r(Q),
    io(Tail, Q, [], X);
io([peek_r | Tail], Q, Q0, X) ->
    E = lists:last(Q0),
    {value,E} = lqueue:peek_r(Q),
    io(Tail, Q, Q0, X);
io([drop | Tail], Q, [], X) ->
    try lqueue:drop(Q) of
        V ->
            ct:fail({?MODULE,?LINE,V})
    catch
        error:empty ->
            io(Tail, Q, [], X)
    end;
io([drop | Tail], Q, [_ | T], X) ->
    Q1 = lqueue:drop(Q),
    io(Tail, Q1, T, X);
io([], Q, QQ, _X) ->
    QQ = lqueue:to_list(Q),
    Length = length(QQ),
    Length = lqueue:len(Q),
    QQ.

%% Test operations on whole queues.
op_test(Config) when is_list(Config) ->
    do_op_test(fun id/1),
    ok.

do_op_test(F) ->
    Len = 50,
    Len2 = 2*Len,
    L1 = lists:seq(1, Len),
    L2 = lists:seq(Len+1, Len2),
    L3 = L1++L2,
    Q0 = F(lqueue:new()),
    [] = lqueue:to_list(Q0),
    Q0 = F(lqueue:from_list([])),
    Q1 = F(lqueue:from_list(L1)),
    Q2 = F(lqueue:from_list(L2)),
    Q3 = F(lqueue:from_list(L3)),
    Len = lqueue:len(Q1),
    Len = lqueue:len(Q2),
    Len2 = lqueue:len(Q3),
    L1 = lqueue:to_list(Q1),
    L2 = lqueue:to_list(Q2),
    L3 = lqueue:to_list(Q3),
    Q3b = lqueue:join(Q0, lqueue:join(lqueue:join(Q1, Q2), Q0)),
    L3 = lqueue:to_list(Q3b),

    FoldQ = lqueue:from_list(L1),
    FoldExp1 = lists:sum(L1),
    FoldAct1 = lqueue:fold(fun(X,A) -> X+A end, 0, FoldQ),
    FoldExp1 = FoldAct1,
    FoldExp2 = [X*X || X <- L1],
    FoldAct2 = lqueue:fold(fun(X,A) -> [X*X|A] end, [], FoldQ),
    FoldExp2 = lists:reverse(FoldAct2),
    ok.

%% Test queue errors.
error(Config) when is_list(Config) ->
    do_error(fun id/1, illegal_queue),
    do_error(fun id/1, {[],illegal_queue}),
    do_error(fun id/1, {illegal_queue,[17]}),
    do_error(fun id/1, {-1, [],[]}),
    do_error(fun id/1, {illegal_length, [],[]}),
    do_error(fun id/1, {0, [],illegal_queue}),
    do_error(fun id/1, {-1, {[],[]}}),
    ok.

trycatch(F, Args) ->
    trycatch(lqueue, F, Args).

trycatch(M, F, Args) ->
    try apply(M, F, Args) of
        V ->
            ct:fail("expected error, got ~p", [V])
    catch
        C:R -> {C,R}
    end.

do_error(F, IQ) ->
    io:format("Illegal Queue: ~p~n", [IQ]),
    {error,badarg} = trycatch(in, [1, IQ]),
    {error,badarg} = trycatch(out, [IQ]),
    {error,badarg} = trycatch(drop, [IQ]),
    {error,badarg} = trycatch(in_r ,[1, IQ]),
    {error,badarg} = trycatch(out_r ,[IQ]),
    {error,badarg} = trycatch(to_list ,[IQ]),
    {error,badarg} = trycatch(from_list, [no_list]),
    {error,badarg} = trycatch(is_empty, [IQ]),
    {error,badarg} = trycatch(len, [IQ]),
    {error,badarg} = trycatch(fold, [fun(_,_) -> ok end,0,IQ]),
    {error,badarg} = trycatch(join, [F(lqueue:new()), IQ]),
    {error,badarg} = trycatch(join, [IQ, F(lqueue:new())]),
    {error,badarg} = trycatch(peek, [IQ]),
    {error,badarg} = trycatch(peek_r, [IQ]),
    ok.

id(X) ->
    X.

%% Test queue errors.
oops(Config) when is_list(Config) ->
    N = 3142,
    Optab = optab(),
    Seed0 = rand:seed(exsplus, {1,2,4}),
    {Is,Seed} = random_list(N, tuple_size(Optab), Seed0, []),
    io:format("~p ", [Is]),
    QA = lqueue:new(),
    QB = {[]},
    emul([QA], [QB], Seed, [element(I, Optab) || I <- Is]).

optab() ->
    {{new,[],        q,     fun ()     -> {[]} end},
     {is_empty,[q],  v,     fun (Q) ->
                                    case Q of
                                        {[]} -> true;
                                        _    -> false
                                    end end},
     {len,[q],       v,     fun ({L})   -> length(L) end},
     {to_list,[q],   v,     fun ({L})   -> L end},
     {from_list,[l], q,     fun (L)     -> {L} end},
     {in,[t,q],      q,     fun (X,{L}) -> {L++[X]} end},
     {in_r,[t,q],    q,     fun (X,{L}) -> {[X|L]} end},
     {out,[q],       {v,q}, fun ({L}=Q) ->
                                    case L of
                                        []    -> {empty,Q};
                                        [X|T] -> {{value,X},{T}}
                                    end
                            end},
     {out_r,[q],     {v,q}, fun ({L}=Q) ->
                                    case L of
                                        []    -> {empty,Q};
                                        _ ->
                                            [X|R] = lists:reverse(L),
                                            T = lists:reverse(R),
                                            {{value,X},{T}}
                                    end
                            end},
     {peek,[q],      v,     fun ({[]})    -> empty;
                                ({[H|_]}) -> {value,H}
                            end},
     {peek_r,[q],    v,     fun ({[]})    -> empty;
                                ({L})     -> {value,lists:last(L)}
                            end},
     {drop,[q],      q,     fun ({[]})    -> erlang:error(empty);
                                ({[_|T]}) -> {T}
                            end},
     {join,[q,q],    q,     fun ({L1}, {L2}) -> {L1++L2} end}
    }.

emul(_, _, _, []) ->
    ok;
emul(QsA0, QsB0, Seed0, [{Op,Ts,S,Fun}|Ops]) ->
    {AsA,Seed} = args(Ts, QsA0, Seed0, []),
    {AsB,Seed} = args(Ts, QsB0, Seed0, []),
    io:format("~n% ~w % ~p ", [Op,AsA]),
    io:format("% ~p :", [AsB]),
    XX = call({lqueue,Op}, AsA),
    YY = call(Fun, AsB),
    case {XX,YY} of
        {{value,X},{value,Y}} ->
            {[Qa|_]=QsA,[{Lb}|_]=QsB} = chk(QsA0, QsB0, S, X, Y),
            case lqueue:to_list(Qa) of
                Lb ->
                    io:format("|~p| ", [Lb]),
                    emul(QsA, QsB, Seed, Ops);
                La ->
                    throw({to_list,[XX,YY,Op,AsA,AsB,La,Lb]})
            end;
        {Exception,Exception} ->
            io:format("!~p! ", [Exception]),
            emul(QsA0, QsB0, Seed, Ops);
        _ ->
            throw({diff,[XX,YY,Op,AsA,AsB]})
    end.

args([], _, Seed, R) ->
    {lists:reverse(R),Seed};
args([q|Ts], [Q|Qs]=Qss, Seed, R) ->
    args(Ts, if Qs =:= [] -> Qss; true -> Qs end, Seed, [Q|R]);
args([l|Ts], Qs, Seed0, R) ->
    {N,Seed1} = rand:uniform_s(17, Seed0),
    {L,Seed} = random_list(N, 4711, Seed1, []),
    args(Ts, Qs, Seed, [L|R]);
args([t|Ts], Qs, Seed0, R) ->
    {T,Seed} = rand:uniform_s(4711, Seed0),
    args(Ts, Qs, Seed, [T|R]);
args([n|Ts], Qs, Seed0, R) ->
    {N,Seed} = rand:uniform_s(17, Seed0),
    args(Ts, Qs, Seed, [N|R]).

random_list(0, _, Seed, R) ->
    {R,Seed};
random_list(N, M, Seed0, R) ->
    {X,Seed} = rand:uniform_s(M, Seed0),
    random_list(N-1, M, Seed, [X|R]).

call(Func, As) ->
    try case Func of
            {M,F} -> apply(M, F, As);
            _     -> apply(Func, As)
        end of
        V ->
            {value,V}
    catch
        Class:Reason ->
            {Class,Reason}
    end.

chk(QsA, QsB, v, X, X) ->
    io:format("<~p> ", [X]),
    {QsA,QsB};
chk(_, _, v, X, Y) ->
    throw({diff,v,[X,Y]});
chk(QsA, QsB, q, Qa, {Lb}=Qb) ->
    case lqueue:to_list(Qa) of
        Lb ->
            io:format("|~p| ", [Lb]),
            {[Qa|QsA],[Qb|QsB]};
        La ->
            throw({diff,q,[Qa,La,Lb]})
    end;
chk(QsA, QsB, T, X, Y)
  when tuple_size(T) =:= tuple_size(X), tuple_size(T) =:= tuple_size(Y) ->
    io:format("{"),
    try
        chk_tuple(QsA, QsB, T, X, Y, 1)
    after
        io:format("}")
    end;
chk(_, _, T, X, Y)
  when is_tuple(T), is_tuple(X), is_tuple(Y) ->
    throw({diff,T,[X,Y]}).

chk_tuple(QsA, QsB, T, _, _, N) when N > tuple_size(T) ->
    {QsA,QsB};
chk_tuple(QsA0, QsB0, T, X, Y, N) ->
    {QsA,QsB} = chk(QsA0, QsB0, element(N, T), element(N, X), element(N, Y)),
    chk_tuple(QsA, QsB, T, X, Y, N+1).

prop_from_list_to_list(_Config) ->
    run_proper(fun() ->
                       ?FORALL(List, list(),
                               begin
                                   equals(List,
                                          lqueue:to_list(lqueue:from_list(List)))
                               end)
               end).

prop_from_list_length(_Config) ->
    run_proper(fun() ->
                       ?FORALL(List, list(),
                               begin
                                   Q = lqueue:from_list(List),
                                   equals(length(List), lqueue:len(Q))
                               end)
               end).

prop_in_out(_Config) ->
    run_proper(fun() ->
                       ?FORALL({Ins0, Outs0},
                               {
                                resize(10_000, list(oneof([in, in_r]))),
                                resize(10_000, list(oneof([out, out_r, drop])))
                               },
                               begin
                                   Ins0Len = length(Ins0),
                                   Outs0Len = length(Outs0),
                                   Min = min(Ins0Len, Outs0Len),
                                   %% Make Ins and Outs have same length
                                   Ins = lists:nthtail(Ins0Len - Min, Ins0),
                                   Outs = lists:nthtail(Outs0Len - Min, Outs0),
                                   {Queue0, Min} = lists:foldl(
                                                     fun(Op, {Q0, Len}) ->
                                                             ?assertEqual(Len, lqueue:len(Q0)),
                                                             Q = lqueue:Op(a, Q0),
                                                             {Q, Len + 1}
                                                     end, {lqueue:new(), 0}, Ins),
                                   {Queue, 0} = lists:foldl(
                                                  fun (Op, {Q0, Len}) ->
                                                          ?assertEqual(Len, lqueue:len(Q0)),
                                                          Q = case Op of
                                                                  out ->
                                                                      {{value, a}, Q1} = lqueue:out(Q0),
                                                                      Q1;
                                                                  out_r ->
                                                                      {{value, a}, Q1} = lqueue:out_r(Q0),
                                                                      Q1;
                                                                  drop ->
                                                                      lqueue:drop(Q0)
                                                              end,
                                                          {Q, Len-1}
                                                  end, {Queue0, Min}, Outs),
                                   lqueue:is_empty(Queue)
                               end)
               end).

prop_join_fold(_Config) ->
    run_proper(fun() ->
                       ?FORALL({L1, L2}, {list(), list()},
                               begin
                                   L = L1 ++ L2,
                                   Q = lqueue:join(
                                         lqueue:from_list(L1),
                                         lqueue:from_list(L2)
                                        ),
                                   ?assertEqual(length(L), lqueue:len(Q)),
                                   It = lqueue:fold(fun(Elem, I) ->
                                                            ?assertEqual(lists:nth(I, L), Elem),
                                                            I + 1
                                                    end, 1, Q),
                                   equals(length(L), It - 1)
                               end)
               end).

prop_fifo(_Config) ->
    run_proper(fun() ->
                       ?FORALL(List, list(),
                               begin
                                   Queue0 = lists:foldl(fun(E, Q0) ->
                                                                lqueue:in(E, Q0)
                                                        end, lqueue:new(), List),
                                   ?assertEqual(length(List), lqueue:len(Queue0)),
                                   Queue = lists:foldl(fun(E, Q0) ->
                                                               {value, E} = lqueue:peek(Q0),
                                                               {{value, E}, Q} = lqueue:out(Q0),
                                                               ?assertMatch(Q, lqueue:drop(Q0)),
                                                               Q
                                                       end, Queue0, List),
                                   ?assertEqual(empty, lqueue:peek(Queue)),
                                   ?assertMatch({empty, Queue}, lqueue:out(Queue)),
                                   ?assertError(empty, lqueue:drop(Queue)),
                                   lqueue:is_empty(Queue)
                               end)
               end).

prop_r(_Config) ->
    run_proper(fun() ->
                       ?FORALL(List, list(),
                               begin
                                   Queue0 = lists:foldl(fun(E, Q0) ->
                                                                lqueue:in_r(E, Q0)
                                                        end, lqueue:new(), List),
                                   ?assertEqual(length(List), lqueue:len(Queue0)),
                                   Queue = lists:foldl(fun(E, Q0) ->
                                                               {value, E} = lqueue:peek_r(Q0),
                                                               {{value, E}, Q} = lqueue:out_r(Q0),
                                                               Q
                                                       end, Queue0, List),
                                   ?assertEqual(empty, lqueue:peek_r(Queue)),
                                   ?assertMatch({empty, Queue}, lqueue:out_r(Queue)),
                                   lqueue:is_empty(Queue)
                               end)
               end).

run_proper(Fun) ->
    ?assert(proper:counterexample(
              Fun(),
              [{numtests, 100},
               {on_output, fun(".", _) -> ok; % don't print the '.'s on new lines
                              (F, A) -> ct:pal(?LOW_IMPORTANCE, F, A)
                           end}])).

%% Test that functions accept deprecated state.
deprecated_state(_Config) ->
    OldState = {4, {[d,c], [a,b]}},
    ?assertNot(lqueue:is_empty(OldState)),
    ?assertEqual(4, lqueue:len(OldState)),
    ?assertEqual([a,b,c,d], lqueue:to_list(OldState)),
    ?assertEqual({value, a}, lqueue:peek(OldState)),
    ?assertEqual({value, d}, lqueue:peek_r(OldState)),
    ?assertEqual(4, lqueue:fold(fun(E, N) when is_atom(E) -> N+1 end, 0, OldState)),
    %% convert to new state
    ?assertEqual({5, [e,d,c], [a,b]}, lqueue:in(e, OldState)),
    ?assertEqual({5, [d,c], [e,a,b]}, lqueue:in_r(e, OldState)),
    ?assertEqual({{value, a}, {3, [d,c], [b]}}, lqueue:out(OldState)),
    ?assertEqual({3, [d,c], [b]}, lqueue:drop(OldState)),
    ?assertEqual({{value, d}, {3, [c], [a,b]}}, lqueue:out_r(OldState)),
    ?assertEqual({5, [e], [a,b,c,d]}, lqueue:join(OldState, {1, {[e], []}})).

out_r_worst_case(_Config) ->
    Q0 = lqueue:in(a, lqueue:new()),
    Q1 = lqueue:in_r(b, lqueue:new()),
    Q2 = lqueue:join(Q0, Q1),
    %% calls lists:reverse/2 internally
    {{value, b}, Q3} = lqueue:out_r(Q2),
    ?assertEqual(1, lqueue:len(Q3)).
