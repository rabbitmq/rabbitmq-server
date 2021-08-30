%% This file is a copy of `mochijson2.erl' from mochiweb, revision
%% d541e9a0f36c00dcadc2e589f20e47fbf46fc76f.  For the license, see
%% `LICENSE-MIT-Mochi'.

%% @copyright 2007 Mochi Media, Inc.
%% @author Bob Ippolito <bob@mochimedia.com>

%% @doc Useful numeric algorithms for floats that cover some deficiencies
%% in the math module. More interesting is digits/1, which implements
%% the algorithm from:
%% https://cs.indiana.edu/~burger/fp/index.html
%% See also "Printing Floating-Point Numbers Quickly and Accurately"
%% in Proceedings of the SIGPLAN '96 Conference on Programming Language
%% Design and Implementation.

-module(rabbit_numerical).
-author("Bob Ippolito <bob@mochimedia.com>").
-export([digits/1, frexp/1, int_pow/2, int_ceil/1]).

%% IEEE 754 Float exponent bias
-define(FLOAT_BIAS, 1022).
-define(MIN_EXP, -1074).
-define(BIG_POW, 4503599627370496).

%% External API

%% @spec digits(number()) -> string()
%% @doc  Returns a string that accurately represents the given integer or float
%%       using a conservative amount of digits. Great for generating
%%       human-readable output, or compact ASCII serializations for floats.
digits(N) when is_integer(N) ->
    integer_to_list(N);
digits(0.0) ->
    "0.0";
digits(Float) ->
    {Frac1, Exp1} = frexp_int(Float),
    [Place0 | Digits0] = digits1(Float, Exp1, Frac1),
    {Place, Digits} = transform_digits(Place0, Digits0),
    R = insert_decimal(Place, Digits),
    case Float < 0 of
        true ->
            [$- | R];
        _ ->
            R
    end.

%% @spec frexp(F::float()) -> {Frac::float(), Exp::float()}
%% @doc  Return the fractional and exponent part of an IEEE 754 double,
%%       equivalent to the libc function of the same name.
%%       F = Frac * pow(2, Exp).
frexp(F) ->
    frexp1(unpack(F)).

%% @spec int_pow(X::integer(), N::integer()) -> Y::integer()
%% @doc  Moderately efficient way to exponentiate integers.
%%       int_pow(10, 2) = 100.
int_pow(_X, 0) ->
    1;
int_pow(X, N) when N > 0 ->
    int_pow(X, N, 1).

%% @spec int_ceil(F::float()) -> integer()
%% @doc  Return the ceiling of F as an integer. The ceiling is defined as
%%       F when F == trunc(F);
%%       trunc(F) when F &lt; 0;
%%       trunc(F) + 1 when F &gt; 0.
int_ceil(X) ->
    T = trunc(X),
    case (X - T) of
        Pos when Pos > 0 -> T + 1;
        _ -> T
    end.


%% Internal API

int_pow(X, N, R) when N < 2 ->
    R * X;
int_pow(X, N, R) ->
    int_pow(X * X, N bsr 1, case N band 1 of 1 -> R * X; 0 -> R end).

insert_decimal(0, S) ->
    "0." ++ S;
insert_decimal(Place, S) when Place > 0 ->
    L = length(S),
    case Place - L of
         0 ->
            S ++ ".0";
        N when N < 0 ->
            {S0, S1} = lists:split(L + N, S),
            S0 ++ "." ++ S1;
        N when N < 6 ->
            %% More places than digits
            S ++ lists:duplicate(N, $0) ++ ".0";
        _ ->
            insert_decimal_exp(Place, S)
    end;
insert_decimal(Place, S) when Place > -6 ->
    "0." ++ lists:duplicate(abs(Place), $0) ++ S;
insert_decimal(Place, S) ->
    insert_decimal_exp(Place, S).

insert_decimal_exp(Place, S) ->
    [C | S0] = S,
    S1 = case S0 of
             [] ->
                 "0";
             _ ->
                 S0
         end,
    Exp = case Place < 0 of
              true ->
                  "e-";
              false ->
                  "e+"
          end,
    [C] ++ "." ++ S1 ++ Exp ++ integer_to_list(abs(Place - 1)).


digits1(Float, Exp, Frac) ->
    Round = ((Frac band 1) =:= 0),
    case Exp >= 0 of
        true ->
            BExp = 1 bsl Exp,
            case (Frac =/= ?BIG_POW) of
                true ->
                    scale((Frac * BExp * 2), 2, BExp, BExp,
                          Round, Round, Float);
                false ->
                    scale((Frac * BExp * 4), 4, (BExp * 2), BExp,
                          Round, Round, Float)
            end;
        false ->
            case (Exp =:= ?MIN_EXP) orelse (Frac =/= ?BIG_POW) of
                true ->
                    scale((Frac * 2), 1 bsl (1 - Exp), 1, 1,
                          Round, Round, Float);
                false ->
                    scale((Frac * 4), 1 bsl (2 - Exp), 2, 1,
                          Round, Round, Float)
            end
    end.

scale(R, S, MPlus, MMinus, LowOk, HighOk, Float) ->
    Est = int_ceil(math:log10(abs(Float)) - 1.0e-10),
    %% Note that the scheme implementation uses a 326 element look-up table
    %% for int_pow(10, N) where we do not.
    case Est >= 0 of
        true ->
            fixup(R, S * int_pow(10, Est), MPlus, MMinus, Est,
                  LowOk, HighOk);
        false ->
            Scale = int_pow(10, -Est),
            fixup(R * Scale, S, MPlus * Scale, MMinus * Scale, Est,
                  LowOk, HighOk)
    end.

fixup(R, S, MPlus, MMinus, K, LowOk, HighOk) ->
    TooLow = case HighOk of
                 true ->
                     (R + MPlus) >= S;
                 false ->
                     (R + MPlus) > S
             end,
    case TooLow of
        true ->
            [(K + 1) | generate(R, S, MPlus, MMinus, LowOk, HighOk)];
        false ->
            [K | generate(R * 10, S, MPlus * 10, MMinus * 10, LowOk, HighOk)]
    end.

generate(R0, S, MPlus, MMinus, LowOk, HighOk) ->
    D = R0 div S,
    R = R0 rem S,
    TC1 = case LowOk of
              true ->
                  R =< MMinus;
              false ->
                  R < MMinus
          end,
    TC2 = case HighOk of
              true ->
                  (R + MPlus) >= S;
              false ->
                  (R + MPlus) > S
          end,
    case TC1 of
        false ->
            case TC2 of
                false ->
                    [D | generate(R * 10, S, MPlus * 10, MMinus * 10,
                                  LowOk, HighOk)];
                true ->
                    [D + 1]
            end;
        true ->
            case TC2 of
                false ->
                    [D];
                true ->
                    case R * 2 < S of
                        true ->
                            [D];
                        false ->
                            [D + 1]
                    end
            end
    end.

unpack(Float) ->
    <<Sign:1, Exp:11, Frac:52>> = <<Float:64/float>>,
    {Sign, Exp, Frac}.

frexp1({_Sign, 0, 0}) ->
    {0.0, 0};
frexp1({Sign, 0, Frac}) ->
    Exp = log2floor(Frac),
    <<Frac1:64/float>> = <<Sign:1, ?FLOAT_BIAS:11, (Frac-1):52>>,
    {Frac1, -(?FLOAT_BIAS) - 52 + Exp};
frexp1({Sign, Exp, Frac}) ->
    <<Frac1:64/float>> = <<Sign:1, ?FLOAT_BIAS:11, Frac:52>>,
    {Frac1, Exp - ?FLOAT_BIAS}.

log2floor(Int) ->
    log2floor(Int, 0).

log2floor(0, N) ->
    N;
log2floor(Int, N) ->
    log2floor(Int bsr 1, 1 + N).


transform_digits(Place, [0 | Rest]) ->
    transform_digits(Place, Rest);
transform_digits(Place, Digits) ->
    {Place, [$0 + D || D <- Digits]}.


frexp_int(F) ->
    case unpack(F) of
        {_Sign, 0, Frac} ->
            {Frac, ?MIN_EXP};
        {_Sign, Exp, Frac} ->
            {Frac + (1 bsl 52), Exp - 53 - ?FLOAT_BIAS}
    end.

%%
%% Tests
%%
-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").

int_ceil_test() ->
    ?assertEqual(1, int_ceil(0.0001)),
    ?assertEqual(0, int_ceil(0.0)),
    ?assertEqual(1, int_ceil(0.99)),
    ?assertEqual(1, int_ceil(1.0)),
    ?assertEqual(-1, int_ceil(-1.5)),
    ?assertEqual(-2, int_ceil(-2.0)),
    ok.

int_pow_test() ->
    ?assertEqual(1, int_pow(1, 1)),
    ?assertEqual(1, int_pow(1, 0)),
    ?assertEqual(1, int_pow(10, 0)),
    ?assertEqual(10, int_pow(10, 1)),
    ?assertEqual(100, int_pow(10, 2)),
    ?assertEqual(1000, int_pow(10, 3)),
    ok.

digits_test() ->
    ?assertEqual("0",
                 digits(0)),
    ?assertEqual("0.0",
                 digits(0.0)),
    ?assertEqual("1.0",
                 digits(1.0)),
    ?assertEqual("-1.0",
                 digits(-1.0)),
    ?assertEqual("0.1",
                 digits(0.1)),
    ?assertEqual("0.01",
                 digits(0.01)),
    ?assertEqual("0.001",
                 digits(0.001)),
    ?assertEqual("1.0e+6",
                 digits(1000000.0)),
    ?assertEqual("0.5",
                 digits(0.5)),
    ?assertEqual("4503599627370496.0",
                 digits(4503599627370496.0)),
    %% small denormalized number
    %% 4.94065645841246544177e-324 =:= 5.0e-324
    <<SmallDenorm/float>> = <<0,0,0,0,0,0,0,1>>,
    ?assertEqual("5.0e-324",
                 digits(SmallDenorm)),
    ?assertEqual(SmallDenorm,
                 list_to_float(digits(SmallDenorm))),
    %% large denormalized number
    %% 2.22507385850720088902e-308
    <<BigDenorm/float>> = <<0,15,255,255,255,255,255,255>>,
    ?assertEqual("2.225073858507201e-308",
                 digits(BigDenorm)),
    ?assertEqual(BigDenorm,
                 list_to_float(digits(BigDenorm))),
    %% small normalized number
    %% 2.22507385850720138309e-308
    <<SmallNorm/float>> = <<0,16,0,0,0,0,0,0>>,
    ?assertEqual("2.2250738585072014e-308",
                 digits(SmallNorm)),
    ?assertEqual(SmallNorm,
                 list_to_float(digits(SmallNorm))),
    %% large normalized number
    %% 1.79769313486231570815e+308
    <<LargeNorm/float>> = <<127,239,255,255,255,255,255,255>>,
    ?assertEqual("1.7976931348623157e+308",
                 digits(LargeNorm)),
    ?assertEqual(LargeNorm,
                 list_to_float(digits(LargeNorm))),
    %% issue #10 - mochinum:frexp(math:pow(2, -1074)).
    ?assertEqual("5.0e-324",
                 digits(math:pow(2, -1074))),
    ok.

frexp_test() ->
    %% zero
    ?assertEqual({0.0, 0}, frexp(0.0)),
    %% one
    ?assertEqual({0.5, 1}, frexp(1.0)),
    %% negative one
    ?assertEqual({-0.5, 1}, frexp(-1.0)),
    %% small denormalized number
    %% 4.94065645841246544177e-324
    <<SmallDenorm/float>> = <<0,0,0,0,0,0,0,1>>,
    ?assertEqual({0.5, -1073}, frexp(SmallDenorm)),
    %% large denormalized number
    %% 2.22507385850720088902e-308
    <<BigDenorm/float>> = <<0,15,255,255,255,255,255,255>>,
    ?assertEqual(
       {0.99999999999999978, -1022},
       frexp(BigDenorm)),
    %% small normalized number
    %% 2.22507385850720138309e-308
    <<SmallNorm/float>> = <<0,16,0,0,0,0,0,0>>,
    ?assertEqual({0.5, -1021}, frexp(SmallNorm)),
    %% large normalized number
    %% 1.79769313486231570815e+308
    <<LargeNorm/float>> = <<127,239,255,255,255,255,255,255>>,
    ?assertEqual(
        {0.99999999999999989, 1024},
        frexp(LargeNorm)),
    %% issue #10 - mochinum:frexp(math:pow(2, -1074)).
    ?assertEqual(
       {0.5, -1073},
       frexp(math:pow(2, -1074))),
    ok.

-endif.
