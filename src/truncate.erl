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
%% Copyright (c) 2007-2014 GoPivotal, Inc.  All rights reserved.
%%

-module(truncate).

-define(ELLIPSIS_LENGTH, 3).

-record(params, {content, struct, content_dec, struct_dec}).

-export([log_event/2, term/2]).
%% exported for testing
-export([test/0]).

log_event({Type, GL, {Pid, Format, Args}}, Params)
  when Type =:= error orelse
       Type =:= info_msg orelse
       Type =:= warning_msg ->
    {Type, GL, {Pid, Format, [term(T, Params) || T <- Args]}};
log_event({Type, GL, {Pid, ReportType, Report}}, Params)
  when Type =:= error_report orelse
       Type =:= info_report orelse
       Type =:= warning_report ->
    {Type, GL, {Pid, ReportType, report(Report, Params)}};
log_event(Event, _Params) ->
    Event.

report([[Thing]], Params) -> report([Thing], Params);
report(List, Params)      -> [case Item of
                                      {K, V} -> {K, term(V, Params)};
                                      _      -> term(Item, Params)
                                  end || Item <- List].

term(Thing, {Content, Struct, ContentDec, StructDec}) ->
    term(Thing, #params{content     = Content,
                        struct     = Struct,
                        content_dec = ContentDec,
                        struct_dec = StructDec});

term(Bin, #params{content = N}) when (is_binary(Bin) orelse is_bitstring(Bin))
                                     andalso size(Bin) > N - ?ELLIPSIS_LENGTH ->
    Suffix = without_ellipsis(N),
    <<Head:Suffix/binary, _/bitstring>> = Bin,
    <<Head/binary, <<"...">>/binary>>;
term(L, #params{struct = N} = Params) when is_list(L) ->
    case io_lib:printable_list(L) of
        true  -> N2 = without_ellipsis(N),
                 case length(L) > N2 of
                     true  -> string:left(L, N2) ++ "...";
                     false -> L
                 end;
        false -> shrink_list(L, Params)
    end;
term(T, Params) when is_tuple(T) ->
    list_to_tuple(shrink_list(tuple_to_list(T), Params));
term(T, _) ->
    T.

without_ellipsis(N) -> erlang:max(N - ?ELLIPSIS_LENGTH, 0).

shrink_list(_, #params{struct = N}) when N =< 0 ->
    ['...'];
shrink_list([], _) ->
    [];
shrink_list([H|T], #params{content     = Content,
                           struct      = Struct,
                           content_dec = ContentDec,
                           struct_dec  = StructDec} = Params) ->
    [term(H, Params#params{content = Content - ContentDec,
                           struct  = Struct  - StructDec})
     | term(T, Params#params{struct = Struct - 1})].

%%----------------------------------------------------------------------------

test() ->
    test_short_examples_exactly(),
    test_large_examples_for_size(),
    ok.

test_short_examples_exactly() ->
    F = fun (Term, Exp) -> Exp = term(Term, {10, 10, 5, 5}) end,
    F([], []),
    F("h", "h"),
    F("hello world", "hello w..."),
    F([[h,e,l,l,o,' ',w,o,r,l,d]], [[h,e,l,l,o,'...']]),
    F([a|b], [a|b]),
    F(<<"hello">>, <<"hello">>),
    F([<<"hello world">>], [<<"he...">>]),
    F(<<1:1>>, <<1:1>>),
    F(<<1:81>>, <<0:56, "...">>),
    F({{{{a}}},{b},c,d,e,f,g,h,i,j,k}, {{{'...'}},{b},c,d,e,f,g,h,i,j,'...'}),
    P = spawn(fun() -> receive die -> ok end end),
    F([0, 0.0, <<1:1>>, F, P], [0, 0.0, <<1:1>>, F, P]),
    P ! die,
    ok.

test_large_examples_for_size() ->
    %% Real world values
    Shrink = fun(Term) -> term(Term, {1000, 100, 50, 5}) end,
    TestSize = fun(Term) ->
                       true = 5000000 < size(term_to_binary(Term)),
                       true = 500000 > size(term_to_binary(Shrink(Term)))
               end,
    TestSize(lists:seq(1, 5000000)),
    TestSize(recursive_list(1000, 10)),
    TestSize(recursive_list(5000, 20)),
    TestSize(gb_sets:from_list([I || I <- lists:seq(1, 1000000)])),
    TestSize(gb_trees:from_orddict([{I, I} || I <- lists:seq(1, 1000000)])),
    ok.

recursive_list(S, 0) -> lists:seq(1, S);
recursive_list(S, N) -> [recursive_list(S div N, N-1) || _ <- lists:seq(1, S)].
