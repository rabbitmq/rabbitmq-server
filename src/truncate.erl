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
%% Copyright (c) 2007-2017 Pivotal Software, Inc.  All rights reserved.
%%

-module(truncate).

-define(ELLIPSIS_LENGTH, 3).

-record(params, {content, struct, content_dec, struct_dec}).

-export([log_event/2, term/2]).

-ifdef(TEST).
-export([term_size/3]).
-endif.

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

report([[Thing]], Params)               -> report([Thing], Params);
report(List, Params) when is_list(List) -> [case Item of
                                                {K, V} -> {K, term(V, Params)};
                                                _      -> term(Item, Params)
                                            end || Item <- List];
report(Other, Params)                   -> term(Other, Params).

term(Thing, {Max, {Content, Struct, ContentDec, StructDec}}) ->
    case exceeds_size(Thing, Max) of
        true  -> term(Thing, true, #params{content     = Content,
                                           struct      = Struct,
                                           content_dec = ContentDec,
                                           struct_dec  = StructDec});
        false -> Thing
    end.

term(Bin, _AllowPrintable, #params{content = N})
  when (is_binary(Bin) orelse is_bitstring(Bin))
       andalso size(Bin) > N - ?ELLIPSIS_LENGTH ->
    Suffix = without_ellipsis(N),
    <<Head:Suffix/binary, _/bitstring>> = Bin,
    <<Head/binary, <<"...">>/binary>>;
term(L, AllowPrintable, #params{struct = N} = Params) when is_list(L) ->
    case AllowPrintable andalso io_lib:printable_list(L) of
        true  -> N2 = without_ellipsis(N),
                 case length(L) > N2 of
                     true  -> string:left(L, N2) ++ "...";
                     false -> L
                 end;
        false -> shrink_list(L, Params)
    end;
term(T, _AllowPrintable, Params) when is_tuple(T) ->
    list_to_tuple(shrink_list(tuple_to_list(T), Params));
term(T, _, _) ->
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
    [term(H, true, Params#params{content = Content - ContentDec,
                                 struct  = Struct  - StructDec})
     | term(T, false, Params#params{struct = Struct - 1})].

%%----------------------------------------------------------------------------

%% We don't use erts_debug:flat_size/1 because that ignores binary
%% sizes. This is all going to be rather approximate though, these
%% sizes are probably not very "fair" but we are just trying to see if
%% we reach a fairly arbitrary limit anyway though.
exceeds_size(Thing, Max) ->
    case term_size(Thing, Max, erlang:system_info(wordsize)) of
        limit_exceeded -> true;
        _              -> false
    end.

term_size(B, M, _W) when is_bitstring(B) -> lim(M, size(B));
term_size(A, M, W) when is_atom(A)       -> lim(M, 2 * W);
term_size(N, M, W) when is_number(N)     -> lim(M, 2 * W);
term_size(T, M, W) when is_tuple(T)      -> tuple_term_size(
                                              T, M, 1, tuple_size(T), W);
term_size([], M, _W) ->
    M;
term_size([H|T], M, W) ->
    case term_size(H, M, W) of
        limit_exceeded -> limit_exceeded;
        M2             -> lim(term_size(T, M2, W), 2 * W)
    end;
term_size(X, M, W) ->
    lim(M, erts_debug:flat_size(X) * W).

lim(S, T) when is_number(S) andalso S > T -> S - T;
lim(_, _)                                 -> limit_exceeded.

tuple_term_size(_T, limit_exceeded, _I, _S, _W) ->
    limit_exceeded;
tuple_term_size(_T, M, I, S, _W) when I > S ->
    M;
tuple_term_size(T, M, I, S, W) ->
    tuple_term_size(T, lim(term_size(element(I, T), M, W), 2 * W), I + 1, S, W).
