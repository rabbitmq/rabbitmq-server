%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2026 Broadcom. All Rights Reserved. The term "Broadcom" refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
%% -----------------------------------------------------------------------------

%% Erlang term parser for JMS topic exchange selectors.
%%
%% Only the atoms recognised by this parser are accepted.

%% -----------------------------------------------------------------------------
-module(sjx_parser).

-export([parse_term/1]).

-define(MAX_NESTING_DEPTH, 16).

-define(ALLOWED_ATOMS, #{
    %% Expression tags
    ident       => true,
    'not'       => true,
    'and'       => true,
    'or'        => true,
    is_null     => true,
    not_null    => true,
    like        => true,
    not_like    => true,
    between     => true,
    not_between => true,
    range       => true,
    %% Comparison operators
    '='         => true,
    '<>'        => true,
    '>'         => true,
    '<'         => true,
    '>='        => true,
    '<='        => true,
    %% Set operators
    in          => true,
    not_in      => true,
    %% Arithmetic operators
    '+'         => true,
    '-'         => true,
    '*'         => true,
    '/'         => true,
    %% Boolean literals
    true        => true,
    false       => true,
    %% LIKE escape specifiers
    no_escape   => true,
    regex       => true
}).

-spec parse_term(string()) -> {ok, term()} | {error, term()}.
parse_term(Str) when is_list(Str) ->
    try
        {Term, Rest} = do_parse_term(skip_ws(Str), 0),
        case skip_ws(Rest) of
            [$. | Tail] ->
                case skip_ws(Tail) of
                    [] -> {ok, Term};
                    _  -> {error, trailing_characters}
                end;
            [] ->
                {ok, Term};
            _ ->
                {error, trailing_characters}
        end
    catch
        throw:{parse_error, Reason} -> {error, Reason};
        error:badarg                -> {error, invalid_term}
    end.

%% ---------------------------------------------------------------------------
%% Terms
%% ---------------------------------------------------------------------------

do_parse_term(_, Depth) when Depth > ?MAX_NESTING_DEPTH ->
    throw({parse_error, max_depth_exceeded});
do_parse_term([${ | Rest], Depth) ->
    parse_tuple(skip_ws(Rest), [], Depth);
do_parse_term([$[ | Rest], Depth) ->
    parse_list(skip_ws(Rest), Depth);
do_parse_term([$<, $< | Rest], _Depth) ->
    parse_binary(skip_ws(Rest));
do_parse_term([$" | Rest], _Depth) ->
    parse_string_chars(Rest, []);
do_parse_term([$' | Rest], _Depth) ->
    parse_quoted_atom(Rest, []);
do_parse_term([$- | Rest0], _Depth) ->
    Rest = skip_ws(Rest0),
    case Rest of
        [C | _] when C >= $0, C =< $9 ->
            {Num, Rest1} = parse_number(Rest),
            {-Num, Rest1};
        _ ->
            throw({parse_error, unexpected_character})
    end;
do_parse_term([$+ | Rest0], _Depth) ->
    Rest = skip_ws(Rest0),
    case Rest of
        [C | _] when C >= $0, C =< $9 ->
            parse_number(Rest);
        _ ->
            throw({parse_error, unexpected_character})
    end;
do_parse_term([C | _] = Str, _Depth) when C >= $0, C =< $9 ->
    parse_number(Str);
do_parse_term([C | _] = Str, _Depth) when C >= $a, C =< $z ->
    parse_bare_atom(Str, []);
do_parse_term([], _Depth) ->
    throw({parse_error, unexpected_end});
do_parse_term(_, _Depth) ->
    throw({parse_error, unexpected_character}).

%% ---------------------------------------------------------------------------
%% Tuples: {Term, ...}
%% ---------------------------------------------------------------------------

parse_tuple([$} | Rest], Acc, _Depth) ->
    {list_to_tuple(lists:reverse(Acc)), Rest};
parse_tuple(Str, Acc, Depth) ->
    {Term, Rest} = do_parse_term(Str, Depth + 1),
    case skip_ws(Rest) of
        [$, | Rest1] ->
            parse_tuple(skip_ws(Rest1), [Term | Acc], Depth);
        [$} | Rest1] ->
            {list_to_tuple(lists:reverse([Term | Acc])), Rest1};
        _ ->
            throw({parse_error, expected_comma_or_closing_brace})
    end.

%% ---------------------------------------------------------------------------
%% Lists: [Term, ...] | []
%% ---------------------------------------------------------------------------

parse_list([$] | Rest], _Depth) ->
    {[], Rest};
parse_list(Str, Depth) ->
    parse_list_elems(Str, [], Depth).

parse_list_elems(Str, Acc, Depth) ->
    {Term, Rest} = do_parse_term(Str, Depth + 1),
    case skip_ws(Rest) of
        [$, | Rest1] ->
            parse_list_elems(skip_ws(Rest1), [Term | Acc], Depth);
        [$] | Rest1] ->
            {lists:reverse([Term | Acc]), Rest1};
        _ ->
            throw({parse_error, expected_comma_or_closing_bracket})
    end.

%% ---------------------------------------------------------------------------
%% Binaries: <<"...">> | <<>>
%% ---------------------------------------------------------------------------

parse_binary([$>, $> | Rest]) ->
    {<<>>, Rest};
parse_binary([$" | Rest]) ->
    {Chars, Rest1} = parse_string_chars(Rest, []),
    case skip_ws(Rest1) of
        [$>, $> | Rest2] ->
            {list_to_binary(Chars), Rest2};
        _ ->
            throw({parse_error, expected_closing_binary})
    end;
parse_binary(_) ->
    throw({parse_error, invalid_binary}).

%% ---------------------------------------------------------------------------
%% Strings: "..."
%% ---------------------------------------------------------------------------

parse_string_chars([$" | Rest], Acc) ->
    {lists:reverse(Acc), Rest};
parse_string_chars([$\\, C | Rest], Acc) ->
    parse_string_chars(Rest, [unescape_char(C) | Acc]);
parse_string_chars([C | Rest], Acc) ->
    parse_string_chars(Rest, [C | Acc]);
parse_string_chars([], _) ->
    throw({parse_error, unterminated_string}).

unescape_char($n)  -> $\n;
unescape_char($t)  -> $\t;
unescape_char($r)  -> $\r;
unescape_char($\\) -> $\\;
unescape_char($")  -> $";
unescape_char(C)   -> C.

%% ---------------------------------------------------------------------------
%% Quoted atoms: '...'
%% ---------------------------------------------------------------------------

parse_quoted_atom([$' | Rest], Acc) ->
    AtomStr = lists:reverse(Acc),
    {validate_atom(AtomStr), Rest};
parse_quoted_atom([$\\, C | Rest], Acc) ->
    parse_quoted_atom(Rest, [C | Acc]);
parse_quoted_atom([C | Rest], Acc) ->
    parse_quoted_atom(Rest, [C | Acc]);
parse_quoted_atom([], _) ->
    throw({parse_error, unterminated_quoted_atom}).

%% ---------------------------------------------------------------------------
%% Bare atoms: [a-z][a-zA-Z0-9_@]*
%% ---------------------------------------------------------------------------

parse_bare_atom([C | Rest], Acc)
  when (C >= $a andalso C =< $z) orelse
       (C >= $A andalso C =< $Z) orelse
       (C >= $0 andalso C =< $9) orelse
       C =:= $_ orelse C =:= $@ ->
    parse_bare_atom(Rest, [C | Acc]);
parse_bare_atom(Rest, Acc) ->
    AtomStr = lists:reverse(Acc),
    {validate_atom(AtomStr), Rest}.

%% ---------------------------------------------------------------------------
%% Numbers: integers and floats (with optional scientific notation)
%% ---------------------------------------------------------------------------

parse_number(Str) ->
    {IntDigits, Rest1} = scan_digits(Str),
    case Rest1 of
        [$., C | Rest2] when C >= $0, C =< $9 ->
            {FracDigits, Rest3} = scan_digits([C | Rest2]),
            FloatStr = IntDigits ++ "." ++ FracDigits,
            maybe_parse_exponent(FloatStr, Rest3);
        _ ->
            {list_to_integer(IntDigits), Rest1}
    end.

maybe_parse_exponent(FloatStr, [E | Rest]) when E =:= $e; E =:= $E ->
    {Sign, Rest1} = case Rest of
        [$+ | R] -> {"+", R};
        [$- | R] -> {"-", R};
        R        -> {"", R}
    end,
    {ExpDigits, Rest2} = scan_digits(Rest1),
    {list_to_float(FloatStr ++ [E] ++ Sign ++ ExpDigits), Rest2};
maybe_parse_exponent(FloatStr, Rest) ->
    {list_to_float(FloatStr), Rest}.

scan_digits(Str) ->
    scan_digits(Str, []).

scan_digits([C | Rest], Acc) when C >= $0, C =< $9 ->
    scan_digits(Rest, [C | Acc]);
scan_digits(Rest, Acc) ->
    {lists:reverse(Acc), Rest}.

%% ---------------------------------------------------------------------------
%% Atom validation
%% ---------------------------------------------------------------------------

validate_atom(AtomStr) ->
    try list_to_existing_atom(AtomStr) of
        Atom ->
            case maps:is_key(Atom, ?ALLOWED_ATOMS) of
                true  -> Atom;
                false -> throw({parse_error, {disallowed_atom, AtomStr}})
            end
    catch
        error:badarg ->
            throw({parse_error, {unknown_atom, AtomStr}})
    end.

%% ---------------------------------------------------------------------------
%% Whitespace
%% ---------------------------------------------------------------------------

skip_ws([$\s | Rest]) -> skip_ws(Rest);
skip_ws([$\t | Rest]) -> skip_ws(Rest);
skip_ws([$\n | Rest]) -> skip_ws(Rest);
skip_ws([$\r | Rest]) -> skip_ws(Rest);
skip_ws(Str)          -> Str.
