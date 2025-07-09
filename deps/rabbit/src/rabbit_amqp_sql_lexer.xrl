%%% This is the definitions file for SQL Filter Expressions:
%%% https://docs.oasis-open.org/amqp/filtex/v1.0/csd01/filtex-v1.0-csd01.html#_Toc67929276
%%%
%%% To manually generate the scanner file rabbit_amqp_sql_lexer.erl run:
%%% leex:file("rabbit_amqp_sql_lexer.xrl", [deterministic]).

Definitions.
WHITESPACE   = [\s\t\f\n\r]
DIGIT        = [0-9]
HEXDIGIT     = [0-9A-F]
INT          = {DIGIT}+
% decimal constant or approximate number constant
FLOAT        = {DIGIT}+\.{DIGIT}+(E[\+\-]?{INT})?
STRING       = '([^']|'')*'|"([^"]|"")*"
BINARY       = 0x({HEXDIGIT}{HEXDIGIT})+
REGULAR_ID   = [a-zA-Z][a-zA-Z0-9_]*
SECTION_ID   = [a-zA-Z][a-zA-Z_-]*\.{REGULAR_ID}
DELIMITED_ID = \[([^\[\]]|\[\[|\]\])*\]

Rules.
{WHITESPACE}+  : skip_token.

% Logical operators
AND    : {token, {'AND', TokenLine}}.
OR     : {token, {'OR', TokenLine}}.
NOT    : {token, {'NOT', TokenLine}}.

% Special operators
LIKE      : {token, {'LIKE', TokenLine}}.
IN        : {token, {'IN', TokenLine}}.
IS        : {token, {'IS', TokenLine}}.
NULL      : {token, {'NULL', TokenLine}}.
ESCAPE    : {token, {'ESCAPE', TokenLine}}.

% Boolean literals
TRUE     : {token, {boolean, TokenLine, true}}.
FALSE    : {token, {boolean, TokenLine, false}}.

% Functions
UTC      : {token, {'UTC', TokenLine}}.

% Comparison operators
% "The ‘<>’ operator is synonymous to the ‘!=’ operator."
<>    : {token, {'<>', TokenLine}}.
!=    : {token, {'<>', TokenLine}}.
=     : {token, {'=', TokenLine}}.
>=    : {token, {'>=', TokenLine}}.
<=    : {token, {'<=', TokenLine}}.
>     : {token, {'>', TokenLine}}.
<     : {token, {'<', TokenLine}}.

% Arithmetic operators
\+    : {token, {'+', TokenLine}}.
-     : {token, {'-', TokenLine}}.
\*    : {token, {'*', TokenLine}}.
/     : {token, {'/', TokenLine}}.
\%    : {token, {'%', TokenLine}}.

% Parentheses and comma
\(    : {token, {'(', TokenLine}}.
\)    : {token, {')', TokenLine}}.
,     : {token, {',', TokenLine}}.

% Literals
{INT}           : {token, {integer, TokenLine, list_to_integer(TokenChars)}}.
{FLOAT}         : {token, {float, TokenLine, list_to_float(TokenChars)}}.
{STRING}        : {token, {string, TokenLine, process_string(TokenChars)}}.
{BINARY}        : {token, {binary, TokenLine, parse_binary(TokenChars)}}.
{SECTION_ID}    : process_section_identifier(TokenChars, TokenLine).
{DELIMITED_ID}  : process_delimited_identifier(TokenChars, TokenLine).
{REGULAR_ID}    : process_regular_identifier(TokenChars, TokenLine).

% Catch any other characters as errors
.    : {error, {illegal_character, TokenChars}}.

Erlang code.

-define(KEYWORDS, [<<"and">>, <<"or">>, <<"not">>,
                   <<"like">>, <<"in">>, <<"is">>, <<"null">>, <<"escape">>,
                   <<"true">>, <<"false">>,
                   <<"exists">>,
                   <<"lower">>, <<"upper">>, <<"left">>, <<"right">>,
                   <<"substring">>, <<"utc">>, <<"date">>]).

parse_binary([$0, $x | HexChars]) ->
    parse_hex_pairs(HexChars, <<>>).

parse_hex_pairs([], Acc) ->
    Acc;
parse_hex_pairs([H1, H2 | Rest], Acc) ->
    Byte = list_to_integer([H1, H2], 16),
    parse_hex_pairs(Rest, <<Acc/binary, Byte>>).

process_string(Chars) ->
    %% remove surrounding quotes
    [Quote | Chars1] = Chars,
    Chars2 = lists:droplast(Chars1),
    Bin = unicode:characters_to_binary(Chars2),
    %% process escaped quotes
    binary:replace(Bin, <<Quote, Quote>>, <<Quote>>, [global]).

process_section_identifier(Chars, TokenLine) ->
    Bin = unicode:characters_to_binary(Chars),
    case rabbit_amqp_util:section_field_name_to_atom(Bin) of
        error ->
            {error, {unsupported_field_name, Chars}};
        Id ->
            {token, {identifier, TokenLine, Id}}
    end.

process_regular_identifier(Chars, TokenLine) ->
    Bin = unicode:characters_to_binary(Chars),
    case lists:member(string:lowercase(Bin), ?KEYWORDS) of
        true ->
            {error, {unsupported_identifier, Chars}};
        false ->
            {token, {identifier, TokenLine, Bin}}
    end.

process_delimited_identifier(Chars, TokenLine) ->
    %% remove surrounding brackets
    Chars1 = lists:droplast(tl(Chars)),
    case lists:any(fun rabbit_amqp_filter_sql:is_control_char/1, Chars1) of
        true ->
            {error, {illegal_control_character_in_identifier, Chars}};
        false ->
            Bin = unicode:characters_to_binary(Chars1),
            %% process escaped brackets
            Bin1 = binary:replace(Bin, <<"[[">>, <<"[">>, [global]),
            Bin2 = binary:replace(Bin1, <<"]]">>, <<"]">>, [global]),
            {token, {identifier, TokenLine, Bin2}}
    end.
