%%% This is the definitions file for SQL Filter Expressions:
%%% https://docs.oasis-open.org/amqp/filtex/v1.0/csd01/filtex-v1.0-csd01.html#_Toc67929276
%%%
%%% To manually generate the scanner file rabbit_amqp_sql_lexer.erl run:
%%% leex:file("rabbit_amqp_sql_lexer.xrl", [deterministic]).

Definitions.
WHITESPACE  = [\s\t\f\n\r]
DIGIT       = [0-9]
HEXDIGIT    = [0-9A-F]
INT         = {DIGIT}+
% Approximate numeric literal with a decimal
FLOAT       = ({DIGIT}+\.{DIGIT}*|\.{DIGIT}+)(E[\+\-]?{INT})?
% Approximate numeric literal in scientific notation without a decimal
EXPONENT    = {DIGIT}+E[\+\-]?{DIGIT}+
% We extend the allowed JMS identifier syntax with '.' and '-' even though
% these two characters return false for Character.isJavaIdentifierPart()
% to allow identifiers such as properties.group-id
IDENTIFIER  = [a-zA-Z_$][a-zA-Z0-9_$.\-]*
STRING      = '([^']|'')*'
BINARY      = 0x({HEXDIGIT}{HEXDIGIT})+

Rules.
{WHITESPACE}+  : skip_token.

% Logical operators (case insensitive)
AND    : {token, {'AND', TokenLine}}.
OR     : {token, {'OR', TokenLine}}.
NOT    : {token, {'NOT', TokenLine}}.

% Special operators (case insensitive)
LIKE      : {token, {'LIKE', TokenLine}}.
IN        : {token, {'IN', TokenLine}}.
IS        : {token, {'IS', TokenLine}}.
NULL      : {token, {'NULL', TokenLine}}.
ESCAPE    : {token, {'ESCAPE', TokenLine}}.

% Boolean literals (case insensitive)
TRUE     : {token, {boolean, TokenLine, true}}.
FALSE    : {token, {boolean, TokenLine, false}}.

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
{FLOAT}         : {token, {float, TokenLine, list_to_float(to_float(TokenChars))}}.
{EXPONENT}      : {token, {float, TokenLine, parse_scientific_notation(TokenChars)}}.
{STRING}        : {token, {string, TokenLine, process_string(TokenChars)}}.
{IDENTIFIER}    : {token, {identifier, TokenLine, unicode:characters_to_binary(TokenChars)}}.
{BINARY}        : {token, {binary, TokenLine, parse_binary(TokenChars)}}.

% Catch any other characters as errors
.    : {error, {illegal_character, TokenChars}}.

Erlang code.

%% "Approximate literals use the Java floating-point literal syntax."
to_float([$. | _] = Chars) ->
    %% . Digits [ExponentPart]
    "0" ++ Chars;
to_float(Chars) ->
    %% Digits . [Digits] [ExponentPart]
    case lists:last(Chars) of
        $. ->
            Chars ++ "0";
        _ ->
            Chars1 = string:replace(Chars, ".E", ".0E"),
            lists:flatten(Chars1)
    end.

parse_scientific_notation(Chars) ->
    {Before, After0} = lists:splitwith(fun(C) -> C =/= $E end, Chars),
    [$E | After] = After0,
    Base = list_to_integer(Before),
    Exp = list_to_integer(After),
    Base * math:pow(10, Exp).

process_string(Chars) ->
    %% remove surrounding quotes
    Chars1 = lists:sublist(Chars, 2, length(Chars) - 2),
    Bin = unicode:characters_to_binary(Chars1),
    process_escaped_quotes(Bin).

process_escaped_quotes(Binary) ->
    binary:replace(Binary, <<"''">>, <<"'">>, [global]).

parse_binary([$0, $x | HexChars]) ->
    parse_hex_pairs(HexChars, <<>>).

parse_hex_pairs([], Acc) ->
    Acc;
parse_hex_pairs([H1, H2 | Rest], Acc) ->
    Byte = list_to_integer([H1, H2], 16),
    parse_hex_pairs(Rest, <<Acc/binary, Byte>>).
