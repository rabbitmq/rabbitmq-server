%%% This is the definitions file for JMS message selectors:
%%% https://jakarta.ee/specifications/messaging/3.1/jakarta-messaging-spec-3.1#message-selector
%%%
%%% To manually generate the scanner file rabbit_jms_selector_lexer.erl run:
%%% leex:file("rabbit_jms_selector_lexer.xrl", [deterministic]).

Definitions.
WHITESPACE  = [\s\t\f\n\r]
DIGIT       = [0-9]
INT         = {DIGIT}+
% Approximate numeric literal with a decimal
FLOAT       = ({DIGIT}+\.{DIGIT}*|\.{DIGIT}+)([eE][\+\-]?{INT})?
% Approximate numeric literal in scientific notation without a decimal
EXPONENT    = {DIGIT}+[eE][\+\-]?{DIGIT}+
% We extend the allowed JMS identifier syntax with '.' and '-' even though
% these two characters return false for Character.isJavaIdentifierPart()
% to allow identifiers such as properties.group-id
IDENTIFIER  = [a-zA-Z_$][a-zA-Z0-9_$.\-]*
STRING      = '([^']|'')*'

Rules.
{WHITESPACE}+  : skip_token.

% Logical operators (case insensitive)
[aA][nN][dD]   : {token, {'AND', TokenLine}}.
[oO][rR]       : {token, {'OR', TokenLine}}.
[nN][oO][tT]   : {token, {'NOT', TokenLine}}.

% Special operators (case insensitive)
[bB][eE][tT][wW][eE][eE][nN] : {token, {'BETWEEN', TokenLine}}.
[lL][iI][kK][eE]             : {token, {'LIKE', TokenLine}}.
[iI][nN]                     : {token, {'IN', TokenLine}}.
[iI][sS]                     : {token, {'IS', TokenLine}}.
[nN][uU][lL][lL]             : {token, {'NULL', TokenLine}}.
[eE][sS][cC][aA][pP][eE]     : {token, {'ESCAPE', TokenLine}}.

% Boolean literals (case insensitive)
[tT][rR][uU][eE]             : {token, {boolean, TokenLine, true}}.
[fF][aA][lL][sS][eE]         : {token, {boolean, TokenLine, false}}.

% Comparison operators
=             : {token, {'=', TokenLine}}.
<>            : {token, {'<>', TokenLine}}.
>=            : {token, {'>=', TokenLine}}.
<=            : {token, {'<=', TokenLine}}.
>             : {token, {'>', TokenLine}}.
<             : {token, {'<', TokenLine}}.

% Arithmetic operators
\+            : {token, {'+', TokenLine}}.
-             : {token, {'-', TokenLine}}.
\*            : {token, {'*', TokenLine}}.
/             : {token, {'/', TokenLine}}.

% Parentheses and comma
\(            : {token, {'(', TokenLine}}.
\)            : {token, {')', TokenLine}}.
,             : {token, {',', TokenLine}}.

% Literals
{INT}         : {token, {integer, TokenLine, list_to_integer(TokenChars)}}.
{FLOAT}       : {token, {float, TokenLine, list_to_float(to_float(TokenChars))}}.
{EXPONENT}    : {token, {float, TokenLine, parse_scientific_notation(TokenChars)}}.
{STRING}      : {token, {string, TokenLine, process_string(TokenChars)}}.
{IDENTIFIER}  : {token, {identifier, TokenLine, unicode:characters_to_binary(TokenChars)}}.

% Catch any other characters as errors
.             : {error, {illegal_character, TokenChars}}.

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
            Chars1 = string:lowercase(Chars),
            Chars2 = string:replace(Chars1, ".e", ".0e"),
            lists:flatten(Chars2)
    end.

parse_scientific_notation(Chars) ->
    Str = string:lowercase(Chars),
    {Before, After0} = lists:splitwith(fun(C) -> C =/= $e end, Str),
    [$e | After] = After0,
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
