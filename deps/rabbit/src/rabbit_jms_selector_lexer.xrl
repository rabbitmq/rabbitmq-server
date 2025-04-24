%%% This is the definitions file for JMS message selectors:
%%% https://jakarta.ee/specifications/messaging/3.1/jakarta-messaging-spec-3.1#message-selector
%%%
%%% To manually generate the scanner file rabbit_jms_selector_lexer.erl run:
%%% leex:file("rabbit_jms_selector_lexer.xrl", [deterministic]).

Definitions.
WHITESPACE  = [\s\t\n\r]
DIGIT       = [0-9]
INT         = {DIGIT}+
FLOAT       = {DIGIT}+\.{DIGIT}+([eE][\+\-]?{INT})?
EXPONENT    = [0-9]+[eE][\+\-]?[0-9]+
IDENTIFIER  = [a-zA-Z_$][a-zA-Z0-9_$]*
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
{FLOAT}       : {token, {float, TokenLine, list_to_float(TokenChars)}}.
{EXPONENT}    : {token, {float, TokenLine, list_to_float(TokenChars)}}.
{STRING}      : {token, {string, TokenLine, process_string(TokenChars)}}.
{IDENTIFIER}  : {token, {identifier, TokenLine, list_to_binary(TokenChars)}}.

% Catch any other characters as errors
.             : {error, {illegal_character, TokenChars}}.

Erlang code.

process_string(Chars) ->
    %% remove surrounding quotes
    Chars1 = lists:sublist(Chars, 2, length(Chars) - 2),
    Bin = unicode:characters_to_binary(Chars1),
    process_escaped_quotes(Bin).

process_escaped_quotes(Binary) ->
    binary:replace(Binary, <<"''">>, <<"'">>, [global]).
