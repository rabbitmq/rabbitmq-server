%%% This is the grammar file for JMS message selectors:
%%% https://jakarta.ee/specifications/messaging/3.1/jakarta-messaging-spec-3.1#message-selector
%%%
%%% To manually generate the parser file rabbit_jms_selector_parser.erl run:
%%% yecc:file("rabbit_jms_selector_parser.yrl", [deterministic]).

Nonterminals
    selector
    conditional_expr
    comparison_expr
    logical_expr
    additive_expr
    multiplicative_expr
    unary_expr
    primary
    literal
    identifier_expr
    string_list
    string_item
    between_expr
    in_expr
    like_expr
    is_null_expr.

Terminals
    integer float boolean string identifier
    '=' '<>' '>' '<' '>=' '<='
    '+' '-' '*' '/'
    'AND' 'OR' 'NOT'
    'BETWEEN' 'LIKE' 'IN' 'IS' 'NULL' 'ESCAPE'
    '(' ')' ','.

Rootsymbol selector.

%% operator precedences (lowest to highest)
Left 100 'OR'.
Left 200 'AND'.
Nonassoc 300 '=' '<>' '>' '<' '>=' '<='.
Left 400 '+' '-'.
Left 500 '*' '/'.
Unary 600 'NOT'.

%% "A selector is a conditional expression"
selector -> conditional_expr : '$1'.

%% Conditional expressions
conditional_expr -> logical_expr : '$1'.

%% Logical expressions
logical_expr -> logical_expr 'AND' logical_expr : {'and', '$1', '$3'}.
logical_expr -> logical_expr 'OR' logical_expr : {'or', '$1', '$3'}.
logical_expr -> 'NOT' logical_expr : {'not', '$2'}.
logical_expr -> comparison_expr : '$1'.

%% Comparison expressions
comparison_expr -> additive_expr '=' additive_expr : {'=', '$1', '$3'}.
comparison_expr -> additive_expr '<>' additive_expr : {'<>', '$1', '$3'}.
comparison_expr -> additive_expr '>' additive_expr : {'>', '$1', '$3'}.
comparison_expr -> additive_expr '<' additive_expr : {'<', '$1', '$3'}.
comparison_expr -> additive_expr '>=' additive_expr : {'>=', '$1', '$3'}.
comparison_expr -> additive_expr '<=' additive_expr : {'<=', '$1', '$3'}.
comparison_expr -> between_expr : '$1'.
comparison_expr -> like_expr : '$1'.
comparison_expr -> in_expr : '$1'.
comparison_expr -> is_null_expr : '$1'.
comparison_expr -> additive_expr : '$1'.

%% BETWEEN expression
between_expr -> additive_expr 'BETWEEN' additive_expr 'AND' additive_expr : {'between', '$1', '$3', '$5'}.
between_expr -> additive_expr 'NOT' 'BETWEEN' additive_expr 'AND' additive_expr : {'not', {'between', '$1', '$4', '$6'}}.

%% LIKE expression
like_expr -> additive_expr 'LIKE' string :
    {'like', '$1', process_like_pattern('$3'), no_escape}.
like_expr -> additive_expr 'LIKE' string 'ESCAPE' string :
    {'like', '$1', process_like_pattern('$3'), process_escape_char('$5')}.
like_expr -> additive_expr 'NOT' 'LIKE' string :
    {'not', {'like', '$1', process_like_pattern('$4'), no_escape}}.
like_expr -> additive_expr 'NOT' 'LIKE' string 'ESCAPE' string :
    {'not', {'like', '$1', process_like_pattern('$4'), process_escape_char('$6')}}.

%% IN expression
in_expr -> additive_expr 'IN' '(' string_list ')' : {'in', '$1', lists:uniq('$4')}.
in_expr -> additive_expr 'NOT' 'IN' '(' string_list ')' : {'not', {'in', '$1', lists:uniq('$5')}}.
string_list -> string_item : ['$1'].
string_list -> string_item ',' string_list : ['$1'|'$3'].
string_item -> string : extract_value('$1').

%% IS NULL expression
is_null_expr -> identifier_expr 'IS' 'NULL' : {'is_null', '$1'}.
is_null_expr -> identifier_expr 'IS' 'NOT' 'NULL' : {'not', {'is_null', '$1'}}.

%% Arithmetic expressions
additive_expr -> additive_expr '+' multiplicative_expr : {'+', '$1', '$3'}.
additive_expr -> additive_expr '-' multiplicative_expr : {'-', '$1', '$3'}.
additive_expr -> multiplicative_expr : '$1'.

multiplicative_expr -> multiplicative_expr '*' unary_expr : {'*', '$1', '$3'}.
multiplicative_expr -> multiplicative_expr '/' unary_expr : {'/', '$1', '$3'}.
multiplicative_expr -> unary_expr : '$1'.

%% Handle unary operators through grammar structure instead of precedence
unary_expr -> '+' primary : {unary_plus, '$2'}.
unary_expr -> '-' primary : {unary_minus, '$2'}.
unary_expr -> primary : '$1'.

%% Primary expressions
primary -> '(' conditional_expr ')' : '$2'.
primary -> literal : '$1'.
primary -> identifier_expr : '$1'.

%% Identifiers (header fields or property references)
identifier_expr -> identifier :
    {identifier, extract_value('$1')}.

%% Literals
literal -> integer : {integer, extract_value('$1')}.
literal -> float : {float, extract_value('$1')}.
literal -> string : {string, extract_value('$1')}.
literal -> boolean : {boolean, extract_value('$1')}.

Erlang code.

extract_value({_Token, _Line, Value}) -> Value.

process_like_pattern({string, Line, Value}) ->
    case unicode:characters_to_list(Value) of
        L when is_list(L) ->
            L;
        _ ->
            return_error(Line, "pattern-value in LIKE must be valid Unicode")
    end.

process_escape_char({string, Line, Value}) ->
    case unicode:characters_to_list(Value) of
        [SingleChar] ->
            SingleChar;
        _ ->
            return_error(Line, "ESCAPE must be a single-character string literal")
    end.
