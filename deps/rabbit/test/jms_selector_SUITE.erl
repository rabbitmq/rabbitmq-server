%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2025 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.

-module(jms_selector_SUITE).

-compile([export_all, nowarn_export_all]).

-include_lib("eunit/include/eunit.hrl").

%%%===================================================================
%%% Common Test callbacks
%%%===================================================================

all() ->
    [
     {group, tests}
    ].

groups() ->
    [{tests, [shuffle],
      [
       logical_operators,
       comparison_operators,
       arithmetic_operators,
       string_comparison,
       in_operator,
       between_operator,
       null_handling,
       literals,
       scientific_notation,
       precedence_and_parentheses,
       type_handling,
       complex_expressions,
       case_sensitivity,
       whitespace_handling,
       identifier_rules,
       jms_headers
      ]
     }].

%%%===================================================================
%%% Test cases
%%%===================================================================

logical_operators(_Config) ->
    %% Basic logical operators
    true = match("country = 'UK' AND weight = 5", headers()),
    true = match("'UK' = country AND 5 = weight", headers()),
    true = match("country = 'France' OR weight < 6", headers()),
    true = match("NOT country = 'France'", headers()),
    false = match("country = 'UK' AND weight > 5", headers()),
    false = match("missing AND premium", headers()),
    false = match("active AND absent", headers()),
    false = match("NOT absent", headers()),
    false = match("premium OR absent", headers()),
    true = match("absent OR active", headers()),
    true = match("active OR absent", headers()),

    %% The JMS spec isn't very clear on whether the following should match.
    %% Option 1:
    %% The conditional expression is invalid because percentage
    %% is an identifier returning an integer instead of a boolean.
    %% Therefore, arguably the conditional expression is invalid and should not match.
    %% Option 2:
    %% This integer could be interpreted as UNKNOWN such that
    %% "UNKNOWN OR TRUE" evalutes to TRUE as per table Table 3‑5.
    %% Qpid Broker-J and ActiveMQ Artemis implement option 2.
    %% That's why we also expect option 2 here.
    true = match("percentage OR active", headers()),
    true = match("active OR percentage", headers()),

    %% Compound logical expressions
    true = match("country = 'UK' AND (weight > 3 OR price < 20)", headers()),
    true = match("NOT (country = 'France' OR country = 'Germany')", headers()),
    false = match("country = 'UK' AND NOT active = TRUE", headers()),
    true = match("(country = 'US' OR country = 'UK') AND (weight > 2 AND weight < 10)", headers()).

comparison_operators(_Config) ->
    %% Equality
    true = match("country = 'UK'", headers()),
    false = match("country = 'US'", headers()),

    %% Inequality
    true = match("country <> 'US'", headers()),
    false = match("country <> 'UK'", headers()),

    %% Greater than
    true = match("weight > 3", headers()),
    false = match("weight > 5", headers()),

    %% Less than
    true = match("weight < 10", headers()),
    false = match("weight < 5", headers()),

    %% Greater than or equal
    true = match("weight >= 5", headers()),
    true = match("weight >= 4", headers()),
    false = match("weight >= 6", headers()),
    %% "Only like type values can be compared. One exception is that it is
    %% valid to compare exact numeric values and approximate numeric value"
    true = match("weight >= 5.0", headers()),
    true = match("weight >= 4.99", headers()),
    false = match("weight >= 5.01", headers()),
    true = match("price >= 10.5", headers()),
    false = match("price >= 10.51", headers()),
    true = match("price >= 10.4", headers()),
    true = match("price >= 10", headers()),
    false = match("price >= 11", headers()),

    %% Less than or equal
    true = match("weight <= 5", headers()),
    true = match("weight <= 6", headers()),
    false = match("weight <= 4", headers()),
    true = match("price <= 10.6", headers()),
    false = match("price <= 10", headers()),

    %% "String and Boolean comparison is restricted to = and <>."
    %% "If the comparison of non-like type values is attempted, the value of the operation is false."
    true = match("active = true", headers()),
    true = match("premium = false", headers()),
    false = match("premium <> false", headers()),
    false = match("premium >= 'false'", headers()),
    false = match("premium <= 'false'", headers()),
    false = match("premium >= 0", headers()),
    false = match("premium <= 0", headers()),

    false = match("country >= 'UK'", headers()),
    false = match("country > 'UA'", headers()),
    false = match("country >= 'UA'", headers()),
    false = match("country < 'UA'", headers()),
    false = match("country <= 'UA'", headers()),
    false = match("country < 'UL'", headers()),
    false = match("country < true", headers()),

    false = match("weight = '5'", headers()),
    false = match("weight >= '5'", headers()),
    false = match("weight <= '5'", headers()),
    false = match("country > 1", headers()),
    false = match("country < 1", headers()).

arithmetic_operators(_Config) ->
    %% Addition
    true = match("weight + 5 = 10", headers()),
    true = match("price + 4.5 = 15", headers()),

    %% Subtraction
    true = match("weight - 2 = 3", headers()),
    true = match("price - 0.5 = 10", headers()),

    %% Multiplication
    true = match("weight * 2 = 10", headers()),
    true = match("quantity * price * discount = 262.5", headers()),

    %% Division
    true = match("weight / 2 = 2.5", headers()),
    true = match("price / 2 = 5.25", headers()),
    true = match("quantity / 10 = 10", headers()),
    true = match("quantity / 10 = 10.000", headers()),

    %% Nested arithmetic
    true = match("(weight + 5) * 2 = 20", headers()),
    true = match("price / (weight - 3) = 5.25", headers()),

    %% Unary operators
    true = match("+temperature = -5", headers()),
    true = match("-temperature = 5", headers()),
    true = match("+weight = 5", headers()),
    true = match("-weight = -5", headers()),
    true = match("6 + -weight = 1", headers()),
    true = match("6 - +weight = 1", headers()),
    true = match("6 + +weight = 11", headers()),
    true = match("6 - -weight = 11", headers()),
    true = match("+(-weight) = -5", headers()),
    true = match("-(+weight) = -5", headers()),
    true = match("-(-weight) = 5", headers()),
    true = match("+(+weight) = 5", headers()),
    false = match("+weight", headers()),

    %% Unary operators followed by identifiers with non-numeric values are invalid
    %% and should therefore not match.
    false = match("+city", headers()),
    false = match("+city = 'London'", headers()),
    false = match("-absent", headers()),

    %% "Comparison or arithmetic with an unknown value always yields an unknown value."
    false = match("absent + 4 = 5", headers()),
    false = match("2 * absent = 0", headers()).

string_comparison(_Config) ->
    %% "Two strings are equal if and only if they contain the same sequence of characters."
    false = match("country = '🇬🇧'", headers()),
    true = match("country = '🇬🇧'", #{<<"country">> => <<"🇬🇧"/utf8>>}),

    %% "A string literal is enclosed in single quotes, with an included
    %% single quote represented by doubled single quote"
    true = match("'UK''s' = 'UK''s'", headers()),
    true = match("country = 'UK''s'", #{<<"country">> => <<"UK's">>}),
    true = match("country = '🇬🇧''s'", #{<<"country">> => <<"🇬🇧's"/utf8>>}),
    true = match("country = ''", #{<<"country">> => <<>>}),
    true = match("country = ''''", #{<<"country">> => <<$'>>}),

    %% Basic LIKE operations
    true = match("description LIKE '%test%'", headers()),
    true = match("description LIKE 'This is a %'", headers()),
    true = match("description LIKE '%a test message'", headers()),
    true = match("description LIKE 'T_i% a %e_%e_sa%'", headers()),
    false = match("description LIKE 'T_i% a %e_%e_sa'", headers()),
    false = match("description LIKE 'is a test message'", headers()),
    true = match("country LIKE 'UK'", headers()),
    true = match("country LIKE 'U_'", headers()),
    true = match("country LIKE '_K'", headers()),
    true = match("country LIKE 'UK%'", headers()),
    true = match("country LIKE '%UK'", headers()),
    true = match("country LIKE 'U%K'", headers()),
    false = match("country LIKE 'US'", headers()),
    false = match("country LIKE '_UK'", headers()),
    false = match("country LIKE 'UK_'", headers()),
    false = match("country LIKE 'U_K'", headers()),
    false = match("city LIKE 'New%'", headers()),

    %% identifier with empty string value
    Empty = #{<<"empty">> => <<"">>},
    true = match("empty LIKE ''", Empty),
    true = match("empty LIKE '%'", Empty),
    true = match("empty LIKE '%%'", Empty),
    true = match("empty LIKE '%%%'", Empty),
    false = match("empty LIKE 'x'", Empty),
    false = match("empty LIKE '%x'", Empty),
    false = match("empty LIKE '%x%'", Empty),
    false = match("empty LIKE '_'", Empty),

    %% LIKE operations with UTF8
    Utf8  = #{<<"food">> => <<"car🥕rot"/utf8>>},
    true = match("food LIKE 'car🥕rot'", Utf8),
    true = match("food LIKE 'car_rot'", Utf8),
    true = match("food LIKE '___🥕___'", Utf8),
    true = match("food LIKE '%🥕%'", Utf8),
    true = match("food LIKE '%_🥕_%'", Utf8),
    true = match("food LIKE '_%🥕%_'", Utf8),
    false = match("food LIKE 'car__rot'", Utf8),
    false = match("food LIKE 'carrot'", Utf8),
    false = match("food LIKE 'car🥕to'", Utf8),

    false = match("invalid_utf8 LIKE '%'", #{<<"invalid_utf8">> => <<0, 1, 128>>}),

    %% Escape character
    true = match("key LIKE 'z_%' ESCAPE 'z'", #{<<"key">> => <<"_foo">>}),
    false = match("key LIKE 'z_%' ESCAPE 'z'", #{<<"key">> => <<"foo">>}),
    true = match("key LIKE '$_%' ESCAPE '$'", #{<<"key">> => <<"_foo">>}),
    false = match("key LIKE '$_%' ESCAPE '$'", #{<<"key">> => <<"foo">>}),
    true = match("key LIKE '_$%' ESCAPE '$'", #{<<"key">> => <<"5%">>}),
    true = match("key LIKE '_$%' ESCAPE '$'", #{<<"key">> => <<"🥕%"/utf8>>}),
    true = match("key LIKE '🍰@%🥕' ESCAPE '@'", #{<<"key">> => <<"🍰%🥕"/utf8>>}),
    false = match("key LIKE '🍰@%🥕' ESCAPE '@'", #{<<"key">> => <<"🍰other🥕"/utf8>>}),
    false = match("key LIKE '🍰@%🥕' ESCAPE '@'", #{<<"key">> => <<"🍰🥕"/utf8>>}),
    true = match("product_id LIKE 'ABC\\_%' ESCAPE '\\'", headers()),
    false = match("product_id LIKE 'ABC\\%' ESCAPE '\\'", headers()),
    false = match("product_id LIKE 'ABC\\_\\%' ESCAPE '\\'", headers()),
    true = match("product_id LIKE 'ABC🥕_123' ESCAPE '🥕'", headers()),
    false = match("product_id LIKE 'ABC🥕%123' ESCAPE '🥕'", headers()),

    %% NOT LIKE
    true = match("country NOT LIKE 'US'", headers()),
    false = match("country NOT LIKE 'U_'", headers()),
    false = match("country NOT LIKE '%U%'", headers()),
    false = match("country NOT LIKE 'U%K'", headers()),
    true = match("country NOT LIKE 'U%S'", headers()),
    true = match("country NOT LIKE 'z_🇬🇧' ESCAPE 'z'", #{<<"country">> => <<"a🇬🇧"/utf8>>}),
    false = match("country NOT LIKE 'z_🇬🇧' ESCAPE 'z'", #{<<"country">> => <<"_🇬🇧"/utf8>>}),

    %% "If identifier of a LIKE or NOT LIKE operation is NULL, the value of the operation is unknown."
    false = match("absent LIKE '%'", headers()),
    false = match("absent NOT LIKE '%'", headers()),
    false = match("missing LIKE '%'", headers()),
    false = match("missing NOT LIKE '%'", headers()),

    %% Combined with other operators
    true = match("description LIKE '%test%' AND country = 'UK'", headers()),
    true = match("(city LIKE 'Paris') OR (description LIKE '%test%')", headers()),
    true = match("city LIKE 'Paris' OR description LIKE '%test%'", headers()).

in_operator(_Config) ->
    %% Basic IN operations
    true = match("country IN ('US', 'UK', 'France')", headers()),
    true = match("country IN ('UK')", headers()),
    true = match("country IN ('🇫🇷', '🇬🇧')", #{<<"country">> => <<"🇬🇧"/utf8>>}),
    false = match("country IN ('US', 'France')", headers()),

    %% NOT IN
    true = match("country NOT IN ('US', 'France', 'Germany')", headers()),
    true = match("country NOT IN ('🇬🇧')", headers()),
    false = match("country NOT IN ('🇫🇷', '🇬🇧')", #{<<"country">> => <<"🇬🇧"/utf8>>}),
    false = match("country NOT IN ('US', 'UK', 'France')", headers()),

    %% Combined with other operators
    true = match("country IN ('UK', 'US') AND weight > 3", headers()),
    true = match("city IN ('Berlin', 'Paris') OR country IN ('UK', 'US')", headers()),

    %% "If identifier of an IN or NOT IN operation is NULL, the value of the operation is unknown."
    false = match("missing IN ('UK', 'US')", headers()),
    false = match("absent IN ('UK', 'US')", headers()),
    false = match("missing NOT IN ('UK', 'US')", headers()),
    false = match("absent NOT IN ('UK', 'US')", headers()).

between_operator(_Config) ->
    %% Basic BETWEEN operations
    true = match("weight BETWEEN 3 AND 7", headers()),
    true = match("weight BETWEEN 5 AND 7", headers()),
    true = match("weight BETWEEN 3 AND 5", headers()),
    false = match("weight BETWEEN 6 AND 10", headers()),
    true = match("price BETWEEN 10 AND 11", headers()),
    true = match("price BETWEEN 10 AND 10.5", headers()),
    false = match("price BETWEEN -1 AND 10", headers()),
    false = match("score BETWEEN tiny_value AND quantity", headers()),
    true = match("score BETWEEN -tiny_value AND quantity", headers()),

    %% NOT BETWEEN
    true = match("weight NOT BETWEEN 6 AND 10", headers()),
    false = match("weight NOT BETWEEN 3 AND 7", headers()),
    false = match("weight NOT BETWEEN 3 AND 5", headers()),
    true = match("score NOT BETWEEN tiny_value AND quantity", headers()),
    false = match("score NOT BETWEEN -tiny_value AND quantity", headers()),

    %% Combined with other operators
    true = match("weight BETWEEN 4 AND 6 AND country = 'UK'", headers()),
    true = match("(price BETWEEN 20 AND 30) OR (weight BETWEEN 5 AND 6)", headers()),

    %% "a string cannot be used in an arithmetic expression"
    false = match("weight BETWEEN 1 AND 'Z'", headers()),
    false = match("country BETWEEN 'A' AND 'Z'", headers()),

    %% "Comparison or arithmetic with an unknown value always yields an unknown value."
    false = match("weight BETWEEN absent AND 10", headers()),
    false = match("weight BETWEEN 2 AND absent", headers()),
    false = match("weight BETWEEN absent AND absent", headers()),
    false = match("absent BETWEEN 2 AND 10", headers()),
    false = match("weight NOT BETWEEN absent AND 10", headers()),
    false = match("weight NOT BETWEEN 2 AND absent", headers()),
    false = match("weight NOT BETWEEN absent AND absent", headers()),
    false = match("absent NOT BETWEEN 2 AND 10", headers()).

null_handling(_Config) ->
    %% IS NULL / IS NOT NULL
    true = match("missing IS NULL", headers()),
    true = match("absent IS NULL", headers()),
    false = match("country IS NULL", headers()),
    true = match("country IS NOT NULL", headers()),
    false = match("missing IS NOT NULL", headers()),
    false = match("absent IS NOT NULL", headers()),
    true = match("country = 'UK' AND missing IS NULL", headers()),
    true = match("country = 'France' OR weight IS NOT NULL", headers()),

    %% "SQL treats a NULL value as unknown.
    %% Comparison or arithmetic with an unknown value always yields an unknown value."
    false = match("missing > 0", headers()),
    false = match("0 < missing", headers()),
    false = match("0 > absent", headers()),
    false = match("0 = missing", headers()),
    false = match("missing >= 0", headers()),
    false = match("missing < 0", headers()),
    false = match("missing <= 0", headers()),
    false = match("missing = 0", headers()),
    false = match("missing <> 0", headers()),
    false = match("missing = missing", headers()),
    false = match("absent = absent", headers()),
    false = match("missing AND true", headers()),
    false = match("missing OR false", headers()).

literals(_Config) ->
    %% Exact numeric literals
    true = match("5 = 5", headers()),
    true = match("weight = 5", headers()),

    %% "Approximate literals use the Java floating-point literal syntax."
    true = match("10.5 = 10.5", headers()),
    true = match("price = 10.5", headers()),
    true = match("5.0 > 4.999", headers()),
    true = match("10 = 10.", headers()),
    true = match("0 = 0.0", headers()),
    true = match("0 = 0.", headers()),
    true = match("0 = .0", headers()),

    true = match("weight = 5.0", headers()), % int = float
    true = match("5. = weight", headers()), % float = int

    %% String literals
    true = match("'UK' = 'UK'", headers()),
    true = match("country = 'UK'", headers()),

    %% Boolean literals
    true = match("TRUE = TRUE", headers()),
    true = match("active = TRUE", headers()),
    true = match("TRUE", headers()),
    true = match("active", headers()),
    true = match("FALSE = FALSE", headers()),
    true = match("premium = FALSE", headers()),
    false = match("FALSE", headers()),
    false = match("premium", headers()),

    %% Literals in expressions
    true = match("weight + 2 > 6", headers()),
    true = match("price * 2 > 20.0", headers()),
    true = match("'UK' <> 'US'", headers()).

scientific_notation(_Config) ->
    %% Basic scientific notation comparisons
    true = match("distance = 1.2E6", headers()),
    true = match("distance = 1200000.0", headers()),
    true = match("tiny_value = 3.5E-4", headers()),
    true = match("tiny_value = 0.00035", headers()),

    %% Scientific notation literals in expressions
    true = match("1.2E3 = 1200", headers()),
    true = match("5E2 = 500", headers()),
    true = match("5.E2 = 500", headers()),
    true = match("-5E-2 = -0.05", headers()),
    true = match("-5.E-2 = -0.05", headers()),
    true = match(".5E-1 = 0.05", headers()),
    true = match("-.5E-1 = -0.05", headers()),
    true = match("1E0 = 1", headers()),

    %% Arithmetic with scientific notation
    true = match("distance / 1.2E5 = 10", headers()),
    true = match("tiny_value * 1E6 = 350", headers()),
    true = match("1.5E2 + 2.5E2 = 400", headers()),
    true = match("3E3 - 2E3 = 1000", headers()),

    %% Comparisons with scientific notation
    true = match("distance > 1E6", headers()),
    true = match("tiny_value < 1E-3", headers()),
    true = match("distance BETWEEN 1E6 AND 2E6", headers()),

    %% Mixed numeric formats
    true = match("distance / 1200 = 1000", headers()),
    true = match("large_value + tiny_value >= large_value", headers()),
    true = match("large_value + large_value > large_value", headers()).

precedence_and_parentheses(_Config) ->
    %% Arithmetic precedence
    true = match("weight + 2 * 3 = 11", headers()),
    true = match("(weight + 2) * 3 = 21", headers()),
    true = match("weight + weight * quantity - -temperature / 2 = 502.5", headers()),

    %% "Logical operators in precedence order: NOT, AND, OR"
    true = match("NOT country = 'US' AND weight > 3", headers()),
    true = match("weight > 3 AND NOT country = 'US'", headers()),
    true = match("NOT (country = 'US' AND weight > 3)", headers()),
    true = match("NOT country = 'US' OR country = 'France' AND weight > 3", headers()),
    true = match("country = 'France' AND weight > 3 OR NOT country = 'US'", headers()),

    %% Mixed precedence
    true = match("weight * 2 > 5 + 3", headers()),
    true = match("price < 20 OR country = 'US' AND weight > 3", headers()),
    true = match("weight > 3 AND price < 20 OR country = 'US'", headers()),
    false = match("weight > 3 AND (price > 20 OR country = 'US')", headers()),

    %% Complex parentheses nesting
    true = match("((weight > 3) AND (price < -1)) OR ((country = 'UK') AND (city = 'London'))", headers()),
    true = match("weight > 3 AND price < -1 OR country = 'UK' AND city = 'London'", headers()),
    true = match("(weight + (price * 2)) > (score + 15)", headers()).

%% "Only like type values can be compared. One exception is that it is
%% valid to compare exact numeric values and approximate numeric values.
%% If the comparison of non-like type values is attempted, the value of the operation is false."
type_handling(_Config) ->
    %% Numeric comparisons
    true = match("weight = 5", headers()), % int = int
    true = match("weight = 5.0", headers()), % int = float
    true = match("price = 10.5", headers()), % float = float

    %% String and numeric
    false = match("country = 5", headers()), % string != number
    false = match("weight = 'UK'", headers()), % number != string

    %% Boolean comparisons
    true = match("active = TRUE", headers()),
    true = match("active <> FALSE", headers()),
    false = match("TRUE = 1", headers()), % boolean != number
    false = match("active = 1", headers()), % boolean != number
    false = match("TRUE = 'TRUE'", headers()), % boolean != string
    false = match("active = 'TRUE'", headers()), % boolean != string

    %% Type-specific operators
    true = match("description LIKE '%test%'", headers()), % LIKE only works on strings
    false = match("weight LIKE '5'", headers()), % LIKE doesn't work on numbers

    %% Arithmetic with different types
    true = match("weight + price = 15.5", headers()), % int + float = float
    true = match("weight * discount = 1.25", headers()), % int * float = float

    %% Division by zero is undefined
    false = match("weight / 0 > 0", headers()),
    false = match("weight / score = 5", headers()),
    false = match("0 / 0 = 0", headers()),
    false = match("0 / 0.0 = 0", headers()),
    false = match("0 / 0. = 0", headers()),
    false = match("-1 / 0 = 0", headers()),
    false = match("score / score = score", headers()),

    true = match("0.0 / 1 = 0", headers()),

    %% Type incompatibility
    false = match("country + weight = 'UK5'", headers()), % can't add string and number
    false = match("active + premium = 1", headers()). % can't add booleans

complex_expressions(_Config) ->
    true = match(
             "country = 'UK' AND price > 10.0 AND (weight BETWEEN 4 AND 6) AND description LIKE '%test%'",
             headers()
            ),
    true = match(
             "(country IN ('UK', 'US') OR city = 'London') AND (weight * 2 >= 10) AND NOT premium",
             headers()
            ),
    true = match(
             "price * quantity * (1 - discount) > 500",
             headers()
            ),
    true = match(
             "country = 'UK' AND (city = 'London' OR description LIKE '%test%') AND" ++
             "(weight > 3 OR premium = TRUE) AND price <= 20",
             headers()
            ),
    true = match(
             "percentage >= 0 AND percentage <= 100 AND weight + temperature = 0",
             headers()
            ),
    true = match(
             "((country = 'UK' OR country = 'US') AND (city IN ('London', 'New York', 'Paris'))) OR " ++
             "(price * (1 - discount) < 10.0 AND quantity > 50 AND description LIKE '%test%') OR " ++
             "(active = TRUE AND premium = FALSE AND (weight BETWEEN 4 AND 10))",
             headers()
            ).

%% "Predefined selector literals and operator names are [...] case insensitive."
%% "Identifiers are case sensitive."
case_sensitivity(_Config) ->
    Headers = headers(),
    HeadersWithCaseSensitiveKeys = maps:merge(Headers,
                                              #{<<"COUNTRY">> => <<"France">>,
                                                <<"Weight">> => 10}),

    %% 1. Test that operators and literals are case insensitive
    true = match("country = 'UK' AnD weight = 5", Headers),
    true = match("country = 'UK' and weight = 5", Headers),
    true = match("country = 'France' Or weight < 6", Headers),
    true = match("country = 'France' or weight < 6", Headers),
    true = match("NoT country = 'France'", Headers),
    true = match("not country = 'France'", Headers),
    true = match("weight BeTwEeN 3 AnD 7", Headers),
    true = match("weight between 3 AnD 7", Headers),
    true = match("description LiKe '%test%'", Headers),
    true = match("description like '%test%'", Headers),
    true = match("country In ('US', 'UK', 'France')", Headers),
    true = match("country in ('US', 'UK', 'France')", Headers),
    true = match("missing Is NuLl", Headers),
    true = match("missing is null", Headers),
    true = match("active = TrUe", Headers),
    true = match("active = true", Headers),
    true = match("premium = FaLsE", Headers),
    true = match("premium = false", Headers),
    true = match("distance = 1.2e6", headers()),
    true = match("tiny_value = 3.5e-4", headers()),
    true = match("3 = 3e0", headers()),
    true = match("3 = 3e-0", headers()),
    true = match("300 = 3e2", headers()),
    true = match("0.03 = 3e-2", headers()),

    %% 2. Test that identifiers are case sensitive
    true = match("country = 'UK'", HeadersWithCaseSensitiveKeys),
    true = match("COUNTRY = 'France'", HeadersWithCaseSensitiveKeys),
    true = match("Weight = 10", HeadersWithCaseSensitiveKeys),

    false = match("COUNTRY = 'UK'", HeadersWithCaseSensitiveKeys),
    false = match("country = 'France'", HeadersWithCaseSensitiveKeys),
    false = match("weight = 10", HeadersWithCaseSensitiveKeys),
    false = match("WEIGHT = 5", HeadersWithCaseSensitiveKeys),

    true = match(
             "country = 'UK' aNd COUNTRY = 'France' and (weight Between 4 AnD 6) AND Weight = 10",
             HeadersWithCaseSensitiveKeys
            ).

%% "Whitespace is the same as that defined for Java:
%% space, horizontal tab, form feed and line terminator."
whitespace_handling(_Config) ->
    %% 1. Space
    true = match("country = 'UK'", headers()),

    %% 2. Multiple spaces
    true = match("country    =    'UK'", headers()),

    %% 3. Horizontal tab (\t)
    true = match("country\t=\t'UK'", headers()),

    %% 4. Form feed (\f)
    true = match("country\f=\f'UK'", headers()),

    %% 5. Line terminators (\n line feed, \r carriage return)
    true = match("country\n=\n'UK'", headers()),
    true = match("country\r=\r'UK'", headers()),

    %% 6. Mixed whitespace
    true = match("country \t\f\n\r = \t\f\n\r 'UK'", headers()),

    %% 7. Complex expression with various whitespace
    true = match("country\t=\t'UK'\nAND\rweight\f>\t3", headers()),

    %% 8. Ensure whitespace is not required
    true = match("country='UK'AND weight=5", headers()),

    %% 9. Whitespace inside string literals should be preserved
    true = match("description = 'This is a test message'", headers()),

    %% 10. Whitespace at beginning and end of expression
    true = match(" \t\n\r country = 'UK' \t\n\r ", headers()).

%% "An identifier is an unlimited-length character sequence that must begin with a
%% Java identifier start character; all following characters must be Java identifier
%% part characters. An identifier start character is any character for which the method
%% Character.isJavaIdentifierStart returns true. This includes '_' and '$'. An
%% identifier part character is any character for which the method
%% Character.isJavaIdentifierPart returns true."
identifier_rules(_Config) ->
    Headers1 = #{<<"simple">> => <<"value">>,
                 <<"a1b2c3">> => <<"value">>,
                 <<"x">> => <<"value">>,
                 <<"_underscore">> => <<"value">>,
                 <<"$dollar">> => <<"value">>,
                 <<"_">> => <<"value">>,
                 <<"$">> => <<"value">>,
                 <<"with_underscore">> => <<"value">>,
                 <<"with$dollar">> => <<"value">>,
                 <<"mixed_$_identifiers_$_123">> => <<"value">>
                },
    true = match("simple = 'value'", Headers1),
    true = match("a1b2c3 = 'value'", Headers1),
    true = match("x = 'value'", Headers1),
    true = match("_underscore = 'value'", Headers1),
    true = match("$dollar = 'value'", Headers1),
    true = match("_ = 'value'", Headers1),
    true = match("$ = 'value'", Headers1),
    true = match("with_underscore = 'value'", Headers1),
    true = match("with$dollar = 'value'", Headers1),
    true = match("mixed_$_identifiers_$_123 = 'value'", Headers1).

%% amqp-bindmap-jms-v1.0-wd10 §3.2
jms_headers(_Config) ->
    Headers = #{durable => true,
                priority => 7,
                message_id => <<"id-123">>,
                creation_time => 1311704463521,
                correlation_id => <<"id-456">>,
                subject => <<"some subject">>,
                user_id => <<"some user ID">>,
                group_id => <<"some group ID">>,
                group_sequence => 999
               },

    true = match("JMSDeliveryMode = 'PERSISTENT'", Headers),
    true = match("'PERSISTENT' = JMSDeliveryMode", Headers),
    false = match("JMSDeliveryMode <> 'PERSISTENT'", Headers),
    false = match("'PERSISTENT' <> JMSDeliveryMode", Headers),
    false = match("JMSDeliveryMode = 'NON_PERSISTENT'", Headers),
    false = match("'NON_PERSISTENT' = JMSDeliveryMode", Headers),
    true = match("'NON_PERSISTENT' <> JMSDeliveryMode", Headers),

    Headers2 = #{durable => false,
                 <<"key1">> => <<"PERSISTENT">>,
                 <<"key2">> => <<"NON_PERSISTENT">>},

    true = match("JMSDeliveryMode = 'NON_PERSISTENT'", Headers2),
    true = match("'NON_PERSISTENT' = JMSDeliveryMode", Headers2),
    true = match("JMSDeliveryMode = key2", Headers2),
    true = match("key2 = JMSDeliveryMode", Headers2),
    false = match("JMSDeliveryMode = key1", Headers2),
    false = match("key1 = JMSDeliveryMode", Headers2),
    true = match("JMSDeliveryMode <> key1", Headers2),
    true = match("key1 <> JMSDeliveryMode", Headers2),

    true = match("JMSPriority > 5", Headers),
    true = match("JMSPriority = 7", Headers),
    false = match("JMSPriority < 7", Headers),

    true = match("JMSMessageID = 'id-123'", Headers),
    false = match("JMSMessageID = 'wrong-id'", Headers),

    true = match("JMSTimestamp = 1311704463521", Headers),
    true = match("JMSTimestamp > 1000000000000", Headers),
    false = match("JMSTimestamp < 1000000000000", Headers),

    true = match("JMSCorrelationID = 'id-456'", Headers),
    false = match("JMSCorrelationID = 'wrong-correlation'", Headers),

    true = match("JMSType = 'some subject'", Headers),
    true = match("JMSType LIKE '%subject'", Headers),
    false = match("JMSType = 'wrong subject'", Headers),

    true = match("JMSXUserID = 'some user ID'", Headers),
    true = match("JMSXUserID LIKE 'some%'", Headers),
    false = match("JMSXUserID = 'different user'", Headers),

    true = match("JMSXGroupID = 'some group ID'", Headers),
    true = match("JMSXGroupID LIKE '%group%'", Headers),
    false = match("JMSXGroupID = 'different group'", Headers),

    true = match("JMSXGroupSeq = 999", Headers),
    true = match("JMSXGroupSeq > 500", Headers),
    false = match("JMSXGroupSeq < 500", Headers),

    %% Combined conditions
    true = match("JMSPriority > 5 AND JMSType LIKE '%subject'", Headers),
    false = match("JMSMessageID = 'wrong-id' AND JMSCorrelationID = 'id-456'", Headers),
    true = match("NOT (JMSXUserID = 'different user' OR JMSXGroupID = 'different group')", Headers),

    {ok, Tokens, _EndLocation} = rabbit_jms_selector_lexer:string("JMSXDeliveryCount > 1"),
    Line = 1,
    ?assertEqual(
       {error, {Line, rabbit_jms_selector_parser,
                "setting message selector on JMSXDeliveryCount is disallowed"}},
       rabbit_jms_selector_parser:parse(Tokens)).

%%%===================================================================
%%% Helpers
%%%===================================================================

headers() ->
    #{
      %% String values
      <<"country">> => <<"UK">>,
      <<"city">> => <<"London">>,
      <<"description">> => <<"This is a test message">>,
      <<"currency">> => <<"GBP">>,
      <<"product_id">> => <<"ABC_123">>,

      %% Numeric values
      <<"weight">> => 5,
      <<"price">> => 10.5,
      <<"quantity">> => 100,
      <<"discount">> => 0.25,
      <<"temperature">> => -5,
      <<"score">> => 0,
      %% Scientific notation values
      <<"distance">> => 1.2E6, % 1,200,000
      <<"tiny_value">> => 3.5E-4, % 0.00035
      <<"large_value">> => 6.02E23, % Avogadro's number

      %% Boolean values
      <<"active">> => true,
      <<"premium">> => false,

      %% Special cases
      <<"missing">> => undefined,
      <<"percentage">> => 75
     }.

match(Selector, Headers) ->
    {ok, Tokens, _EndLocation} = rabbit_jms_selector_lexer:string(Selector),
    {ok, Expr} = rabbit_jms_selector_parser:parse(Tokens),
    rabbit_fifo_filter_jms:eval(Expr, Headers).
