%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2025 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.

-module(amqp_jms_unit_SUITE).

-compile([export_all, nowarn_export_all]).

-include_lib("eunit/include/eunit.hrl").
-include_lib("amqp10_common/include/amqp10_filter.hrl").
-include_lib("amqp10_common/include/amqp10_framing.hrl").

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
       like_operator,
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
       header_section,
       properties_section,
       multiple_sections,
       parse_errors
      ]
     }].

%%%===================================================================
%%% Test cases
%%%===================================================================

logical_operators(_Config) ->
    %% Basic logical operators
    true = match("country = 'UK' AND weight = 5", app_props()),
    true = match("'UK' = country AND 5 = weight", app_props()),
    true = match("country = 'France' OR weight < 6", app_props()),
    true = match("NOT country = 'France'", app_props()),
    false = match("country = 'UK' AND weight > 5", app_props()),
    false = match("missing AND premium", app_props()),
    false = match("active AND absent", app_props()),
    false = match("NOT absent", app_props()),
    false = match("premium OR absent", app_props()),
    true = match("absent OR active", app_props()),
    true = match("active OR absent", app_props()),

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
    true = match("percentage OR active", app_props()),
    true = match("active OR percentage", app_props()),

    %% See tables 3-4 and 3-6 in
    %% https://jakarta.ee/specifications/messaging/3.1/jakarta-messaging-spec-3.1#null-values
    %% NOT (Unknown AND True) = NOT Unknown = Unknown
    false = match("NOT (absent IN ('v1', 'v2') AND active)", app_props()),
    %% NOT (Unknown AND Unknown) = NOT Unknown = Unknown
    false = match("NOT (absent IN ('v1', 'v2') AND absent LIKE 'v3')", app_props()),
    %% NOT (Unknown AND False) = NOT False = True
    true = match("NOT (absent IN ('v1', 'v2') AND premium)", app_props()),
    %% NOT (True AND Unknown) = NOT Unknown = Unknown
    false = match("NOT (active AND absent IN ('v1', 'v2'))", app_props()),
    %% NOT (True AND False) = NOT False = True
    true = match("NOT (active AND premium)", app_props()),
    %% NOT (Unknown OR False) = NOT Unknown = Unknown
    false = match("NOT (absent IN ('v1', 'v2') OR premium)", app_props()),
    %% NOT (Unknown OR Unknown) = NOT Unknown = Unknown
    false = match("NOT (absent IN ('v1', 'v2') OR absent LIKE 'v3')", app_props()),
    %% NOT (Unknown OR True) = NOT True = False
    false = match("NOT (absent IN ('v1', 'v2') OR active)", app_props()),
    %% NOT (NOT (Unknown OR True)) = NOT (Not True) = Not False = True
    true = match("NOT (NOT (absent IN ('v1', 'v2') OR active))", app_props()),
    %% NOT (False Or Unknown) = NOT Unknown = Unknown
    false = match("NOT (premium OR absent IN ('v1', 'v2'))", app_props()),
    %% NOT (NOT (False Or Unknown)) = NOT (NOT Unknown) = Not Unknown = Unknown
    false = match("NOT (NOT (premium OR absent IN ('v1', 'v2')))", app_props()),

    %% Compound logical expressions
    true = match("country = 'UK' AND (weight > 3 OR price < 20)", app_props()),
    true = match("NOT (country = 'France' OR country = 'Germany')", app_props()),
    false = match("country = 'UK' AND NOT active = TRUE", app_props()),
    true = match("(country = 'US' OR country = 'UK') AND (weight > 2 AND weight < 10)", app_props()).

comparison_operators(_Config) ->
    %% Equality
    true = match("country = 'UK'", app_props()),
    false = match("country = 'US'", app_props()),

    %% Inequality
    true = match("country <> 'US'", app_props()),
    false = match("country <> 'UK'", app_props()),

    %% Greater than
    true = match("weight > 3", app_props()),
    false = match("weight > 5", app_props()),

    %% Less than
    true = match("weight < 10", app_props()),
    false = match("weight < 5", app_props()),

    %% Greater than or equal
    true = match("weight >= 5", app_props()),
    true = match("weight >= 4", app_props()),
    false = match("weight >= 6", app_props()),
    %% "Only like type values can be compared. One exception is that it is
    %% valid to compare exact numeric values and approximate numeric value"
    true = match("weight >= 5.0", app_props()),
    true = match("weight >= 4.99", app_props()),
    false = match("weight >= 5.01", app_props()),
    true = match("price >= 10.5", app_props()),
    false = match("price >= 10.51", app_props()),
    true = match("price >= 10.4", app_props()),
    true = match("price >= 10", app_props()),
    false = match("price >= 11", app_props()),

    %% Less than or equal
    true = match("weight <= 5", app_props()),
    true = match("weight <= 6", app_props()),
    false = match("weight <= 4", app_props()),
    true = match("price <= 10.6", app_props()),
    false = match("price <= 10", app_props()),

    %% "String and Boolean comparison is restricted to = and <>."
    %% "If the comparison of non-like type values is attempted, the value of the operation is false."
    true = match("active = true", app_props()),
    true = match("premium = false", app_props()),
    false = match("premium <> false", app_props()),
    false = match("premium >= 'false'", app_props()),
    false = match("premium <= 'false'", app_props()),
    false = match("premium >= 0", app_props()),
    false = match("premium <= 0", app_props()),

    false = match("country >= 'UK'", app_props()),
    false = match("country > 'UA'", app_props()),
    false = match("country >= 'UA'", app_props()),
    false = match("country < 'UA'", app_props()),
    false = match("country <= 'UA'", app_props()),
    false = match("country < 'UL'", app_props()),
    false = match("country < true", app_props()),

    false = match("weight = '5'", app_props()),
    false = match("weight >= '5'", app_props()),
    false = match("weight <= '5'", app_props()),
    false = match("country > 1", app_props()),
    false = match("country < 1", app_props()).

arithmetic_operators(_Config) ->
    %% Addition
    true = match("weight + 5 = 10", app_props()),
    true = match("price + 4.5 = 15", app_props()),

    %% Subtraction
    true = match("weight - 2 = 3", app_props()),
    true = match("price - 0.5 = 10", app_props()),

    %% Multiplication
    true = match("weight * 2 = 10", app_props()),
    true = match("quantity * price * discount = 262.5", app_props()),

    %% Division
    true = match("weight / 2 = 2.5", app_props()),
    true = match("price / 2 = 5.25", app_props()),
    true = match("quantity / 10 = 10", app_props()),
    true = match("quantity / 10 = 10.000", app_props()),

    %% Nested arithmetic
    true = match("(weight + 5) * 2 = 20", app_props()),
    true = match("price / (weight - 3) = 5.25", app_props()),

    %% Unary operators
    true = match("+temperature = -5", app_props()),
    true = match("-temperature = 5", app_props()),
    true = match("+weight = 5", app_props()),
    true = match("-weight = -5", app_props()),
    true = match("6 + -weight = 1", app_props()),
    true = match("6 - +weight = 1", app_props()),
    true = match("6 + +weight = 11", app_props()),
    true = match("6 - -weight = 11", app_props()),
    true = match("+(-weight) = -5", app_props()),
    true = match("-(+weight) = -5", app_props()),
    true = match("-(-weight) = 5", app_props()),
    true = match("+(+weight) = 5", app_props()),
    false = match("+weight", app_props()),

    %% Unary operators followed by identifiers with non-numeric values are invalid
    %% and should therefore not match.
    false = match("+city", app_props()),
    false = match("+city = 'London'", app_props()),
    false = match("-absent", app_props()),

    %% "Comparison or arithmetic with an unknown value always yields an unknown value."
    false = match("absent + 4 = 5", app_props()),
    false = match("2 * absent = 0", app_props()).

string_comparison(_Config) ->
    %% "Two strings are equal if and only if they contain the same sequence of characters."
    false = match("country = '🇬🇧'", app_props()),
    true = match("country = '🇬🇧'", [{{utf8, <<"country">>}, {utf8, <<"🇬🇧"/utf8>>}}]),

    %% "A string literal is enclosed in single quotes, with an included
    %% single quote represented by doubled single quote"
    true = match("'UK''s' = 'UK''s'", app_props()),
    true = match("country = 'UK''s'", [{{utf8, <<"country">>}, {utf8, <<"UK's">>}}]),
    true = match("country = '🇬🇧''s'", [{{utf8, <<"country">>}, {utf8, <<"🇬🇧's"/utf8>>}}]),
    true = match("country = ''", [{{utf8, <<"country">>}, {utf8, <<>>}}]),
    true = match("country = ''''", [{{utf8, <<"country">>}, {utf8, <<$'>>}}]).

like_operator(_Config) ->
    %% Basic LIKE operations
    true = match("description LIKE '%test%'", app_props()),
    true = match("description LIKE 'This is a %'", app_props()),
    true = match("description LIKE '%a test message'", app_props()),
    true = match("description LIKE 'T_i% a %e_%e_sa%'", app_props()),
    false = match("description LIKE 'T_i% a %e_%e_sa'", app_props()),
    false = match("description LIKE 'is a test message'", app_props()),
    true = match("country LIKE 'UK'", app_props()),
    true = match("country LIKE 'U_'", app_props()),
    true = match("country LIKE '_K'", app_props()),
    true = match("country LIKE 'UK%'", app_props()),
    true = match("country LIKE '%UK'", app_props()),
    true = match("country LIKE 'U%K'", app_props()),
    false = match("country LIKE 'US'", app_props()),
    false = match("country LIKE '_UK'", app_props()),
    false = match("country LIKE 'UK_'", app_props()),
    false = match("country LIKE 'U_K'", app_props()),
    false = match("city LIKE 'New%'", app_props()),
    true = match("key LIKE 'a%a'", [{{utf8, <<"key">>}, {utf8, <<"aa">>}}]),
    false = match("key LIKE 'a%a'", [{{utf8, <<"key">>}, {utf8, <<"a">>}}]),

    %% identifier with empty string value
    Empty = [{{utf8, <<"empty">>}, {utf8, <<"">>}}],
    true = match("empty LIKE ''", Empty),
    true = match("empty LIKE '%'", Empty),
    true = match("empty LIKE '%%'", Empty),
    true = match("empty LIKE '%%%'", Empty),
    false = match("empty LIKE 'x'", Empty),
    false = match("empty LIKE '%x'", Empty),
    false = match("empty LIKE '%x%'", Empty),
    false = match("empty LIKE '_'", Empty),

    %% LIKE operations with UTF8
    Utf8  = [{{utf8, <<"food">>}, {utf8, <<"car🥕rot"/utf8>>}}],
    true = match("food LIKE 'car🥕rot'", Utf8),
    true = match("food LIKE 'car_rot'", Utf8),
    true = match("food LIKE '___🥕___'", Utf8),
    true = match("food LIKE '%🥕%'", Utf8),
    true = match("food LIKE '%_🥕_%'", Utf8),
    true = match("food LIKE '_%🥕%_'", Utf8),
    false = match("food LIKE 'car__rot'", Utf8),
    false = match("food LIKE 'carrot'", Utf8),
    false = match("food LIKE 'car🥕to'", Utf8),

    false = match("invalid_utf8 LIKE '%a'", [{{utf8, <<"invalid_utf8">>}, {binary, <<0, 1, 128>>}}]),
    false = match("invalid_utf8 LIKE '_a'", [{{utf8, <<"invalid_utf8">>}, {binary, <<255>>}}]),
    true = match("key LIKE '_#.\\|()[]{} ^$*+?%'", [{{utf8, <<"key">>}, {utf8, <<"##.\\|()[]{} ^$*+???">>}}]),
    true = match("key LIKE '##.\\|()[]{} ^$*+???'", [{{utf8, <<"key">>}, {utf8, <<"##.\\|()[]{} ^$*+???">>}}]),

    %% Escape character
    true = match("key LIKE 'z_%' ESCAPE 'z'", [{{utf8, <<"key">>}, {utf8, <<"_foo">>}}]),
    false = match("key LIKE 'z_%' ESCAPE 'z'", [{{utf8, <<"key">>}, {utf8, <<"foo">>}}]),
    true = match("key LIKE '$_%' ESCAPE '$'", [{{utf8, <<"key">>}, {utf8, <<"_foo">>}}]),
    false = match("key LIKE '$_%' ESCAPE '$'", [{{utf8, <<"key">>}, {utf8, <<"foo">>}}]),
    true = match("key LIKE '_$%' ESCAPE '$'", [{{utf8, <<"key">>}, {utf8, <<"5%">>}}]),
    true = match("key LIKE '_$%' ESCAPE '$'", [{{utf8, <<"key">>}, {utf8, <<"🥕%"/utf8>>}}]),
    true = match("key LIKE '🍰@%🥕' ESCAPE '@'", [{{utf8, <<"key">>}, {utf8, <<"🍰%🥕"/utf8>>}}]),
    false = match("key LIKE '🍰@%🥕' ESCAPE '@'", [{{utf8, <<"key">>}, {utf8, <<"🍰other🥕"/utf8>>}}]),
    false = match("key LIKE '🍰@%🥕' ESCAPE '@'", [{{utf8, <<"key">>}, {utf8, <<"🍰🥕"/utf8>>}}]),
    true = match("key LIKE '!_#.\\|()[]{} ^$*+?!%' ESCAPE '!'", [{{utf8, <<"key">>}, {utf8, <<"_#.\\|()[]{} ^$*+?%">>}}]),
    false = match("key LIKE '!_#.\\|()[]{} ^$*+?!%' ESCAPE '!'", [{{utf8, <<"key">>}, {utf8, <<"##.\\|()[]{} ^$*+?%">>}}]),

    true = match("product_id LIKE 'ABC\\_%' ESCAPE '\\'", app_props()),
    false = match("product_id LIKE 'ABC\\%' ESCAPE '\\'", app_props()),
    false = match("product_id LIKE 'ABC\\_\\%' ESCAPE '\\'", app_props()),
    true = match("product_id LIKE 'ABC🥕_123' ESCAPE '🥕'", app_props()),
    false = match("product_id LIKE 'ABC🥕%123' ESCAPE '🥕'", app_props()),

    %% NOT LIKE
    true = match("country NOT LIKE 'US'", app_props()),
    false = match("country NOT LIKE 'U_'", app_props()),
    false = match("country NOT LIKE '%U%'", app_props()),
    false = match("country NOT LIKE 'U%K'", app_props()),
    true = match("country NOT LIKE 'U%S'", app_props()),
    true = match("country NOT LIKE 'z_🇬🇧' ESCAPE 'z'", [{{utf8, <<"country">>}, {utf8, <<"a🇬🇧"/utf8>>}}]),
    false = match("country NOT LIKE 'z_🇬🇧' ESCAPE 'z'", [{{utf8, <<"country">>}, {utf8, <<"_🇬🇧"/utf8>>}}]),

    %% "If identifier of a LIKE or NOT LIKE operation is NULL, the value of the operation is unknown."
    false = match("absent LIKE '%'", app_props()),
    false = match("absent NOT LIKE '%'", app_props()),
    false = match("missing LIKE '%'", app_props()),
    false = match("missing NOT LIKE '%'", app_props()),

    %% Combined with other operators
    true = match("description LIKE '%test%' AND country = 'UK'", app_props()),
    true = match("(city LIKE 'Paris') OR (description LIKE '%test%')", app_props()),
    true = match("city LIKE 'Paris' OR description LIKE '%test%'", app_props()).

in_operator(_Config) ->
    AppPropsUtf8 = [{{utf8, <<"country">>}, {utf8, <<"🇬🇧"/utf8>>}}],

    %% Basic IN operations
    true = match("country IN ('US', 'UK', 'France')", app_props()),
    true = match("country IN ('UK')", app_props()),
    true = match("country IN ('🇫🇷', '🇬🇧')", AppPropsUtf8),
    false = match("country IN ('US', 'France')", app_props()),

    %% NOT IN
    true = match("country NOT IN ('US', 'France', 'Germany')", app_props()),
    true = match("country NOT IN ('🇬🇧')", app_props()),
    false = match("country NOT IN ('🇫🇷', '🇬🇧')", AppPropsUtf8),
    false = match("country NOT IN ('US', 'UK', 'France')", app_props()),

    %% Combined with other operators
    true = match("country IN ('UK', 'US') AND weight > 3", app_props()),
    true = match("city IN ('Berlin', 'Paris') OR country IN ('UK', 'US')", app_props()),

    %% "If identifier of an IN or NOT IN operation is NULL, the value of the operation is unknown."
    false = match("missing IN ('UK', 'US')", app_props()),
    false = match("absent IN ('UK', 'US')", app_props()),
    false = match("missing NOT IN ('UK', 'US')", app_props()),
    false = match("absent NOT IN ('UK', 'US')", app_props()).

between_operator(_Config) ->
    %% Basic BETWEEN operations
    true = match("weight BETWEEN 3 AND 7", app_props()),
    true = match("weight BETWEEN 5 AND 7", app_props()),
    true = match("weight BETWEEN 3 AND 5", app_props()),
    false = match("weight BETWEEN 6 AND 10", app_props()),
    true = match("price BETWEEN 10 AND 11", app_props()),
    true = match("price BETWEEN 10 AND 10.5", app_props()),
    false = match("price BETWEEN -1 AND 10", app_props()),
    false = match("score BETWEEN tiny_value AND quantity", app_props()),
    true = match("score BETWEEN -tiny_value AND quantity", app_props()),

    %% NOT BETWEEN
    true = match("weight NOT BETWEEN 6 AND 10", app_props()),
    false = match("weight NOT BETWEEN 3 AND 7", app_props()),
    false = match("weight NOT BETWEEN 3 AND 5", app_props()),
    true = match("score NOT BETWEEN tiny_value AND quantity", app_props()),
    false = match("score NOT BETWEEN -tiny_value AND quantity", app_props()),

    %% Combined with other operators
    true = match("weight BETWEEN 4 AND 6 AND country = 'UK'", app_props()),
    true = match("(price BETWEEN 20 AND 30) OR (weight BETWEEN 5 AND 6)", app_props()),

    %% "a string cannot be used in an arithmetic expression"
    false = match("weight BETWEEN 1 AND 'Z'", app_props()),
    false = match("country BETWEEN 'A' AND 'Z'", app_props()),

    %% "Comparison or arithmetic with an unknown value always yields an unknown value."
    false = match("weight BETWEEN absent AND 10", app_props()),
    false = match("weight BETWEEN 2 AND absent", app_props()),
    false = match("weight BETWEEN absent AND absent", app_props()),
    false = match("absent BETWEEN 2 AND 10", app_props()),
    false = match("weight NOT BETWEEN absent AND 10", app_props()),
    false = match("weight NOT BETWEEN 2 AND absent", app_props()),
    false = match("weight NOT BETWEEN absent AND absent", app_props()),
    false = match("absent NOT BETWEEN 2 AND 10", app_props()).

null_handling(_Config) ->
    %% IS NULL / IS NOT NULL
    true = match("missing IS NULL", app_props()),
    true = match("absent IS NULL", app_props()),
    false = match("country IS NULL", app_props()),
    true = match("country IS NOT NULL", app_props()),
    false = match("missing IS NOT NULL", app_props()),
    false = match("absent IS NOT NULL", app_props()),
    true = match("country = 'UK' AND missing IS NULL", app_props()),
    true = match("country = 'France' OR weight IS NOT NULL", app_props()),

    %% "SQL treats a NULL value as unknown.
    %% Comparison or arithmetic with an unknown value always yields an unknown value."
    false = match("missing > 0", app_props()),
    false = match("0 < missing", app_props()),
    false = match("0 > absent", app_props()),
    false = match("0 = missing", app_props()),
    false = match("missing >= 0", app_props()),
    false = match("missing < 0", app_props()),
    false = match("missing <= 0", app_props()),
    false = match("missing = 0", app_props()),
    false = match("missing <> 0", app_props()),
    false = match("missing = missing", app_props()),
    false = match("absent = absent", app_props()),
    false = match("missing AND true", app_props()),
    false = match("missing OR false", app_props()).

literals(_Config) ->
    %% Exact numeric literals
    true = match("5 = 5", app_props()),
    true = match("weight = 5", app_props()),

    %% "Approximate literals use the Java floating-point literal syntax."
    true = match("10.5 = 10.5", app_props()),
    true = match("price = 10.5", app_props()),
    true = match("5.0 > 4.999", app_props()),
    true = match("10 = 10.", app_props()),
    true = match("0 = 0.0", app_props()),
    true = match("0 = 0.", app_props()),
    true = match("0 = .0", app_props()),

    true = match("weight = 5.0", app_props()), % int = float
    true = match("5. = weight", app_props()), % float = int

    %% String literals
    true = match("'UK' = 'UK'", app_props()),
    true = match("country = 'UK'", app_props()),

    %% Boolean literals
    true = match("TRUE = TRUE", app_props()),
    true = match("active = TRUE", app_props()),
    true = match("TRUE", app_props()),
    true = match("active", app_props()),
    true = match("FALSE = FALSE", app_props()),
    true = match("premium = FALSE", app_props()),
    false = match("FALSE", app_props()),
    false = match("premium", app_props()),

    %% Literals in expressions
    true = match("weight + 2 > 6", app_props()),
    true = match("price * 2 > 20.0", app_props()),
    true = match("'UK' <> 'US'", app_props()).

scientific_notation(_Config) ->
    %% Basic scientific notation comparisons
    true = match("distance = 1.2E6", app_props()),
    true = match("distance = 1200000.0", app_props()),
    true = match("tiny_value = 3.5E-4", app_props()),
    true = match("tiny_value = 0.00035", app_props()),

    %% Scientific notation literals in expressions
    true = match("1.2E3 = 1200", app_props()),
    true = match("5E2 = 500", app_props()),
    true = match("5.E2 = 500", app_props()),
    true = match("-5E-2 = -0.05", app_props()),
    true = match("-5.E-2 = -0.05", app_props()),
    true = match(".5E-1 = 0.05", app_props()),
    true = match("-.5E-1 = -0.05", app_props()),
    true = match("1E0 = 1", app_props()),

    %% Arithmetic with scientific notation
    true = match("distance / 1.2E5 = 10", app_props()),
    true = match("tiny_value * 1E6 = 350", app_props()),
    true = match("1.5E2 + 2.5E2 = 400", app_props()),
    true = match("3E3 - 2E3 = 1000", app_props()),

    %% Comparisons with scientific notation
    true = match("distance > 1E6", app_props()),
    true = match("tiny_value < 1E-3", app_props()),
    true = match("distance BETWEEN 1E6 AND 2E6", app_props()),

    %% Mixed numeric formats
    true = match("distance / 1200 = 1000", app_props()),
    true = match("large_value + tiny_value >= large_value", app_props()),
    true = match("large_value + large_value > large_value", app_props()).

precedence_and_parentheses(_Config) ->
    %% Arithmetic precedence
    true = match("weight + 2 * 3 = 11", app_props()),
    true = match("(weight + 2) * 3 = 21", app_props()),
    true = match("weight + weight * quantity - -temperature / 2 = 502.5", app_props()),

    %% "Logical operators in precedence order: NOT, AND, OR"
    true = match("NOT country = 'US' AND weight > 3", app_props()),
    true = match("weight > 3 AND NOT country = 'US'", app_props()),
    true = match("NOT (country = 'US' AND weight > 3)", app_props()),
    true = match("NOT country = 'US' OR country = 'France' AND weight > 3", app_props()),
    true = match("country = 'France' AND weight > 3 OR NOT country = 'US'", app_props()),

    %% Mixed precedence
    true = match("weight * 2 > 5 + 3", app_props()),
    true = match("price < 20 OR country = 'US' AND weight > 3", app_props()),
    true = match("weight > 3 AND price < 20 OR country = 'US'", app_props()),
    false = match("weight > 3 AND (price > 20 OR country = 'US')", app_props()),

    %% Complex parentheses nesting
    true = match("((weight > 3) AND (price < -1)) OR ((country = 'UK') AND (city = 'London'))", app_props()),
    true = match("weight > 3 AND price < -1 OR country = 'UK' AND city = 'London'", app_props()),
    true = match("(weight + (price * 2)) > (score + 15)", app_props()).

%% "Only like type values can be compared. One exception is that it is
%% valid to compare exact numeric values and approximate numeric values.
%% If the comparison of non-like type values is attempted, the value of the operation is false."
type_handling(_Config) ->
    %% Numeric comparisons
    true = match("weight = 5", app_props()), % int = int
    true = match("weight = 5.0", app_props()), % int = float
    true = match("price = 10.5", app_props()), % float = float

    %% String and numeric
    false = match("country = 5", app_props()), % string != number
    false = match("weight = 'UK'", app_props()), % number != string

    %% Boolean comparisons
    true = match("active = TRUE", app_props()),
    true = match("active <> FALSE", app_props()),
    false = match("TRUE = 1", app_props()), % boolean != number
    false = match("active = 1", app_props()), % boolean != number
    false = match("TRUE = 'TRUE'", app_props()), % boolean != string
    false = match("active = 'TRUE'", app_props()), % boolean != string

    %% Type-specific operators
    true = match("description LIKE '%test%'", app_props()), % LIKE only works on strings
    false = match("weight LIKE '5'", app_props()), % LIKE doesn't work on numbers

    %% Arithmetic with different types
    true = match("weight + price = 15.5", app_props()), % int + float = float
    true = match("weight * discount = 1.25", app_props()), % int * float = float

    %% Division by zero is undefined
    false = match("weight / 0 > 0", app_props()),
    false = match("weight / score = 5", app_props()),
    false = match("0 / 0 = 0", app_props()),
    false = match("0 / 0.0 = 0", app_props()),
    false = match("0 / 0. = 0", app_props()),
    false = match("-1 / 0 = 0", app_props()),
    false = match("score / score = score", app_props()),

    true = match("0.0 / 1 = 0", app_props()),

    %% Type incompatibility
    false = match("country + weight = 'UK5'", app_props()), % can't add string and number
    false = match("active + premium = 1", app_props()). % can't add booleans

complex_expressions(_Config) ->
    true = match(
             "country = 'UK' AND price > 10.0 AND (weight BETWEEN 4 AND 6) AND description LIKE '%test%'",
             app_props()
            ),
    true = match(
             "(country IN ('UK', 'US') OR city = 'London') AND (weight * 2 >= 10) AND NOT premium",
             app_props()
            ),
    true = match(
             "price * quantity * (1 - discount) > 500",
             app_props()
            ),
    true = match(
             "country = 'UK' AND (city = 'London' OR description LIKE '%test%') AND" ++
             "(weight > 3 OR premium = TRUE) AND price <= 20",
             app_props()
            ),
    true = match(
             "percentage >= 0 AND percentage <= 100 AND weight + temperature = 0",
             app_props()
            ),
    true = match(
             "((country = 'UK' OR country = 'US') AND (city IN ('London', 'New York', 'Paris'))) OR " ++
             "(price * (1 - discount) < 10.0 AND quantity > 50 AND description LIKE '%test%') OR " ++
             "(active = TRUE AND premium = FALSE AND (weight BETWEEN 4 AND 10))",
             app_props()
            ).

%% "Predefined selector literals and operator names are [...] case insensitive."
%% "Identifiers are case sensitive."
case_sensitivity(_Config) ->
    AppProps = app_props(),

    %% 1. Test that operators and literals are case insensitive
    true = match("country = 'UK' AnD weight = 5", AppProps),
    true = match("country = 'UK' and weight = 5", AppProps),
    true = match("country = 'France' Or weight < 6", AppProps),
    true = match("country = 'France' or weight < 6", AppProps),
    true = match("NoT country = 'France'", AppProps),
    true = match("not country = 'France'", AppProps),
    true = match("weight BeTwEeN 3 AnD 7", AppProps),
    true = match("weight between 3 AnD 7", AppProps),
    true = match("description LiKe '%test%'", AppProps),
    true = match("description like '%test%'", AppProps),
    true = match("country In ('US', 'UK', 'France')", AppProps),
    true = match("country in ('US', 'UK', 'France')", AppProps),
    true = match("missing Is NuLl", AppProps),
    true = match("missing is null", AppProps),
    true = match("active = TrUe", AppProps),
    true = match("active = true", AppProps),
    true = match("premium = FaLsE", AppProps),
    true = match("premium = false", AppProps),
    true = match("distance = 1.2e6", app_props()),
    true = match("tiny_value = 3.5e-4", app_props()),
    true = match("3 = 3e0", app_props()),
    true = match("3 = 3e-0", app_props()),
    true = match("300 = 3e2", app_props()),
    true = match("0.03 = 3e-2", app_props()),

    %% 2. Test that identifiers are case sensitive
    AppPropsCaseSensitiveKeys = AppProps ++ [{{utf8, <<"COUNTRY">>}, {utf8, <<"France">>}},
                                             {{utf8, <<"Weight">>}, {uint, 10}}],

    true = match("country = 'UK'", AppPropsCaseSensitiveKeys),
    true = match("COUNTRY = 'France'", AppPropsCaseSensitiveKeys),
    true = match("Weight = 10", AppPropsCaseSensitiveKeys),

    false = match("COUNTRY = 'UK'", AppPropsCaseSensitiveKeys),
    false = match("country = 'France'", AppPropsCaseSensitiveKeys),
    false = match("weight = 10", AppPropsCaseSensitiveKeys),
    false = match("WEIGHT = 5", AppPropsCaseSensitiveKeys),

    true = match(
             "country = 'UK' aNd COUNTRY = 'France' and (weight Between 4 AnD 6) AND Weight = 10",
             AppPropsCaseSensitiveKeys
            ).

%% "Whitespace is the same as that defined for Java:
%% space, horizontal tab, form feed and line terminator."
whitespace_handling(_Config) ->
    %% 1. Space
    true = match("country = 'UK'", app_props()),

    %% 2. Multiple spaces
    true = match("country    =    'UK'", app_props()),

    %% 3. Horizontal tab (\t)
    true = match("country\t=\t'UK'", app_props()),

    %% 4. Form feed (\f)
    true = match("country\f=\f'UK'", app_props()),

    %% 5. Line terminators (\n line feed, \r carriage return)
    true = match("country\n=\n'UK'", app_props()),
    true = match("country\r=\r'UK'", app_props()),

    %% 6. Mixed whitespace
    true = match("country \t\f\n\r = \t\f\n\r 'UK'", app_props()),

    %% 7. Complex expression with various whitespace
    true = match("country\t=\t'UK'\nAND\rweight\f>\t3", app_props()),

    %% 8. Ensure whitespace is not required
    true = match("country='UK'AND weight=5", app_props()),

    %% 9. Whitespace inside string literals should be preserved
    true = match("description = 'This is a test message'", app_props()),

    %% 10. Whitespace at beginning and end of expression
    true = match(" \t\n\r country = 'UK' \t\n\r ", app_props()).

%% "An identifier is an unlimited-length character sequence that must begin with a
%% Java identifier start character; all following characters must be Java identifier
%% part characters. An identifier start character is any character for which the method
%% Character.isJavaIdentifierStart returns true. This includes '_' and '$'. An
%% identifier part character is any character for which the method
%% Character.isJavaIdentifierPart returns true."
identifier_rules(_Config) ->
    Identifiers = [<<"simple">>,
                   <<"a1b2c3">>,
                   <<"x">>,
                   <<"_underscore">>,
                   <<"$dollar">>,
                   <<"_">>,
                   <<"$">>,
                   <<"with_underscore">>,
                   <<"with$dollar">>,
                   <<"mixed_$_identifiers_$_123">>],
    AppProps = [{{utf8, Id}, {utf8, <<"value">>}} || Id <- Identifiers],
    true = match("simple = 'value'", AppProps),
    true = match("a1b2c3 = 'value'", AppProps),
    true = match("x = 'value'", AppProps),
    true = match("_underscore = 'value'", AppProps),
    true = match("$dollar = 'value'", AppProps),
    true = match("_ = 'value'", AppProps),
    true = match("$ = 'value'", AppProps),
    true = match("with_underscore = 'value'", AppProps),
    true = match("with$dollar = 'value'", AppProps),
    true = match("mixed_$_identifiers_$_123 = 'value'", AppProps).

header_section(_Config) ->
    Hdr = #'v1_0.header'{priority = {ubyte, 7}},
    Ps = #'v1_0.properties'{},
    APs = [],
    true = match("header.priority > 5", Hdr, Ps, APs),
    true = match("header.priority = 7", Hdr, Ps, APs),
    false = match("header.priority < 7", Hdr, Ps, APs),

    %% Since the default priority is 4 in both AMQP and JMS, we expect the
    %% following expression to evaluate to true if matched against a message
    %% without an explicit priority level set.
    true = match("header.priority = 4", []).

properties_section(_Config) ->
    Ps = #'v1_0.properties'{
            message_id = {utf8, <<"id-123">>},
            user_id = {binary,<<"some user ID">>},
            to = {utf8, <<"to some queue">>},
            subject = {utf8, <<"some subject">>},
            reply_to = {utf8, <<"reply to some topic">>},
            correlation_id = {ulong, 789},
            content_type = {symbol, <<"text/plain">>},
            content_encoding = {symbol, <<"deflate">>},
            absolute_expiry_time = {timestamp, 1311999988888},
            creation_time = {timestamp, 1311704463521},
            group_id = {utf8, <<"some group ID">>},
            group_sequence = {uint, 999},
            reply_to_group_id = {utf8, <<"other group ID">>}},
    APs = [],

    true = match("properties.message-id = 'id-123'", Ps, APs),
    false = match("'id-123' <> properties.message-id", Ps, APs),
    true = match("properties.message-id LIKE 'id-%'", Ps, APs),
    true = match("properties.message-id IN ('id-123', 'id-456')", Ps, APs),

    true = match("properties.user-id = 'some user ID'", Ps, APs),
    true = match("properties.user-id LIKE '%user%'", Ps, APs),
    false = match("properties.user-id = 'other user ID'", Ps, APs),

    true = match("properties.to = 'to some queue'", Ps, APs),
    true = match("properties.to LIKE 'to some%'", Ps, APs),
    true = match("properties.to NOT LIKE '%topic'", Ps, APs),

    true = match("properties.subject = 'some subject'", Ps, APs),
    true = match("properties.subject LIKE '%subject'", Ps, APs),
    true = match("properties.subject IN ('some subject', 'other subject')", Ps, APs),

    true = match("properties.reply-to = 'reply to some topic'", Ps, APs),
    true = match("properties.reply-to LIKE 'reply%topic'", Ps, APs),
    false = match("properties.reply-to LIKE 'reply%queue'", Ps, APs),

    true = match("properties.correlation-id = 789", Ps, APs),
    true = match("500 < properties.correlation-id", Ps, APs),
    true = match("properties.correlation-id BETWEEN 700 AND 800", Ps, APs),
    false = match("properties.correlation-id < 700", Ps, APs),

    true = match("properties.content-type = 'text/plain'", Ps, APs),
    true = match("properties.content-type LIKE 'text/%'", Ps, APs),
    true = match("properties.content-type IN ('text/plain', 'text/html')", Ps, APs),

    true = match("'deflate' = properties.content-encoding", Ps, APs),
    false = match("properties.content-encoding = 'gzip'", Ps, APs),
    true = match("properties.content-encoding NOT IN ('gzip', 'compress')", Ps, APs),

    true = match("properties.absolute-expiry-time = 1311999988888", Ps, APs),
    true = match("properties.absolute-expiry-time > 1311999988000", Ps, APs),
    true = match("properties.absolute-expiry-time BETWEEN 1311999988000 AND 1311999989000", Ps, APs),

    true = match("properties.creation-time = 1311704463521", Ps, APs),
    true = match("properties.creation-time < 1311999988888", Ps, APs),
    true = match("properties.creation-time NOT BETWEEN 1311999988000 AND 1311999989000", Ps, APs),

    true = match("properties.group-id = 'some group ID'", Ps, APs),
    true = match("properties.group-id LIKE 'some%ID'", Ps, APs),
    false = match("properties.group-id = 'other group ID'", Ps, APs),

    true = match("properties.group-sequence = 999", Ps, APs),
    true = match("properties.group-sequence >= 999", Ps, APs),
    true = match("properties.group-sequence BETWEEN 900 AND 1000", Ps, APs),
    false = match("properties.group-sequence > 999", Ps, APs),

    true = match("properties.reply-to-group-id = 'other group ID'", Ps, APs),
    true = match("properties.reply-to-group-id LIKE '%group ID'", Ps, APs),
    true = match("properties.reply-to-group-id <> 'some group ID'", Ps, APs),
    true = match("properties.reply-to-group-id IS NOT NULL", Ps, APs),
    false = match("properties.reply-to-group-id IS NULL", Ps, APs),

    true = match("properties.message-id = 'id-123' and 'some subject' = properties.subject", Ps, APs),
    true = match("properties.group-sequence < 500 or properties.correlation-id > 700", Ps, APs),
    true = match("(properties.content-type LIKE 'text/%') AND properties.content-encoding = 'deflate'", Ps, APs),

    true = match("properties.subject IS NULL", #'v1_0.properties'{}, APs),
    false = match("properties.subject IS NOT NULL", #'v1_0.properties'{}, APs).

multiple_sections(_Config) ->
    Hdr = #'v1_0.header'{durable = true,
                         priority = {ubyte, 7}},
    Ps = #'v1_0.properties'{
            message_id = {utf8, <<"id-123">>},
            user_id = {binary,<<"some user ID">>},
            to = {utf8, <<"to some queue">>},
            subject = {utf8, <<"some subject">>},
            reply_to = {utf8, <<"reply to some topic">>},
            correlation_id = {ulong, 789},
            content_type = {symbol, <<"text/plain">>},
            content_encoding = {symbol, <<"deflate">>},
            absolute_expiry_time = {timestamp, 1311999988888},
            creation_time = {timestamp, 1311704463521},
            group_id = {utf8, <<"some group ID">>},
            group_sequence = {uint, 999},
            reply_to_group_id = {utf8, <<"other group ID">>}},
    APs = [{{utf8, <<"key_1">>}, {byte, -1}}],

    true = match("-1.0 = key_1 AND 4 < header.priority AND properties.group-sequence > 90", Hdr, Ps, APs),
    false = match("-1.0 = key_1 AND 4 < header.priority AND properties.group-sequence < 90", Hdr, Ps, APs).

parse_errors(_Config) ->
    %% Parsing a non-UTF-8 encoded message selector should fail.
    ?assertEqual(error, parse([255])),
    %% Invalid token.
    ?assertEqual(error, parse("!!!")),
    %% Invalid grammar.
    ?assertEqual(error, parse("AND NOT")),
    %% Escape charater at end of pattern hould fail because it doesn't make any sense.
    ?assertEqual(error, parse("id LIKE 'pattern*' ESCAPE '*'")),
    ?assertEqual(error, parse("id LIKE '_pattern*' ESCAPE '*'")),
    %% Control characters in user provided pattern shouldn't be allowed.
    ?assertEqual(error, parse("id LIKE '\n'")),
    ?assertEqual(error, parse("id LIKE '\r'")),
    ?assertEqual(error, parse("id LIKE '_\n'")),
    ?assertEqual(error, parse("id LIKE '%\r'")),
    ?assertEqual(error, parse("id LIKE '!\r' ESCAPE '!'")),
    %% Expressions with more than 4096 characters should be prohibited.
    ManyCharacters = lists:append(lists:duplicate(4096, "x")) ++ " IS NULL",
    ?assertEqual(error, parse(ManyCharacters)),
    %% Expression with more than 200 tokens should be prohibited.
    ManyTokens = "id IN (" ++ string:join(["'" ++ integer_to_list(N) ++ "'"|| N <- lists:seq(1, 100)], ",") ++ ")",
    ?assertEqual(error, parse(ManyTokens)),
    %% "header." or "properties." prefixed identifiers
    %% that do not refer to supported field names are disallowed.
    ?assertEqual(error, parse("header.invalid")),
    ?assertEqual(error, parse("properties.invalid")),
    ok.

%%%===================================================================
%%% Helpers
%%%===================================================================

app_props() ->
    [
     %% String or symbolic values
     {{utf8, <<"country">>}, {symbol, <<"UK">>}},
     {{utf8, <<"city">>}, {utf8, <<"London">>}},
     {{utf8, <<"description">>}, {utf8, <<"This is a test message">>}},
     {{utf8, <<"currency">>}, {symbol, <<"GBP">>}},
     {{utf8, <<"product_id">>}, {symbol, <<"ABC_123">>}},

     %% Numeric values
     {{utf8, <<"weight">>}, {ushort, 5}},
     {{utf8, <<"price">> }, {double, 10.5}},
     {{utf8, <<"quantity">>}, {uint, 100}},
     {{utf8, <<"discount">>}, {double, 0.25}},
     {{utf8, <<"temperature">>}, {int, -5}},
     {{utf8, <<"score">>}, {ulong, 0}},
     %% Scientific notation values
     {{utf8, <<"distance">>}, {float, 1.2E6}}, % 1,200,000
     {{utf8, <<"tiny_value">>}, {double, 3.5E-4}}, % 0.00035
     {{utf8, <<"large_value">>}, {double, 6.02E23}}, % Avogadro's number

     %% Boolean values
     {{utf8, <<"active">>}, true},
     {{utf8, <<"premium">>}, {boolean, false}},

     %% Special cases
     {{utf8, <<"missing">>}, null},
     {{utf8, <<"percentage">>}, {ubyte, 75}}
    ].

match(Selector, AppProps) ->
    match(Selector, #'v1_0.properties'{}, AppProps).

match(Selector, Props, AppProps) ->
    match(Selector, #'v1_0.header'{}, Props, AppProps).

match(Selector, Header, Props, AppProps)
  when is_list(AppProps) ->
    {ok, ParsedSelector} = parse(Selector),
    AP = #'v1_0.application_properties'{content = AppProps},
    Body = #'v1_0.amqp_value'{content = {symbol, <<"some message body">>}},
    Sections = [Header, Props, AP, Body],
    Payload = amqp_encode_bin(Sections),
    Mc = mc_amqp:init_from_stream(Payload, #{}),
    rabbit_amqp_filter_jms:eval(ParsedSelector, Mc).

parse(Selector) ->
    Descriptor = {ulong, ?DESCRIPTOR_CODE_SELECTOR_FILTER},
    Filter = {described, Descriptor, {utf8, Selector}},
    rabbit_amqp_filter_jms:parse(Filter).

amqp_encode_bin(L) when is_list(L) ->
    iolist_to_binary([amqp10_framing:encode_bin(X) || X <- L]).
