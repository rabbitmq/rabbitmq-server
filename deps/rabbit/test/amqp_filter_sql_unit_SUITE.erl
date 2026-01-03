%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2026 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.

-module(amqp_filter_sql_unit_SUITE).

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
       single_quoted_strings,
       double_quoted_strings,
       binary_constants,
       like_operator,
       in_operator,
       null_handling,
       literals,
       scientific_notation,
       precedence_and_parentheses,
       type_handling,
       complex_expressions,
       case_sensitivity,
       whitespace_handling,
       identifiers,
       header_section,
       properties_section,
       multiple_sections,
       section_qualifier,
       utc_function,
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
    true = match("country != 'US'", app_props()),
    false = match("country <> 'UK'", app_props()),
    false = match("country != 'UK'", app_props()),

    %% Greater than
    true = match("weight > 3", app_props()),
    false = match("weight > 5", app_props()),
    true = match("country > 'DE'", app_props()),
    false = match("country > 'US'", app_props()),
    true = match("'Zurich' > city", app_props()),

    %% Less than
    true = match("weight < 10", app_props()),
    false = match("weight < 5", app_props()),
    true = match("country < 'US'", app_props()),
    false = match("country < 'DE'", app_props()),

    %% Greater than or equal
    true = match("weight >= 5", app_props()),
    true = match("weight >= 4", app_props()),
    false = match("weight >= 6", app_props()),
    true = match("country >= 'UK'", app_props()),
    true = match("country >= 'DE'", app_props()),
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
    true = match("country <= 'US'", app_props()),
    true = match("country <= 'UK'", app_props()),
    false = match("country <= 'DE'", app_props()),

    %% "Boolean comparison is restricted to = and <>."
    %% "If the comparison of non-like type values is attempted, the value of the operation is false."
    true = match("active = TRUE", app_props()),
    true = match("premium = FALSE", app_props()),
    false = match("premium <> FALSE", app_props()),
    false = match("premium >= 'false'", app_props()),
    false = match("premium <= 'false'", app_props()),
    false = match("premium >= 0", app_props()),
    false = match("premium <= 0", app_props()),

    false = match("weight = '5'", app_props()),
    false = match("weight >= '5'", app_props()),
    false = match("weight <= '5'", app_props()),
    false = match("country <= TRUE", app_props()),
    false = match("country >= TRUE", app_props()),
    false = match("country > 1", app_props()),
    false = match("country >= 1", app_props()),
    false = match("country < 1", app_props()),
    false = match("country <= 1", app_props()).

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

    %% Modulo
    true = match("weight % 2 = 1", app_props()),
    true = match("quantity % weight = 0.00", app_props()),
    true = match("score = quantity % quantity", app_props()),
    true = match("quantity % percentage = 25", app_props()),
    true = match("24 < quantity % percentage", app_props()),
    true = match("7 % temperature = 2", app_props()), % mod negative number
    false = match("quantity % score = 0", app_props()), % mod 0
    true = match("101 % percentage % weight = 1", app_props()), % left associative
    true = match("(quantity + 1) % percentage % weight = 1", app_props()),
    true = match("101 % (percentage % 30) = 11", app_props()),

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

single_quoted_strings(_Config) ->
    %% "Two strings are equal if and only if they contain the same sequence of characters."
    false = match("country = '🇬🇧'", app_props()),
    true = match("country = '🇬🇧'", [{{utf8, <<"country">>}, {utf8, <<"🇬🇧"/utf8>>}}]),

    %%  "A quotation mark inside the string is represented by two consecutive quotation marks."
    true = match("'UK''s' = 'UK''s'", app_props()),
    true = match("country = 'UK''s'", [{{utf8, <<"country">>}, {utf8, <<"UK's">>}}]),
    true = match("country = '🇬🇧''s'", [{{utf8, <<"country">>}, {utf8, <<"🇬🇧's"/utf8>>}}]),
    true = match("country = ''", [{{utf8, <<"country">>}, {utf8, <<>>}}]),
    true = match("country = ''''", [{{utf8, <<"country">>}, {utf8, <<$'>>}}]).

double_quoted_strings(_Config) ->
    %% Basic double-quoted string equality
    true = match("\"UK\" = \"UK\""),
    true = match("country = \"UK\"", app_props()),
    false = match("country = \"US\"", app_props()),

    %% Mix of single and double quotes
    true = match("'UK' = \"UK\""),
    true = match("\"UK\" = 'UK'"),
    true = match("country = 'UK' AND country = \"UK\"", app_props()),

    %% Empty strings
    true = match("\"\" = ''"),
    true = match("\"\" = country", [{{utf8, <<"country">>}, {utf8, <<>>}}]),
    true = match("'' = country", [{{utf8, <<"country">>}, {utf8, <<>>}}]),

    %% Escaped quotes inside strings
    true = match("country = \"UK\"\"s\"", [{{utf8, <<"country">>}, {utf8, <<"UK\"s">>}}]),
    true = match("country = \"\"\"\"", [{{utf8, <<"country">>}, {utf8, <<$">>}}]),
    true = match("country = \"\"\"\"\"\"", [{{utf8, <<"country">>}, {utf8, <<$", $">>}}]),
    true = match(" \"\"\"\"\"\" = '\"\"' "),
    true = match("\"UK\"\"s\" = \"UK\"\"s\""),
    true = match("\"They said \"\"Hello\"\"\" = key", [{{utf8, <<"key">>}, {utf8, <<"They said \"Hello\"">>}}]),

    %% Single quotes inside double-quoted strings (no escaping needed)
    true = match("country = \"UK's\"", [{{utf8, <<"country">>}, {utf8, <<"UK's">>}}]),
    true = match("key = \"It's working\"", [{{utf8, <<"key">>}, {utf8, <<"It's working">>}}]),

    %% Double quotes inside single-quoted strings (no escaping needed)
    true = match("country = 'UK\"s'", [{{utf8, <<"country">>}, {utf8, <<"UK\"s">>}}]),
    true = match("key = 'They said \"Hello\"'", [{{utf8, <<"key">>}, {utf8, <<"They said \"Hello\"">>}}]),

    %% LIKE operator with double-quoted strings
    true = match("description LIKE \"%test%\"", app_props()),
    true = match("description LIKE \"This is a %\"", app_props()),
    true = match("country LIKE \"U_\"", app_props()),
    true = match("country LIKE \"UK\"", app_props()),
    false = match("country LIKE \"US\"", app_props()),

    %% ESCAPE with double-quoted strings
    true = match("product_id LIKE \"ABC\\_%\" ESCAPE \"\\\"", app_props()),
    true = match("key LIKE \"z_%\" ESCAPE \"z\"", [{{utf8, <<"key">>}, {utf8, <<"_foo">>}}]),

    %% IN operator with double-quoted strings
    true = match("country IN (\"US\", \"UK\", \"France\")", app_props()),
    true = match("country IN ('US', \"UK\", 'France')", app_props()),
    true = match("\"London\" IN (city, country)", app_props()),
    false = match("country IN (\"US\", \"France\")", app_props()),

    %% NOT LIKE with double-quoted strings
    true = match("country NOT LIKE \"US\"", app_props()),
    false = match("country NOT LIKE \"U_\"", app_props()),

    %% Complex expressions with double-quoted strings
    true = match("country = \"UK\" AND description LIKE \"%test%\" AND city = 'London'", app_props()),
    true = match("(country IN (\"UK\", \"US\") OR city = \"London\") AND weight > 3", app_props()),

    %% Unicode in double-quoted strings
    true = match("country = \"🇬🇧\"", [{{utf8, <<"country">>}, {utf8, <<"🇬🇧"/utf8>>}}]),
    true = match("\"🇬🇧\" = '🇬🇧'"),
    false = match("\"🇬🇧\" != '🇬🇧'"),
    true = match("country = \"🇬🇧\"\"s\"", [{{utf8, <<"country">>}, {utf8, <<"🇬🇧\"s"/utf8>>}}]),

    %% Whitespace inside double-quoted strings
    true = match("description = \"This is a test message\"", app_props()),
    true = match("key = \"  spaces  \"", [{{utf8, <<"key">>}, {utf8, <<"  spaces  ">>}}]),

    %% Properties section with double-quoted strings
    Props = #'v1_0.properties'{
               message_id = {utf8, <<"id-123">>},
               subject = {utf8, <<"test">>}
              },
    true = match("p.message_id = \"id-123\"", Props, []),
    true = match("p.subject = \"test\"", Props, []),
    true = match("p.message_id = 'id-123' AND p.subject = \"test\"", Props, []),

    true = match("country < \"US\"", app_props()),
    true = match("\"US\" >= country", app_props()),
    ok.

binary_constants(_Config) ->
    true = match("0x48656C6C6F = 0x48656C6C6F", app_props()),  % "Hello" = "Hello"
    false = match("0x48656C6C6F = 0x48656C6C6F21", app_props()), % "Hello" != "Hello!"

    AppProps = [
                {{utf8, <<"data">>}, {binary, <<"Hello">>}},
                {{utf8, <<"signature">>}, {binary, <<16#DE, 16#AD, 16#BE, 16#EF>>}},
                {{utf8, <<"empty">>}, {binary, <<>>}},
                {{utf8, <<"single">>}, {binary, <<255>>}},
                {{utf8, <<"zeros">>}, {binary, <<0, 0, 0>>}}
               ],

    true = match("data = 0x48656C6C6F", AppProps),  % data = "Hello"
    false = match("data = 0x48656C6C", AppProps),   % data != "Hell"
    true = match("signature = 0xDEADBEEF", AppProps),
    false = match("signature = 0xDEADBEEE", AppProps),
    true = match("single = 0xFF", AppProps),
    false = match("single = 0xFE", AppProps),
    true = match("zeros = 0x000000", AppProps),
    false = match("empty = 0x00", AppProps),

    true = match("signature IN (0xCAFEBABE, 0xDEADBEEF)", AppProps),
    false = match("signature IN (0xCAFEBABE, 0xFEEDFACE)", AppProps),
    true = match("data IN (0x48656C6C6F, 0x576F726C64)", AppProps), % "Hello" or "World"
    true = match("data IN (data)", AppProps),

    true = match("signature NOT IN (0xCAFEBABE, 0xFEEDFACE)", AppProps),
    false = match("signature NOT IN (0xDEADBEEF, 0xCAFEBABE)", AppProps),

    true = match("0xAB <> 0xAC", AppProps),
    true = match("0xAB != 0xAC", AppProps),
    false = match("0xAB = 0xAC", AppProps),

    true = match("data = 0x48656C6C6F AND signature = 0xDEADBEEF", AppProps),
    true = match("data = 0x576F726C64 OR data = 0x48656C6C6F", AppProps),
    false = match("data = 0x576F726C64 AND signature = 0xDEADBEEF", AppProps),

    true = match("missing_binary IS NULL", AppProps),
    false = match("data IS NULL", AppProps),
    true = match("data IS NOT NULL", AppProps),

    Props = #'v1_0.properties'{
               user_id = {binary, <<255>>},
               correlation_id = {binary, <<"correlation">>}
              },
    true = match("p.user_id = 0xFF", Props, []),
    false = match("p.user_id = 0xAA", Props, []),
    true = match("p.correlation_id = 0x636F7272656C6174696F6E", Props, []),

    true = match(
             "(data = 0x576F726C64 OR data = 0x48656C6C6F) AND signature IN (0xDEADBEEF, 0xCAFEBABE)",
             AppProps
            ),

    %% Whitespace around binary constants
    true = match("signature = 0xDEADBEEF", AppProps),
    true = match("signature=0xDEADBEEF", AppProps),
    true = match("signature =  0xDEADBEEF", AppProps),

    false = match("weight = 0x05", app_props()), % number != binary
    false = match("active = 0x01", app_props()), % boolean != binary

    %% Arithmetic operations with binary constants should fail
    %% since binaries are not numerical values.
    false = match("0x01 + 0x02 = 0x03", AppProps),
    false = match("signature + 1 = 0xDEADBEF0", AppProps),

    %% "The left operand is of greater value than the right operand if:
    %% the left operand is of the same type as the right operand and the value is greater"
    true = match("0xBB > 0xAA", AppProps),
    true = match("0x010101 > zeros", AppProps),
    true = match("0x010101 >= zeros", AppProps),
    false = match("0x010101 < zeros", AppProps),
    false = match("0x010101 <= zeros", AppProps),
    true = match("0xFE < single", AppProps),
    true = match("0xFE <= single", AppProps),
    ok.

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
    true = match("'London' IN (city, country)", app_props()),

    true = match("price IN (h.priority - 0.5)",
                 #'v1_0.header'{priority = {ubyte, 11}}, #'v1_0.properties'{}, app_props()),
    false = match("price IN (h.priority + 0.5)",
                  #'v1_0.header'{priority = {ubyte, 11}}, #'v1_0.properties'{}, app_props()),
    true = match("10.0 IN (TRUE, p.group_sequence)",
                 #'v1_0.properties'{group_sequence = {uint, 10}}, app_props()),
    true = match("10.00 IN (FALSE, p.group_sequence)",
                 #'v1_0.properties'{group_sequence = {uint, 10}}, app_props()),

    %% NOT IN
    true = match("country NOT IN ('US', 'France', 'Germany')", app_props()),
    true = match("country NOT IN ('🇬🇧')", app_props()),
    false = match("country NOT IN ('🇫🇷', '🇬🇧')", AppPropsUtf8),
    false = match("country NOT IN ('US', 'UK', 'France')", app_props()),
    false = match("'London' NOT IN (city, country)", app_props()),
    false = match("10.0 NOT IN (TRUE, p.group_sequence)",
                  #'v1_0.properties'{group_sequence = {uint, 10}}, app_props()),

    %% Combined with other operators
    true = match("country IN ('UK', 'US') AND weight > 3", app_props()),
    true = match("city IN ('Berlin', 'Paris') OR country IN ('UK', 'US')", app_props()),

    %% "If identifier of an IN or NOT IN operation is NULL, the value of the operation is unknown."
    false = match("missing IN ('UK', 'US')", app_props()),
    false = match("absent IN ('UK', 'US')", app_props()),
    false = match("missing NOT IN ('UK', 'US')", app_props()),
    false = match("absent NOT IN ('UK', 'US')", app_props()).

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
    false = match("0 != missing", app_props()),
    false = match("missing = missing", app_props()),
    false = match("absent = absent", app_props()),
    false = match("missing AND TRUE", app_props()),
    false = match("missing OR FALSE", app_props()).

literals(_Config) ->
    %% Exact numeric literals
    true = match("5 = 5", app_props()),
    true = match("weight = 5", app_props()),

    %% "Approximate literals use the Java floating-point literal syntax."
    true = match("10.5 = 10.5", app_props()),
    true = match("price = 10.5", app_props()),
    true = match("5.0 > 4.999", app_props()),
    true = match("0 = 0.0", app_props()),

    true = match("weight = 5.0", app_props()), % int = float
    true = match("5.0 = weight", app_props()), % float = int

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
    true = match("'UK' <> 'US'", app_props()),

    ?assertEqual(error, parse("5. > 0")),
    ?assertEqual(error, parse(".5 > 0")),
    ?assertEqual(error, parse(".5E2 > 0")),
    ?assertEqual(error, parse("5E2 > 0")),
    ok.

scientific_notation(_Config) ->
    %% Basic scientific notation comparisons
    true = match("distance = 1.2E6", app_props()),
    true = match("distance = 1200000.0", app_props()),
    true = match("tiny_value = 3.5E-4", app_props()),
    true = match("tiny_value = 0.00035", app_props()),

    %% Scientific notation literals in expressions
    true = match("1.2E3 = 1200", app_props()),
    true = match("5.0E2 = 500", app_props()),
    true = match("5.0E+2 = 500", app_props()),
    true = match("5.0E-2 = 0.05", app_props()),
    true = match("-5.0E-2 = -0.05", app_props()),
    true = match("1.0E0 = 1", app_props()),

    %% Arithmetic with scientific notation
    true = match("distance / 1.2E5 = 10", app_props()),
    true = match("tiny_value * 1.0E+6 = 350", app_props()),
    true = match("1.5E2 + 2.5E2 = 400", app_props()),
    true = match("3.0E3 - 2.0E3 = 1000", app_props()),

    %% Comparisons with scientific notation
    true = match("distance > 1.0E6", app_props()),
    true = match("tiny_value < 1.0E-3", app_props()),

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
    true = match("weight = -(-81) % percentage -1", app_props()),
    true = match("weight -(-2.0) = -(-81) % (percentage -1)", app_props()),
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
    false = match("-1 / 0 = 0", app_props()),
    false = match("score / score = score", app_props()),

    true = match("0.0 / 1 = 0", app_props()),

    %% Type incompatibility
    false = match("country + weight = 'UK5'", app_props()), % can't add string and number
    false = match("active + premium = 1", app_props()). % can't add booleans

complex_expressions(_Config) ->
    true = match(
             "country = 'UK' AND price > 10.0 AND description LIKE '%test%' AND 2 = 101 % 3",
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
             "(active AND NOT premium)",
             app_props()
            ).

case_sensitivity(_Config) ->
    %% Test that identifiers are case sensitive
    AppPropsCaseSensitiveKeys = app_props() ++ [{{utf8, <<"COUNTRY">>}, {utf8, <<"France">>}},
                                                {{utf8, <<"Weight">>}, {uint, 10}}],

    true = match("country = 'UK'", AppPropsCaseSensitiveKeys),
    true = match("COUNTRY = 'France'", AppPropsCaseSensitiveKeys),
    true = match("Weight = 10", AppPropsCaseSensitiveKeys),

    false = match("COUNTRY = 'UK'", AppPropsCaseSensitiveKeys),
    false = match("country = 'France'", AppPropsCaseSensitiveKeys),
    false = match("weight = 10", AppPropsCaseSensitiveKeys),
    false = match("WEIGHT = 5", AppPropsCaseSensitiveKeys),

    true = match(
             "country = 'UK' AND COUNTRY = 'France' AND weight < 6 AND Weight = 10",
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

identifiers(_Config) ->
    Identifiers = [<<"simple">>,
                   <<"x">>,
                   <<"with_underscore_123">>,
                   <<"🥕"/utf8>>,
                   <<"ニンジン"/utf8>>,
                   <<"with    four    spaces">>,
                   <<" ">>,
                   <<"">>,
                   <<"NOT">>,
                   <<"not">>,
                   <<"AND">>,
                   <<"OR">>,
                   <<"IN">>,
                   <<"NULL">>,
                   <<"-">>,
                   <<"+">>,
                   <<"FALSE">>,
                   <<"!@#$%^&*()_+~`|{}?<>">>,
                   <<"[ key ]">>,
                   <<"[[key]]">>,
                   <<"]">>,
                   <<"][">>,
                   <<"[]">>,
                   <<"properties.to">>,
                   <<"p.to">>
                  ],
    AppProps = [{{utf8, Id}, {boolean, true}} || Id <- Identifiers],

    %% regular identifiers
    true = match("simple", AppProps),
    true = match("x", AppProps),

    true = match("with_underscore_123", AppProps),
    true = match("application_properties.with_underscore_123", AppProps),
    true = match("a.with_underscore_123", AppProps),
    true = match("[with_underscore_123]", AppProps),

    %% delimited identifiers
    true = match("[🥕]", AppProps),
    true = match("[ニンジン]", AppProps),
    true = match("[with    four    spaces]", AppProps),
    true = match("[ ]", AppProps),
    true = match("[]", AppProps),
    true = match("[]", AppProps),
    true = match("[NOT]", AppProps),
    true = match("[not]", AppProps),
    true = match("[AND]", AppProps),
    true = match("[OR]", AppProps),
    true = match("[IN]", AppProps),
    true = match("[NULL]", AppProps),
    true = match("[-]", AppProps),
    true = match("[+]", AppProps),
    true = match("[FALSE]", AppProps),
    true = match("[!@#$%^&*()_+~`|{}?<>]", AppProps),
    true = match("[[[ key ]]]", AppProps),
    true = match("[[[[[key]]]]]", AppProps),
    true = match("[]]]", AppProps),
    true = match("[]][[]", AppProps),
    true = match("[[[]]]", AppProps),

    Props = #'v1_0.properties'{to = {utf8, <<"q1">>}},
    true = match("properties.to = 'q1'", Props, AppProps),
    true = match("p.to = 'q1'", Props, AppProps),
    true = match("[properties.to] = TRUE", Props, AppProps),
    true = match("[p.to] = TRUE", Props, AppProps),

    %% Reserved keywords should not be allowed in regular identifiers.
    ?assertEqual(error, parse("not")),
    ?assertEqual(error, parse("Not")),
    ?assertEqual(error, parse("and")),
    ?assertEqual(error, parse("or")),
    ?assertEqual(error, parse("true")),
    ?assertEqual(error, parse("True")),
    ?assertEqual(error, parse("false")),
    ?assertEqual(error, parse("False")),
    ?assertEqual(error, parse("upper")),
    ?assertEqual(error, parse("lower")),
    ?assertEqual(error, parse("left")),
    ?assertEqual(error, parse("right")),
    ?assertEqual(error, parse("substring")),
    ?assertEqual(error, parse("utc")),
    ?assertEqual(error, parse("date")),
    ?assertEqual(error, parse("exists")),
    ?assertEqual(error, parse("null")),
    ?assertEqual(error, parse("is")),
    ?assertEqual(error, parse("Is")),
    ?assertEqual(error, parse("in")),
    ?assertEqual(error, parse("like")),
    ?assertEqual(error, parse("escape")),
    ?assertEqual(error, parse("nan")),
    ?assertEqual(error, parse("inf")),

    %% Regular identifier allows only:
    %% <letter> {<letter> | <underscore> | <digit> }
    ?assertEqual(error, parse("my.key")),
    ?assertEqual(error, parse("my$key")),
    ?assertEqual(error, parse("$mykey")),
    ?assertEqual(error, parse("_mykey")),
    ?assertEqual(error, parse("1mykey")),

    %% Even in delimited identifiers, "Control characters are not permitted".
    ?assertEqual(error, parse("[\n]")),
    ?assertEqual(error, parse("[\r]")),

    ok.

header_section(_Config) ->
    Hdr = #'v1_0.header'{priority = {ubyte, 7}},
    Ps = #'v1_0.properties'{},
    APs = [],
    true = match("h.priority > 5", Hdr, Ps, APs),
    true = match("h.priority = 7", Hdr, Ps, APs),
    false = match("h.priority < 7", Hdr, Ps, APs),

    %% Since the default priority is 4, we expect the following expression to evaluate
    %% to true if matched against a message without an explicit priority level set.
    true = match("h.priority = 4").

properties_section(_Config) ->
    Ps = #'v1_0.properties'{
            message_id = {utf8, <<"id-123">>},
            user_id = {binary, <<10, 11, 12>>},
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

    true = match("p.message_id = 'id-123'", Ps, APs),
    false = match("'id-123' != p.message_id", Ps, APs),
    true = match("p.message_id LIKE 'id-%'", Ps, APs),
    true = match("p.message_id IN ('id-123', 'id-456')", Ps, APs),

    true = match("p.user_id = 0x0A0B0C", Ps, APs),
    false = match("p.user_id = 0xFF", Ps, APs),

    true = match("p.to = 'to some queue'", Ps, APs),
    true = match("p.to LIKE 'to some%'", Ps, APs),
    true = match("p.to NOT LIKE '%topic'", Ps, APs),

    true = match("p.subject = 'some subject'", Ps, APs),
    true = match("p.subject LIKE '%subject'", Ps, APs),
    true = match("p.subject IN ('some subject', 'other subject')", Ps, APs),

    true = match("p.reply_to = 'reply to some topic'", Ps, APs),
    true = match("p.reply_to LIKE 'reply%topic'", Ps, APs),
    false = match("p.reply_to LIKE 'reply%queue'", Ps, APs),

    true = match("p.correlation_id = 789", Ps, APs),
    true = match("500 < p.correlation_id", Ps, APs),
    false = match("p.correlation_id < 700", Ps, APs),

    true = match("p.content_type = 'text/plain'", Ps, APs),
    true = match("p.content_type LIKE 'text/%'", Ps, APs),
    true = match("p.content_type IN ('text/plain', 'text/html')", Ps, APs),

    true = match("'deflate' = p.content_encoding", Ps, APs),
    false = match("p.content_encoding = 'gzip'", Ps, APs),
    true = match("p.content_encoding NOT IN ('gzip', 'compress')", Ps, APs),

    true = match("p.absolute_expiry_time = 1311999988888", Ps, APs),
    true = match("p.absolute_expiry_time > 1311999988000", Ps, APs),

    true = match("p.creation_time = 1311704463521", Ps, APs),
    true = match("p.creation_time < 1311999988888", Ps, APs),

    true = match("p.group_id = 'some group ID'", Ps, APs),
    true = match("p.group_id LIKE 'some%ID'", Ps, APs),
    false = match("p.group_id = 'other group ID'", Ps, APs),

    true = match("p.group_sequence = 999", Ps, APs),
    true = match("p.group_sequence >= 999", Ps, APs),
    false = match("p.group_sequence > 999", Ps, APs),

    true = match("p.reply_to_group_id = 'other group ID'", Ps, APs),
    true = match("p.reply_to_group_id LIKE '%group ID'", Ps, APs),
    true = match("p.reply_to_group_id != 'some group ID'", Ps, APs),
    true = match("p.reply_to_group_id IS NOT NULL", Ps, APs),
    false = match("p.reply_to_group_id IS NULL", Ps, APs),

    true = match("p.message_id = 'id-123' AND 'some subject' = p.subject", Ps, APs),
    true = match("p.group_sequence < 500 OR p.correlation_id > 700", Ps, APs),
    true = match("(p.content_type LIKE 'text/%') AND p.content_encoding = 'deflate'", Ps, APs),

    true = match("p.subject IS NULL", #'v1_0.properties'{}, APs),
    false = match("p.subject IS NOT NULL", #'v1_0.properties'{}, APs).

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

    true = match("-1.0 = key_1 AND 4 < header.priority AND properties.group_sequence > 90", Hdr, Ps, APs),
    false = match("-1.0 = key_1 AND 4 < header.priority AND properties.group_sequence < 90", Hdr, Ps, APs).

section_qualifier(_Config) ->
    Hdr = #'v1_0.header'{priority = {ubyte, 7}},
    Ps = #'v1_0.properties'{message_id = {utf8, <<"id-123">>}},
    APs = [{{utf8, <<"key_1">>}, {byte, -1}}],

    %% supported section qualifiers
    true = match("header.priority = 7", Hdr, Ps, APs),
    true = match("h.priority = 7", Hdr, Ps, APs),
    true = match("properties.message_id = 'id-123'", Hdr, Ps, APs),
    true = match("p.message_id = 'id-123'", Hdr, Ps, APs),
    true = match("application_properties.key_1 = -1", Hdr, Ps, APs),
    true = match("a.key_1 = -1", Hdr, Ps, APs),
    true = match("key_1 = -1", Hdr, Ps, APs),

    %% (currently) unsupported section qualifiers
    ?assertEqual(error, parse("delivery_annotations.abc")),
    ?assertEqual(error, parse("delivery-annotations.abc")),
    ?assertEqual(error, parse("d.abc")),
    ?assertEqual(error, parse("message_annotations.abc")),
    ?assertEqual(error, parse("message-annotations.abc")),
    ?assertEqual(error, parse("m.abc")),
    ?assertEqual(error, parse("application-properties.foo = 'bar'")),
    ?assertEqual(error, parse("footer.abc")),
    ?assertEqual(error, parse("f.abc")),
    ok.

utc_function(_Config) ->
    true = match("UTC() > 1000000000000"), % After year 2001
    true = match("UTC() < 9999999999999"), % Before year 2286

    %% UTC() should work multiple times in same expression
    true = match("UTC() < UTC() + 30000"),
    true = match("UTC() > UTC() - 30000"),

    BeforeTest = os:system_time(millisecond) - 30_000,
    Props = #'v1_0.properties'{
               creation_time = {timestamp, BeforeTest},
               absolute_expiry_time = {timestamp, BeforeTest + 3_600_000} % 1 hour later
              },

    true = match("UTC() >= p.creation_time", Props, []),
    true = match("p.creation_time <= UTC()", Props, []),
    false = match("p.creation_time >= UTC()", Props, []),
    true = match("UTC() < p.absolute_expiry_time", Props, []),
    true = match("p.absolute_expiry_time > UTC()", Props, []),
    true = match("UTC() - properties.creation_time < 300000", Props, []),
    true = match("country = 'UK' AND UTC() > 0", Props, app_props()),
    true = match("(FALSE OR p.creation_time < UTC()) AND weight = 5", Props, app_props()),
    true = match("p.creation_time IS NULL AND UTC() > 0", #'v1_0.properties'{}, []),

    %% Timestamp in application-properties
    true = match("ts1 < UTC()",  [{{utf8, <<"ts1">>}, {timestamp, BeforeTest}}]),

    %% Test with different amount of white spaces
    true = match("UTC()>=p.creation_time", Props, []),
    true = match("UTC () >= p.creation_time", Props, []),
    true = match("UTC ( ) >= p.creation_time", Props, []),
    true = match("  UTC   (   )   >=   p.creation_time", Props, []),
    true = match("(UTC()) >= p.creation_time", Props, []),
    true = match("( UTC () ) >= p.creation_time", Props, []),

    %% Ensure UTC() doesn't accept arguments
    ?assertEqual(error, parse("UTC(123)")),
    ?assertEqual(error, parse("UTC( 123 )")),
    ?assertEqual(error, parse("UTC('arg')")),
    ?assertEqual(error, parse("UTC(TRUE)")),
    ok.

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
    %% Invalid binary constants
    %% No hex digits
    ?assertEqual(error, parse("data = 0x")),
    %% Odd number of hex digits
    ?assertEqual(error, parse("data = 0x1")),
    ?assertEqual(error, parse("data = 0x123")),
    %% Invalid hex digit
    ?assertEqual(error, parse("data = 0xG1")),
    ?assertEqual(error, parse("data = 0x1G")),
    %% Lowercase hex letters not allowed
    ?assertEqual(error, parse("data = 0x1a")),
    ?assertEqual(error, parse("data = 0xab")),
    ?assertEqual(error, parse("data = 0xAb")),
    ?assertEqual(error, parse("data = 0xdead")),
    %% LIKE operator should not work with binary constants because
    %% "The pattern expression is evaluated as a string."
    ?assertEqual(error, parse("data LIKE 0x48")),
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

match(Selector) ->
    match(Selector, []).

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
    rabbit_amqp_filter_sql:eval(ParsedSelector, Mc).

parse(Selector) ->
    Descriptor = {ulong, ?DESCRIPTOR_CODE_SQL_FILTER},
    Filter = {described, Descriptor, {utf8, Selector}},
    rabbit_amqp_filter_sql:parse(Filter).

amqp_encode_bin(L) when is_list(L) ->
    iolist_to_binary([amqp10_framing:encode_bin(X) || X <- L]).
