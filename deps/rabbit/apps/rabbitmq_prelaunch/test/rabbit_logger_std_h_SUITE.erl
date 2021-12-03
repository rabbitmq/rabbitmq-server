-module(rabbit_logger_std_h_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").

-compile(export_all).

all() ->
    [
     {group, parallel_tests}
    ].

groups() ->
    [
     {parallel_tests, [parallel], [
         every_day_rotation_is_detected,
         every_week_rotation_is_detected,
         every_month_rotation_is_detected,

         parse_date_spec_case1,
         parse_date_spec_case2,
         parse_date_spec_case3,
         parse_date_spec_case4,
         parse_date_spec_case5,
         parse_date_spec_case6
     ]}
    ].

init_per_suite(_, Config) -> Config.
end_per_suite(_, Config) -> Config.

init_per_group(_, Config) -> Config.
end_per_group(_, Config) -> Config.

init_per_testcase(_, Config) -> Config.
end_per_testcase(_, Config) -> Config.

every_day_rotation_is_detected(_) ->
    ?assertNot(
      rabbit_logger_std_h:is_date_based_rotation_needed(
        #{every => day, hour => 12},
        {{2021, 01, 15}, {10, 00, 00}},
        {{2021, 01, 15}, {11, 00, 00}})),
    ?assert(
      rabbit_logger_std_h:is_date_based_rotation_needed(
        #{every => day, hour => 12},
        {{2021, 01, 15}, {10, 00, 00}},
        {{2021, 01, 15}, {12, 00, 00}})),
    ?assert(
      rabbit_logger_std_h:is_date_based_rotation_needed(
        #{every => day, hour => 12},
        {{2021, 01, 15}, {10, 00, 00}},
        {{2021, 01, 15}, {13, 00, 00}})),
    ?assert(
      rabbit_logger_std_h:is_date_based_rotation_needed(
        #{every => day, hour => 12},
        {{2021, 01, 15}, {11, 00, 00}},
        {{2021, 01, 15}, {13, 00, 00}})),
    ?assertNot(
      rabbit_logger_std_h:is_date_based_rotation_needed(
        #{every => day, hour => 12},
        {{2021, 01, 15}, {12, 00, 00}},
        {{2021, 01, 15}, {13, 00, 00}})),
    ?assert(
      rabbit_logger_std_h:is_date_based_rotation_needed(
        #{every => day, hour => 12},
        {{2021, 01, 14}, {12, 00, 00}},
        {{2021, 01, 15}, {12, 00, 00}})),
    ?assertNot(
      rabbit_logger_std_h:is_date_based_rotation_needed(
        #{every => day, hour => 12},
        {{2021, 01, 14}, {12, 00, 00}},
        {{2021, 01, 15}, {11, 00, 00}})),
    ?assert(
      rabbit_logger_std_h:is_date_based_rotation_needed(
        #{every => day, hour => 12},
        {{2020, 11, 15}, {12, 00, 00}},
        {{2021, 01, 15}, {11, 00, 00}})),
    ?assert(
      rabbit_logger_std_h:is_date_based_rotation_needed(
        #{every => day, hour => 12},
        {{2020, 11, 15}, {12, 00, 00}},
        {{2021, 01, 15}, {12, 00, 00}})).

every_week_rotation_is_detected(_) ->
    ?assertNot(
      rabbit_logger_std_h:is_date_based_rotation_needed(
        #{every => week, day_of_week => 3, hour => 12},
        {{2021, 01, 11}, {12, 00, 00}},
        {{2021, 01, 12}, {12, 00, 00}})),
    ?assert(
      rabbit_logger_std_h:is_date_based_rotation_needed(
        #{every => week, day_of_week => 3, hour => 12},
        {{2021, 01, 11}, {12, 00, 00}},
        {{2021, 01, 13}, {12, 00, 00}})),
    ?assert(
      rabbit_logger_std_h:is_date_based_rotation_needed(
        #{every => week, day_of_week => 3, hour => 12},
        {{2021, 01, 11}, {12, 00, 00}},
        {{2021, 01, 14}, {12, 00, 00}})),
    ?assertNot(
      rabbit_logger_std_h:is_date_based_rotation_needed(
        #{every => week, day_of_week => 3, hour => 12},
        {{2021, 01, 13}, {12, 00, 00}},
        {{2021, 01, 14}, {12, 00, 00}})),
    ?assertNot(
      rabbit_logger_std_h:is_date_based_rotation_needed(
        #{every => week, day_of_week => 3, hour => 12},
        {{2021, 01, 14}, {12, 00, 00}},
        {{2021, 01, 15}, {12, 00, 00}})),
    ?assert(
      rabbit_logger_std_h:is_date_based_rotation_needed(
        #{every => week, day_of_week => 3, hour => 12},
        {{2021, 01, 13}, {11, 00, 00}},
        {{2021, 01, 13}, {12, 00, 00}})),
    ?assert(
      rabbit_logger_std_h:is_date_based_rotation_needed(
        #{every => week, day_of_week => 3, hour => 12},
        {{2021, 01, 06}, {12, 00, 00}},
        {{2021, 01, 13}, {12, 00, 00}})),
    ?assert(
      rabbit_logger_std_h:is_date_based_rotation_needed(
        #{every => week, day_of_week => 3, hour => 12},
        {{2021, 01, 07}, {12, 00, 00}},
        {{2021, 01, 14}, {12, 00, 00}})),
    ?assertNot(
      rabbit_logger_std_h:is_date_based_rotation_needed(
        #{every => week, day_of_week => 3, hour => 12},
        {{2021, 01, 06}, {12, 00, 00}},
        {{2021, 01, 12}, {12, 00, 00}})),
    ?assert(
      rabbit_logger_std_h:is_date_based_rotation_needed(
        #{every => week, day_of_week => 3, hour => 12},
        {{2021, 01, 06}, {11, 00, 00}},
        {{2021, 01, 12}, {12, 00, 00}})),
    ?assertNot(
      rabbit_logger_std_h:is_date_based_rotation_needed(
        #{every => week, day_of_week => 3, hour => 12},
        {{2021, 01, 06}, {12, 00, 00}},
        {{2021, 01, 13}, {11, 00, 00}})).

every_month_rotation_is_detected(_) ->
    ?assertNot(
      rabbit_logger_std_h:is_date_based_rotation_needed(
        #{every => month, day_of_month => 15, hour => 12},
        {{2021, 01, 15}, {10, 00, 00}},
        {{2021, 01, 15}, {11, 00, 00}})),
    ?assert(
      rabbit_logger_std_h:is_date_based_rotation_needed(
        #{every => month, day_of_month => 15, hour => 12},
        {{2021, 01, 15}, {10, 00, 00}},
        {{2021, 01, 15}, {12, 00, 00}})),
    ?assertNot(
      rabbit_logger_std_h:is_date_based_rotation_needed(
        #{every => month, day_of_month => 15, hour => 12},
        {{2021, 01, 13}, {12, 00, 00}},
        {{2021, 01, 14}, {12, 00, 00}})),
    ?assert(
      rabbit_logger_std_h:is_date_based_rotation_needed(
        #{every => month, day_of_month => 15, hour => 12},
        {{2021, 01, 14}, {12, 00, 00}},
        {{2021, 01, 15}, {12, 00, 00}})),
    ?assertNot(
      rabbit_logger_std_h:is_date_based_rotation_needed(
        #{every => month, day_of_month => 15, hour => 12},
        {{2021, 01, 15}, {12, 00, 00}},
        {{2021, 01, 16}, {12, 00, 00}})),
    ?assert(
      rabbit_logger_std_h:is_date_based_rotation_needed(
        #{every => month, day_of_month => 15, hour => 12},
        {{2021, 01, 14}, {12, 00, 00}},
        {{2021, 02, 14}, {12, 00, 00}})),
    ?assert(
      rabbit_logger_std_h:is_date_based_rotation_needed(
        #{every => month, day_of_month => 15, hour => 12},
        {{2021, 01, 16}, {12, 00, 00}},
        {{2021, 02, 16}, {12, 00, 00}})),

    ?assert(
      rabbit_logger_std_h:is_date_based_rotation_needed(
        #{every => month, day_of_month => 30, hour => 12},
        {{2021, 01, 29}, {12, 00, 00}},
        {{2021, 01, 30}, {12, 00, 00}})),
    ?assertNot(
      rabbit_logger_std_h:is_date_based_rotation_needed(
        #{every => month, day_of_month => 30, hour => 12},
        {{2021, 01, 30}, {12, 00, 00}},
        {{2021, 01, 31}, {12, 00, 00}})),
    ?assert(
      rabbit_logger_std_h:is_date_based_rotation_needed(
        #{every => month, day_of_month => 30, hour => 12},
        {{2021, 02, 27}, {12, 00, 00}},
        {{2021, 02, 28}, {12, 00, 00}})),

    ?assertNot(
      rabbit_logger_std_h:is_date_based_rotation_needed(
        #{every => month, day_of_month => last, hour => 12},
        {{2021, 01, 29}, {12, 00, 00}},
        {{2021, 01, 30}, {12, 00, 00}})),
    ?assert(
      rabbit_logger_std_h:is_date_based_rotation_needed(
        #{every => month, day_of_month => last, hour => 12},
        {{2021, 01, 30}, {12, 00, 00}},
        {{2021, 01, 31}, {12, 00, 00}})),
    ?assert(
      rabbit_logger_std_h:is_date_based_rotation_needed(
        #{every => month, day_of_month => last, hour => 12},
        {{2021, 01, 30}, {12, 00, 00}},
        {{2021, 02, 01}, {12, 00, 00}})).

parse_date_spec_case1(_) ->
      ?assertEqual(false, rabbit_logger_std_h:parse_date_spec("")).

parse_date_spec_case2(_) ->
      ?assertEqual(#{every => day, hour => 0},
          rabbit_logger_std_h:parse_date_spec("$D0")),
      ?assertEqual(#{every => day, hour => 16},
          rabbit_logger_std_h:parse_date_spec("$D16")),
      ?assertEqual(#{every => day, hour => 23},
          rabbit_logger_std_h:parse_date_spec("$D23")).

parse_date_spec_case3(_) ->
      ?assertEqual(
          #{every => week, day_of_week => 0, hour => 0},
          rabbit_logger_std_h:parse_date_spec("$W0")),
      ?assertEqual(
          #{every => week, day_of_week => 0, hour => 23},
          rabbit_logger_std_h:parse_date_spec("$W0D23")),
      ?assertEqual(
          #{every => week, day_of_week => 5, hour => 16},
          rabbit_logger_std_h:parse_date_spec("$W5D16")).

parse_date_spec_case4(_) ->
      ?assertEqual(
          #{every => month, day_of_month => 1, hour => 0},
          rabbit_logger_std_h:parse_date_spec("$M1D0")),
      ?assertEqual(
          #{every => month, day_of_month => 5, hour => 6},
          rabbit_logger_std_h:parse_date_spec("$M5D6")).

parse_date_spec_case5(_) ->
      ?assertEqual(
          error,
          rabbit_logger_std_h:parse_date_spec("INVALID")),
      ?assertEqual(
          error,
          rabbit_logger_std_h:parse_date_spec("in$valid")),
      ?assertEqual(
          error,
          rabbit_logger_std_h:parse_date_spec("$$D0")),
      ?assertEqual(
          error,
          rabbit_logger_std_h:parse_date_spec("$D99")).

parse_date_spec_case6(_) ->
      ?assertEqual(
          #{every => hour, minute => 30},
          rabbit_logger_std_h:parse_date_spec("$H30")),
      ?assertEqual(
          #{every => hour, minute => 3},
          rabbit_logger_std_h:parse_date_spec("$H3")),
      ?assertEqual(
          #{day_of_week => 0,every => week,hour => 0, minute => 30},
          rabbit_logger_std_h:parse_date_spec("$W0H30")).
