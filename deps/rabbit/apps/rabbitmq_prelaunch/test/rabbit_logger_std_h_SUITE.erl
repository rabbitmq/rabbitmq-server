-module(rabbit_logger_std_h_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").

-export([all/0,
         groups/0,
         init_per_suite/2,
         end_per_suite/2,
         init_per_group/2,
         end_per_group/2,
         init_per_testcase/2,
         end_per_testcase/2,

         every_day_rotation_is_detected/1,
         every_week_rotation_is_detected/1,
         every_month_rotation_is_detected/1
        ]).

all() ->
    [
     {group, parallel_tests}
    ].

groups() ->
    [
     {parallel_tests, [parallel], [every_day_rotation_is_detected,
                                   every_week_rotation_is_detected,
                                   every_month_rotation_is_detected]}
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
