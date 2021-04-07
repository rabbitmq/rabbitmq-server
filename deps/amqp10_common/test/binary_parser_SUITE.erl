-module(binary_parser_SUITE).

-compile(export_all).

-export([
         ]).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").

%%%===================================================================
%%% Common Test callbacks
%%%===================================================================

all() ->
    [
     {group, tests}
    ].


all_tests() ->
    [
     array_with_extra_input
    ].

groups() ->
    [
     {tests, [], all_tests()}
    ].

init_per_suite(Config) ->
    Config.

end_per_suite(_Config) ->
    ok.

init_per_group(_Group, Config) ->
    Config.

end_per_group(_Group, _Config) ->
    ok.

init_per_testcase(_TestCase, Config) ->
    Config.

end_per_testcase(_TestCase, _Config) ->
    ok.

%%%===================================================================
%%% Test cases
%%%===================================================================

array_with_extra_input(_Config) ->
    Bin = <<83,16,192,85,10,177,0,0,0,1,48,161,12,114,97,98,98,105,116, 109,113,45,98,111,120,112,255,255,0,0,96,0,50,112,0,0,19,136,163,5,101,110,45,85,83,224,14,2,65,5,102,105,45,70,73,5,101,110,45,85,83,64,64,193,24,2,163,20,68,69,70,69,78,83,73,67,83,46,84,69,83,84,46,83,85,73,84,69,65>>,
    ?assertExit({failed_to_parse_array_extra_input_remaining,
        %% element type, input, accumulated result
        65, <<105,45,70,73,5,101,110,45,85,83>>, [true,true]},
        amqp10_binary_parser:parse_all(Bin)),
    ok.
