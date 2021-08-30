-module(binary_generator_SUITE).

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
     booleans,
     symbol,
     timestamp,
     numerals,
     utf8,
     list,
     map,
     described,
     array
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

booleans(_Config) ->
    roundtrip(true),
    roundtrip(false),
    roundtrip({boolean, false}),
    roundtrip({boolean, true}),
    ok.

symbol(_Config) ->
    roundtrip({symbol, <<"SYMB">>}),
    ok.

timestamp(_Config) ->
    roundtrip({timestamp, erlang:system_time(millisecond)}),
    ok.
numerals(_Config) ->
    roundtrip({ubyte, 0}),
    roundtrip({ubyte, 16#FF}),
    roundtrip({ushort, 0}),
    roundtrip({ushort, 16#FFFF}),
    roundtrip({uint, 0}),
    roundtrip({uint, 16#FFFFFFFF}),
    roundtrip({ulong, 0}),
    roundtrip({ulong, 16#FFFFFFFFFFFFFFFF}),
    roundtrip({byte, 0}),
    roundtrip({byte, 16#7F}),
    roundtrip({byte, -16#80}),
    roundtrip({short, 0}),
    roundtrip({short, 16#7FFF}),
    roundtrip({short, -16#8000}),
    roundtrip({int, 0}),
    roundtrip({int, 16#7FFFFFFF}),
    roundtrip({int, -16#80000000}),
    roundtrip({long, 0}),
    roundtrip({long, 16#7FFFFFFFFFFFFFFF}),
    roundtrip({long, -16#8000000000000000}),
    roundtrip({float, 0.0}),
    roundtrip({float, 1.0}),
    roundtrip({float, -1.0}),
    roundtrip({double, 0.0}),
    roundtrip({double, 1.0}),
    roundtrip({double, -1.0}),
    ok.

utf8(_Config) ->
    roundtrip({utf8, <<"hi">>}),
    roundtrip({utf8, binary:copy(<<"asdfghjk">>, 64)}),
    ok.

list(_Config) ->
    roundtrip({list, [{utf8, <<"hi">>},
                      {int, 123},
                      {binary, <<"data">>},
                      {array, int, [{int, 1}, {int, 2}, {int, 3}]},
                      {described,
                       {utf8, <<"URL">>},
                       {utf8, <<"http://example.org/hello-world">>}}
                     ]}),
    ok.

map(_Config) ->
    roundtrip({map, [
                     {{utf8, <<"key1">>}, {utf8, <<"value1">>}},
                     {{utf8, <<"key2">>}, {int, 33}}
                    ]}),
    ok.



described(_Config) ->
    roundtrip({described,
               {utf8, <<"URL">>},
               {utf8, <<"http://example.org/hello-world">>}}),
    ok.

array(_Config) ->
    roundtrip({array, symbol, [{symbol, <<"ANONYMOUS">>}]}),
    roundtrip({array, symbol, []}),
    roundtrip({array, ubyte, [{ubyte, 1}, {ubyte, 255}]}),
    roundtrip({array, byte, [{byte, 1}, {byte, -128}, {byte, 127}]}),
    roundtrip({array, ushort, [{ushort, 0}, {ushort, 16#FFFF}]}),
    roundtrip({array, short, [{short, 0}, {short, -16#8000},
                              {short, 16#7FFF}]}),
    % uint
    roundtrip({array, uint, [{uint, 0},  {uint, 16#FFFFFFFF}]}),
    roundtrip({array, int, [{int, 0}, {int, -16#8000000},
                            {int, 16#7FFFFFFF}]}),
    roundtrip({array, ulong, [{ulong, 0}, {ulong, 16#FFFFFFFFFFFFFFFF}]}),
    roundtrip({array, long, [{long, 0}, {long, -16#8000000000000},
                             {long, 16#7FFFFFFFFFFFFF}]}),
    roundtrip({array, boolean, [{boolean, true}, {boolean, false}]}),
    % array of arrays
    % TODO: does the inner type need to be consistent across the array?
    roundtrip({array, array, []}),
    roundtrip({array, array, [{array, symbol, [{symbol, <<"ANONYMOUS">>}]}]}),

    Desc = {utf8, <<"URL">>},
    roundtrip({array, {described, Desc, utf8},
               [{described, Desc, {utf8, <<"http://example.org/hello">>}}]}),
    roundtrip({array, {described, Desc, utf8}, []}),
    ok.

%% Utility

roundtrip(Term) ->
    Bin = iolist_to_binary(amqp10_binary_generator:generate(Term)),
    % generate returns an iolist but parse expects a binary
    ?assertMatch({Term, _}, amqp10_binary_parser:parse(Bin)).
