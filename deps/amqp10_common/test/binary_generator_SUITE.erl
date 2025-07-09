-module(binary_generator_SUITE).

-compile(nowarn_export_all).
-compile(export_all).

-export([
         ]).

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
     null,
     booleans,
     symbol,
     timestamp,
     numerals,
     utf8,
     char,
     list,
     map,
     described,
     array
    ].

groups() ->
    [
     {tests, [parallel], all_tests()}
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

null(_Config) ->
    roundtrip(null),
    ok.

booleans(_Config) ->
    roundtrip(true),
    roundtrip(false),
    ?assertEqual(true, roundtrip_return({boolean, true})),
    ?assertEqual(false, roundtrip_return({boolean, false})).

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
    roundtrip({uint, 0}), %% uint:uint0
    roundtrip({uint, 1}), %% uint:smalluint
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

    %% float +Inf
    roundtrip({as_is, 16#72, <<16#7F, 16#80, 16#00, 16#00>>}),
    %% double +Inf
    roundtrip({as_is, 16#82, <<16#7F, 16#F0, 16#00, 16#00,
                               16#00, 16#00, 16#00, 16#00>>}),

    %% decimal32
    roundtrip({as_is, 16#74, <<16#22, 16#50, 16#00, 16#00>>}), % 0
    roundtrip({as_is, 16#74, <<16#22, 16#50, 16#00, 16#2A>>}), % 42
    roundtrip({as_is, 16#74, <<16#A2, 16#40, 16#00, 16#48>>}), % -123.45
    roundtrip({as_is, 16#74, <<16#78, 16#00, 16#00, 16#00>>}), % +Infinity
    roundtrip({as_is, 16#74, <<16#7C, 16#00, 16#00, 16#00>>}), % NaN
    %% decimal64
    roundtrip({as_is, 16#84, <<16#22, 16#34, 16#00, 16#00,
                               16#00, 16#00, 16#00, 16#00>>}), % 0
    %% decimal128
    roundtrip({as_is, 16#94, <<16#22, 16#08, 16#00, 16#00,
                               16#00, 16#00, 16#00, 16#00,
                               16#00, 16#00, 16#00, 16#00,
                               16#00, 16#00, 16#00, 16#00>>}), % 0
    ok.

utf8(_Config) ->
    roundtrip({utf8, <<"hi">>}),
    roundtrip({utf8, binary:copy(<<"abcdefgh">>, 64)}),
    ok.

char(_Config) ->
    roundtrip({char, $🎉}),
    ok.

list(_Config) ->
    %% list:list0
    roundtrip({list, []}),
    %% list:list8
    roundtrip({list, [
                      {utf8, <<"hi">>},
                      {int, 123},
                      {binary, <<"data">>},
                      {array, int, [{int, 1}, {int, -2147483648}, {int, 2147483647}]},
                      {described,
                       {utf8, <<"URL">>},
                       {utf8, <<"http://example.org/hello-world">>}}
                     ]}),
    %% list:list32
    roundtrip({list, [true || _ <- lists:seq(1, 256)]}),
    ok.

map(_Config) ->
    roundtrip({map, [
                     {{utf8, <<"key1">>}, {utf8, <<"value1">>}},
                     {{utf8, <<"key2">>}, {int, 33}}
                    ]}),
    roundtrip({map, [{{int, N}, {utf8, <<"value">>}} || N <- lists:seq(1, 256)]}),
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
    roundtrip({array, boolean, [true, false]}),

    ?assertEqual({array, boolean, [true, false]},
                 roundtrip_return({array, boolean, [{boolean, true}, {boolean, false}]})),

    %% array of arrays
    roundtrip({array, array, []}),
    roundtrip({array, array, [{array, symbol, [{symbol, <<"ANONYMOUS">>}]}]}),
    roundtrip({array, array, [{array, symbol, [{symbol, <<"ANONYMOUS">>}]},
                              {array, symbol, [{symbol, <<"sym1">>},
                                               {symbol, <<"sym2">>}]}]}),

    %% array of lists
    roundtrip({array, list, []}),
    roundtrip({array, list, [{list, [{symbol, <<"sym">>}]},
                             {list, [null,
                                     {described,
                                      {utf8, <<"URL">>},
                                      {utf8, <<"http://example.org/hello-world">>}}]},
                             {list, []},
                             {list, [true, false, {byte, -128}]}
                            ]}),

    %% array of maps
    roundtrip({array, map, []}),
    roundtrip({array, map, [{map, [{{symbol, <<"k1">>}, {utf8, <<"v1">>}}]},
                            {map, []},
                            {map, [{{described,
                                     {utf8, <<"URL">>},
                                     {utf8, <<"http://example.org/hello-world">>}},
                                    {byte, -1}},
                                   {{int, 0}, {ulong, 0}}
                                  ]}
                           ]}),

    Desc = {utf8, <<"URL">>},
    roundtrip({array, {described, Desc, utf8}, []}),
    roundtrip({array, {described, Desc, utf8},
               [{described, Desc, {utf8, <<"http://example.org/hello1">>}},
                {described, Desc, {utf8, <<"http://example.org/hello2">>}}]}),

    %% array:array32
    roundtrip({array, boolean, [true || _ <- lists:seq(1, 256)]}),
    ok.

%% Utility

roundtrip(Term) ->
    Bin = iolist_to_binary(amqp10_binary_generator:generate(Term)),
    ?assertEqual({Term, size(Bin)}, amqp10_binary_parser:parse(Bin)),
    ?assertEqual([Term], amqp10_binary_parser:parse_many(Bin, [])).

%% Return the roundtripped term.
roundtrip_return(Term) ->
    Bin = iolist_to_binary(amqp10_binary_generator:generate(Term)),
    %% We assert only that amqp10_binary_parser:parse/1 and
    %% amqp10_binary_parser:parse_many/2 return the same term.
    {RoundTripTerm, BytesParsed} = amqp10_binary_parser:parse(Bin),
    ?assertEqual(size(Bin), BytesParsed),
    ?assertEqual([RoundTripTerm], amqp10_binary_parser:parse_many(Bin, [])),
    RoundTripTerm.
