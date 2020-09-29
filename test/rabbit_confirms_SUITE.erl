-module(rabbit_confirms_SUITE).

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
     confirm,
     reject,
     remove_queue
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

confirm(_Config) ->
    XName = rabbit_misc:r(<<"/">>, exchange, <<"X">>),
    QName = rabbit_misc:r(<<"/">>, queue, <<"Q">>),
    QName2 = rabbit_misc:r(<<"/">>, queue, <<"Q2">>),
    U0 = rabbit_confirms:init(),
    ?assertEqual(0, rabbit_confirms:size(U0)),
    ?assertEqual(undefined, rabbit_confirms:smallest(U0)),
    ?assertEqual(true, rabbit_confirms:is_empty(U0)),

    U1 = rabbit_confirms:insert(1, [QName], XName, U0),
    ?assertEqual(1, rabbit_confirms:size(U1)),
    ?assertEqual(1, rabbit_confirms:smallest(U1)),
    ?assertEqual(false, rabbit_confirms:is_empty(U1)),

    {[{1, XName}], U2} = rabbit_confirms:confirm([1], QName, U1),
    ?assertEqual(0, rabbit_confirms:size(U2)),
    ?assertEqual(undefined, rabbit_confirms:smallest(U2)),
    ?assertEqual(true, rabbit_confirms:is_empty(U2)),

    U3 = rabbit_confirms:insert(2, [QName], XName, U1),
    ?assertEqual(2, rabbit_confirms:size(U3)),
    ?assertEqual(1, rabbit_confirms:smallest(U3)),
    ?assertEqual(false, rabbit_confirms:is_empty(U3)),

    {[{1, XName}], U4} = rabbit_confirms:confirm([1], QName, U3),
    ?assertEqual(1, rabbit_confirms:size(U4)),
    ?assertEqual(2, rabbit_confirms:smallest(U4)),
    ?assertEqual(false, rabbit_confirms:is_empty(U4)),

    U5 = rabbit_confirms:insert(2, [QName, QName2], XName, U1),
    ?assertEqual(2, rabbit_confirms:size(U5)),
    ?assertEqual(1, rabbit_confirms:smallest(U5)),
    ?assertEqual(false, rabbit_confirms:is_empty(U5)),

    {[{1, XName}], U6} = rabbit_confirms:confirm([1, 2], QName, U5),
    ?assertEqual(2, rabbit_confirms:smallest(U6)),

    {[{2, XName}], U7} = rabbit_confirms:confirm([2], QName2, U6),
    ?assertEqual(0, rabbit_confirms:size(U7)),
    ?assertEqual(undefined, rabbit_confirms:smallest(U7)),


    U8 = rabbit_confirms:insert(2, [QName], XName, U1),
    {[{1, XName}, {2, XName}], _U9} = rabbit_confirms:confirm([1, 2], QName, U8),
    ok.


reject(_Config) ->
    XName = rabbit_misc:r(<<"/">>, exchange, <<"X">>),
    QName = rabbit_misc:r(<<"/">>, queue, <<"Q">>),
    QName2 = rabbit_misc:r(<<"/">>, queue, <<"Q2">>),
    U0 = rabbit_confirms:init(),
    ?assertEqual(0, rabbit_confirms:size(U0)),
    ?assertEqual(undefined, rabbit_confirms:smallest(U0)),
    ?assertEqual(true, rabbit_confirms:is_empty(U0)),

    U1 = rabbit_confirms:insert(1, [QName], XName, U0),

    {ok, {1, XName}, U2} = rabbit_confirms:reject(1, U1),
    {error, not_found} = rabbit_confirms:reject(1, U2),
    ?assertEqual(0, rabbit_confirms:size(U2)),
    ?assertEqual(undefined, rabbit_confirms:smallest(U2)),

    U3 = rabbit_confirms:insert(2, [QName, QName2], XName, U1),

    {ok, {1, XName}, U4} = rabbit_confirms:reject(1, U3),
    {error, not_found} = rabbit_confirms:reject(1, U4),
    ?assertEqual(1, rabbit_confirms:size(U4)),
    ?assertEqual(2, rabbit_confirms:smallest(U4)),

    {ok, {2, XName}, U5} = rabbit_confirms:reject(2, U3),
    {error, not_found} = rabbit_confirms:reject(2, U5),
    ?assertEqual(1, rabbit_confirms:size(U5)),
    ?assertEqual(1, rabbit_confirms:smallest(U5)),

    ok.

remove_queue(_Config) ->
    XName = rabbit_misc:r(<<"/">>, exchange, <<"X">>),
    QName = rabbit_misc:r(<<"/">>, queue, <<"Q">>),
    QName2 = rabbit_misc:r(<<"/">>, queue, <<"Q2">>),
    U0 = rabbit_confirms:init(),

    U1 = rabbit_confirms:insert(1, [QName, QName2], XName, U0),
    U2 = rabbit_confirms:insert(2, [QName2], XName, U1),
    {[{2, XName}], U3} = rabbit_confirms:remove_queue(QName2, U2),
    ?assertEqual(1, rabbit_confirms:size(U3)),
    ?assertEqual(1, rabbit_confirms:smallest(U3)),
    {[{1, XName}], U4} = rabbit_confirms:remove_queue(QName, U3),
    ?assertEqual(0, rabbit_confirms:size(U4)),
    ?assertEqual(undefined, rabbit_confirms:smallest(U4)),

    U5 = rabbit_confirms:insert(1, [QName], XName, U0),
    U6 = rabbit_confirms:insert(2, [QName], XName, U5),
    {[{1, XName}, {2, XName}], _U} = rabbit_confirms:remove_queue(QName, U6),

    ok.


%% Utility
