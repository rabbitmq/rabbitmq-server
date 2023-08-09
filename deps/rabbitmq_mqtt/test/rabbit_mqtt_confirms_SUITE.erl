-module(rabbit_mqtt_confirms_SUITE).

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
    [
     {tests, [shuffle],
      [confirm,
       reject,
       remove_queue
      ]}].

-define(MOD, rabbit_mqtt_confirms).

confirm(_Config) ->
    QName = rabbit_misc:r(<<"/">>, queue, <<"Q">>),
    QName2 = rabbit_misc:r(<<"/">>, queue, <<"Q2">>),
    U0 = ?MOD:init(),
    ?assertEqual(0, ?MOD:size(U0)),

    U1 = ?MOD:insert(1, [QName], U0),
    ?assertEqual(1, ?MOD:size(U1)),
    ?assert(?MOD:contains(1, U1)),

    {[1], U2} = ?MOD:confirm([1], QName, U1),
    ?assertEqual(0, ?MOD:size(U2)),
    ?assertNot(?MOD:contains(1, U2)),

    U3 = ?MOD:insert(2, [QName], U1),
    ?assertEqual(2, ?MOD:size(U3)),
    ?assert(?MOD:contains(1, U3)),
    ?assert(?MOD:contains(2, U3)),

    {[1], U4} = ?MOD:confirm([1], QName, U3),
    ?assertEqual(1, ?MOD:size(U4)),
    ?assertNot(?MOD:contains(1, U4)),
    ?assert(?MOD:contains(2, U4)),

    U5 = ?MOD:insert(2, [QName, QName2], U1),
    ?assertEqual(2, ?MOD:size(U5)),
    ?assert(?MOD:contains(1, U5)),
    ?assert(?MOD:contains(2, U5)),

    {[1], U6} = ?MOD:confirm([1, 2], QName, U5),
    {[2], U7} = ?MOD:confirm([2], QName2, U6),
    ?assertEqual(0, ?MOD:size(U7)),

    U8 = ?MOD:insert(2, [QName], U1),
    {Confirmed, _U9} = ?MOD:confirm([1, 2], QName, U8),
    ?assertEqual([1, 2], lists:sort(Confirmed)),
    ok.


reject(_Config) ->
    QName = rabbit_misc:r(<<"/">>, queue, <<"Q">>),
    QName2 = rabbit_misc:r(<<"/">>, queue, <<"Q2">>),
    U0 = ?MOD:init(),
    ?assertEqual(0, ?MOD:size(U0)),

    U1 = ?MOD:insert(1, [QName], U0),
    ?assert(?MOD:contains(1, U1)),

    {[1], U2} = ?MOD:reject([1], U1),
    {[], U2} = ?MOD:reject([1], U2),
    ?assertEqual(0, ?MOD:size(U2)),
    ?assertNot(?MOD:contains(1, U2)),

    U3 = ?MOD:insert(2, [QName, QName2], U1),

    {[1], U4} = ?MOD:reject([1], U3),
    {[], U4} = ?MOD:reject([1], U4),
    ?assertEqual(1, ?MOD:size(U4)),

    {[2], U5} = ?MOD:reject([2], U3),
    {[], U5} = ?MOD:reject([2], U5),
    ?assertEqual(1, ?MOD:size(U5)),
    ?assert(?MOD:contains(1, U5)),
    ok.

remove_queue(_Config) ->
    QName = rabbit_misc:r(<<"/">>, queue, <<"Q">>),
    QName2 = rabbit_misc:r(<<"/">>, queue, <<"Q2">>),
    U0 = ?MOD:init(),

    U1 = ?MOD:insert(1, [QName, QName2], U0),
    U2 = ?MOD:insert(2, [QName2], U1),
    {[2], U3} = ?MOD:remove_queue(QName2, U2),
    ?assertEqual(1, ?MOD:size(U3)),
    ?assert(?MOD:contains(1, U3)),
    {[1], U4} = ?MOD:remove_queue(QName, U3),
    ?assertEqual(0, ?MOD:size(U4)),
    ?assertNot(?MOD:contains(1, U4)),

    U5 = ?MOD:insert(1, [QName], U0),
    U6 = ?MOD:insert(2, [QName], U5),
    {Confirmed, U7} = ?MOD:remove_queue(QName, U6),
    ?assertEqual([1, 2], lists:sort(Confirmed)),
    ?assertEqual(0, ?MOD:size(U7)),
    ok.
