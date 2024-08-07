-module(rabbit_fifo_prop_SUITE).

-compile(nowarn_export_all).
-compile(export_all).

-export([
         ]).

-include_lib("proper/include/proper.hrl").
-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("rabbit/src/rabbit_fifo.hrl").
-include_lib("rabbit/src/rabbit_fifo_dlx.hrl").
-include_lib("rabbit_common/include/rabbit_framing.hrl").
-include_lib("rabbit_common/include/rabbit.hrl").

-define(MACHINE_VERSION, 4).

%%%===================================================================
%%% Common Test callbacks
%%%===================================================================

all() ->
    [
     {group, tests}
    ].


all_tests() ->
    [
     test_run_log,
     snapshots,
     scenario2,
     scenario3,
     scenario4,
     scenario5,
     scenario6,
     scenario7,
     scenario8,
     scenario9,
     scenario10,
     scenario11,
     scenario12,
     scenario13,
     scenario14,
     scenario14b,
     scenario15,
     scenario16,
     scenario17,
     scenario18,
     scenario19,
     scenario20,
     scenario21,
     scenario22,
     scenario23,
     scenario24,
     scenario25,
     scenario26,
     scenario27,
     scenario28,
     scenario29,
     scenario30,
     scenario31,
     scenario32,
     upgrade,
     messages_total,
     simple_prefetch,
     simple_prefetch_without_checkout_cancel,
     simple_prefetch_01,
     single_active,
     single_active_01,
     single_active_02,
     single_active_03,
     single_active_04,
     single_active_ordering,
     single_active_ordering_01,
     single_active_ordering_03,
     max_length,
     snapshots_dlx,
     dlx_01,
     dlx_02,
     dlx_03,
     dlx_04,
     dlx_05,
     dlx_06,
     dlx_07,
     dlx_08,
     dlx_09,
     single_active_ordering_02
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
    ok = meck:new(rabbit_feature_flags, [passthrough]),
    meck:expect(rabbit_feature_flags, is_enabled, fun (_) -> false end),
    Config.

end_per_testcase(_TestCase, _Config) ->
    meck:unload(),
    ok.

%%%===================================================================
%%% Test cases
%%%===================================================================

scenario2(_Config) ->
    C1 = {<<>>, c:pid(0,346,1)},
    C2 = {<<>>,c:pid(0,379,1)},
    E = c:pid(0,327,1),
    Commands = [make_checkout(C1, {auto,1,simple_prefetch}),
                make_enqueue(E,1,msg(<<"msg1">>)),
                make_checkout(C1, cancel),
                make_enqueue(E,2,msg(<<"msg2">>)),
                make_checkout(C2, {auto,1,simple_prefetch}),
                make_settle(C1, [0]),
                make_settle(C2, [0])
               ],
    run_snapshot_test(#{name => ?FUNCTION_NAME}, Commands),
    ok.

scenario3(_Config) ->
    C1 = {<<>>, c:pid(0,179,1)},
    E = c:pid(0,176,1),
    Commands = [make_checkout(C1, {auto,2,simple_prefetch}),
                make_enqueue(E,1,msg(<<"msg1">>)),
                make_return(C1, [0]),
                make_enqueue(E,2,msg(<<"msg2">>)),
                make_enqueue(E,3,msg(<<"msg3">>)),
                make_settle(C1, [1]),
                make_settle(C1, [2])
               ],
    run_snapshot_test(#{name => ?FUNCTION_NAME}, Commands),
    ok.

scenario4(_Config) ->
    C1 = {<<>>, c:pid(0,179,1)},
    E = c:pid(0,176,1),
    Commands = [make_checkout(C1, {auto,1,simple_prefetch}),
                make_enqueue(E,1,msg(<<"msg">>)),
                make_settle(C1, [0])
               ],
    run_snapshot_test(#{name => ?FUNCTION_NAME}, Commands),
    ok.

scenario5(_Config) ->
    C1 = {<<>>, c:pid(0,505,0)},
    E = c:pid(0,465,9),
    Commands = [make_enqueue(E,1,msg(<<0>>)),
                make_checkout(C1, {auto,1,simple_prefetch}),
                make_enqueue(E,2,msg(<<>>)),
                make_settle(C1,[0])],
    run_snapshot_test(#{name => ?FUNCTION_NAME}, Commands),
    ok.

scenario6(_Config) ->
    E = c:pid(0,465,9),
    Commands = [make_enqueue(E,1,msg(<<>>)), %% 1 msg on queue - snap: prefix 1
                make_enqueue(E,2,msg(<<>>)) %% 1. msg on queue - snap: prefix 1
               ],
    run_snapshot_test(#{name => ?FUNCTION_NAME,
                        max_length => 1}, Commands),
    ok.

scenario7(_Config) ->
    C1 = {<<>>, c:pid(0,208,0)},
    E = c:pid(0,188,0),
    Commands = [
                make_enqueue(E,1,msg(<<>>)),
                make_checkout(C1, {auto,1,simple_prefetch}),
                make_enqueue(E,2,msg(<<>>)),
                make_enqueue(E,3,msg(<<>>)),
                make_settle(C1,[0])],
    run_snapshot_test(#{name => ?FUNCTION_NAME,
                        max_length => 1}, Commands),
    ok.

scenario8(_Config) ->
    C1 = {<<>>, c:pid(0,208,0)},
    E = c:pid(0,188,0),
    Commands = [
                make_enqueue(E,1,msg(<<>>)),
                make_enqueue(E,2,msg(<<>>)),
                make_checkout(C1, {auto,1,simple_prefetch}),
                % make_checkout(C1, cancel),
                {down, E, noconnection},
                make_settle(C1, [0])],
    run_snapshot_test(#{name => ?FUNCTION_NAME,
                        max_length => 1}, Commands),
    ok.

scenario9(_Config) ->
    E = c:pid(0,188,0),
    Commands = [
                make_enqueue(E,1,msg(<<>>)),
                make_enqueue(E,2,msg(<<>>)),
                make_enqueue(E,3,msg(<<>>))],
    run_snapshot_test(#{name => ?FUNCTION_NAME,
                        max_length => 1}, Commands),
    ok.

scenario10(_Config) ->
    C1 = {<<>>, c:pid(0,208,0)},
    E = c:pid(0,188,0),
    Commands = [
                make_checkout(C1, {auto,1,simple_prefetch}),
                make_enqueue(E,1,msg(<<>>)),
                make_settle(C1, [0])
               ],
    run_snapshot_test(#{name => ?FUNCTION_NAME,
                        max_length => 1}, Commands),
    ok.

scenario11(_Config) ->
    C1 = {<<>>, c:pid(0,215,0)},
    E = c:pid(0,217,0),
    Commands = [
                make_enqueue(E,1,msg(<<"1">>)), % 1
                make_checkout(C1, {auto,1,simple_prefetch}), % 2
                make_checkout(C1, cancel), % 3
                make_enqueue(E,2,msg(<<"22">>)), % 4
                make_checkout(C1, {auto,1,simple_prefetch}), % 5
                make_settle(C1, [0]), % 6
                make_checkout(C1, cancel) % 7
                ],
    run_snapshot_test(#{name => ?FUNCTION_NAME,
                        max_length => 2}, Commands),
    ok.

scenario12(_Config) ->
    E = c:pid(0,217,0),
    Commands = [make_enqueue(E,1,msg(<<0>>)),
                make_enqueue(E,2,msg(<<0>>)),
                make_enqueue(E,3,msg(<<0>>))],
    run_snapshot_test(#{name => ?FUNCTION_NAME,
                        max_bytes => 2}, Commands),
    ok.

scenario13(_Config) ->
    E = c:pid(0,217,0),
    Commands = [make_enqueue(E,1,msg(<<0>>)),
                make_enqueue(E,2,msg(<<>>)),
                make_enqueue(E,3,msg(<<>>)),
                make_enqueue(E,4,msg(<<>>))
               ],
    run_snapshot_test(#{name => ?FUNCTION_NAME,
                        max_length => 2}, Commands),
    ok.

scenario14(_Config) ->
    E = c:pid(0,217,0),
    Commands = [make_enqueue(E,1,msg(<<0,0>>))],
    run_snapshot_test(#{name => ?FUNCTION_NAME,
                        max_bytes => 1}, Commands),
    ok.

scenario14b(_Config) ->
    E = c:pid(0,217,0),
    Commands = [
                make_enqueue(E,1,msg(<<0>>)),
                make_enqueue(E,2,msg(<<0>>))
               ],
    run_snapshot_test(#{name => ?FUNCTION_NAME,
                        max_bytes => 1}, Commands),
    ok.

scenario15(_Config) ->
    C1 = {<<>>, c:pid(0,179,1)},
    E = c:pid(0,176,1),
    Commands = [make_checkout(C1, {auto,2,simple_prefetch}),
                make_enqueue(E, 1, msg(<<"msg1">>)),
                make_enqueue(E, 2, msg(<<"msg2">>)),
                make_return(C1, [0]),
                make_return(C1, [2]),
                make_settle(C1, [1])
               ],
    run_snapshot_test(#{name => ?FUNCTION_NAME,
                        delivery_limit => 1}, Commands),
    ok.

scenario16(_Config) ->
    C1Pid = c:pid(0,883,1),
    C1 = {<<>>, C1Pid},
    C2 = {<<>>, c:pid(0,882,1)},
    E = c:pid(0,176,1),
    Commands = [
                make_checkout(C1, {auto,1,simple_prefetch}),
                make_enqueue(E, 1, msg(<<"msg1">>)),
                make_checkout(C2, {auto,1,simple_prefetch}),
                {down, C1Pid, noproc}, %% msg1 allocated to C2
                make_return(C2, [0]), %% msg1 returned
                make_enqueue(E, 2, msg(<<>>)),
                make_settle(C2, [0])
               ],
    run_snapshot_test(#{name => ?FUNCTION_NAME,
                        delivery_limit => 1}, Commands),
    ok.

scenario17(_Config) ->
    C1Pid = test_util:fake_pid(rabbit@fake_node1),
    C1 = {<<0>>, C1Pid},
    % C2Pid = test_util:fake_pid(fake_node1),
    C2 = {<<>>, C1Pid},
    E = test_util:fake_pid(rabbit@fake_node2),
    Commands = [
                make_checkout(C1, {auto,1,simple_prefetch}),
                make_enqueue(E,1,msg(<<"one">>)),
                make_checkout(C2, {auto,1,simple_prefetch}),
                {down, C1Pid, noconnection},
                make_checkout(C2, cancel),
                make_enqueue(E,2,msg(<<"two">>)),
                {nodeup,rabbit@fake_node1},
                %% this has no effect as was returned
                make_settle(C1, [0]),
                %% this should settle "one"
                make_settle(C1, [1])
                ],
    run_snapshot_test(#{name => ?FUNCTION_NAME,
                        single_active_consumer_on => true
                       }, Commands),
    ok.

scenario18(_Config) ->
    E = c:pid(0,176,1),
    Commands = [make_enqueue(E,1,msg(<<"1">>)),
                make_enqueue(E,2,msg(<<"2">>)),
                make_enqueue(E,3,msg(<<"3">>)),
                make_enqueue(E,4,msg(<<"4">>)),
                make_enqueue(E,5,msg(<<"5">>))
               ],
    run_snapshot_test(#{name => ?FUNCTION_NAME,
                        %% max_length => 3,
                        max_in_memory_length => 1}, Commands),
    ok.

scenario19(_Config) ->
    C1Pid = c:pid(0,883,1),
    C1 = {<<>>, C1Pid},
    E = c:pid(0,176,1),
    Commands = [make_enqueue(E,1,msg(<<"1">>)),
                make_enqueue(E,2,msg(<<"2">>)),
                make_checkout(C1, {auto,2,simple_prefetch}),
                make_enqueue(E,3,msg(<<"3">>)),
                make_settle(C1, [0, 1])
               ],
    run_snapshot_test(#{name => ?FUNCTION_NAME,
                        max_in_memory_bytes => 370,
                        max_in_memory_length => 1}, Commands),
    ok.

scenario20(_Config) ->
    C1Pid = c:pid(0,883,1),
    C1 = {<<>>, C1Pid},
    E = c:pid(0,176,1),
    Commands = [make_enqueue(E,1,msg(<<>>)),
                make_enqueue(E,2,msg(<<1>>)),
                make_checkout(C1, {auto,2,simple_prefetch}),
                {down, C1Pid, noconnection},
                make_enqueue(E,3,msg(<<0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0>>)),
                make_enqueue(E,4,msg(<<0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0>>)),
                make_enqueue(E,5,msg(<<0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0>>)),
                make_enqueue(E,6,msg(<<0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0>>)),
                make_enqueue(E,7,msg(<<0,0,0,0,0,0,0,0,0,0,0,0,0,0>>))
               ],
    run_snapshot_test(#{name => ?FUNCTION_NAME,
                        max_length => 4,
                        % max_bytes => 97,
                        max_in_memory_length => 1}, Commands),
    ok.

scenario21(_Config) ->
    C1Pid = c:pid(0,883,1),
    C1 = {<<>>, C1Pid},
    E = c:pid(0,176,1),
    Commands = [
                make_checkout(C1, {auto,2,simple_prefetch}),
                make_enqueue(E,1,msg(<<"1">>)),
                make_enqueue(E,2,msg(<<"2">>)),
                make_enqueue(E,3,msg(<<"3">>)),
                rabbit_fifo:make_discard(C1, [0]),
                rabbit_fifo:make_settle(C1, [1])
               ],
    run_snapshot_test(#{name => ?FUNCTION_NAME,
                        release_cursor_interval => 1,
                        dead_letter_handler => {at_most_once, {?MODULE, banana, []}}},
                      Commands),
    ok.

scenario22(_Config) ->
    % C1Pid = c:pid(0,883,1),
    % C1 = {<<>>, C1Pid},
    E = c:pid(0,176,1),
    Commands = [
                make_enqueue(E,1,msg(<<"1">>)),
                make_enqueue(E,2,msg(<<"2">>)),
                make_enqueue(E,3,msg(<<"3">>)),
                make_enqueue(E,4,msg(<<"4">>)),
                make_enqueue(E,5,msg(<<"5">>))
               ],
    run_snapshot_test(#{name => ?FUNCTION_NAME,
                        release_cursor_interval => 1,
                        max_length => 3,
                        dead_letter_handler => {at_most_once, {?MODULE, banana, []}}},
                      Commands),
    ok.

scenario24(_Config) ->
    C1Pid = c:pid(0,242,0),
    C1 = {<<>>, C1Pid},
    C2 = {<<0>>, C1Pid},
    E = c:pid(0,240,0),
    Commands = [
                make_checkout(C1, {auto,2,simple_prefetch}), %% 1
                make_checkout(C2, {auto,1,simple_prefetch}), %% 2
                make_enqueue(E,1,msg(<<"1">>)), %% 3
                make_enqueue(E,2,msg(<<"2b">>)), %% 4
                make_enqueue(E,3,msg(<<"3">>)), %% 5
                make_enqueue(E,4,msg(<<"4">>)), %% 6
                {down, E, noconnection} %% 7
               ],
    run_snapshot_test(#{name => ?FUNCTION_NAME,
                        release_cursor_interval => 0,
                        deliver_limit => undefined,
                        max_length => 3,
                        overflow_strategy => drop_head,
                        dead_letter_handler => {at_most_once, {?MODULE, banana, []}}
                       },
                      Commands),
    ok.

scenario25(_Config) ->
    C1Pid = c:pid(0,282,0),
    C2Pid = c:pid(0,281,0),
    C1 = {<<>>, C1Pid},
    C2 = {<<>>, C2Pid},
    E = c:pid(0,280,0),
    Commands = [
                make_checkout(C1, {auto,2,simple_prefetch}), %% 1
                make_enqueue(E,1,msg(<<0>>)), %% 2
                make_checkout(C2, {auto,1,simple_prefetch}), %% 3
                make_enqueue(E,2,msg(<<>>)), %% 4
                make_enqueue(E,3,msg(<<>>)), %% 5
                {down, C1Pid, noproc}, %% 6
                make_enqueue(E,4,msg(<<>>)), %% 7
                rabbit_fifo:make_purge() %% 8
               ],
    run_snapshot_test(#{name => ?FUNCTION_NAME,
                        max_bytes => undefined,
                        release_cursor_interval => 0,
                        deliver_limit => undefined,
                        overflow_strategy => drop_head,
                        dead_letter_handler => {at_most_once, {?MODULE, banana, []}}
                       },
                      Commands),
    ok.

scenario26(_Config) ->
    C1Pid = c:pid(0,242,0),
    C1 = {<<>>, C1Pid},
    E1 = c:pid(0,436,0),
    E2 = c:pid(0,435,0),
    Commands = [
                make_enqueue(E1,2,msg(<<>>)), %% 1
                make_enqueue(E1,3,msg(<<>>)), %% 2
                make_enqueue(E2,1,msg(<<>>)), %% 3
                make_enqueue(E2,2,msg(<<>>)), %% 4
                make_enqueue(E1,4,msg(<<>>)), %% 5
                make_enqueue(E1,5,msg(<<>>)), %% 6
                make_enqueue(E1,6,msg(<<>>)), %% 7
                make_enqueue(E1,7,msg(<<>>)), %% 8
                make_enqueue(E1,1,msg(<<>>)), %% 9
                make_checkout(C1, {auto,5,simple_prefetch}), %% 1
                make_enqueue(E1,8,msg(<<>>)), %% 2
                make_enqueue(E1,9,msg(<<>>)), %% 2
                make_enqueue(E1,10,msg(<<>>)), %% 2
                {down, C1Pid, noconnection}
               ],
    run_snapshot_test(#{name => ?FUNCTION_NAME,
                        release_cursor_interval => 0,
                        deliver_limit => undefined,
                        max_length => 8,
                        overflow_strategy => drop_head,
                        dead_letter_handler => {at_most_once, {?MODULE, banana, []}}
                       },
                      Commands),
    ok.

scenario28(_Config) ->
    E = c:pid(0,151,0),
    Conf = #{dead_letter_handler => {at_most_once, {rabbit_fifo_prop_SUITE,banana,[]}},
             delivery_limit => undefined,
             max_in_memory_bytes => undefined,
             max_length => 1,name => ?FUNCTION_NAME,overflow_strategy => drop_head,
             release_cursor_interval => 100,single_active_consumer_on => false},
    Commands = [
                make_enqueue(E,2,msg( <<>>)),
                make_enqueue(E,3,msg( <<>>)),
                make_enqueue(E,1,msg( <<>>))
               ],
    ?assert(single_active_prop(Conf, Commands, false)),
    ok.

scenario27(_Config) ->
    C1Pid = test_util:fake_pid(fakenode@fake),
    % C2Pid = c:pid(0,281,0),
    C1 = {<<>>, C1Pid},
    C2 = {<<>>, C1Pid},
    E = c:pid(0,151,0),
    E2 = c:pid(0,152,0),
    Commands = [
                make_enqueue(E,1,msg(<<>>)),
                make_enqueue(E2,1,msg(<<28,202>>)),
                make_enqueue(E,2,msg(<<"Î2">>)),
                {down, E, noproc},
                make_enqueue(E2,2,msg(<<"ê">>)),
                {nodeup,fakenode@fake},
                make_enqueue(E2,3,msg(<<>>)),
                make_enqueue(E2,4,msg(<<>>)),
                make_enqueue(E2,5,msg(<<>>)),
                make_enqueue(E2,6,msg(<<>>)),
                make_enqueue(E2,7,msg(<<>>)),
                make_enqueue(E2,8,msg(<<>>)),
                make_enqueue(E2,9,msg(<<>>)),
                {purge},
                make_enqueue(E2,10,msg(<<>>)),
                make_enqueue(E2,11,msg(<<>>)),
                make_enqueue(E2,12,msg(<<>>)),
                make_enqueue(E2,13,msg(<<>>)),
                make_enqueue(E2,14,msg(<<>>)),
                make_enqueue(E2,15,msg(<<>>)),
                make_enqueue(E2,16,msg(<<>>)),
                make_enqueue(E2,17,msg(<<>>)),
                make_enqueue(E2,18,msg(<<>>)),
                {nodeup,fakenode@fake},
                make_enqueue(E2,19,msg(<<>>)),
                make_checkout(C1, {auto,77,simple_prefetch}),
                make_enqueue(E2,20,msg(<<>>)),
                make_enqueue(E2,21,msg(<<>>)),
                make_enqueue(E2,22,msg(<<>>)),
                make_enqueue(E2,23,msg(<<"Ýý">>)),
                make_checkout(C2, {auto,66,simple_prefetch}),
                {purge},
                make_enqueue(E2,24,msg(<<>>))
               ],
    ?assert(
       single_active_prop(#{name => ?FUNCTION_NAME,
                            max_bytes => undefined,
                            release_cursor_interval => 100,
                            deliver_limit => 1,
                            max_length => 1,
                            max_in_memory_length => 8,
                            max_in_memory_bytes => 691,
                            overflow_strategy => drop_head,
                            single_active_consumer_on => true,
                            dead_letter_handler => {at_most_once, {?MODULE, banana, []}}
                           }, Commands, false)),
    ok.

scenario30(_Config) ->
    C1Pid = c:pid(0,242,0),
    C1 = {<<>>, C1Pid},
    E = c:pid(0,240,0),
    Commands = [
                make_enqueue(E,1,msg(<<>>)), %% 1
                make_enqueue(E,2,msg(<<1>>)), %% 2
                make_checkout(C1, {auto,1,simple_prefetch}), %% 3
                {down, C1Pid, noconnection}, %% 4
                make_enqueue(E,3,msg(<<>>)) %% 5
               ],
    run_snapshot_test(#{name => ?FUNCTION_NAME,
                        release_cursor_interval => 0,
                        deliver_limit => undefined,
                        max_length => 1,
                        max_in_memory_length => 1,
                        overflow_strategy => drop_head,
                        dead_letter_handler => {at_most_once, {?MODULE, banana, []}},
                        single_active_consumer_on => true
                       },
                      Commands),
    ok.

scenario31(_Config) ->
    C1Pid = c:pid(0,242,0),
    C1 = {<<>>, C1Pid},
    E1 = c:pid(0,314,0),
    E2 = c:pid(0,339,0),
    Commands = [
                % [{1,{enqueue,<0.314.0>,1,<<>>}},
                %  {2,{enqueue,<0.339.0>,2,<<>>}},
                %  {3,
                %   {checkout,{<<>>,<10689.342.0>},
                %    {auto,1,simple_prefetch},
                %    #{ack => true,args => [],prefetch => 1,username => <<"user">>}}},
                %  {4,{purge}}]
                make_enqueue(E1,1,msg(<<>>)), %% 1
                make_enqueue(E1,0,msg(<<>>)), %% 1
                make_enqueue(E1,1,msg(<<>>)), %% 1
                make_enqueue(E2,2,msg(<<1>>)), %% 2
                make_checkout(C1, {auto,1,simple_prefetch}), %% 3
                {purge} %% 4
               ],
    run_snapshot_test(#{name => ?FUNCTION_NAME,
                        release_cursor_interval => 0,
                        deliver_limit => undefined,
                        overflow_strategy => drop_head,
                        dead_letter_handler => {at_most_once, {?MODULE, banana, []}}
                       },
                      Commands),
    ok.

scenario32(_Config) ->
    E1 = c:pid(0,314,0),
    Commands = [
                make_enqueue(E1,1,msg(<<0>>)), %% 1
                make_enqueue(E1,2,msg(<<0,0>>)), %% 2
                make_enqueue(E1,4,msg(<<0,0,0,0>>)), %% 3
                make_enqueue(E1,3,msg(<<0,0,0>>)), %% 4
                make_enqueue(E1,4,msg(<<0,0,0,0>>)) %% 3
               ],
    run_snapshot_test(#{name => ?FUNCTION_NAME,
                        release_cursor_interval => 0,
                        max_length => 3,
                        deliver_limit => undefined,
                        overflow_strategy => drop_head,
                        dead_letter_handler => {at_most_once, {?MODULE, banana, []}}
                       },
                      Commands),
    ok.

scenario29(_Config) ->
    C1Pid = c:pid(0,242,0),
    C1 = {<<>>, C1Pid},
    E = c:pid(0,240,0),
    Commands = [
                make_enqueue(E,1,msg(<<>>)), %% 1
                make_enqueue(E,2,msg(<<>>)), %% 2
                make_checkout(C1, {auto,2,simple_prefetch}), %% 2
                make_enqueue(E,3,msg(<<>>)), %% 3
                make_enqueue(E,4,msg(<<>>)), %% 4
                make_enqueue(E,5,msg(<<>>)), %% 5
                make_enqueue(E,6,msg(<<>>)), %% 6
                make_enqueue(E,7,msg(<<>>)), %% 7
                {down, E, noconnection} %% 8
               ],
    run_snapshot_test(#{name => ?FUNCTION_NAME,
                        release_cursor_interval => 0,
                        deliver_limit => undefined,
                        max_length => 5,
                        max_in_memory_length => 1,
                        overflow_strategy => drop_head,
                        dead_letter_handler => {at_most_once, {?MODULE, banana, []}},
                        single_active_consumer_on => true
                       },
                      Commands),
    ok.
scenario23(_Config) ->
    C1Pid = c:pid(0,242,0),
    C1 = {<<>>, C1Pid},
    E = c:pid(0,240,0),
    Commands = [
                make_enqueue(E,1,msg(<<>>)), %% 1
                make_checkout(C1, {auto,2,simple_prefetch}), %% 2
                make_enqueue(E,2,msg(<<>>)), %% 3
                make_enqueue(E,3,msg(<<>>)), %% 4
                {down, E, noconnection}, %% 5
                make_enqueue(E,4,msg(<<>>)) %% 6
               ],
    run_snapshot_test(#{name => ?FUNCTION_NAME,
                        release_cursor_interval => 0,
                        deliver_limit => undefined,
                        max_length => 2,
                        overflow_strategy => drop_head,
                        dead_letter_handler => {at_most_once, {?MODULE, banana, []}}
                       },
                      Commands),
    ok.

single_active_01(_Config) ->
    C1Pid = test_util:fake_pid(rabbit@fake_node1),
    C1 = {<<0>>, C1Pid},
    C2Pid = test_util:fake_pid(rabbit@fake_node2),
    C2 = {<<>>, C2Pid},
    E = test_util:fake_pid(rabbit@fake_node2),
    Commands = [
                make_checkout(C1, {auto,1,simple_prefetch}),
                make_enqueue(E,1,msg(<<"one">>)),
                make_checkout(C2, {auto,1,simple_prefetch}),
                make_checkout(C1, cancel),
                {nodeup,rabbit@fake_node1}
                ],
    ?assert(
       single_active_prop(#{name => ?FUNCTION_NAME,
                            single_active_consumer_on => true
                       }, Commands, false)),
    ok.

single_active_02(_Config) ->
    C1Pid = test_util:fake_pid(node()),
    C1 = {<<0>>, C1Pid},
    C2Pid = test_util:fake_pid(node()),
    C2 = {<<>>, C2Pid},
    E = test_util:fake_pid(node()),
    Commands = [
                make_checkout(C1, {auto,1,simple_prefetch}),
                make_enqueue(E,1,msg(<<"one">>)),
                {down,E,noconnection},
                make_checkout(C2, {auto,1,simple_prefetch}),
                make_checkout(C2, cancel),
                {down,E,noconnection}
                ],
    Conf = config(?FUNCTION_NAME, undefined, undefined, true, 1,
                  undefined, undefined),
    ?assert(single_active_prop(Conf, Commands, false)),
    ok.

single_active_03(_Config) ->
    C1Pid = test_util:fake_pid(node()),
    C1 = {<<0>>, C1Pid},
    Pid = test_util:fake_pid(node()),
    E = test_util:fake_pid(rabbit@fake_node2),
    Commands = [
                make_checkout(C1, {auto,2,simple_prefetch}),
                make_enqueue(E, 1, msg(<<0>>)),
                make_enqueue(E, 2, msg(<<1>>)),
                {down, Pid, noconnection},
                {nodeup, node()}
                ],
    Conf = config(?FUNCTION_NAME, 0, 0, true, 0, undefined, undefined),
    ?assert(single_active_prop(Conf, Commands, true)),
    ok.

single_active_04(_Config) ->
    E = test_util:fake_pid(rabbit@fake_node2),
    Commands = [
                make_enqueue(E, 1, msg(<<>>)),
                make_enqueue(E, 2, msg(<<>>)),
                make_enqueue(E, 3, msg(<<>>)),
                make_enqueue(E, 4, msg(<<>>))
                ],
    Conf = config(?FUNCTION_NAME, 3, 587, true, 3),
    ?assert(single_active_prop(Conf, Commands, true)),
    ok.

test_run_log(_Config) ->
    meck:expect(rabbit_feature_flags, is_enabled,
                fun (_) -> true end),
    run_proper(
      fun () ->
              ?FORALL({Length, Bytes, SingleActiveConsumer, DeliveryLimit},
                      frequency([{10, {0, 0, false, 0}},
                                 {5, {oneof([range(1, 10), undefined]),
                                      oneof([range(1, 1000), undefined]),
                                      boolean(),
                                      oneof([range(1, 3), undefined])
                                     }}]),
                      ?FORALL(O, ?LET(Ops, log_gen(100), expand(Ops, #{})),
                              collect({log_size, length(O)},
                                      dump_generated(
                                        config(?FUNCTION_NAME,
                                               Length,
                                               Bytes,
                                               SingleActiveConsumer,
                                               DeliveryLimit), O))))
      end, [], 10).

snapshots(_Config) ->
    meck:expect(rabbit_feature_flags, is_enabled, fun (_) -> true end),
    run_proper(
      fun () ->
              ?FORALL({Length, Bytes, SingleActiveConsumer,
                       DeliveryLimit, Overflow, DeadLetterHandler},
                      frequency([{10, {0, 0, false, 0, drop_head, undefined}},
                                 {5, {oneof([range(1, 10), undefined]),
                                      oneof([range(1, 1000), undefined]),
                                      boolean(),
                                      oneof([range(1, 3), undefined]),
                                      oneof([drop_head, reject_publish]),
                                      oneof([undefined,
                                             {at_most_once, {?MODULE, banana, []}}])
                                     }}]),
                      begin
                          Config = config(?FUNCTION_NAME,
                                          Length,
                                          Bytes,
                                          SingleActiveConsumer,
                                          DeliveryLimit,
                                          Overflow,
                                          DeadLetterHandler),
                          ?FORALL(O, ?LET(Ops, log_gen(256), expand(Ops, Config)),
                                  collect({log_size, length(O)},
                                          snapshots_prop(Config, O)))
                      end)
      end, [], 256).

snapshots_dlx(_Config) ->
    meck:expect(rabbit_feature_flags, is_enabled, fun (_) -> true end),
    Size = 256,
    run_proper(
      fun () ->
              ?FORALL({Length, Bytes, SingleActiveConsumer, DeliveryLimit},
                      frequency([{10, {0, 0, false, 0}},
                                 {5, {oneof([range(1, 10), undefined]),
                                      oneof([range(1, 1000), undefined]),
                                      boolean(),
                                      oneof([range(1, 1000), undefined])
                                     }}]),
                      begin
                          Config = config(?FUNCTION_NAME,
                                          Length,
                                          Bytes,
                                          SingleActiveConsumer,
                                          DeliveryLimit,
                                          reject_publish,
                                          at_least_once),
                          ?FORALL(O, ?LET(Ops, log_gen_dlx(Size), expand(Ops, Config)),
                                  collect({log_size, length(O)},
                                          snapshots_prop(Config, O)))
                      end)
      end, [], Size).

single_active(_Config) ->
    %% validates that there can only ever be a single active consumer at a time
    %% as well as that message deliveries are done in order
    Size = 1000,
    meck:expect(rabbit_feature_flags, is_enabled, fun (_) -> true end),
    run_proper(
      fun () ->
              ?FORALL({Length, Bytes, DeliveryLimit},
                      frequency([{10, {undefined, undefined, undefined}},
                                 {5, {oneof([range(1, 10), undefined]),
                                      oneof([range(1, 1000), undefined]),
                                      oneof([range(1, 3), undefined])
                                     }}]),
                      begin
                          Config  = config(?FUNCTION_NAME,
                                           Length,
                                           Bytes,
                                           true,
                                           DeliveryLimit),
                      ?FORALL(O, ?LET(Ops, log_gen(Size), expand(Ops, Config)),
                              collect({log_size, length(O)},
                                      single_active_prop(Config, O, false)))
                      end)
      end, [], Size).

upgrade(_Config) ->
    Size = 256,
    %% upgrade is always done using _old_ command formats
    meck:expect(rabbit_feature_flags, is_enabled, fun (_) -> false end),
    run_proper(
      fun () ->
              ?FORALL({Length, Bytes, DeliveryLimit, SingleActive},
                      frequency([{5, {undefined, undefined, undefined, false}},
                                 {5, {oneof([range(1, 10), undefined]),
                                      oneof([range(1, 1000), undefined]),
                                      oneof([range(1, 3), undefined]),
                                      oneof([true, false])
                                     }}]),
                      begin
                          Config  = config(?FUNCTION_NAME,
                                           Length,
                                           Bytes,
                                           SingleActive,
                                           DeliveryLimit,
                                           drop_head,
                                           undefined
                                          ),
                      ?FORALL(O, ?LET(Ops, log_gen(Size), expand(Ops, Config)),
                              collect({log_size, length(O)},
                                      upgrade_prop(Config, O)))
                      end)
      end, [], Size).

messages_total(_Config) ->
    meck:expect(rabbit_feature_flags, is_enabled, fun (_) -> false end),
    Size = 256,
    run_proper(
      fun () ->
              ?FORALL({Length, Bytes, DeliveryLimit, SingleActive},
                      frequency([{5, {undefined, undefined, undefined, false}},
                                 {5, {oneof([range(1, 10), undefined]),
                                      oneof([range(1, 1000), undefined]),
                                      oneof([range(1, 3), undefined]),
                                      oneof([true, false])
                                     }}]),
                      begin
                          Config  = config(?FUNCTION_NAME,
                                           Length,
                                           Bytes,
                                           SingleActive,
                                           DeliveryLimit),
                      ?FORALL(O, ?LET(Ops, log_gen(Size), expand(Ops, Config)),
                              collect({log_size, length(O)},
                                      messages_total_prop(Config, O)))
                      end)
      end, [], Size).

simple_prefetch(_Config) ->
    Size = 500,
    meck:expect(rabbit_feature_flags, is_enabled, fun (_) -> true end),
    run_proper(
      fun () ->
              ?FORALL({Length, Bytes, DeliveryLimit, SingleActive},
                      frequency([{5, {undefined, undefined, undefined, false}},
                                 {5, {oneof([range(1, 10), undefined]),
                                      oneof([range(1, 1000), undefined]),
                                      oneof([range(1, 3), undefined]),
                                      oneof([true, false])
                                     }}]),
                      begin
                          Config  = config(?FUNCTION_NAME,
                                           Length,
                                           Bytes,
                                           SingleActive,
                                           DeliveryLimit),
                          ?FORALL(O, ?LET(Ops, log_gen(Size), expand(Ops, Config)),
                                  collect({log_size, length(O)},
                                          simple_prefetch_prop(Config, O, true)))
                      end)
      end, [], Size).

simple_prefetch_without_checkout_cancel(_Config) ->
    Size = 256,
    meck:expect(rabbit_feature_flags, is_enabled, fun (_) -> true end),
    run_proper(
      fun () ->
              ?FORALL({Length, Bytes, DeliveryLimit, SingleActive},
                      frequency([{5, {undefined, undefined, undefined, false}},
                                 {5, {oneof([range(1, 10), undefined]),
                                      oneof([range(1, 1000), undefined]),
                                      oneof([range(1, 3), undefined]),
                                      oneof([true, false])
                                     }}]),
                      begin
                          Config  = config(?FUNCTION_NAME,
                                           Length,
                                           Bytes,
                                           SingleActive,
                                           DeliveryLimit),
                          ?FORALL(O, ?LET(Ops, log_gen_without_checkout_cancel(Size),
                                          expand(Ops, Config)),
                                  collect({log_size, length(O)},
                                          simple_prefetch_prop(Config, O, false)))
                      end)
      end, [], Size).

simple_prefetch_01(_Config) ->
    C1Pid = test_util:fake_pid(rabbit@fake_node1),
    C1 = {<<0>>, C1Pid},
    E = test_util:fake_pid(rabbit@fake_node2),
    Commands = [
                make_enqueue(E,1,msg(<<"1">>)),
                make_enqueue(E,2,msg(<<"22">>)),
                make_checkout(C1, {auto,2,simple_prefetch}, #{prefetch => 2}),
                make_checkout(C1, cancel),
                make_checkout(C1, {auto,1,simple_prefetch}, #{prefetch => 1}),
                make_settle(C1, [0]),
                make_settle(C1, [1])
               ],
    ?assert(
       simple_prefetch_prop(#{name => ?FUNCTION_NAME,
                              max_length => 2,
                              delivery_limit => 3
                             },
                            Commands, true)),
    ok.

single_active_ordering(_Config) ->
    Size = 500,
    meck:expect(rabbit_feature_flags, is_enabled, fun (_) -> true end),
    Fun = {-1, fun ({Prev, _}) -> {Prev + 1, Prev + 1} end},
    run_proper(
      fun () ->
              ?FORALL(O, ?LET(Ops, log_gen_ordered(Size), expand(Ops, Fun)),
                      collect({log_size, length(O)},
                              single_active_prop(
                                config(?FUNCTION_NAME,
                                       undefined,
                                       undefined,
                                       true,
                                       undefined), O,
                                true)))
      end, [], Size).

single_active_ordering_01(_Config) ->
    C1Pid = test_util:fake_pid(node()),
    C1 = {<<0>>, C1Pid},
    E = test_util:fake_pid(rabbit@fake_node2),
    E2 = test_util:fake_pid(rabbit@fake_node2),
    Commands = [
                make_enqueue(E, 1, msg(<<"0">>)),
                make_enqueue(E, 2, msg(<<"1">>)),
                make_checkout(C1, {auto,2,simple_prefetch}),
                make_enqueue(E2, 1, msg(<<"2">>)),
                make_settle(C1, [0])
                ],
    Conf = config(?FUNCTION_NAME, 0, 0, true, 0),
    ?assert(single_active_prop(Conf, Commands, true)),
    ok.

single_active_ordering_02(_Config) ->
    %% this results in the pending enqueue being enqueued and violating
    %% ordering
% [{checkout, %   {<<>>,<0.177.0>}, %   {auto,1,simple_prefetch},
%  {enqueue,<0.172.0>,2,1},
%  {down,<0.172.0>,noproc},
%  {settle,{<<>>,<0.177.0>},[0]}]
    C1Pid = test_util:fake_pid(node()),
    C1 = {<<0>>, C1Pid},
    E = test_util:fake_pid(node()),
    Commands = [
                make_checkout(C1, {auto,1,simple_prefetch}),
                make_enqueue(E, 2, msg(<<"1">>)),
                %% CANNOT HAPPEN
                {down,E,noproc},
                make_settle(C1, [0])
                ],
    Conf = config(?FUNCTION_NAME, 0, 0, true, 0),
    ?assert(single_active_prop(Conf, Commands, true)),
    ok.

single_active_ordering_03(_Config) ->
    C1Pid = test_util:fake_pid(node()),
    C1 = {<<1>>, C1Pid},
    C2Pid = test_util:fake_pid(rabbit@fake_node2),
    C2 = {<<2>>, C2Pid},
    E = test_util:fake_pid(rabbit@fake_node2),
    Commands = [
                make_enqueue(E, 1, msg(<<"0">>)),
                make_enqueue(E, 2, msg(<<"1">>)),
                make_enqueue(E, 3, msg(<<"2">>)),
                make_checkout(C1, {auto,1,simple_prefetch}),
                make_checkout(C2, {auto,1,simple_prefetch}),
                make_settle(C1, [0]),
                make_checkout(C1, cancel),
                {down, C1Pid, noconnection}
                ],
    Conf0 = config(?FUNCTION_NAME, 0, 0, true, 0),
    Conf = Conf0#{release_cursor_interval => 100},
    Indexes = lists:seq(1, length(Commands)),
    Entries = lists:zip(Indexes, Commands),
    try run_log(test_init(Conf), Entries) of
        {State, Effects} ->
            ct:pal("Effects: ~tp~n", [Effects]),
            ct:pal("State: ~tp~n", [State]),
            %% assert C1 has no messages
            ?assertNotMatch(#{C1 := _}, State#rabbit_fifo.consumers),
            true;
        _ ->
            true
    catch
        Err ->
            ct:pal("Commands: ~tp~nConf~tp~n", [Commands, Conf]),
            ct:pal("Err: ~tp~n", [Err]),
            false
    end.

max_length(_Config) ->
    %% tests that max length is never transgressed
    Size = 1000,
    run_proper(
      fun () ->
              ?FORALL({Length, SingleActiveConsumer, DeliveryLimit},
                      {oneof([range(1, 100), undefined]),
                       boolean(),
                       range(1, 3)
                      },
                      begin
                          Config = config(?FUNCTION_NAME,
                                          Length,
                                          undefined,
                                          SingleActiveConsumer,
                                          DeliveryLimit),
                          ?FORALL(O, ?LET(Ops, log_gen_config(Size),
                                          expand(Ops, Config)),
                                  collect({log_size, length(O)},
                                          max_length_prop(Config, O)))
                      end)
      end, [], Size).

%% Test that rabbit_fifo_dlx can check out a prefix message.
dlx_01(_Config) ->
    C1Pid = c:pid(0,883,1),
    C1 = {<<>>, C1Pid},
    E = c:pid(0,176,1),
    Commands = [
                rabbit_fifo_dlx:make_checkout(ignore_pid, 1),
                make_checkout(C1, {auto,1,simple_prefetch}),
                make_enqueue(E,1,msg(<<"1">>)),
                make_enqueue(E,2,msg(<<"2">>)),
                rabbit_fifo:make_discard(C1, [0]),
                rabbit_fifo_dlx:make_settle([0]),
                rabbit_fifo:make_discard(C1, [1]),
                rabbit_fifo_dlx:make_settle([1])
               ],
    Config = config(?FUNCTION_NAME, 8, undefined, false, 2,
                    reject_publish, at_least_once),
    ?assert(snapshots_prop(Config, Commands)),
    ok.

%% Test that dehydrating dlx_consumer works.
dlx_02(_Config) ->
    C1Pid = c:pid(0,883,1),
    C1 = {<<>>, C1Pid},
    E = c:pid(0,176,1),
    Commands = [
                rabbit_fifo_dlx:make_checkout(ignore_pid, 1),
                make_checkout(C1, {auto,1,simple_prefetch}),
                make_enqueue(E,1,msg(<<"1">>)),
                %% State contains release cursor A.
                rabbit_fifo:make_discard(C1, [0]),
                make_enqueue(E,2,msg(<<"2">>)),
                %% State contains release cursor B
                %% with the 1st msg being checked out to dlx_consumer and
                %% being dehydrated.
                rabbit_fifo_dlx:make_settle([0])
                %% Release cursor A got emitted.
               ],
    Config = config(?FUNCTION_NAME, 10, undefined, false, 5,
                    reject_publish, at_least_once),
    ?assert(snapshots_prop(Config, Commands)),
    ok.

%% Test that dehydrating discards queue works.
dlx_03(_Config) ->
    C1Pid = c:pid(0,883,1),
    C1 = {<<>>, C1Pid},
    E = c:pid(0,176,1),
    Commands = [
                make_enqueue(E,1,msg(<<"1">>)),
                %% State contains release cursor A.
                make_checkout(C1, {auto,1,simple_prefetch}),
                rabbit_fifo:make_discard(C1, [0]),
                make_enqueue(E,2,msg(<<"2">>)),
                %% State contains release cursor B.
                %% 1st message sitting in discards queue got dehydrated.
                rabbit_fifo_dlx:make_checkout(ignore_pid, 1),
                rabbit_fifo_dlx:make_settle([0])
                %% Release cursor A got emitted.
               ],
    Config = config(?FUNCTION_NAME, 10, undefined, false, 5,
                    reject_publish, at_least_once),
    ?assert(snapshots_prop(Config, Commands)),
    ok.

dlx_04(_Config) ->
    C1Pid = c:pid(0,883,1),
    C1 = {<<>>, C1Pid},
    E = c:pid(0,176,1),
    Commands = [
                rabbit_fifo_dlx:make_checkout(ignore_pid, 3),
                make_enqueue(E,1,msg(<<>>)),
                make_enqueue(E,2,msg(<<>>)),
                make_enqueue(E,3,msg(<<>>)),
                make_enqueue(E,4,msg(<<>>)),
                make_enqueue(E,5,msg(<<>>)),
                make_enqueue(E,6,msg(<<>>)),
                make_checkout(C1, {auto,6,simple_prefetch}),
                rabbit_fifo:make_discard(C1, [0,1,2,3,4,5]),
                rabbit_fifo_dlx:make_settle([0,1,2])
               ],
    Config = config(?FUNCTION_NAME, undefined, undefined, true, 1,
                    reject_publish, at_least_once),
    ?assert(snapshots_prop(Config, Commands)),
    ok.

%% Test that discards queue gets dehydrated with 1 message that has empty message body.
dlx_05(_Config) ->
    C1Pid = c:pid(0,883,1),
    C1 = {<<>>, C1Pid},
    E = c:pid(0,176,1),
    Commands = [
                make_enqueue(E,1,msg(<<>>)),
                make_enqueue(E,2,msg(<<"msg2">>)),
                %% 0,1 in messages
                make_checkout(C1, {auto,1,simple_prefetch}),
                rabbit_fifo:make_discard(C1, [0]),
                %% 0 in discards, 1 in checkout
                make_enqueue(E,3,msg(<<"msg3">>)),
                %% 0 in discards (rabbit_fifo_dlx msg_bytes is still 0 because body of msg 0 is empty),
                %% 1 in checkout, 2 in messages
                rabbit_fifo_dlx:make_checkout(ignore_pid, 1),
                %% 0 in dlx_checkout, 1 in checkout, 2 in messages
                make_settle(C1, [1]),
                %% 0 in dlx_checkout, 2 in checkout
                rabbit_fifo_dlx:make_settle([0])
                %% 2 in checkout
               ],
    Config = config(?FUNCTION_NAME, 0, 0, false, 0,
                    reject_publish, at_least_once),
    ?assert(snapshots_prop(Config, Commands)),
    ok.

% Test that after recovery we can differentiate between index message and (prefix) disk message
dlx_06(_Config) ->
    C1Pid = c:pid(0,883,1),
    C1 = {<<>>, C1Pid},
    E = c:pid(0,176,1),
    Commands = [
                make_enqueue(E,1,msg(<<>>)),
                %% The following message has 3 bytes.
                %% If we cannot differentiate between disk message and prefix disk message,
                %% rabbit_fifo:delete_indexes/2 will not know whether it's a disk message or
                %% prefix disk message and it will therefore falsely think that 3 is an index
                %% instead of a size header resulting in message 3 being deleted from the index
                %% after recovery.
                make_enqueue(E,2,msg(<<"111">>)),
                make_enqueue(E,3,msg(<<>>)),
                %% 0,1,2 in messages
                rabbit_fifo_dlx:make_checkout(ignore_pid, 2),
                make_checkout(C1, {auto,3,simple_prefetch}),
                %% 0,1,2 in checkout
                rabbit_fifo:make_discard(C1, [0,1,2]),
                %% 0,1 in dlx_checkout, 3 in discards
                rabbit_fifo_dlx:make_settle([0,1])
                %% 3 in dlx_checkout
               ],
    Config = config(?FUNCTION_NAME, undefined, 749, false, 1,
                    reject_publish, at_least_once),
    ?assert(snapshots_prop(Config, Commands)),
    ok.

dlx_07(_Config) ->
    C1Pid = c:pid(0,883,1),
    C1 = {<<>>, C1Pid},
    E = c:pid(0,176,1),
    Commands = [
                make_checkout(C1, {auto,1,simple_prefetch}),
                make_enqueue(E,1,msg(<<"12">>)),
                %% 0 in checkout
                rabbit_fifo:make_discard(C1, [0]),
                %% 0 in discard
                make_enqueue(E,2,msg(<<"1234567">>)),
                %% 0 in discard, 1 in checkout
                rabbit_fifo:make_discard(C1, [1]),
                %% 0, 1 in discard
                rabbit_fifo_dlx:make_checkout(ignore_pid, 1),
                %% 0 in dlx_checkout, 1 in discard
                make_enqueue(E,3,msg(<<"123">>)),
                %% 0 in dlx_checkout, 1 in discard, 2 in checkout
                rabbit_fifo_dlx:make_checkout(ignore_pid, 2),
                %% 0,1 in dlx_checkout, 2 in checkout
                rabbit_fifo_dlx:make_settle([0]),
                %% 1 in dlx_checkout, 2 in checkout
                make_settle(C1, [2]),
                %% 1 in dlx_checkout
                make_enqueue(E,4,msg(<<>>)),
                %% 1 in dlx_checkout, 3 in checkout
                rabbit_fifo_dlx:make_settle([0,1])
                %% 3 in checkout
               ],
    Config = config(?FUNCTION_NAME, undefined, undefined, false, undefined,
                    reject_publish, at_least_once),
    ?assert(snapshots_prop(Config, Commands)),
    ok.

%% This test fails if discards queue is not normalized for comparison.
dlx_08(_Config) ->
    C1Pid = c:pid(0,883,1),
    C1 = {<<>>, C1Pid},
    E = c:pid(0,176,1),
    Commands = [
                make_enqueue(E,1,msg(<<>>)),
                %% 0 in messages
                make_checkout(C1, {auto,1,simple_prefetch}),
                %% 0 in checkout
                make_enqueue(E,2,msg(<<>>)),
                %% 1 in messages, 0 in checkout
                rabbit_fifo:make_discard(C1, [0]),
                %% 1 in checkout, 0 in discards
                make_enqueue(E,3,msg(<<>>)),
                %% 2 in messages, 1 in checkout, 0 in discards
                rabbit_fifo:make_discard(C1, [1]),
                %% 2 in checkout, 0,1 in discards
                rabbit_fifo:make_discard(C1, [2]),
                %% 0,1,2 in discards
                make_enqueue(E,4,msg(<<>>)),
                %% 3 in checkout, 0,1,2 in discards
                %% last command emitted this release cursor
                make_settle(C1, [3]),
                make_enqueue(E,5,msg(<<>>)),
                make_enqueue(E,6,msg(<<>>)),
                rabbit_fifo:make_discard(C1, [4]),
                rabbit_fifo:make_discard(C1, [5]),
                make_enqueue(E,7,msg(<<>>)),
                make_enqueue(E,8,msg(<<>>)),
                make_enqueue(E,9,msg(<<>>)),
                rabbit_fifo:make_discard(C1, [6]),
                rabbit_fifo:make_discard(C1, [7]),
                rabbit_fifo_dlx:make_checkout(ignore_pid, 1),
                make_enqueue(E,10,msg(<<>>)),
                rabbit_fifo:make_discard(C1, [8]),
                rabbit_fifo_dlx:make_settle([0]),
                rabbit_fifo:make_discard(C1, [9]),
                rabbit_fifo_dlx:make_settle([1]),
                rabbit_fifo_dlx:make_settle([2])
               ],
    Config = config(?FUNCTION_NAME, undefined, undefined, false, undefined,
                    reject_publish, at_least_once),
    ?assert(snapshots_prop(Config, Commands)),
    ok.

dlx_09(_Config) ->
    C1Pid = c:pid(0,883,1),
    C1 = {<<>>, C1Pid},
    E = c:pid(0,176,1),
    Commands = [
                make_checkout(C1, {auto,2,simple_prefetch}),
                make_enqueue(E,1,msg(<<>>)),
                %% 0 in checkout
                make_enqueue(E,2,msg(<<>>)),
                %% 0,1 in checkout
                rabbit_fifo:make_return(C1, [0]),
                %% 1,2 in checkout
                rabbit_fifo:make_discard(C1, [1]),
                %% 2 in checkout, 1 in discards
                rabbit_fifo:make_discard(C1, [2])
                %% 1,2 in discards
               ],
    Config = config(?FUNCTION_NAME, undefined, undefined, false, undefined,
                    reject_publish, at_least_once),
    ?assert(snapshots_prop(Config, Commands)),
    ok.

config(Name, Length, MaxBytes, SingleActive, DeliveryLimit) ->
    config(Name, Length, MaxBytes, SingleActive, DeliveryLimit,
       drop_head, {at_most_once, {?MODULE, banana, []}}).

config(Name, Length, MaxBytes, SingleActive, DeliveryLimit,
       Overflow, DeadLetterHandler) ->
    #{name => Name,
      max_length => map_max(Length),
      max_bytes => map_max(MaxBytes),
      dead_letter_handler => DeadLetterHandler,
      single_active_consumer_on => SingleActive,
      delivery_limit => map_max(DeliveryLimit),
      % max_in_memory_length => map_max(InMemoryLength),
      % max_in_memory_bytes => map_max(InMemoryBytes),
      overflow_strategy => Overflow}.

map_max(0) -> undefined;
map_max(N) -> N.

max_length_prop(Conf0, Commands) ->
    Conf = Conf0#{release_cursor_interval => 100},
    Indexes = lists:seq(1, length(Commands)),
    Entries = lists:zip(Indexes, Commands),
    Invariant = fun (#rabbit_fifo{cfg = #cfg{max_length = MaxLen}} = S) ->
                        #{num_ready_messages := MsgReady} = rabbit_fifo:overview(S),
                        MsgReady =< MaxLen
                end,
    try run_log(test_init(Conf), Entries, Invariant) of
        {_State, _Effects} ->
            true;
        _ ->
            true
    catch
        Err ->
            ct:pal("Commands: ~tp~nConf~tp~n", [Commands, Conf]),
            ct:pal("Err: ~tp~n", [Err]),
            false
    end.

validate_idx_order([], _ReleaseCursorIdx) ->
    true;
validate_idx_order(Idxs, ReleaseCursorIdx) ->
    Min = lists:min(Idxs),
    case Min < ReleaseCursorIdx of
        true ->
            throw({invalid_log_index, Min, ReleaseCursorIdx});
        false ->
            ok
    end.

%%TODO write separate generator for dlx using single_active_prop() or
%% messages_total_prop() as base template.
%%
%% E.g. enqueue few messages and have a consumer rejecting those.
%% The invariant could be: Delivery effects to dlx_worker must match the number of dead-lettered messages.
%%
%% Other invariants could be:
%% * if new consumer subscribes, messages are checked out to new consumer
%% * if dlx_worker fails receiving DOWN, messages are still in state.

single_active_prop(Conf0, Commands, ValidateOrder) ->
    Conf = Conf0#{release_cursor_interval => 100},
    Indexes = lists:seq(1, length(Commands)),
    Entries = lists:zip(Indexes, Commands),
    %% invariant: there can only be one active consumer at any one time
    %% there can however be multiple cancelled consumers
    Invariant = fun (#rabbit_fifo{consumers = Consumers}) ->
                        Up = maps:filter(fun (_, #consumer{status = S}) ->
                                                 S == up
                                         end, Consumers),
                        map_size(Up) =< 1
                end,

    try run_log(test_init(Conf), Entries, Invariant) of
        {_State, Effects} when ValidateOrder ->
            %% validate message ordering
            lists:foldl(fun ({send_msg, Pid, {delivery, Tag, Msgs}, ra_event},
                             Acc) ->
                                validate_msg_order({Tag, Pid}, Msgs, Acc);
                            (_, Acc) ->
                                Acc
                        end, -1, Effects),
            true;
        _ ->
            true
    catch
        Err ->
            ct:pal("Commands: ~tp~nConf~tp~n", [Commands, Conf]),
            ct:pal("Err: ~tp~n", [Err]),
            false
    end.

messages_total_prop(Conf0, Commands) ->
    Conf = Conf0#{release_cursor_interval => 100},
    Indexes = lists:seq(1, length(Commands)),
    Entries = lists:zip(Indexes, Commands),
    InitState = test_init(Conf),
    run_log(InitState, Entries, messages_total_invariant()),
    true.

messages_total_invariant() ->
    fun(#rabbit_fifo{messages = M,
                     consumers = C,
                     returns = R,
                     dlx = #rabbit_fifo_dlx{discards = D,
                                            consumer = DlxCon}} = S) ->
            Base = rabbit_fifo_q:len(M) + lqueue:len(R),
            Tot0 = maps:fold(fun (_, #consumer{checked_out = Ch}, Acc) ->
                                     Acc + map_size(Ch)
                            end, Base, C),
            Tot1 = Tot0 + lqueue:len(D),
            Tot = case DlxCon of
                      undefined ->
                          Tot1;
                      #dlx_consumer{checked_out = DlxChecked} ->
                          Tot1 + map_size(DlxChecked)
                  end,
            QTot = rabbit_fifo:query_messages_total(S),
            case Tot == QTot of
                true -> true;
                false ->
                    ct:pal("message invariant failed Expected ~b Got ~b",
                           [Tot, QTot]),
                    false
            end
    end.

simple_prefetch_prop(Conf0, Commands, WithCheckoutCancel) ->
    Conf = Conf0#{release_cursor_interval => 100},
    Indexes = lists:seq(1, length(Commands)),
    Entries = lists:zip(Indexes, Commands),
    InitState = test_init(Conf),
    run_log(InitState, Entries,
            simple_prefetch_invariant(WithCheckoutCancel)),
    true.

simple_prefetch_invariant(WithCheckoutCancel) ->
    fun(#rabbit_fifo{consumers = Consumers}) ->
            maps:fold(
              fun(_, _, false) ->
                      false;
                 (Id, #consumer{cfg = #consumer_cfg{credit_mode =
                                                    {simple_prefetch, MaxCredit}},
                                checked_out = CheckedOut,
                                credit = Credit}, true) ->
                      valid_simple_prefetch(MaxCredit, Credit,
                                            maps:size(CheckedOut),
                                            WithCheckoutCancel, Id)
              end, true, Consumers)
    end.

valid_simple_prefetch(Prefetch, Credit, _, _, CId)
  when Credit > Prefetch ->
    ct:pal("simple_prefetch invariant failed: consumer ~tp has credit (~b) > prefetch (~b)",
           [CId, Credit, Prefetch]),
    false;
valid_simple_prefetch(_, Credit, _, _, CId)
  when Credit < 0 ->
    ct:pal("simple_prefetch invariant failed: consumer ~tp has negative credit (~b)",
           [CId, Credit]),
    false;
valid_simple_prefetch(Prefetch, _, CheckedOut, false, CId)
  when CheckedOut > Prefetch ->
    %% We only check this invariant when there are no {checkout, cancel, _} Ra commands because
    %% if a consumer is cancelled while it has unacked messages, and a new consumer with same
    %% consumer ID but lower Prefetch subscribes, we know that the number of messages checked out
    %% to the new consumer can be (temporarily) higher than the new consumer's Prefetch value.
    ct:pal("simple_prefetch invariant failed: consumer ~tp has checked_out (~b) > prefetch (~b)",
           [CId, CheckedOut, Prefetch]),
    false;
valid_simple_prefetch(_, _, _, _, _) ->
    true.

upgrade_prop(Conf0, Commands) ->
    FromVersion = 3,
    ToVersion = 4,
    FromMod = rabbit_fifo:which_module(FromVersion),
    ToMod = rabbit_fifo:which_module(ToVersion),
    Conf = Conf0#{release_cursor_interval => 0},
    Indexes = lists:seq(1, length(Commands)),
    Entries = lists:zip(Indexes, Commands),
    InitState = test_init_v(Conf, FromVersion),
    [begin
         {PreEntries, PostEntries} = lists:split(SplitPos, Entries),
         %% run log v1
         {V3, _V1Effs} = run_log(InitState, PreEntries,
                                 fun (_) -> true end, FromVersion),

         %% perform conversion
         #rabbit_fifo{} = V4 = element(1, rabbit_fifo:apply(
                                            meta(length(PreEntries) + 1),
                                            {machine_version, FromVersion, ToVersion},
                                            V3)),
         %% assert invariants
         Fields = [num_ready_messages,
                   smallest_raft_index,
                   num_enqueuers,
                   num_consumers,
                   enqueue_message_bytes,
                   checkout_message_bytes
                  ],
         V3Overview = maps:with(Fields, FromMod:overview(V3)),
         V4Overview = maps:with(Fields, ToMod:overview(V4)),
         case V3Overview == V4Overview of
             true -> ok;
             false ->
                 ct:pal("upgrade_prop failed expected~n~tp~nGot:~n~tp",
                        [V3Overview, V4Overview]),
                 ?assertEqual(V3Overview, V4Overview)
         end,
         %% check we can run the post entries from the converted state
         run_log(V4, PostEntries, fun (_) -> true end, ToVersion)
     end || SplitPos <- lists:seq(1, length(Entries))],
    true.

%% single active consumer ordering invariant:
%% only redelivered messages can go backwards
validate_msg_order(_, [], S) ->
    S;
validate_msg_order(Cid, [{_, {H, Num}} | Rem], PrevMax) ->
    Redelivered = is_map(H) andalso maps:is_key(delivery_count, H),
    case undefined of
        _ when Num == PrevMax + 1 ->
            %% forwards case
            validate_msg_order(Cid, Rem, Num);
        _ when Redelivered andalso Num =< PrevMax ->
            %% the seq is lower but this is a redelivery
            %% when the consumer changed and the next messages has been redelivered
            %% we may go backwards but keep the highest seen
            validate_msg_order(Cid, Rem, PrevMax);
        _ ->
            ct:pal("out of order ~w Prev ~w Curr ~w Redel ~w",
                   [Cid, PrevMax, Num, Redelivered]),
            throw({outoforder, Cid, PrevMax, Num})
    end.




dump_generated(Conf, Commands) ->
    ct:pal("Commands: ~tp~nConf~tp~n", [Commands, Conf]),
    true.

snapshots_prop(Conf, Commands) ->
    try run_snapshot_test(Conf, Commands) of
        _ -> true
    catch
        Err ->
            ct:pal("Commands: ~tp~nConf~tp~n", [Commands, Conf]),
            ct:pal("Err: ~tp~n", [Err]),
            false
    end.

log_gen(Size) ->
    Nodes = [node(),
             fakenode@fake,
             fakenode@fake2
            ],
    ?LET(EPids, vector(2, pid_gen(Nodes)),
         ?LET(CPids, vector(2, pid_gen(Nodes)),
              resize(Size,
                     list(
                       frequency(
                         [{20, enqueue_gen(oneof(EPids))},
                          {40, {input_event,
                                frequency([{10, settle},
                                           {2, return},
                                           {2, discard},
                                           {2, requeue}])}},
                          {2, checkout_gen(oneof(CPids))},
                          {1, checkout_cancel_gen(oneof(CPids))},
                          {1, down_gen(oneof(EPids ++ CPids))},
                          {1, nodeup_gen(Nodes)},
                          {1, purge}
                         ]))))).

log_gen_upgrade_snapshots(Size) ->
    Nodes = [node(),
             fakenode@fake,
             fakenode@fake2
            ],
    ?LET(EPids, vector(2, pid_gen(Nodes)),
         ?LET(CPids, vector(2, pid_gen(Nodes)),
              resize(Size,
                     list(
                       frequency(
                         [{20, enqueue_gen(oneof(EPids))},
                          {40, {input_event,
                                frequency([{10, settle},
                                           {2, return},
                                           {2, discard},
                                           {2, requeue}
                                          ])}},
                          {2, checkout_gen(oneof(CPids))},
                          {1, checkout_cancel_gen(oneof(CPids))},
                          {1, down_gen(oneof(EPids ++ CPids))},
                          {1, nodeup_gen(Nodes)},
                          {1, purge}
                         ]))))).

log_gen_dlx(Size) ->
    Nodes = [node(),
             fakenode@fake,
             fakenode@fake2
            ],
    ?LET(EPids, vector(2, pid_gen(Nodes)),
         ?LET(CPids, vector(2, pid_gen(Nodes)),
              resize(Size,
                     list(
                       frequency(
                         [{20, enqueue_gen(oneof(EPids))},
                          {40, {input_event,
                                frequency([{1, settle},
                                           {1, return},
                                           %% dead-letter many messages
                                           {5, discard},
                                           {1, requeue}])}},
                          {2, checkout_gen(oneof(CPids))},
                          {1, checkout_cancel_gen(oneof(CPids))},
                          {1, down_gen(oneof(EPids ++ CPids))},
                          {1, nodeup_gen(Nodes)},
                          {1, purge},
                          %% same dlx_worker can subscribe multiple times,
                          %% e.g. after it dlx_worker crashed
                          %% "last subscriber wins"
                          {2, {checkout_dlx, choose(1,10)}}
                         ]))))).


log_gen_config(Size) ->
    Nodes = [node(),
             fakenode@fake,
             fakenode@fake2
            ],
    ?LET(EPids, vector(2, pid_gen(Nodes)),
         ?LET(CPids, vector(2, pid_gen(Nodes)),
              resize(Size,
                     list(
                       frequency(
                         [{20, enqueue_gen(oneof(EPids))},
                          {40, {input_event,
                                frequency([{5, settle},
                                           {5, return},
                                           {2, discard},
                                           {2, requeue}])}},
                          {2, checkout_gen(oneof(CPids))},
                          {1, checkout_cancel_gen(oneof(CPids))},
                          {1, down_gen(oneof(EPids ++ CPids))},
                          {1, nodeup_gen(Nodes)},
                          {1, purge},
                          {1, ?LET({MaxInMem,
                                    MaxLen},
                                   {choose(1, 10),
                                    choose(1, 10)},
                                   {update_config,
                                    #{max_in_memory_length => MaxInMem,
                                      max_length => MaxLen}})
                          }]))))).

log_gen_ordered(Size) ->
    Nodes = [node(),
             fakenode@fake,
             fakenode@fake2
            ],
    ?LET(EPids, vector(1, pid_gen(Nodes)),
         ?LET(CPids, vector(8, pid_gen(Nodes)),
              resize(Size,
                     list(
                       frequency(
                         [{20, enqueue_gen(oneof(EPids), 10, 0)},
                          {40, {input_event,
                                frequency([{15, settle},
                                           {1, return},
                                           {1, discard},
                                           {1, requeue}])}},
                          {7, checkout_gen(oneof(CPids))},
                          {2, checkout_cancel_gen(oneof(CPids))},
                          {2, down_gen(oneof(EPids ++ CPids))},
                          {1, nodeup_gen(Nodes)}
                         ]))))).

log_gen_without_checkout_cancel(Size) ->
    Nodes = [node(),
             fakenode@fake,
             fakenode@fake2
            ],
    ?LET(EPids, vector(2, pid_gen(Nodes)),
         ?LET(CPids, vector(2, pid_gen(Nodes)),
              resize(Size,
                     list(
                       frequency(
                         [{20, enqueue_gen(oneof(EPids))},
                          {40, {input_event,
                                frequency([{10, settle},
                                           {2, return},
                                           {2, discard},
                                           {2, requeue}])}},
                          {2, checkout_gen(oneof(CPids))},
                          {1, down_gen(oneof(EPids ++ CPids))},
                          {1, nodeup_gen(Nodes)},
                          {1, purge}
                         ]))))).

monotonic_gen() ->
    ?LET(_, integer(), erlang:unique_integer([positive, monotonic])).

pid_gen(Nodes) ->
    ?LET(Node, oneof(Nodes),
         test_util:fake_pid(atom_to_binary(Node, utf8))).

down_gen(Pid) ->
    ?LET(E, {down, Pid, oneof([noconnection, noproc])}, E).

nodeup_gen(Nodes) ->
    {nodeup, oneof(Nodes)}.

enqueue_gen(Pid) ->
    enqueue_gen(Pid, 10, 1).

enqueue_gen(Pid, _Enq, _Del) ->
    ?LET(E, {enqueue, Pid, enqueue, msg_gen()}, E).

msg_gen() ->
    ?LET(Bin, binary(),
         mc:prepare(
           store, mc_amqpl:from_basic_message(
                    #basic_message{exchange_name = #resource{name = <<"e">>,
                                                             kind = exchange,
                                                             virtual_host = <<"/">>},
                                   routing_keys = [<<>>],
                                   content =
                                   #content{payload_fragments_rev = [Bin],
                                            properties = #'P_basic'{}}}))).

msg(Bin) when is_binary(Bin) ->
    #basic_message{content = #content{payload_fragments_rev = [Bin],
                                      properties = #'P_basic'{}}}.

checkout_cancel_gen(Pid) ->
    {checkout, Pid, cancel}.

checkout_gen(Pid) ->
    %% pid, tag, prefetch
    ?LET(C, {checkout, {binary(), Pid}, choose(1, 10)}, C).

-record(t, {state :: rabbit_fifo:state(),
            index = 1 :: non_neg_integer(), %% raft index
            enqueuers = #{} :: #{pid() => term()},
            consumers = #{} :: #{{binary(), pid()} => term()},
            effects = queue:new() :: queue:queue(),
            %% to transform the body
            enq_body_fun = {0, fun ra_lib:id/1},
            config :: map(),
            log = [] :: list(),
            down = #{} :: #{pid() => noproc | noconnection},
            enq_cmds = #{} :: #{ra:index() => rabbit_fifo:enqueue()},
            is_v4 = false :: boolean()
           }).

expand(Ops, Config) ->
    expand(Ops, Config, {undefined, fun ra_lib:id/1}).

%% generates a sequence of Raft commands
expand(Ops, Config, EnqFun) ->
    %% execute each command against a rabbit_fifo state and capture all relevant
    %% effects
    InitConfig0 = #{name => proper,
                    queue_resource => #resource{virtual_host = <<"/">>,
                                                kind = queue,
                                                name = <<"blah">>},
                    release_cursor_interval => 1},
    InitConfig = case Config of
                     #{dead_letter_handler := at_least_once} ->
                         %% Configure rabbit_fifo config with at_least_once so that
                         %% rabbit_fifo_dlx outputs dlx_delivery effects
                         %% which we are going to settle immediately in enq_effs/2.
                         %% Therefore the final generated Raft commands will include
                         %% {dlx, {checkout, ...}} and {dlx, {settle, ...}} Raft commands.
                         maps:put(dead_letter_handler, at_least_once, InitConfig0);
                     _ ->
                         InitConfig0
                 end,
    IsV4 = rabbit_feature_flags:is_enabled('rabbitmq_4.0.0'),
    T = #t{state = rabbit_fifo:init(InitConfig),
           enq_body_fun = EnqFun,
           config = Config,
           is_v4 = IsV4},
    #t{effects = Effs} = T1 = lists:foldl(fun handle_op/2, T, Ops),
    %% process the remaining effect
    #t{log = Log} = lists:foldl(fun do_apply/2,
                                T1#t{effects = queue:new()},
                                queue:to_list(Effs)),
    lists:reverse(Log).

handle_op({enqueue, Pid, When, Data},
          #t{enqueuers = Enqs0,
             enq_body_fun = {EnqSt0, Fun},
             down = Down,
             effects = Effs} = T) ->
    case Down of
        #{Pid := noproc} ->
            %% if it's a noproc then it cannot exist - can it?
            %% drop operation
            T;
        _ ->
            Enqs = maps:update_with(Pid, fun (Seq) -> Seq + 1 end, 1, Enqs0),
            MsgSeq = maps:get(Pid, Enqs),
            {EnqSt, Msg} = Fun({EnqSt0, Data}),
            Cmd = make_enqueue(Pid, MsgSeq, Msg),
            case When of
                enqueue ->
                    do_apply(Cmd, T#t{enqueuers = Enqs,
                                      enq_body_fun = {EnqSt, Fun}});
                delay ->
                    %% just put the command on the effects queue
                    T#t{effects = queue:in(Cmd, Effs),
                        enqueuers = Enqs,
                        enq_body_fun = {EnqSt, Fun}}
            end
    end;
handle_op({checkout, Pid, cancel}, #t{consumers  = Cons0} = T) ->
    case maps:keys(
           maps:filter(fun ({_, P}, _) when P == Pid -> true;
                           (_, _) -> false
                       end, Cons0)) of
        [CId | _] ->
            Cons = maps:remove(CId, Cons0),
            Cmd = rabbit_fifo:make_checkout(CId, cancel, #{}),
            do_apply(Cmd, T#t{consumers = Cons});
        _ ->
            T
    end;
handle_op({checkout, CId, Prefetch}, #t{consumers  = Cons0} = T) ->
    case Cons0 of
        #{CId := _} ->
            %% ignore if it already exists
            T;
        _ ->
            Spec = case T#t.is_v4 of
                       true ->
                           {auto, {simple_prefetch, Prefetch}};
                       false ->
                           {auto, Prefetch, simple_prefetch}
                   end,

            Cons = maps:put(CId, T#t.index,  Cons0),
            Cmd = rabbit_fifo:make_checkout(CId, Spec,
                                            #{ack => true,
                                              prefetch => Prefetch,
                                              username => <<"user">>,
                                              args => []}),

            do_apply(Cmd, T#t{consumers = Cons})
    end;
handle_op({down, Pid, Reason} = Cmd, #t{down = Down} = T) ->
    case Down of
        #{Pid := noproc} ->
            %% it it permanently down, cannot upgrade
            T;
        _ ->
            %% it is either not down or down with noconnection
            do_apply(Cmd, T#t{down = maps:put(Pid, Reason, Down)})
    end;
handle_op({nodeup, _} = Cmd, T) ->
    do_apply(Cmd, T);
handle_op({input_event, requeue}, #t{effects = Effs} = T) ->
    %% this simulates certain settlements arriving out of order
    case queue:out(Effs) of
        {{value, Cmd}, Q} ->
            T#t{effects = queue:in(Cmd, Q)};
        _ ->
            T
    end;
handle_op({input_event, Settlement}, #t{effects = Effs,
                                        consumers = Cons,
                                        down = Down,
                                        is_v4 = IsV4} = T) ->
    case queue:out(Effs) of
        {{value, {settle, CId, MsgIds}}, Q} ->
            CKey = case maps:get(CId, Cons, undefined) of
                       K when is_integer(K) andalso IsV4 ->
                           K;
                       _ ->
                           CId
                   end,
            Cmd = case Settlement of
                      settle ->
                          rabbit_fifo:make_settle(CKey, MsgIds);
                      return ->
                          rabbit_fifo:make_return(CKey, MsgIds);
                      discard ->
                          rabbit_fifo:make_discard(CKey, MsgIds)
                  end,
            do_apply(Cmd, T#t{effects = Q});
        {{value, {enqueue, Pid, _, _} = Cmd}, Q} ->
            case maps:is_key(Pid, Down) of
                true ->
                    %% enqueues cannot arrive after down for the same process
                    %% drop message
                    T#t{effects = Q};
                false ->
                    do_apply(Cmd, T#t{effects = Q})
            end;
        {{value, {dlx, {settle, MsgIds}}}, Q} ->
            Cmd = rabbit_fifo_dlx:make_settle(MsgIds),
            do_apply(Cmd, T#t{effects = Q});
        _ ->
            T
    end;
handle_op(purge, T) ->
    do_apply(rabbit_fifo:make_purge(), T);
handle_op({update_config, Changes}, #t{config = Conf} = T) ->
    Config = maps:merge(Conf, Changes),
    do_apply(rabbit_fifo:make_update_config(Config), T);
handle_op({checkout_dlx, Prefetch},
          #t{config = #{dead_letter_handler := at_least_once}} = T) ->
    Cmd = rabbit_fifo_dlx:make_checkout(ignore_pid, Prefetch),
    do_apply(Cmd, T).


do_apply(Cmd, #t{effects = Effs,
                 index = Index, state = S0,
                 down = Down,
                 enq_cmds = EnqCmds0,
                 log = Log} = T) ->
    case Cmd of
        {enqueue, Pid, _, _Msg} when is_map_key(Pid, Down) ->
            %% down
            T;
        _ ->
            EnqCmds = case Cmd  of
                          {enqueue, _Pid, _, _Msg} ->
                              EnqCmds0#{Index => Cmd};
                          _ ->
                              EnqCmds0
                      end,

            {St, Effects} = case rabbit_fifo:apply(meta(Index), Cmd, S0) of
                                {S, _, E} when is_list(E) ->
                                    {S, E};
                                {S, _, E} ->
                                    {S, [E]};
                                {S, _} ->
                                    {S, []}
                            end,

            T#t{state = St,
                index = Index + 1,
                enq_cmds = EnqCmds,
                effects = enq_effs(Effects, Effs, EnqCmds),
                log = [Cmd | Log]}
    end.

enq_effs([], Q, _) -> Q;
enq_effs([{send_msg, P, {delivery, CTag, Msgs}, _Opts} | Rem], Q, Cmds) ->
    MsgIds = [I || {I, _} <- Msgs],
    %% always make settle commands by default
    %% they can be changed depending on the input event later
    Cmd = rabbit_fifo:make_settle({CTag, P}, MsgIds),
    enq_effs(Rem, queue:in(Cmd, Q), Cmds);
enq_effs([{log, RaIdxs, Fun, _} | Rem], Q, Cmds) ->
    M = [maps:get(I, Cmds) || I <- RaIdxs],
    Effs = Fun(M),
    enq_effs(Effs ++ Rem, Q, Cmds);
enq_effs([{send_msg, _, {dlx_delivery, Msgs}, _Opts} | Rem], Q, Cmds) ->
    MsgIds = [I || {I, _} <- Msgs],
    Cmd = rabbit_fifo_dlx:make_settle(MsgIds),
    enq_effs(Rem, queue:in(Cmd, Q), Cmds);
enq_effs([_ | Rem], Q, Cmds) ->
    enq_effs(Rem, Q, Cmds).


%% Utility
run_proper(Fun, Args, NumTests) ->
    ?assert(
       proper:counterexample(
         erlang:apply(Fun, Args),
         [{numtests, NumTests},
          {on_output, fun(".", _) -> ok; % don't print the '.'s on new lines
                         (F, A) -> ct:pal(?LOW_IMPORTANCE, F, A)
                      end}])).

run_snapshot_test(Conf, Commands) ->
    Indexes = lists:seq(1, length(Commands)),
    Entries = lists:zip(Indexes, Commands),
    ct:pal("running snapshot test 2 with ~b commands using config ~tp",
           [length(Commands), Conf]),
    Fun = fun (_E, S, _Effs) ->
                  MsgTotFun = messages_total_invariant(),
                  MsgTotFun(S)
          end,
    _ = run_log(test_init(Conf), Entries, Fun),
    true.


hd_or([H | _]) -> H;
hd_or(_) -> {undefined}.

%% transforms [1,2,3] into [[1,2,3], [1,2], [1]]
prefixes(Source, N, Acc) when N > length(Source) ->
    lists:reverse(Acc);
prefixes(Source, N, Acc) ->
    {X, _} = lists:split(N, Source),
    prefixes(Source, N+1, [X | Acc]).

run_log(InitState, Entries) ->
    run_log(InitState, Entries, fun(_) -> true end).

run_log(InitState, Entries, InvariantFun) ->
    run_log(InitState, Entries, InvariantFun, ?MACHINE_VERSION).

run_log(InitState, Entries, InvariantFun0, MachineVersion)
  when is_function(InvariantFun0, 1) ->
    InvariantFun = fun (_E, S, _Effs) ->
                           InvariantFun0(S)
                   end,
    run_log(InitState, Entries, InvariantFun, MachineVersion);
run_log(InitState, Entries, InvariantFun, MachineVersion)
  when is_integer(MachineVersion) ->
    Invariant = fun(E, S, Effs) ->
                       case InvariantFun(E, S, Effs) of
                           true -> ok;
                           false ->
                               throw({invariant, E, S})
                       end
                end,
    FifoMod = rabbit_fifo:which_module(MachineVersion),

    lists:foldl(fun ({Idx, E0}, {Acc0, Efx0}) ->
                        {Meta, E} = case E0 of
                                        {M1, E1} when is_map(M1) ->
                                            M0 = meta(Idx, MachineVersion),
                                            {maps:merge(M0, M1), E1};
                                        _ ->
                                            {meta(Idx, MachineVersion), E0}
                                    end,

                        case FifoMod:apply(Meta, E, Acc0) of
                            {Acc, _, Efx} when is_list(Efx) ->
                                Invariant(E, Acc, lists:flatten(Efx)),
                                {Acc, Efx0 ++ Efx};
                            {Acc, _, Efx}  ->
                                Invariant(E, Acc, lists:flatten(Efx)),
                                {Acc, Efx0 ++ [Efx]};
                            {Acc, _}  ->
                                Invariant(E, Acc, []),
                                {Acc, Efx0}
                        end
                end, {InitState, []}, Entries).

test_init(Conf) ->
    test_init(rabbit_fifo, Conf).

test_init(Mod, Conf) ->
    Default = #{queue_resource => blah,
                release_cursor_interval => 0,
                metrics_handler => {?MODULE, metrics_handler, []}},
    Mod:init(maps:merge(Default, Conf)).

test_init_v1(Conf) ->
    test_init(rabbit_fifo_v1, Conf).

test_init_v(Conf, Version) ->
    test_init(rabbit_fifo:which_module(Version), Conf).

meta(Idx) ->
    meta(Idx, 3).

meta(Idx, Vsn) ->
    #{machine_version => Vsn, index => Idx, term => 1, system_time => 0}.

make_checkout(Cid, Spec) ->
    make_checkout(Cid, Spec, #{}).
make_checkout(Cid, Spec, Meta) ->
    rabbit_fifo:make_checkout(Cid, Spec, Meta).

make_enqueue(Pid, Seq, Msg) ->
    rabbit_fifo:make_enqueue(Pid, Seq, Msg).

make_settle(Cid, MsgIds) ->
    rabbit_fifo:make_settle(Cid, MsgIds).

make_return(Cid, MsgIds) ->
    rabbit_fifo:make_return(Cid, MsgIds).
