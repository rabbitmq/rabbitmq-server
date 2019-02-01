-module(amqqueue_backward_compatibility_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").

-include("amqqueue.hrl").

-export([all/0,
         groups/0,
         init_per_suite/2,
         end_per_suite/2,
         init_per_group/2,
         end_per_group/2,
         init_per_testcase/2,
         end_per_testcase/2,

         new_amqqueue_v1_is_amqqueue/1,
         new_amqqueue_v2_is_amqqueue/1,
         random_term_is_not_amqqueue/1,

         amqqueue_v1_is_durable/1,
         amqqueue_v2_is_durable/1,
         random_term_is_not_durable/1,

         amqqueue_v1_state_matching/1,
         amqqueue_v2_state_matching/1,
         random_term_state_matching/1,

         amqqueue_v1_type_matching/1,
         amqqueue_v2_type_matching/1,
         random_term_type_matching/1,

         upgrade_v1_to_v2/1
        ]).

-define(long_tuple, {random_tuple, a, b, c, d, e, f, g, h, i, j, k, l, m,
                     n, o, p, q, r, s, t, u, v, w, x, y, z}).

all() ->
    [
     {group, parallel_tests}
    ].

groups() ->
    [
     {parallel_tests, [parallel], [new_amqqueue_v1_is_amqqueue,
                                   new_amqqueue_v2_is_amqqueue,
                                   random_term_is_not_amqqueue,
                                   amqqueue_v1_is_durable,
                                   amqqueue_v2_is_durable,
                                   random_term_is_not_durable,
                                   amqqueue_v1_state_matching,
                                   amqqueue_v2_state_matching,
                                   random_term_state_matching,
                                   amqqueue_v1_type_matching,
                                   amqqueue_v2_type_matching,
                                   random_term_type_matching]}
    ].

init_per_suite(_, Config) -> Config.
end_per_suite(_, Config) -> Config.

init_per_group(_, Config) -> Config.
end_per_group(_, Config) -> Config.

init_per_testcase(_, Config) -> Config.
end_per_testcase(_, Config) -> Config.

new_amqqueue_v1_is_amqqueue(_) ->
    VHost = <<"/">>,
    Name = rabbit_misc:r(VHost, queue, my_amqqueue_v1),
    Queue = amqqueue:new_with_version(amqqueue_v1,
                                      Name,
                                      self(),
                                      false,
                                      false,
                                      none,
                                      [],
                                      VHost,
                                      #{},
                                      ?amqqueue_v1_type),
    ?assert(?is_amqqueue(Queue)),
    ?assert(?is_amqqueue_v1(Queue)),
    ?assert(not ?is_amqqueue_v2(Queue)),
    ?assert(?amqqueue_is_classic(Queue)),
    ?assert(amqqueue:is_classic(Queue)),
    ?assert(not ?amqqueue_is_quorum(Queue)),
    ?assert(not ?amqqueue_vhost_equals(Queue, <<"frazzle">>)),
    ?assert(?amqqueue_has_valid_pid(Queue)),
    ?assert(?amqqueue_pid_equals(Queue, self())),
    ?assert(?amqqueue_pids_are_equal(Queue, Queue)),
    ?assert(?amqqueue_pid_runs_on_local_node(Queue)),
    ?assert(amqqueue:qnode(Queue) == node()).

new_amqqueue_v2_is_amqqueue(_) ->
    VHost = <<"/">>,
    Name = rabbit_misc:r(VHost, queue, my_amqqueue_v2),
    Queue = amqqueue:new_with_version(amqqueue_v2,
                                      Name,
                                      self(),
                                      false,
                                      false,
                                      none,
                                      [],
                                      VHost,
                                      #{},
                                      classic),
    ?assert(?is_amqqueue(Queue)),
    ?assert(?is_amqqueue_v2(Queue)),
    ?assert(not ?is_amqqueue_v1(Queue)),
    ?assert(?amqqueue_is_classic(Queue)),
    ?assert(amqqueue:is_classic(Queue)),
    ?assert(not ?amqqueue_is_quorum(Queue)),
    ?assert(not ?amqqueue_vhost_equals(Queue, <<"frazzle">>)),
    ?assert(?amqqueue_has_valid_pid(Queue)),
    ?assert(?amqqueue_pid_equals(Queue, self())),
    ?assert(?amqqueue_pids_are_equal(Queue, Queue)),
    ?assert(?amqqueue_pid_runs_on_local_node(Queue)),
    ?assert(amqqueue:qnode(Queue) == node()).

random_term_is_not_amqqueue(_) ->
    Term = ?long_tuple,
    ?assert(not ?is_amqqueue(Term)),
    ?assert(not ?is_amqqueue_v2(Term)),
    ?assert(not ?is_amqqueue_v1(Term)).

%% -------------------------------------------------------------------

amqqueue_v1_is_durable(_) ->
    VHost = <<"/">>,
    Name = rabbit_misc:r(VHost, queue, my_amqqueue_v1),
    TransientQueue = amqqueue:new_with_version(amqqueue_v1,
                                               Name,
                                               self(),
                                               false,
                                               false,
                                               none,
                                               [],
                                               VHost,
                                               #{},
                                               ?amqqueue_v1_type),
    DurableQueue = amqqueue:new_with_version(amqqueue_v1,
                                             Name,
                                             self(),
                                             true,
                                             false,
                                             none,
                                             [],
                                             VHost,
                                             #{},
                                             ?amqqueue_v1_type),
    ?assert(not ?amqqueue_is_durable(TransientQueue)),
    ?assert(?amqqueue_is_durable(DurableQueue)).

amqqueue_v2_is_durable(_) ->
    VHost = <<"/">>,
    Name = rabbit_misc:r(VHost, queue, my_amqqueue_v1),
    TransientQueue = amqqueue:new_with_version(amqqueue_v2,
                                               Name,
                                               self(),
                                               false,
                                               false,
                                               none,
                                               [],
                                               VHost,
                                               #{},
                                               classic),
    DurableQueue = amqqueue:new_with_version(amqqueue_v2,
                                             Name,
                                             self(),
                                             true,
                                             false,
                                             none,
                                             [],
                                             VHost,
                                             #{},
                                             classic),
    ?assert(not ?amqqueue_is_durable(TransientQueue)),
    ?assert(?amqqueue_is_durable(DurableQueue)).

random_term_is_not_durable(_) ->
    Term = ?long_tuple,
    ?assert(not ?amqqueue_is_durable(Term)).

%% -------------------------------------------------------------------

amqqueue_v1_state_matching(_) ->
    VHost = <<"/">>,
    Name = rabbit_misc:r(VHost, queue, my_amqqueue_v1),
    Queue1 = amqqueue:new_with_version(amqqueue_v1,
                                       Name,
                                       self(),
                                       true,
                                       false,
                                       none,
                                       [],
                                       VHost,
                                       #{},
                                       ?amqqueue_v1_type),
    ?assert(?amqqueue_state_is(Queue1, live)),
    Queue2 = amqqueue:set_state(Queue1, stopped),
    ?assert(?amqqueue_state_is(Queue2, stopped)).

amqqueue_v2_state_matching(_) ->
    VHost = <<"/">>,
    Name = rabbit_misc:r(VHost, queue, my_amqqueue_v1),
    Queue1 = amqqueue:new_with_version(amqqueue_v2,
                                       Name,
                                       self(),
                                       true,
                                       false,
                                       none,
                                       [],
                                       VHost,
                                       #{},
                                       classic),
    ?assert(?amqqueue_state_is(Queue1, live)),
    Queue2 = amqqueue:set_state(Queue1, stopped),
    ?assert(?amqqueue_state_is(Queue2, stopped)).

random_term_state_matching(_) ->
    Term = ?long_tuple,
    ?assert(not ?amqqueue_state_is(Term, live)).

%% -------------------------------------------------------------------

amqqueue_v1_type_matching(_) ->
    VHost = <<"/">>,
    Name = rabbit_misc:r(VHost, queue, my_amqqueue_v1),
    Queue = amqqueue:new_with_version(amqqueue_v1,
                                      Name,
                                      self(),
                                      true,
                                      false,
                                      none,
                                      [],
                                      VHost,
                                      #{},
                                      ?amqqueue_v1_type),
    ?assert(?amqqueue_is_classic(Queue)),
    ?assert(amqqueue:is_classic(Queue)),
    ?assert(not ?amqqueue_is_quorum(Queue)).

amqqueue_v2_type_matching(_) ->
    VHost = <<"/">>,
    Name = rabbit_misc:r(VHost, queue, my_amqqueue_v1),
    ClassicQueue = amqqueue:new_with_version(amqqueue_v2,
                                             Name,
                                             self(),
                                             true,
                                             false,
                                             none,
                                             [],
                                             VHost,
                                             #{},
                                             classic),
    ?assert(?amqqueue_is_classic(ClassicQueue)),
    ?assert(amqqueue:is_classic(ClassicQueue)),
    ?assert(not ?amqqueue_is_quorum(ClassicQueue)),
    ?assert(not amqqueue:is_quorum(ClassicQueue)),
    QuorumQueue = amqqueue:new_with_version(amqqueue_v2,
                                            Name,
                                            self(),
                                            true,
                                            false,
                                            none,
                                            [],
                                            VHost,
                                            #{},
                                            quorum),
    ?assert(not ?amqqueue_is_classic(QuorumQueue)),
    ?assert(not amqqueue:is_classic(QuorumQueue)),
    ?assert(?amqqueue_is_quorum(QuorumQueue)),
    ?assert(amqqueue:is_quorum(QuorumQueue)).

random_term_type_matching(_) ->
    Term = ?long_tuple,
    ?assert(not ?amqqueue_is_classic(Term)),
    ?assert(not ?amqqueue_is_quorum(Term)),
    ?assertException(error, function_clause, amqqueue:is_classic(Term)),
    ?assertException(error, function_clause, amqqueue:is_quorum(Term)).

%% -------------------------------------------------------------------

upgrade_v1_to_v2(_) ->
    VHost = <<"/">>,
    Name = rabbit_misc:r(VHost, queue, my_amqqueue_v1),
    OldQueue = amqqueue:new_with_version(amqqueue_v1,
                                         Name,
                                         self(),
                                         true,
                                         false,
                                         none,
                                         [],
                                         VHost,
                                         #{},
                                         ?amqqueue_v1_type),
    ?assert(?is_amqqueue_v1(OldQueue)),
    ?assert(not ?is_amqqueue_v2(OldQueue)),
    NewQueue = amqqueue:upgrade_to(amqqueue_v2, OldQueue),
    ?assert(not ?is_amqqueue_v1(NewQueue)),
    ?assert(?is_amqqueue_v2(NewQueue)).
