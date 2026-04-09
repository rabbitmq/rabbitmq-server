%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2026 Broadcom. All Rights Reserved. The term "Broadcom" refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
%%

-module(rabbit_db_topic_exchange_SUITE).

-include_lib("proper/include/proper.hrl").
-include_lib("rabbit_common/include/rabbit.hrl").
-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").

-include_lib("rabbitmq_ct_helpers/include/rabbit_assert.hrl").

-define(TOPIC_TRIE_PROJECTION, rabbit_khepri_topic_trie_v4).
-define(TOPIC_BINDING_PROJECTION, rabbit_khepri_topic_binding_v4).

-export([all/0,
         groups/0,
         init_per_suite/1,
         end_per_suite/1,
         init_per_group/2,
         end_per_group/2,
         init_per_testcase/2,
         end_per_testcase/2,

         topic_trie_cleanup/1,
         topic_match_basic/1,
         topic_match_wildcards/1,
         topic_match_hash_middle/1,
         topic_match_binding_keys/1,
         topic_match_empty_routing_key/1,
         topic_match_deep_hierarchy/1,
         topic_match_all_wildcards/1,
         topic_match_alternating_wildcards/1,
         topic_match_overlapping_filters/1,
         topic_match_empty_segments/1,
         topic_match_large_fanout/1,
         topic_match_exchange_to_exchange/1,
         topic_match_unicode/1,
         prop_topic_match/1,
         prop_topic_binding_lifecycle/1
        ]).

all() ->
    [
     {group, cluster_size_3}
    ].

groups() ->
    [
     {cluster_size_3, [], khepri_tests()}
    ].

khepri_tests() ->
    [
     topic_trie_cleanup,
     topic_match_basic,
     topic_match_wildcards,
     topic_match_hash_middle,
     topic_match_binding_keys,
     topic_match_empty_routing_key,
     topic_match_deep_hierarchy,
     topic_match_all_wildcards,
     topic_match_alternating_wildcards,
     topic_match_overlapping_filters,
     topic_match_empty_segments,
     topic_match_large_fanout,
     topic_match_exchange_to_exchange,
     topic_match_unicode,
     prop_topic_match,
     prop_topic_binding_lifecycle
    ].

init_per_suite(Config) ->
    rabbit_ct_helpers:log_environment(),
    rabbit_ct_helpers:run_setup_steps(Config).

end_per_suite(Config) ->
    rabbit_ct_helpers:run_teardown_steps(Config).

init_per_group(cluster_size_3 = _Group, Config0) ->
    Config = rabbit_ct_helpers:set_config(Config0, [{rmq_nodes_count, 3}]),
    Config1 = rabbit_ct_helpers:run_steps(
                Config,
                rabbit_ct_broker_helpers:setup_steps() ++
                rabbit_ct_client_helpers:setup_steps()),
    case Config1 of
        _ when is_list(Config1) ->
            Ret = rabbit_ct_broker_helpers:enable_feature_flag(
                    Config1, topic_binding_projection_v4),
            case Ret of
                ok ->
                    Config1;
                {skip, _} = Skip ->
                    _ = rabbit_ct_helpers:run_steps(
                          Config1,
                          rabbit_ct_client_helpers:teardown_steps() ++
                          rabbit_ct_broker_helpers:teardown_steps()),
                    Skip
            end;
        {skip, _} = Skip ->
            Skip
    end.

end_per_group(_Group, Config) ->
    rabbit_ct_helpers:run_steps(Config,
      rabbit_ct_client_helpers:teardown_steps() ++
      rabbit_ct_broker_helpers:teardown_steps()).

init_per_testcase(Testcase, Config) ->
    XName = rabbit_misc:r(<<"/">>, exchange, <<"amq.topic">>),
    {ok, X} = rabbit_ct_broker_helpers:rpc(Config, 0, rabbit_exchange, lookup, [XName]),
    Config1 = rabbit_ct_helpers:set_config(Config, [{exchange_name, XName},
                                                    {exchange, X}]),
    rabbit_ct_helpers:testcase_started(Config1, Testcase).

end_per_testcase(Testcase, Config) ->
    rabbit_ct_helpers:testcase_finished(Config, Testcase).

%% ---------------------------------------------------------------------------
%% Khepri-specific Tests
%% ---------------------------------------------------------------------------

%% https://github.com/rabbitmq/rabbitmq-server/issues/15024
topic_trie_cleanup(Config) ->
    [_, OldNode, NewNode] = Nodes = rabbit_ct_broker_helpers:get_node_configs(Config, nodename),

    %% This test has to be isolated to avoid flakes.
    VHost = <<"test-vhost-topic-trie">>,
    ok = rabbit_ct_broker_helpers:rpc(Config, OldNode, rabbit_vhost, add, [VHost, <<"test-user">>]),

    ExchangeName = rabbit_misc:r(VHost, exchange, <<"test-topic-exchange">>),
    {ok, _Exchange} = rabbit_ct_broker_helpers:rpc(Config, OldNode, rabbit_exchange, declare,
                        [ExchangeName, topic, _Durable = true, _AutoDelete = false,
                         _Internal = false, _Args = [], <<"test-user">>]),

    BindingKeys = [
                   <<"a.b.c">>,
                   <<"a.b.d">>,
                   <<"a.b.e">>,
                   <<"a.c.d">>,
                   <<"a.c.e">>,
                   <<"b.c.d">>,
                   <<"a.*.c">>,
                   <<"a.*.d">>,
                   <<"*.b.c">>,
                   <<"*.b.d">>,
                   <<"a.b.*">>,
                   <<"a.c.*">>,
                   <<"*.*">>,
                   <<"a.*">>,
                   <<"*.b">>,
                   <<"*">>,
                   <<"a.#">>,
                   <<"a.b.#">>,
                   <<"a.c.#">>,
                   <<"#.c">>,
                   <<"#.b.c">>,
                   <<"#.b.d">>,
                   <<"#">>,
                   <<"#.#">>,
                   <<"a.*.#">>,
                   <<"*.b.#">>,
                   <<"*.#">>,
                   <<"#.*">>,
                   <<"#.*.#">>,
                   <<"orders.created.#">>,
                   <<"orders.updated.#">>,
                   <<"orders.*.confirmed">>,
                   <<"orders.#">>,
                   <<"events.user.#">>,
                   <<"events.system.#">>,
                   <<"events.#">>
                  ],

    ShuffledBindingKeys = [BK || {_, BK} <- lists:sort([{rand:uniform(), BK} || BK <- BindingKeys])],

    Bindings = [begin
                    QueueName = rabbit_misc:r(VHost, queue,
                                              list_to_binary("queue-" ++ integer_to_list(Idx))),
                    Ret = rabbit_ct_broker_helpers:rpc(
                            Config, OldNode,
                            rabbit_amqqueue, declare, [QueueName, true, false, [], self(), <<"test-user">>]),
                    case Ret of
                        {new, _Q} -> ok;
                        {existing, _Q} -> ok
                    end,
                    #binding{source = ExchangeName,
                             key = BindingKey,
                             destination = QueueName,
                             args = []}
                end || {Idx, BindingKey} <- lists:enumerate(ShuffledBindingKeys)],

    [ok = rabbit_ct_broker_helpers:rpc(Config, OldNode, rabbit_binding, add, [B, <<"test-user">>])
     || B <- Bindings],

    lists:foreach(
      fun(Node) ->
              TrieEntries = read_trie_table(Config, Node, VHost, ?TOPIC_TRIE_PROJECTION),
              BindingEntries = read_binding_table(Config, Node, ?TOPIC_BINDING_PROJECTION),
              ct:pal("Bindings added on node ~s: ~p, trie entries: ~p, binding entries: ~p~n",
                     [Node, length(Bindings), length(TrieEntries), length(BindingEntries)])
      end, Nodes),

    ShuffledBindings = [B || {_, B} <- lists:sort([{rand:uniform(), B} || B <- Bindings])],

    [ok = rabbit_ct_broker_helpers:rpc(Config, OldNode, rabbit_binding, remove, [B, <<"test-user">>])
     || B <- ShuffledBindings],

    try
        lists:foreach(
          fun(Node) ->
                  ?awaitMatch(
                     [],
                     begin
                         TrieAfterDelete = read_trie_table(
                                             Config, Node, VHost,
                                             ?TOPIC_TRIE_PROJECTION),
                         ct:pal(
                           "ETS entries after delete on node ~s: trie=~p",
                           [Node, length(TrieAfterDelete)]),
                         TrieAfterDelete
                     end,
                     30000),
                  ?awaitMatch(
                     [],
                     begin
                         BindingsAfterDelete = read_binding_table(
                                                 Config, Node,
                                                 ?TOPIC_BINDING_PROJECTION),
                         ct:pal(
                           "ETS entries after delete on node ~s: bindings=~p",
                           [Node, length(BindingsAfterDelete)]),
                         BindingsAfterDelete
                     end,
                     30000)
          end, Nodes),

        %% Check whether old projections exist and need cleanup.
        HasOldProjection = try
                               VHostEntriesInOldTable = read_trie_table(
                                                          Config, OldNode, VHost, rabbit_khepri_topic_trie),
                               ct:pal("Old ETS table entries after delete: ~p~n", [length(VHostEntriesInOldTable)]),
                               true
                           catch
                               error:{exception, badarg, _} ->
                                   ct:pal("The old projection was not registered, nothing to test"),
                                   false
                           end,

        case HasOldProjection of
            true ->
                ?assertEqual(ok, rabbit_ct_broker_helpers:stop_broker(Config, OldNode)),
                ?assertEqual(ok, rabbit_ct_broker_helpers:forget_cluster_node(Config, NewNode, OldNode)),

                ct:pal("Restart new node (node 2)"),
                ?assertEqual(ok, rabbit_ct_broker_helpers:restart_broker(Config, NewNode)),

                ct:pal("Wait for projections to be restored"),
                ?awaitMatch(
                   Entries when is_list(Entries),
                   catch read_trie_table(Config, NewNode, VHost, ?TOPIC_TRIE_PROJECTION),
                   60000),

                ct:pal("Check that the old projections are gone"),
                lists:foreach(
                  fun(ProjectionName) ->
                          ?assertError(
                             {exception, badarg, _},
                             read_trie_table(Config, NewNode, VHost, ProjectionName))
                  end, [rabbit_khepri_topic_trie,
                        rabbit_khepri_topic_trie_v2,
                        rabbit_khepri_topic_trie_v3]);
            false ->
                ok
        end
    after
        ok = rabbit_ct_broker_helpers:rpc(Config, NewNode, rabbit_vhost, delete, [VHost, <<"test-user">>])
    end,

    passed.

topic_match_basic(Config) ->
    VHost = <<"test-vhost-basic">>,
    setup_vhost(Config, VHost),
    XName = rabbit_misc:r(VHost, exchange, <<"amq.topic">>),

    QFoo = rabbit_misc:r(VHost, queue, <<"q-foo">>),
    QBar = rabbit_misc:r(VHost, queue, <<"q-bar">>),
    QBaz = rabbit_misc:r(VHost, queue, <<"q-baz">>),

    add_binding(Config, XName, <<"foo.bar">>, QFoo),
    add_binding(Config, XName, <<"foo.baz">>, QBar),
    add_binding(Config, XName, <<"other.key">>, QBaz),

    try
        ?assertEqual([QFoo],
                     do_match(Config, XName, <<"foo.bar">>, #{})),
        ?assertEqual([QBar],
                     do_match(Config, XName, <<"foo.baz">>, #{})),
        ?assertEqual([QBaz],
                     do_match(Config, XName, <<"other.key">>, #{})),
        ?assertEqual([],
                     do_match(Config, XName, <<"no.match">>, #{}))
    after
        cleanup_vhost(Config, VHost)
    end,

    passed.

topic_match_wildcards(Config) ->
    VHost = <<"test-vhost-wildcards">>,
    setup_vhost(Config, VHost),
    XName = rabbit_misc:r(VHost, exchange, <<"amq.topic">>),

    QStar = rabbit_misc:r(VHost, queue, <<"q-star">>),
    QHash = rabbit_misc:r(VHost, queue, <<"q-hash">>),
    QExact = rabbit_misc:r(VHost, queue, <<"q-exact">>),
    QDeep = rabbit_misc:r(VHost, queue, <<"q-deep">>),
    QAll = rabbit_misc:r(VHost, queue, <<"q-all">>),

    add_binding(Config, XName, <<"foo.*">>, QStar),
    add_binding(Config, XName, <<"foo.#">>, QHash),
    add_binding(Config, XName, <<"foo.bar">>, QExact),
    add_binding(Config, XName, <<"foo.*.baz">>, QDeep),
    add_binding(Config, XName, <<"#">>, QAll),

    try
        ?assertEqual(
           [QAll, QExact, QHash, QStar],
           sort_dests(do_match(Config, XName, <<"foo.bar">>, #{}))),
        ?assertEqual(
           [QAll, QDeep, QHash],
           sort_dests(do_match(Config, XName, <<"foo.bar.baz">>, #{}))),
        ?assertEqual(
           [QAll, QHash],
           sort_dests(do_match(Config, XName, <<"foo.bar.baz.qux">>, #{}))),
        ?assertEqual(
           [QAll, QHash, QStar],
           sort_dests(do_match(Config, XName, <<"foo.qux">>, #{}))),
        ?assertEqual(
           [QAll],
           sort_dests(do_match(Config, XName, <<"bar.foo">>, #{})))
    after
        cleanup_vhost(Config, VHost)
    end,

    passed.

%% Tests '#' appearing in the middle and beginning of filters, which is
%% valid in RabbitMQ's AMQP implementation, but not in MQTT.
topic_match_hash_middle(Config) ->
    VHost = <<"test-vhost-hash-middle">>,
    setup_vhost(Config, VHost),
    XName = rabbit_misc:r(VHost, exchange, <<"amq.topic">>),

    Q1 = rabbit_misc:r(VHost, queue, <<"q-hash-c">>),
    Q2 = rabbit_misc:r(VHost, queue, <<"q-hash-bc">>),
    Q3 = rabbit_misc:r(VHost, queue, <<"q-hash-mid">>),
    Q4 = rabbit_misc:r(VHost, queue, <<"q-hash-star">>),

    add_binding(Config, XName, <<"#.c">>, Q1),
    add_binding(Config, XName, <<"#.b.c">>, Q2),
    add_binding(Config, XName, <<"a.#.d">>, Q3),
    add_binding(Config, XName, <<"#.*.#">>, Q4),

    try
        ?assertEqual(
           sort_dests([Q1, Q2, Q4]),
           sort_dests(do_match(Config, XName, <<"a.b.c">>, #{}))),
        ?assertEqual(
           sort_dests([Q1, Q4]),
           sort_dests(do_match(Config, XName, <<"c">>, #{}))),
        ?assertEqual(
           sort_dests([Q1, Q2, Q4]),
           sort_dests(do_match(Config, XName, <<"x.y.b.c">>, #{}))),
        ?assertEqual(
           sort_dests([Q3, Q4]),
           sort_dests(do_match(Config, XName, <<"a.d">>, #{}))),
        ?assertEqual(
           sort_dests([Q3, Q4]),
           sort_dests(do_match(Config, XName, <<"a.b.c.d">>, #{}))),
        ?assertEqual(
           sort_dests([Q4]),
           sort_dests(do_match(Config, XName, <<"x">>, #{}))),
        ?assertEqual(
           sort_dests([Q1, Q4]),
           sort_dests(do_match(Config, XName, <<"x.c">>, #{})))
    after
        cleanup_vhost(Config, VHost)
    end,

    passed.

topic_match_binding_keys(Config) ->
    VHost = <<"test-vhost-bkeys">>,
    setup_vhost(Config, VHost),
    XName = rabbit_misc:r(VHost, exchange, <<"amq.topic">>),

    QStar = rabbit_misc:r(VHost, queue, <<"q-star">>),
    QExact = rabbit_misc:r(VHost, queue, <<"q-exact">>),

    add_binding(Config, XName, <<"foo.*">>, QStar),
    add_binding(Config, XName, <<"foo.bar">>, QExact),

    try
        Result = do_match(Config, XName, <<"foo.bar">>, #{return_binding_keys => true}),
        ?assertEqual([{QExact, <<"foo.bar">>},
                      {QStar, <<"foo.*">>}],
                     lists:sort(Result))
    after
        cleanup_vhost(Config, VHost)
    end,

    passed.

topic_match_empty_routing_key(Config) ->
    VHost = <<"test-vhost-empty-rk">>,
    setup_vhost(Config, VHost),
    XName = rabbit_misc:r(VHost, exchange, <<"amq.topic">>),

    QHash = rabbit_misc:r(VHost, queue, <<"q-hash">>),
    QExact = rabbit_misc:r(VHost, queue, <<"q-exact">>),

    add_binding(Config, XName, <<"#">>, QHash),
    add_binding(Config, XName, <<"foo.bar">>, QExact),

    try
        %% Empty routing key should only match '#'
        ?assertEqual(
           [QHash],
           sort_dests(do_match(Config, XName, <<>>, #{}))),
        %% Non-empty routing key should also match '#'
        ?assertEqual(
           sort_dests([QExact, QHash]),
           sort_dests(do_match(Config, XName, <<"foo.bar">>, #{})))
    after
        cleanup_vhost(Config, VHost)
    end,

    passed.

%% Tests a deep 26-level topic hierarchy.
topic_match_deep_hierarchy(Config) ->
    VHost = <<"test-vhost-deep">>,
    setup_vhost(Config, VHost),
    XName = rabbit_misc:r(VHost, exchange, <<"amq.topic">>),

    T = <<"a.b.c.d.e.f.g.h.i.j.k.l.m.n.o.p.q.r.s.t.u.v.w.x.y.z">>,
    QAll = rabbit_misc:r(VHost, queue, <<"q-all">>),
    QPrefix = rabbit_misc:r(VHost, queue, <<"q-prefix">>),
    QPrefixStar = rabbit_misc:r(VHost, queue, <<"q-prefix-star">>),

    add_binding(Config, XName, <<"#">>, QAll),
    add_binding(Config, XName, <<T/binary, ".#">>, QPrefix),
    add_binding(Config, XName, <<T/binary, ".*">>, QPrefixStar),

    try
        ?assertEqual(
           sort_dests([QAll, QPrefix]),
           sort_dests(do_match(Config, XName, T, #{}))),
        ?assertEqual(
           sort_dests([QAll, QPrefix, QPrefixStar]),
           sort_dests(do_match(Config, XName, <<T/binary, ".1">>, #{})))
    after
        cleanup_vhost(Config, VHost)
    end,

    passed.

%% Tests a filter with 26 single-level wildcards followed by '#'.
topic_match_all_wildcards(Config) ->
    VHost = <<"test-vhost-all-wild">>,
    setup_vhost(Config, VHost),
    XName = rabbit_misc:r(VHost, exchange, <<"amq.topic">>),

    T = <<"a.b.c.d.e.f.g.h.i.j.k.l.m.n.o.p.q.r.s.t.u.v.w.x.y.z">>,
    W = <<"*.*.*.*.*.*.*.*.*.*.*.*.*.*.*.*.*.*.*.*.*.*.*.*.*.*.#">>,
    QWild = rabbit_misc:r(VHost, queue, <<"q-wild">>),

    add_binding(Config, XName, W, QWild),

    try
        ?assertEqual(
           [QWild],
           sort_dests(do_match(Config, XName, T, #{})))
    after
        cleanup_vhost(Config, VHost)
    end,

    passed.

%% Tests alternating literal and wildcard segments.
topic_match_alternating_wildcards(Config) ->
    VHost = <<"test-vhost-alt-wild">>,
    setup_vhost(Config, VHost),
    XName = rabbit_misc:r(VHost, exchange, <<"amq.topic">>),

    T = <<"a.b.c.d.e.f.g.h.i.j.k.l.m.n.o.p.q.r.s.t.u.v.w.x.y.z">>,
    W = <<"a.*.c.*.e.*.g.*.i.*.k.*.m.*.o.*.q.*.s.*.u.*.w.*.y.*.#">>,
    QAlt = rabbit_misc:r(VHost, queue, <<"q-alt">>),

    add_binding(Config, XName, W, QAlt),

    try
        ?assertEqual(
           [QAlt],
           sort_dests(do_match(Config, XName, T, #{})))
    after
        cleanup_vhost(Config, VHost)
    end,

    passed.

%% Tests multiple overlapping wildcard filters against the same topic.
topic_match_overlapping_filters(Config) ->
    VHost = <<"test-vhost-overlap">>,
    setup_vhost(Config, VHost),
    XName = rabbit_misc:r(VHost, exchange, <<"amq.topic">>),

    Q1 = rabbit_misc:r(VHost, queue, <<"q1">>),
    Q2 = rabbit_misc:r(VHost, queue, <<"q2">>),
    Q3 = rabbit_misc:r(VHost, queue, <<"q3">>),
    Q4 = rabbit_misc:r(VHost, queue, <<"q4">>),
    Q5 = rabbit_misc:r(VHost, queue, <<"q5">>),
    Q6 = rabbit_misc:r(VHost, queue, <<"q6">>),
    Q7 = rabbit_misc:r(VHost, queue, <<"q7">>),
    Q8 = rabbit_misc:r(VHost, queue, <<"q8">>),

    add_binding(Config, XName, <<"a.b">>, Q1),
    add_binding(Config, XName, <<"a.b.#">>, Q2),
    add_binding(Config, XName, <<"a.b.c">>, Q3),
    add_binding(Config, XName, <<"a.b.*">>, Q4),
    add_binding(Config, XName, <<"a.b.d">>, Q5),
    add_binding(Config, XName, <<"a.*.*">>, Q6),
    add_binding(Config, XName, <<"a.*.#">>, Q7),
    add_binding(Config, XName, <<"*.b.c">>, Q8),

    try
        ?assertEqual(
           sort_dests([Q2, Q3, Q4, Q6, Q7, Q8]),
           sort_dests(do_match(Config, XName, <<"a.b.c">>, #{}))),
        ?assertEqual(
           sort_dests([Q1, Q2, Q7]),
           sort_dests(do_match(Config, XName, <<"a.b">>, #{})))
    after
        cleanup_vhost(Config, VHost)
    end,

    passed.

%% Tests routing keys with empty segments (consecutive dots).
topic_match_empty_segments(Config) ->
    VHost = <<"test-vhost-empty-seg">>,
    setup_vhost(Config, VHost),
    XName = rabbit_misc:r(VHost, exchange, <<"amq.topic">>),

    Q1 = rabbit_misc:r(VHost, queue, <<"q-hash">>),
    Q2 = rabbit_misc:r(VHost, queue, <<"q-star">>),
    Q3 = rabbit_misc:r(VHost, queue, <<"q-deep">>),

    %% Leading dot means the first segment is empty.
    add_binding(Config, XName, <<".#">>, Q1),
    add_binding(Config, XName, <<".*">>, Q2),
    add_binding(Config, XName, <<".*.a.b.c">>, Q3),

    try
        ?assertEqual(
           sort_dests([Q1, Q3]),
           sort_dests(do_match(Config, XName, <<".0.a.b.c">>, #{})))
    after
        cleanup_vhost(Config, VHost)
    end,

    passed.

topic_match_large_fanout(Config) ->
    VHost = <<"test-vhost-fanout">>,
    setup_vhost(Config, VHost),
    XName = rabbit_misc:r(VHost, exchange, <<"amq.topic">>),

    NumQueues = 500,
    Queues = [begin
                  QName = rabbit_misc:r(VHost, queue,
                                        list_to_binary("q-fan-" ++ integer_to_list(I))),
                  add_binding(Config, XName, <<"events.user.login">>, QName),
                  QName
              end || I <- lists:seq(1, NumQueues)],

    try
        Result = do_match(Config, XName, <<"events.user.login">>, #{}),
        ?assertEqual(lists:sort(Queues),
                     lists:sort(Result))
    after
        cleanup_vhost(Config, VHost)
    end,

    passed.

%% Exchange-to-exchange topic bindings: a topic exchange routes to both
%% queue and exchange destinations.
topic_match_exchange_to_exchange(Config) ->
    VHost = <<"test-vhost-e2e">>,
    setup_vhost(Config, VHost),
    XSrc = rabbit_misc:r(VHost, exchange, <<"amq.topic">>),

    QDirect = rabbit_misc:r(VHost, queue, <<"q-direct">>),
    XDest = rabbit_misc:r(VHost, exchange, <<"x-dest">>),
    QViaExchange = rabbit_misc:r(VHost, queue, <<"q-via-exchange">>),

    add_binding(Config, XSrc, <<"events.*.critical">>, QDirect),
    add_exchange_binding(Config, XSrc, <<"events.#">>, XDest),
    add_binding(Config, XSrc, <<"events.app.critical">>, QViaExchange),

    try
        Result = do_match(Config, XSrc, <<"events.app.critical">>, #{}),
        ?assertEqual(sort_dests([QDirect, XDest, QViaExchange]),
                     sort_dests(Result)),

        ResultBKeys = do_match(Config, XSrc, <<"events.app.critical">>,
                               #{return_binding_keys => true}),
        ?assert(lists:member(
                  {QDirect, <<"events.*.critical">>},
                  ResultBKeys)),
        ?assert(lists:member(
                  {QViaExchange, <<"events.app.critical">>},
                  ResultBKeys)),
        %% Exchange destinations do not carry a binding key
        ?assert(lists:member(XDest, ResultBKeys)),

        ResultWild = do_match(Config, XSrc, <<"events.db.info">>, #{}),
        ?assertEqual([XDest], ResultWild)
    after
        cleanup_vhost(Config, VHost)
    end,

    passed.

%% Unicode characters in both routing keys and binding keys.
topic_match_unicode(Config) ->
    VHost = <<"test-vhost-unicode">>,
    setup_vhost(Config, VHost),
    XName = rabbit_misc:r(VHost, exchange, <<"amq.topic">>),

    QCjk = rabbit_misc:r(VHost, queue, <<"q-cjk">>),
    QEmoji = rabbit_misc:r(VHost, queue, <<"q-emoji">>),
    QCyrillic = rabbit_misc:r(VHost, queue, <<"q-cyrillic">>),
    QMixed = rabbit_misc:r(VHost, queue, <<"q-mixed">>),
    QWild = rabbit_misc:r(VHost, queue, <<"q-wild">>),
    QHash = rabbit_misc:r(VHost, queue, <<"q-hash">>),
    QAll = rabbit_misc:r(VHost, queue, <<"q-all">>),

    add_binding(Config, XName, <<"传感器.温度.摄氏"/utf8>>, QCjk),
    add_binding(Config, XName, <<"🏠.🌡️.📊"/utf8>>, QEmoji),
    add_binding(Config, XName, <<"датчик.температура.цельсий"/utf8>>, QCyrillic),
    add_binding(Config, XName, <<"sensor.données.état"/utf8>>, QMixed),
    add_binding(Config, XName, <<"传感器.*.摄氏"/utf8>>, QWild),
    add_binding(Config, XName, <<"датчик.#"/utf8>>, QHash),
    add_binding(Config, XName, <<"#">>, QAll),

    try
        ?assertEqual(
           sort_dests([QCjk, QWild, QAll]),
           sort_dests(do_match(Config, XName,
                               <<"传感器.温度.摄氏"/utf8>>, #{}))),

        ?assertEqual(
           sort_dests([QEmoji, QAll]),
           sort_dests(do_match(Config, XName,
                               <<"🏠.🌡️.📊"/utf8>>, #{}))),

        ?assertEqual(
           sort_dests([QCyrillic, QHash, QAll]),
           sort_dests(do_match(Config, XName,
                               <<"датчик.температура.цельсий"/utf8>>, #{}))),

        ?assertEqual(
           sort_dests([QMixed, QAll]),
           sort_dests(do_match(Config, XName,
                               <<"sensor.données.état"/utf8>>, #{}))),

        ?assertEqual(
           sort_dests([QWild, QAll]),
           sort_dests(do_match(Config, XName,
                               <<"传感器.压力.摄氏"/utf8>>, #{}))),

        ?assertEqual(
           sort_dests([QHash, QAll]),
           sort_dests(do_match(Config, XName,
                               <<"датчик.давление"/utf8>>, #{}))),

        ResultBKeys = do_match(Config, XName, <<"传感器.温度.摄氏"/utf8>>,
                               #{return_binding_keys => true}),
        ?assert(lists:member(
                  {QCjk, <<"传感器.温度.摄氏"/utf8>>},
                  ResultBKeys)),
        ?assert(lists:member(
                  {QWild, <<"传感器.*.摄氏"/utf8>>},
                  ResultBKeys)),
        ?assert(lists:member(
                  {QAll, <<"#">>},
                  ResultBKeys))
    after
        cleanup_vhost(Config, VHost)
    end,

    passed.

%% Generates random topic filters and routing keys, creates bindings
%% on the broker via RPC, and verifies the routing results match an
%% independent reference implementation.
prop_topic_match(Config) ->
    VHost = <<"test-vhost-prop">>,
    setup_vhost(Config, VHost),
    XName = rabbit_misc:r(VHost, exchange, <<"amq.topic">>),

    try
        run_proper(
          fun() ->
                  ?FORALL(
                     {Filters, Topics},
                     {resize(10, list(topic_filter_gen())),
                      resize(10, list(topic_gen()))},
                     begin
                         NumberedFilters = lists:enumerate(Filters),
                         Queues = lists:map(
                           fun({N, FilterBin}) ->
                                   QName = rabbit_misc:r(
                                             VHost, queue,
                                             <<"prop-q-", (integer_to_binary(N))/binary>>),
                                   add_binding(Config, XName, FilterBin, QName),
                                   {N, FilterBin, QName}
                           end, NumberedFilters),
                         Result = lists:all(
                           fun(TopicBin) ->
                                   Actual = lists:usort(
                                              do_match(Config, XName, TopicBin, #{})),
                                   Expected = lists:usort(
                                     [QName
                                      || {_N, F, QName} <- Queues,
                                         ref_match_filter(F, TopicBin)]),
                                   case Actual =:= Expected of
                                       true ->
                                           true;
                                       false ->
                                           ct:pal("Topic: ~s~n"
                                                  "Filters: ~p~n"
                                                  "Expected: ~p~n"
                                                  "Actual: ~p~n",
                                                  [TopicBin, Filters,
                                                   Expected, Actual]),
                                           false
                                   end
                           end, Topics),
                         lists:foreach(
                           fun({_N, FilterBin, QName}) ->
                                   B = #binding{source = XName,
                                                key = FilterBin,
                                                destination = QName,
                                                args = []},
                                   rabbit_ct_broker_helpers:rpc(
                                     Config, 0,
                                     rabbit_binding, remove, [B, <<"test-user">>])
                           end, Queues),
                         Result
                     end)
          end)
    after
        cleanup_vhost(Config, VHost)
    end,
    passed.

%% Property: inserting and deleting bindings maintains correct projection
%% table invariants. After inserting N unique bindings, the binding table
%% should contain exactly N entries for the vhost. After deleting all
%% bindings (in random order), both projection tables should be empty.
prop_topic_binding_lifecycle(Config) ->
    VHost = <<"test-vhost-lifecycle">>,
    setup_vhost(Config, VHost),
    XName = rabbit_misc:r(VHost, exchange, <<"amq.topic">>),

    try
        run_proper(
          fun() ->
                  ?FORALL(
                     FilterQueuePairs,
                     resize(15, non_empty(list(
                       {topic_filter_gen(),
                        oneof([<<"q1">>, <<"q2">>, <<"q3">>])}))),
                     begin
                         Bindings = lists:map(
                           fun({FilterBin, QBin}) ->
                                   QName = rabbit_misc:r(VHost, queue, QBin),
                                   add_binding(Config, XName, FilterBin, QName),
                                   #binding{source = XName,
                                            key = FilterBin,
                                            destination = QName,
                                            args = []}
                           end, FilterQueuePairs),

                         UniqueBindings = lists:usort(
                                            [{F, Q} || #binding{key = F, destination = Q} <- Bindings]),
                         ExpectedBindingCount = length(UniqueBindings),

                         HasNonEmptyKey = lists:any(
                                            fun({F, _}) -> F =/= <<>> end,
                                            UniqueBindings),

                         BindingCount = count_binding_entries(Config, VHost),
                         TrieCount = count_trie_entries(Config, VHost),

                         BindingCountOk = BindingCount =:= ExpectedBindingCount,
                         %% Empty binding keys (<<>>) bind at the root node
                         %% without creating any trie edges.
                         TrieCountOk = case HasNonEmptyKey of
                                           true -> TrieCount > 0;
                                           false -> TrieCount >= 0
                                       end,

                         case BindingCountOk andalso TrieCountOk of
                             false ->
                                 ct:pal("After insert:~n"
                                        "  Pairs: ~p~n"
                                        "  Expected binding count: ~b~n"
                                        "  Actual binding count: ~b~n"
                                        "  Trie entries: ~b~n",
                                        [FilterQueuePairs, ExpectedBindingCount,
                                         BindingCount, TrieCount]);
                             true ->
                                 ok
                         end,

                         ShuffledBindings = shuffle(lists:usort(Bindings)),
                         lists:foreach(
                           fun(B) ->
                                   rabbit_ct_broker_helpers:rpc(
                                     Config, 0,
                                     rabbit_binding, remove, [B, <<"test-user">>])
                           end, ShuffledBindings),

                         BindingCountAfter = count_binding_entries(Config, VHost),
                         TrieCountAfter = count_trie_entries(Config, VHost),

                         EmptyOk = BindingCountAfter =:= 0 andalso
                                   TrieCountAfter =:= 0,

                         case EmptyOk of
                             false ->
                                 ct:pal("After delete:~n"
                                        "  Binding entries remaining: ~b~n"
                                        "  Trie entries remaining: ~b~n",
                                        [BindingCountAfter, TrieCountAfter]);
                             true ->
                                 ok
                         end,

                         BindingCountOk andalso TrieCountOk andalso EmptyOk
                     end)
          end)
    after
        cleanup_vhost(Config, VHost)
    end,
    passed.

%% Independent reference implementation of AMQP topic matching.
ref_match_filter(FilterBin, TopicBin) ->
    FilterWords = rabbit_db_topic_exchange:split_topic_key_binary(FilterBin),
    TopicWords = rabbit_db_topic_exchange:split_topic_key_binary(TopicBin),
    ref_match(FilterWords, TopicWords).

ref_match([], []) ->
    true;
ref_match([<<"#">>], _) ->
    true;
ref_match([<<"#">> | FRest], Topic) ->
    ref_match_hash(FRest, Topic);
ref_match([<<"*">> | FRest], [_ | TRest]) ->
    ref_match(FRest, TRest);
ref_match([W | FRest], [W | TRest]) ->
    ref_match(FRest, TRest);
ref_match(_, _) ->
    false.

ref_match_hash(FRest, Topic) ->
    ref_match(FRest, Topic)
    orelse
    case Topic of
        [_ | TRest] -> ref_match_hash(FRest, TRest);
        [] -> false
    end.

run_proper(Fun) ->
    ?assert(proper:counterexample(
              Fun(),
              [{numtests, 100},
               {on_output, fun(".", _) -> ok;
                              (F, A) -> ct:pal(?LOW_IMPORTANCE, F, A)
                           end}])).

topic_gen() ->
    ?LET(Words, resize(5, list(topic_word_gen())),
         list_to_binary(lists:join(".", Words))).

topic_filter_gen() ->
    frequency([{3, ?LET(Pat, resize(5, list(mqtt_segment_gen())),
                        mk_filter_binary_mqtt(Pat))},
               {1, ?LET(Pat, resize(5, list(amqp_segment_gen())),
                        mk_filter_binary_amqp(Pat))}]).

topic_word_gen() ->
    oneof([<<"a">>, <<"b">>, <<"c">>, <<"d">>,
           <<"foo">>, <<"bar">>, <<"baz">>,
           <<>>]).

%% MQTT-style: '#' can only appear at the end.
mqtt_segment_gen() ->
    frequency([{5, literal},
               {2, star},
               {1, hash}]).

%% AMQP-style: '#' can appear anywhere.
amqp_segment_gen() ->
    frequency([{5, literal},
               {2, star},
               {2, hash}]).

mk_filter_binary_mqtt(Pat) ->
    mk_filter_binary_mqtt(Pat, []).

mk_filter_binary_mqtt([], Acc) ->
    list_to_binary(lists:join(".", lists:reverse(Acc)));
mk_filter_binary_mqtt([hash | _], Acc) ->
    list_to_binary(lists:join(".", lists:reverse(["#" | Acc])));
mk_filter_binary_mqtt([star | Rest], Acc) ->
    mk_filter_binary_mqtt(Rest, ["*" | Acc]);
mk_filter_binary_mqtt([literal | Rest], Acc) ->
    Word = lists:nth(rand:uniform(4), ["a", "b", "c", "d"]),
    mk_filter_binary_mqtt(Rest, [Word | Acc]).

mk_filter_binary_amqp(Pat) ->
    Segments = [case S of
                    literal -> lists:nth(rand:uniform(4), ["a", "b", "c", "d"]);
                    star -> "*";
                    hash -> "#"
                end || S <- Pat],
    list_to_binary(lists:join(".", Segments)).

%% ---------------------------------------------------------------------------
%% Helpers
%% ---------------------------------------------------------------------------

setup_vhost(Config, VHost) ->
    ok = rabbit_ct_broker_helpers:rpc(Config, 0, rabbit_vhost, add, [VHost, <<"test-user">>]).

cleanup_vhost(Config, VHost) ->
    ok = rabbit_ct_broker_helpers:rpc(Config, 0, rabbit_vhost, delete, [VHost, <<"test-user">>]).

add_binding(Config, XName, BindingKey, QName) ->
    Ret = rabbit_ct_broker_helpers:rpc(
            Config, 0,
            rabbit_amqqueue, declare,
            [QName, true, false, [], self(), <<"test-user">>]),
    case Ret of
        {new, _Q} -> ok;
        {existing, _Q} -> ok
    end,
    Binding = #binding{source = XName,
                       key = BindingKey,
                       destination = QName,
                       args = []},
    ok = rabbit_ct_broker_helpers:rpc(Config, 0, rabbit_binding, add, [Binding, <<"test-user">>]).

add_exchange_binding(Config, XSrc, BindingKey, XDest) ->
    {ok, _} = rabbit_ct_broker_helpers:rpc(
                Config, 0,
                rabbit_exchange, declare,
                [XDest, fanout, true, false, false, [], <<"test-user">>]),
    Binding = #binding{source = XSrc,
                       key = BindingKey,
                       destination = XDest,
                       args = []},
    ok = rabbit_ct_broker_helpers:rpc(Config, 0, rabbit_binding, add, [Binding, <<"test-user">>]).

do_match(Config, XName, RoutingKey, Opts) ->
    rabbit_ct_broker_helpers:rpc(
      Config, 0,
      rabbit_db_topic_exchange, match, [XName, RoutingKey, Opts]).

sort_dests(List) ->
    lists:usort(List).

read_trie_table(Config, Node, VHost, Table) ->
    Entries = rabbit_ct_broker_helpers:rpc(Config, Node, ets, tab2list, [Table]),
    [Entry || Entry <- Entries,
              trie_entry_matches_vhost(Entry, VHost)].

trie_entry_matches_vhost(Entry, VHost) when is_tuple(Entry) ->
    case element(1, Entry) of
        {{V, _}, _, _} when is_binary(V) -> V =:= VHost;
        _ -> false
    end.

read_binding_table(Config, Node, Table) ->
    rabbit_ct_broker_helpers:rpc(Config, Node, ets, tab2list, [Table]).

count_binding_entries(Config, VHost) ->
    All = rabbit_ct_broker_helpers:rpc(
            Config, 0, ets, tab2list, [?TOPIC_BINDING_PROJECTION]),
    length([E || E = {{_NodeId, _BKey, #resource{virtual_host = V}}} <- All,
                 V =:= VHost]).

count_trie_entries(Config, VHost) ->
    All = rabbit_ct_broker_helpers:rpc(
            Config, 0, ets, tab2list, [?TOPIC_TRIE_PROJECTION]),
    length([E || E = {{XSrc, _NodeId, _Word}, _ChildId, _Count} <- All,
                 element(1, XSrc) =:= VHost]).

shuffle(List) ->
    [X || {_, X} <- lists:sort([{rand:uniform(), X} || X <- List])].
