%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2016-2021 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(rabbit_mgmt_test_db_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("rabbit_common/include/rabbit_core_metrics.hrl").
-include_lib("rabbitmq_management_agent/include/rabbit_mgmt_records.hrl").
-include_lib("rabbitmq_management_agent/include/rabbit_mgmt_metrics.hrl").
-include("rabbit_mgmt.hrl").
-include_lib("rabbitmq_ct_helpers/include/rabbit_mgmt_test.hrl").
-import(rabbit_mgmt_test_util, [assert_list/2,
                                reset_management_settings/1]).

-import(rabbit_misc, [pget/2]).

-compile(export_all).
-compile({no_auto_import, [ceil/1]}).

all() ->
    [
     {group, non_parallel_tests}
    ].

groups() ->
    [
     {non_parallel_tests, [], [
                               queue_coarse_test,
                               connection_coarse_test,
                               fine_stats_aggregation_time_test,
                               fine_stats_aggregation_test,
                               all_consumers_test
                              ]}
    ].

%% -------------------------------------------------------------------
%% Testsuite setup/teardown.
%% -------------------------------------------------------------------

init_per_suite(Config) ->
    rabbit_ct_helpers:log_environment(),
    inets:start(),
    Config.

end_per_suite(Config) ->
    Config.

init_per_group(_, Config) ->
    Config1 = rabbit_ct_helpers:set_config(Config, [
                                                    {rmq_nodename_suffix, ?MODULE}
                                                   ]),
    Config2 = rabbit_ct_helpers:merge_app_env(
        rabbit_mgmt_test_util:merge_stats_app_env(Config1, 1000, 1),
            {rabbitmq_management_agent, [{rates_mode, detailed}]}),
    rabbit_ct_helpers:run_setup_steps(Config2,
                                      rabbit_ct_broker_helpers:setup_steps() ++
                                          rabbit_ct_client_helpers:setup_steps()).

end_per_group(_, Config) ->
    rabbit_ct_helpers:run_teardown_steps(Config,
                                         rabbit_ct_client_helpers:teardown_steps() ++
                                             rabbit_ct_broker_helpers:teardown_steps()).

init_per_testcase(Testcase, Config) ->
    reset_management_settings(Config),
    rabbit_ct_helpers:testcase_started(Config, Testcase).

end_per_testcase(Testcase, Config) ->
    reset_management_settings(Config),
    rabbit_ct_helpers:testcase_finished(Config, Testcase).

%% -------------------------------------------------------------------
%% Testcases.
%% -------------------------------------------------------------------

trace_fun(Config, MFs) ->
    Nodename1 = rabbit_ct_broker_helpers:get_node_config(Config, 0, nodename),
    dbg:tracer(process, {fun(A,_) ->
                                 ct:pal(?LOW_IMPORTANCE,
                                        "TRACE: ~p", [A])
                         end, ok}),
    dbg:n(Nodename1),
    dbg:p(all,c),
    [ dbg:tpl(M, F, cx) || {M, F} <- MFs].

queue_coarse_test(Config) ->
    ok = rabbit_ct_broker_helpers:rpc(Config, 0, ?MODULE, queue_coarse_test1, [Config]).

queue_coarse_test1(_Config) ->
    [rabbit_mgmt_metrics_collector:override_lookups(T, [{exchange, fun dummy_lookup/1},
                                                        {queue,    fun dummy_lookup/1}])
     || {T, _} <- ?CORE_TABLES],
    First = exometer_slide:timestamp(),
    stats_series(fun stats_q/2, [[{test, 1}, {test2, 1}], [{test, 10}], [{test, 20}]]),
    timer:sleep(1150 * 2), %% The x2 factor is arbitrary: it makes CI happy.
    Last = exometer_slide:timestamp(),
    Interval = 1,
    R = range(First, Last, Interval),
    simple_details(get_q(test, R), messages, 20, R),
    simple_details(get_vhost(R), messages, 21, R),
    simple_details(get_overview_q(R), messages, 21, R),
    delete_q(test),
    timer:sleep(1150),
    Next = last_ts(First, Interval),
    R1 = range(First, Next, Interval),
    simple_details(get_vhost(R1), messages, 1, R1),
    simple_details(get_overview_q(R1), messages, 1, R1),
    delete_q(test2),
    timer:sleep(1150),
    Next2 = last_ts(First, Interval),
    R2 = range(First, Next2, Interval),
    simple_details(get_vhost(R2), messages, 0, R2),
    simple_details(get_overview_q(R2), messages, 0, R2),
    [rabbit_mgmt_metrics_collector:reset_lookups(T) || {T, _} <- ?CORE_TABLES],
    ok.

%% Generate a well-formed interval from Start using Interval steps
last_ts(First, Interval) ->
    Now = exometer_slide:timestamp(),
    ceil(((Now - First) / Interval * 1000)) * Interval + First.

ceil(X) when X < 0 ->
    trunc(X);
ceil(X) ->
    T = trunc(X),
    case X - T == 0 of
        true -> T;
        false -> T + 1
    end.

connection_coarse_test(Config) ->
    ok = rabbit_ct_broker_helpers:rpc(Config, 0, ?MODULE, connection_coarse_test1, [Config]).

connection_coarse_test1(_Config) ->
    First = exometer_slide:timestamp(),
    create_conn(test),
    create_conn(test2),
    stats_series(fun stats_conn/2, [[{test, 2}, {test2, 5}], [{test, 5}, {test2, 1}],
                                    [{test, 10}]]),
    Last = last_ts(First, 5),
    R = range(First, Last, 5),
    simple_details(get_conn(test, R), recv_oct, 10, R),
    simple_details(get_conn(test2, R), recv_oct, 1, R),
    delete_conn(test),
    delete_conn(test2),
    timer:sleep(1150),
    assert_list([], rabbit_mgmt_db:get_all_connections(R)),
    ok.

fine_stats_aggregation_test(Config) ->
    ok = rabbit_ct_broker_helpers:rpc(Config, 0, ?MODULE, fine_stats_aggregation_test1, [Config]).

fine_stats_aggregation_test1(_Config) ->
    [rabbit_mgmt_metrics_collector:override_lookups(T, [{exchange, fun dummy_lookup/1},
                                                        {queue,    fun dummy_lookup/1}])
     || {T, _} <- ?CORE_TABLES],
    First = exometer_slide:timestamp(),
    create_conn(test),
    create_conn(test2),
    create_ch(ch1, [{connection, pid(test)}]),
    create_ch(ch2, [{connection, pid(test2)}]),
    %% Publish differences
    channel_series(ch1, [{[{x, 50}], [{q1, x, 15}, {q2, x, 2}], [{q1, 5}, {q2, 5}]},
                         {[{x, 25}], [{q1, x, 10}, {q2, x, 3}], [{q1, -2}, {q2, -3}]},
                         {[{x, 25}], [{q1, x, 25}, {q2, x, 5}], [{q1, -1}, {q2, -1}]}]),
    channel_series(ch2, [{[{x, 5}], [{q1, x, 15}, {q2, x, 1}], []},
                         {[{x, 2}], [{q1, x, 10}, {q2, x, 2}], []},
                         {[{x, 3}], [{q1, x, 25}, {q2, x, 2}], []}]),
    timer:sleep(1150),

    fine_stats_aggregation_test0(true, First),
    delete_q(q2),
    timer:sleep(1150),
    fine_stats_aggregation_test0(false, First),
    delete_ch(ch1),
    delete_ch(ch2),
    delete_conn(test),
    delete_conn(test2),
    delete_x(x),
    delete_v(<<"/">>),
    [rabbit_mgmt_metrics_collector:reset_lookups(T) || {T, _} <- ?CORE_TABLES],
    ok.

fine_stats_aggregation_test0(Q2Exists, First) ->
    Last = exometer_slide:timestamp(),
    R = range(First, Last, 1),
    Ch1 = get_ch(ch1, R),

    Ch2 = get_ch(ch2, R),
    X   = get_x(x, R),
    Q1  = get_q(q1, R),
    V   = get_vhost(R),
    O   = get_overview(R),
    assert_fine_stats(m, publish,     100, Ch1, R),
    assert_fine_stats(m, publish,     10,  Ch2, R),
    assert_fine_stats(m, publish_in,  110, X, R),
    assert_fine_stats(m, publish_out, 115, X, R),
    assert_fine_stats(m, publish,     100, Q1, R),
    assert_fine_stats(m, deliver_get, 2,   Q1, R),
    assert_fine_stats(m, deliver_get, 3,   Ch1, R),
    assert_fine_stats(m, publish,     110, V, R),
    assert_fine_stats(m, deliver_get, 3,   V, R),
    assert_fine_stats(m, publish,     110, O, R),
    assert_fine_stats(m, deliver_get, 3,   O, R),
    assert_fine_stats({pub, x},   publish, 100, Ch1, R),
    assert_fine_stats({pub, x},   publish, 10,  Ch2, R),
    assert_fine_stats({in,  ch1}, publish, 100, X, R),
    assert_fine_stats({in,  ch2}, publish, 10,  X, R),
    assert_fine_stats({out, q1},  publish, 100, X, R),
    assert_fine_stats({in,  x},   publish, 100, Q1, R),
    assert_fine_stats({del, ch1}, deliver_get, 2, Q1, R),
    assert_fine_stats({del, q1},  deliver_get, 2, Ch1, R),
    case Q2Exists of
        true  -> Q2  = get_q(q2, R),
                 assert_fine_stats(m, publish,     15,  Q2, R),
                 assert_fine_stats(m, deliver_get, 1,   Q2, R),
                 assert_fine_stats({out, q2},  publish, 15,  X, R),
                 assert_fine_stats({in,  x},   publish, 15,  Q2, R),
                 assert_fine_stats({del, ch1}, deliver_get, 1, Q2, R),
                 assert_fine_stats({del, q2},  deliver_get, 1, Ch1, R);
        false -> assert_fine_stats_neg({out, q2}, X),
                 assert_fine_stats_neg({del, q2}, Ch1)
    end,
    ok.

fine_stats_aggregation_time_test(Config) ->
    %% trace_fun(Config, [{rabbit_mgmt_db, get_data_from_nodes}]),
    ok = rabbit_ct_broker_helpers:rpc(Config, 0, ?MODULE, fine_stats_aggregation_time_test1, [Config]).

fine_stats_aggregation_time_test1(_Config) ->
    [rabbit_mgmt_metrics_collector:override_lookups(T, [{exchange, fun dummy_lookup/1},
                                                        {queue,    fun dummy_lookup/1}])
     || {T, _} <- ?CORE_TABLES],
    First = exometer_slide:timestamp(),
    create_ch(ch),
    channel_series(ch, [{[{x, 50}], [{q, x, 15}], [{q, 5}]},
                        {[{x, 25}], [{q, x, 10}], [{q, 5}]},
                        {[{x, 25}], [{q, x, 25}], [{q, 10}]}]),
    timer:sleep(1150),
    Last = exometer_slide:timestamp(),

    channel_series(ch, [{[{x, 10}], [{q, x, 5}], [{q, 2}]}]),
    Next = exometer_slide:timestamp(),


    R1 = range(First, Last, 1),
    assert_fine_stats(m, publish,     100, get_ch(ch, R1), R1),
    assert_fine_stats(m, publish,     50,  get_q(q, R1), R1),
    assert_fine_stats(m, deliver_get, 20,  get_q(q, R1), R1),


    R2 = range(Last, Next, 1),
    assert_fine_stats(m, publish,     110, get_ch(ch, R2), R2),
    assert_fine_stats(m, publish,     55,  get_q(q, R2), R2),
    assert_fine_stats(m, deliver_get, 22,  get_q(q, R2), R2),

    delete_q(q),
    delete_ch(ch),
    delete_x(x),
    delete_v(<<"/">>),

    [rabbit_mgmt_metrics_collector:reset_lookups(T) || {T, _} <- ?CORE_TABLES],
    ok.

assert_fine_stats(m, Type, N, Obj, R) ->
    Act = pget(message_stats, Obj),
    simple_details(Act, Type, N, R);
assert_fine_stats({T2, Name}, Type, N, Obj, R) ->
    Act = find_detailed_stats(Name, pget(expand(T2), Obj)),
    simple_details(Act, Type, N, R).

assert_fine_stats_neg({T2, Name}, Obj) ->
    detailed_stats_absent(Name, pget(expand(T2), Obj)).

 %% {{{resource,<<"/">>,queue,<<"test_lazy">>},
 %%   <0.953.0>,<<"amq.ctag-sPlBtNl8zwIGkYhNjJrATA">>},
 %%  false,true,0,[]},
all_consumers_test(Config) ->
    ok = rabbit_ct_broker_helpers:rpc(Config, 0, ?MODULE, all_consumers_test1, [Config]).

all_consumers_test1(_Config) ->
    %% Tests that we can list all the consumers while channels are in the process of
    %% being deleted. Thus, channel details might be missing.
    %% `merge_channel_into_obj` is also used when listing queues, which might now report
    %% empty channel details while the queue is being deleted. But will not crash.
    [rabbit_mgmt_metrics_collector:override_lookups(T, [{exchange, fun dummy_lookup/1},
                                                        {queue,    fun dummy_lookup/1}])
     || {T, _} <- ?CORE_TABLES],
    create_cons(q1, ch1, <<"ctag">>, false, true, 0, []),
    timer:sleep(1150),
    [Consumer] = rabbit_mgmt_db:get_all_consumers(),
    [] = proplists:get_value(channel_details, Consumer),
    %% delete_cons(co),
    [rabbit_mgmt_metrics_collector:reset_lookups(T) || {T, _} <- ?CORE_TABLES],
    ok.
%%----------------------------------------------------------------------------
%% Events in
%%----------------------------------------------------------------------------

create_conn(Name) ->
    rabbit_core_metrics:connection_created(pid(Name), [{pid, pid(Name)},
                                                       {name, a2b(Name)}]).

create_ch(Name, Extra) ->
    rabbit_core_metrics:channel_created(pid(Name), [{pid, pid(Name)},
                                                    {name, a2b(Name)}] ++ Extra).
create_ch(Name) ->
    create_ch(Name, []).

create_cons(QName, ChName, Tag, Exclusive, AckRequired, PrefetchCount, Args) ->
    rabbit_core_metrics:consumer_created(pid(ChName), Tag, Exclusive,
                                         AckRequired, q(QName),
                                         PrefetchCount, false, waiting, Args).

stats_series(Fun, ListsOfPairs) ->
    [begin
     [Fun(Name, Msgs) || {Name, Msgs} <- List],
     timer:sleep(1150)
     end || List <- ListsOfPairs].

stats_q(Name, Msgs) ->
    rabbit_core_metrics:queue_stats(q(Name), Msgs, Msgs, Msgs, Msgs).

stats_conn(Name, Oct) ->
    rabbit_core_metrics:connection_stats(pid(Name), Oct, Oct, Oct).

channel_series(Name, ListOfStats) ->
    [begin
     stats_ch(Name, XStats, QXStats, QStats),
     timer:sleep(1150)
     end || {XStats, QXStats, QStats} <- ListOfStats].

stats_ch(Name, XStats, QXStats, QStats) ->
    [rabbit_core_metrics:channel_stats(exchange_stats, publish, {pid(Name), x(XName)}, N)
     || {XName, N} <- XStats],
    [rabbit_core_metrics:channel_stats(queue_exchange_stats, publish,
                                       {pid(Name), {q(QName), x(XName)}}, N)
     || {QName, XName, N} <- QXStats],
    [rabbit_core_metrics:channel_stats(queue_stats, deliver_no_ack, {pid(Name), q(QName)}, N)
     || {QName, N} <- QStats],
    ok.

delete_q(Name) ->
    rabbit_core_metrics:queue_deleted(q(Name)),
    rabbit_event:notify(queue_deleted, [{name, q(Name)}]).

delete_conn(Name) ->
    Pid = pid_del(Name),
    rabbit_core_metrics:connection_closed(Pid),
    rabbit_event:notify(connection_closed, [{pid, Pid}]).

delete_cons(QName, ChName, Tag) ->
    Pid = pid_del(ChName),
    rabbit_core_metrics:consumer_deleted(Pid, Tag, q(QName)),
    rabbit_event:notify(consumer_deleted, [{channel, Pid},
                                           {queue, q(QName)},
                                           {consumer_tag, Tag}]).

delete_ch(Name) ->
    Pid = pid_del(Name),
    rabbit_core_metrics:channel_closed(Pid),
    rabbit_core_metrics:channel_exchange_down({Pid, x(x)}),
    rabbit_event:notify(channel_closed, [{pid, Pid}]).

delete_x(Name) ->
    rabbit_event:notify(exchange_deleted, [{name, x(Name)}]).

delete_v(Name) ->
    rabbit_event:notify(vhost_deleted, [{name, Name}]).

%%----------------------------------------------------------------------------
%% Events out
%%----------------------------------------------------------------------------

range(F, L, I) ->
    R = #range{first = F, last = L, incr = I * 1000},
    {R, R, R, R}.

get_x(Name, Range) ->
    [X] = rabbit_mgmt_db:augment_exchanges([x2(Name)], Range, full),
    X.

get_q(Name, Range) ->
    [Q] = rabbit_mgmt_db:augment_queues([q2(Name)], Range, full),
    Q.

get_vhost(Range) ->
    [VHost] = rabbit_mgmt_db:augment_vhosts([[{name, <<"/">>}]], Range),
    VHost.

get_conn(Name, Range) -> rabbit_mgmt_db:get_connection(a2b(Name), Range).
get_ch(Name, Range) -> rabbit_mgmt_db:get_channel(a2b(Name), Range).

get_overview(Range) -> rabbit_mgmt_db:get_overview(Range).
get_overview_q(Range) -> pget(queue_totals, get_overview(Range)).

details0(R, AR, A, L) ->
    [{rate,     R},
     {samples,  [[{sample, S}, {timestamp, T}] || {T, S} <- L]},
     {avg_rate, AR},
     {avg,      A}].

simple_details(Result, Thing, N, {#range{first = First, last = Last}, _, _, _} = _R) ->
    ?assertEqual(N, proplists:get_value(Thing, Result)),
    Details = proplists:get_value(atom_suffix(Thing, "_details"), Result),
    ?assert(0 =/= proplists:get_value(rate, Details)),
    Samples = proplists:get_value(samples, Details),
    TSs = [proplists:get_value(timestamp, S) || S <- Samples],
    ?assert(First =< lists:min(TSs)),
    ?assert(Last >= lists:max(TSs)).

atom_suffix(Atom, Suffix) ->
    list_to_atom(atom_to_list(Atom) ++ Suffix).

find_detailed_stats(Name, List) ->
    [S] = filter_detailed_stats(Name, List),
    S.

detailed_stats_absent(Name, List) ->
    [] = filter_detailed_stats(Name, List).

filter_detailed_stats(Name, List) ->
    lists:foldl(fun(L, Acc) ->
                        {[{stats, Stats}], [{_, Details}]} =
                            lists:partition(fun({K, _}) -> K == stats end, L),
                        case (pget(name, Details) =:= a2b(Name)) of
                            true ->
                                [Stats | Acc];
                            false ->
                                Acc
                        end
                end, [], List).

expand(in)  -> incoming;
expand(out) -> outgoing;
expand(del) -> deliveries;
expand(pub) -> publishes.

%%----------------------------------------------------------------------------
%% Util
%%----------------------------------------------------------------------------

x(Name) -> rabbit_misc:r(<<"/">>, exchange, a2b(Name)).
x2(Name) -> q2(Name).
q(Name) -> rabbit_misc:r(<<"/">>, queue, a2b(Name)).
q2(Name) -> [{name,  a2b(Name)},
             {pid, self()},  % fake a local pid
             {vhost, <<"/">>}].

pid(Name) ->
    case get({pid, Name}) of
        undefined -> P = spawn(fun() -> ok end),
                     put({pid, Name}, P),
                     P;
        Pid       -> Pid
    end.

pid_del(Name) ->
    Pid = pid(Name),
    erase({pid, Name}),
    Pid.

a2b(A) -> list_to_binary(atom_to_list(A)).

dummy_lookup(_Thing) -> true.
