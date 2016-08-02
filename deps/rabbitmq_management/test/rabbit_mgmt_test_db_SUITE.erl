%% The contents of this file are subject to the Mozilla Public License
%% Version 1.1 (the "License"); you may not use this file except in
%% compliance with the License. You may obtain a copy of the License at
%% http://www.mozilla.org/MPL/
%%
%% Software distributed under the License is distributed on an "AS IS"
%% basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See the
%% License for the specific language governing rights and limitations
%% under the License.
%%
%% The Original Code is RabbitMQ.
%%
%% The Initial Developer of the Original Code is GoPivotal, Inc.
%% Copyright (c) 2016 Pivotal Software, Inc.  All rights reserved.
%%

-module(rabbit_mgmt_test_db_SUITE).

-include_lib("common_test/include/ct.hrl").
-include("include/rabbit_mgmt.hrl").
-include("include/rabbit_mgmt_test.hrl").

-import(rabbit_mgmt_test_util, [assert_list/2, assert_item/2,
                                reset_management_settings/1]).

-import(rabbit_misc, [pget/2]).

-compile(export_all).

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
                               fine_stats_aggregation_test
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
    rabbit_ct_helpers:run_setup_steps(Config1,
                                      rabbit_ct_broker_helpers:setup_steps() ++
                                          rabbit_ct_client_helpers:setup_steps() ++
                                          [fun rabbit_mgmt_test_util:reset_management_settings/1]).

end_per_group(_, Config) ->
    rabbit_ct_helpers:run_teardown_steps(Config,
                                         [fun rabbit_mgmt_test_util:reset_management_settings/1] ++
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

queue_coarse_test(Config) ->
    ok = rabbit_ct_broker_helpers:rpc(Config, 0, ?MODULE, queue_coarse_test1, [Config]).

queue_coarse_test1(_Config) ->
    rabbit_mgmt_event_collector:override_lookups([{exchange, fun dummy_lookup/1},
                                                  {queue,    fun dummy_lookup/1}]),
    create_q(test, 0),
    create_q(test2, 0),
    stats_q(test, 0, 10),
    stats_q(test2, 0, 1),
    R = range(0, 1, 1),
    Exp = fun(N) -> simple_details(messages, N, R) end,
    assert_item(Exp(10), get_q(test, R)),
    assert_item(Exp(11), get_vhost(R)),
    assert_item(Exp(11), get_overview_q(R)),
    delete_q(test, 0),
    assert_item(Exp(1), get_vhost(R)),
    assert_item(Exp(1), get_overview_q(R)),
    delete_q(test2, 0),
    assert_item(Exp(0), get_vhost(R)),
    assert_item(Exp(0), get_overview_q(R)),
    rabbit_mgmt_event_collector:reset_lookups(),
    ok.

connection_coarse_test(Config) ->
    ok = rabbit_ct_broker_helpers:rpc(Config, 0, ?MODULE, connection_coarse_test1, [Config]).

connection_coarse_test1(_Config) ->
    create_conn(test, 0),
    create_conn(test2, 0),
    stats_conn(test, 0, 10),
    stats_conn(test2, 0, 1),
    R = range(0, 1, 1),
    Exp = fun(N) -> simple_details(recv_oct, N, R) end,
    assert_item(Exp(10), get_conn(test, R)),
    assert_item(Exp(1), get_conn(test2, R)),
    delete_conn(test, 1),
    delete_conn(test2, 1),
    assert_list([], rabbit_mgmt_db:get_all_connections(R)),
    ok.

fine_stats_aggregation_test(Config) ->
    ok = rabbit_ct_broker_helpers:rpc(Config, 0, ?MODULE, fine_stats_aggregation_test1, [Config]).

fine_stats_aggregation_test1(_Config) ->
    application:set_env(rabbitmq_management, rates_mode, detailed),
    restart_mgmt_db(),
    rabbit_mgmt_event_collector:override_lookups([{exchange, fun dummy_lookup/1},
                                                  {queue,    fun dummy_lookup/1}]),
    create_ch(ch1, 0),
    create_ch(ch2, 0),
    stats_ch(ch1, 0, [{x, 100}], [{q1, x, 100},
                                  {q2, x, 10}], [{q1, 2},
                                                 {q2, 1}]),
    stats_ch(ch2, 0, [{x, 10}], [{q1, x, 50},
                                 {q2, x, 5}], []),
    fine_stats_aggregation_test0(true),
    delete_q(q2, 0),
    fine_stats_aggregation_test0(false),
    delete_q(q1, 0),
    delete_ch(ch1, 1),
    delete_ch(ch2, 1),
    application:set_env(rabbitmq_management, rates_mode, basic),
    restart_mgmt_db(),
    ok.

fine_stats_aggregation_test0(Q2Exists) ->
    R = range(0, 1, 1),
    Ch1 = get_ch(ch1, R),
    Ch2 = get_ch(ch2, R),
    X   = get_x(x, R),
    Q1  = get_q(q1, R),
    V   = get_vhost(R),
    O   = get_overview(R),
    assert_fine_stats(m, publish,     100, Ch1, R),
    assert_fine_stats(m, publish,     10,  Ch2, R),
    assert_fine_stats(m, publish_in,  110, X, R),
    assert_fine_stats(m, publish_out, 165, X, R),
    assert_fine_stats(m, publish,     150, Q1, R),
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
    assert_fine_stats({out, q1},  publish, 150, X, R),
    assert_fine_stats({in,  x},   publish, 150, Q1, R),
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
    ok = rabbit_ct_broker_helpers:rpc(Config, 0, ?MODULE, fine_stats_aggregation_time_test1, [Config]).

fine_stats_aggregation_time_test1(_Config) ->
    rabbit_mgmt_event_collector:override_lookups([{exchange, fun dummy_lookup/1},
                                                  {queue,    fun dummy_lookup/1}]),
    create_ch(ch, 0),
    stats_ch(ch, 0, [{x, 100}], [{q, x, 50}], [{q, 20}]),
    stats_ch(ch, 5, [{x, 110}], [{q, x, 55}], [{q, 22}]),

    R1 = range(0, 1, 1),
    assert_fine_stats(m, publish,     100, get_ch(ch, R1), R1),
    assert_fine_stats(m, publish,     50,  get_q(q, R1), R1),
    assert_fine_stats(m, deliver_get, 20,  get_q(q, R1), R1),

    R2 = range(5, 6, 1),
    assert_fine_stats(m, publish,     110, get_ch(ch, R2), R2),
    assert_fine_stats(m, publish,     55,  get_q(q, R2), R2),
    assert_fine_stats(m, deliver_get, 22,  get_q(q, R2), R2),

    delete_q(q, 0),
    delete_ch(ch, 1),
    rabbit_mgmt_event_collector:reset_lookups(),
    ok.

assert_fine_stats(m, Type, N, Obj, R) ->
    Act = pget(message_stats, Obj),
    assert_item(simple_details(Type, N, R), Act);
assert_fine_stats({T2, Name}, Type, N, Obj, R) ->
    Act = find_detailed_stats(Name, pget(expand(T2), Obj)),
    assert_item(simple_details(Type, N, R), Act).

assert_fine_stats_neg({T2, Name}, Obj) ->
    detailed_stats_absent(Name, pget(expand(T2), Obj)).

%%----------------------------------------------------------------------------
%% Events in
%%----------------------------------------------------------------------------

create_q(Name, Timestamp) ->
    %% Technically we do not need this, the DB ignores it, but let's
    %% be symmetrical...
    event(queue_created, [{name, q(Name)}], Timestamp).

create_conn(Name, Timestamp) ->
    event(connection_created, [{pid,  pid(Name)},
                               {name, a2b(Name)}], Timestamp).

create_ch(Name, Timestamp) ->
    event(channel_created, [{pid,  pid(Name)},
                            {name, a2b(Name)}], Timestamp).

stats_q(Name, Timestamp, Msgs) ->
    event(queue_stats, [{name,     q(Name)},
                        {messages, Msgs}], Timestamp).

stats_conn(Name, Timestamp, Oct) ->
    event(connection_stats, [{pid ,     pid(Name)},
                             {recv_oct, Oct}], Timestamp).

stats_ch(Name, Timestamp, XStats, QXStats, QStats) ->
    XStats1 = [{x(XName), [{publish, N}]} || {XName, N} <- XStats],
    QXStats1 = [{{q(QName), x(XName)}, [{publish, N}]}
                || {QName, XName, N} <- QXStats],
    QStats1 = [{q(QName), [{deliver_no_ack, N}]} || {QName, N} <- QStats],
    event(channel_stats,
          [{pid,  pid(Name)},
           {channel_exchange_stats, XStats1},
           {channel_queue_exchange_stats, QXStats1},
           {channel_queue_stats, QStats1}], Timestamp).

delete_q(Name, Timestamp) ->
    event(queue_deleted, [{name, q(Name)}], Timestamp).

delete_conn(Name, Timestamp) ->
    event(connection_closed, [{pid, pid_del(Name)}], Timestamp).

delete_ch(Name, Timestamp) ->
    event(channel_closed, [{pid, pid_del(Name)}], Timestamp).

event(Type, Stats, Timestamp) ->
    ok = gen_server:call(rabbit_mgmt_event_collector,
                         {event, #event{type      = Type,
                                        props     = Stats,
                                        reference = none,
                                        timestamp = Timestamp * 1000}}).

%%----------------------------------------------------------------------------
%% Events out
%%----------------------------------------------------------------------------

range(F, L, I) ->
    R = #range{first = F * 1000, last = L * 1000, incr = I * 1000},
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

simple_details(Thing, N, {#range{first = First, last = Last}, _, _, _}) ->
    [{Thing, N},
     {atom_suffix(Thing, "_details"),
      details0(0.0, 0.0, N * 1.0, [{Last, N}, {First, N}])}].

atom_suffix(Atom, Suffix) ->
    list_to_atom(atom_to_list(Atom) ++ Suffix).

find_detailed_stats(Name, List) ->
    [S] = filter_detailed_stats(Name, List),
    S.

detailed_stats_absent(Name, List) ->
    [] = filter_detailed_stats(Name, List).

filter_detailed_stats(Name, List) ->
    [Stats || [{stats, Stats}, {_, Details}] <- List,
              pget(name, Details) =:= a2b(Name)].

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

dummy_lookup(_Thing) -> {ok, ignore_this}.

restart_mgmt_db() ->
    supervisor2:terminate_child(rabbit_mgmt_sup_sup, rabbit_mgmt_sup),
    rabbit_mgmt_sup_sup:start_child().
