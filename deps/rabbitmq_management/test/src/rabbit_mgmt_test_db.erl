%%   The contents of this file are subject to the Mozilla Public License
%%   Version 1.1 (the "License"); you may not use this file except in
%%   compliance with the License. You may obtain a copy of the License at
%%   http://www.mozilla.org/MPL/
%%
%%   Software distributed under the License is distributed on an "AS IS"
%%   basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See the
%%   License for the specific language governing rights and limitations
%%   under the License.
%%
%%   The Original Code is RabbitMQ Management Console.
%%
%%   The Initial Developer of the Original Code is VMware, Inc.
%%   Copyright (c) 2010-2012 VMware, Inc.  All rights reserved.
%%

-module(rabbit_mgmt_test_db).

-include("rabbit_mgmt.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("rabbit_common/include/rabbit.hrl").

-import(rabbit_misc, [pget/2]).
-import(rabbit_mgmt_test_util, [assert_list/2, assert_item/2, test_item/2]).

-define(debugVal2(E),
	((fun (__V) ->
		  ?debugFmt(<<"~s = ~p">>, [(??E), __V]),
		  __V
	  end)(E))).

%%----------------------------------------------------------------------------
%% Tests
%%----------------------------------------------------------------------------

queue_coarse_test() ->
    create_q(test, 0),
    create_q(test2, 0),
    stats_q(test, 0, 10),
    stats_q(test2, 0, 1),
    Exp = fun(N) -> simple_details(messages, N) end,
    assert_item(Exp(10), get_q(test, range(0, 1, 1))),
    assert_item(Exp(11), get_vhost(range(0, 1, 1))),
    assert_item(Exp(11), get_overview_q(range(0, 1, 1))),
    delete_q(test, 1),
    assert_item(Exp(1), get_vhost(range(0, 1, 1))),
    assert_item(Exp(1), get_overview_q(range(0, 1, 1))),
    delete_q(test2, 1),
    assert_item(Exp(0), get_vhost(range(0, 1, 1))),
    assert_item(Exp(0), get_overview_q(range(0, 1, 1))),
    ok.

connection_coarse_test() ->
    create_conn(test, 0),
    create_conn(test2, 0),
    stats_conn(test, 0, 10),
    stats_conn(test2, 0, 1),
    Exp = fun(N) -> simple_details(recv_oct, N) end,
    assert_item(Exp(10), get_conn(test, range(0, 1, 1))),
    assert_item(Exp(1), get_conn(test2, range(0, 1, 1))),
    delete_conn(test, 1),
    delete_conn(test2, 1),
    assert_list([], rabbit_mgmt_db:get_all_connections(range(0, 1, 1))),
    ok.

fine_stats_aggregation_test() ->
    create_ch(ch1, 0),
    create_ch(ch2, 0),
    stats_ch_x  (ch1, 0, [{x, 100}]),
    stats_ch_x  (ch2, 0, [{x, 10}]),
    stats_ch_q_x(ch1, 0, [{q1, x, 100},
                          {q2, x, 10}]),
    stats_ch_q_x(ch2, 0, [{q1, x, 50},
                          {q2, x, 5}]),
    stats_ch_q  (ch1, 0, [{q1, 2},
                          {q2, 1}]),
    fine_stats_aggregation_test0(true),
    delete_q(q2, 0),
    fine_stats_aggregation_test0(false),
    delete_ch(ch1, 1),
    delete_ch(ch2, 1),
    ok.

fine_stats_aggregation_test0(Q2Exists) ->
    R = range(0, 1, 1),
    Ch1 = get_ch(ch1, R),
    Ch2 = get_ch(ch2, R),
    X   = get_x(x, R),
    Q1  = get_q(q1, R),
    V   = get_vhost(R),
    O   = get_overview(R),
    Assert = fun (m, Type, N, Obj) ->
                     Act = pget(message_stats, Obj),
                     assert_item(simple_details(Type, N), Act);
                 ({T2, Name}, Type, N, Obj) ->
                     Act = find_detailed_stats(Name, pget(expand(T2), Obj)),
                     assert_item(simple_details(Type, N), Act)
             end,
    AssertNegative = fun ({T2, Name}, Obj) ->
                             detailed_stats_absent(Name, pget(expand(T2), Obj))
                     end,
    Assert(m, publish,     100, Ch1),
    Assert(m, publish,     10,  Ch2),
    Assert(m, publish_in,  110, X),
    Assert(m, publish_out, 165, X),
    Assert(m, publish,     150, Q1),
    Assert(m, deliver_get, 2,   Q1),
    Assert(m, deliver_get, 3,   Ch1),
    Assert(m, publish,     110, V),
    Assert(m, deliver_get, 3,   V),
    Assert(m, publish,     110, O),
    Assert(m, deliver_get, 3,   O),
    Assert({pub, x},   publish, 100, Ch1),
    Assert({pub, x},   publish, 10,  Ch2),
    Assert({in,  ch1}, publish, 100, X),
    Assert({in,  ch2}, publish, 10,  X),
    Assert({out, q1},  publish, 150, X),
    Assert({in,  x},   publish, 150, Q1),
    Assert({del, ch1}, deliver_get, 2, Q1),
    Assert({del, q1},  deliver_get, 2, Ch1),
    case Q2Exists of
        true  -> Q2  = get_q(q2, R),
                 Assert(m, publish,     15,  Q2),
                 Assert(m, deliver_get, 1,   Q2),
                 Assert({out, q2},  publish, 15,  X),
                 Assert({in,  x},   publish, 15,  Q2),
                 Assert({del, ch1}, deliver_get, 1, Q2),
                 Assert({del, q2},  deliver_get, 1, Ch1);
        false -> AssertNegative({out, q2}, X),
                 AssertNegative({del, q2}, Ch1)
    end,
    ok.

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

stats_ch_x(Name, Timestamp, Stats) ->
    stats_ch(Name, Timestamp, channel_exchange_stats,
             [{x(XName), [{publish, N}]} || {XName, N} <- Stats]).

stats_ch_q(Name, Timestamp, Stats) ->
    stats_ch(Name, Timestamp, channel_queue_stats,
             [{q(QName), [{deliver_no_ack, N}]} || {QName, N} <- Stats]).

stats_ch_q_x(Name, Timestamp, Stats) ->
    stats_ch(
      Name, Timestamp, channel_queue_exchange_stats,
      [{{q(QName), x(XName)}, [{publish, N}]} || {QName, XName, N} <- Stats]).

stats_ch(Name, Timestamp, Type, Stats) ->
    event(channel_stats, [{pid,  pid(Name)},
                          {Type, Stats}], Timestamp).

delete_q(Name, Timestamp) ->
    event(queue_deleted, [{name, q(Name)}], Timestamp).

delete_conn(Name, Timestamp) ->
    event(connection_closed, [{pid, pid_del(Name)}], Timestamp).

delete_ch(Name, Timestamp) ->
    event(channel_closed, [{pid, pid_del(Name)}], Timestamp).

event(Type, Stats, Timestamp) ->
    gen_server:cast({global, rabbit_mgmt_db},
                    {event, #event{type      = Type,
                                   props     = Stats,
                                   timestamp = sec_to_triple(Timestamp)}}).

sec_to_triple(Sec) -> {Sec div 1000000, Sec rem 1000000, 0}.

%%----------------------------------------------------------------------------
%% Events out
%%----------------------------------------------------------------------------

range(F, L, I) ->
    R = #range{first = F * 1000, last = L * 1000, incr = I * 1000},
    {R, R, R}.

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
get_ch(Name, Range) -> rabbit_mgmt_db:get_channel(a2b(Name), Range, full).

get_overview(Range) -> rabbit_mgmt_db:get_overview(Range).
get_overview_q(Range) -> pget(queue_totals, get_overview(Range)).

details(R, AR, L) ->
    [{rate,     R},
     {interval, 5000},
     {samples,  [[{sample, S}, {timestamp, T * 1000}] || {T, S} <- L]},
     {avg_rate, AR}].

simple_details(Thing, N) ->
    [{Thing, N},
     {atom_suffix(Thing, "_details"), details(0, 0.0, [{1, N}, {0, N}])}].

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
