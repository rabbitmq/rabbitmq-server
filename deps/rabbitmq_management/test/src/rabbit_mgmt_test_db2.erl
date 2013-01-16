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

-module(rabbit_mgmt_test_db2).

-include("rabbit_mgmt.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("rabbit_common/include/rabbit.hrl").

-import(rabbit_mgmt_test_util, [assert_list/2, assert_item/2, test_item/2]).

-define(debugVal2(E),
	((fun (__V) ->
		  ?debugFmt(<<"~s = ~p">>, [(??E), __V]),
		  __V
	  end)(E))).

%%----------------------------------------------------------------------------
%% Tests
%%----------------------------------------------------------------------------

simple_queue_coarse_test() ->
    create_q(test, 0),
    create_q(test2, 0),
    stats_q(test, 0, 10),
    stats_q(test2, 0, 1),
    Exp = fun(N) -> [{messages, N},
                     {messages_details, details(0, 0.0, [{1, N}, {0, N}])}]
          end,
    assert_item(Exp(10), get_q(test, range(0, 1, 1))),
    assert_item(Exp(11), get_vhost(range(0, 1, 1))),
    delete_q(test, 1),
    assert_item(Exp(1), get_vhost(range(0, 1, 1))),
    delete_q(test2, 1),
    assert_item(Exp(0), get_vhost(range(0, 1, 1))),
    ok.

simple_connection_coarse_test() ->
    create_conn(test, 0),
    create_conn(test2, 0),
    stats_conn(test, 0, 10),
    stats_conn(test2, 0, 1),
    Exp = fun(N) -> [{recv_oct, N},
                     {recv_oct_details, details(0, 0.0, [{1, N}, {0, N}])}]
          end,
    assert_item(Exp(10), get_conn(test, range(0, 1, 1))),
    assert_item(Exp(1), get_conn(test2, range(0, 1, 1))),
    delete_conn(test, 1),
    delete_conn(test2, 1),
    assert_list([], rabbit_mgmt_db:get_all_connections(range(0, 1, 1))),
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

stats_q(Name, Timestamp, Msgs) ->
    event(queue_stats, [{name,     q(Name)},
                        {messages, Msgs}], Timestamp).

stats_conn(Name, Timestamp, Oct) ->
    event(connection_stats, [{pid ,     pid(Name)},
                             {recv_oct, Oct}], Timestamp).

delete_q(Name, Timestamp) ->
    event(queue_deleted, [{name, q(Name)}], Timestamp).

delete_conn(Name, Timestamp) ->
    event(connection_closed, [{pid, pid_del(Name)}], Timestamp).

event(Type, Stats, Timestamp) ->
    gen_server:cast({global, rabbit_mgmt_db},
                    {event, #event{type      = Type,
                                   props     = Stats,
                                   timestamp = sec_to_triple(Timestamp)}}).

sec_to_triple(Sec) -> {Sec div 1000000, Sec rem 1000000, 0}.

%%----------------------------------------------------------------------------
%% Events out
%%----------------------------------------------------------------------------

range(F, L, I) -> #range{first = F * 1000, last = L * 1000, incr = I * 1000}.

get_q(Name, Range) ->
    [Q] = rabbit_mgmt_db:augment_queues([q2(Name)], Range, full),
    Q.

get_vhost(Range) ->
    [VHost] = rabbit_mgmt_db:augment_vhosts([[{name, <<"/">>}]], Range),
    VHost.

get_conn(Name, Range) -> rabbit_mgmt_db:get_connection(a2b(Name), Range).

details(R, AR, L) ->
    [{rate,     R},
     {interval, 5000},
     {samples,  [[{sample, S}, {timestamp, T * 1000}] || {T, S} <- L]},
     {avg_rate, AR}].

%%----------------------------------------------------------------------------
%% Util
%%----------------------------------------------------------------------------

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
