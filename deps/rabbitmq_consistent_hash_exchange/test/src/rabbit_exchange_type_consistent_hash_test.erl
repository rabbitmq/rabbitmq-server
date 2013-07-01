%% The contents of this file are subject to the Mozilla Public License
%% Version 1.1 (the "License"); you may not use this file except in
%% compliance with the License. You may obtain a copy of the License
%% at http://www.mozilla.org/MPL/
%%
%% Software distributed under the License is distributed on an "AS IS"
%% basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See
%% the License for the specific language governing rights and
%% limitations under the License.
%%
%% The Original Code is RabbitMQ Consistent Hash Exchange.
%%
%% The Initial Developer of the Original Code is GoPivotal, Inc.
%% Copyright (c) 2011-2013 GoPivotal, Inc.  All rights reserved.
%%

-module(rabbit_exchange_type_consistent_hash_test).
-export([test/0]).
-include_lib("amqp_client/include/amqp_client.hrl").

%% Because the routing is probabilistic, we can't really test a great
%% deal here.

test() ->
    %% Run the test twice to test we clean up correctly
    t([<<"q0">>, <<"q1">>, <<"q2">>, <<"q3">>]),
    t([<<"q4">>, <<"q5">>, <<"q6">>, <<"q7">>]).

t(Qs) ->
    ok = test_with_rk(Qs),
    ok = test_with_header(Qs),
    ok.

test_with_rk(Qs) ->
    test0(fun () ->
                  #'basic.publish'{exchange = <<"e">>, routing_key = rnd()}
          end,
          fun() ->
                  #amqp_msg{props = #'P_basic'{}, payload = <<>>}
          end, [], Qs).

test_with_header(Qs) ->
    test0(fun () ->
                  #'basic.publish'{exchange = <<"e">>}
          end,
          fun() ->
                  H = [{<<"hashme">>, longstr, rnd()}],
                  #amqp_msg{props = #'P_basic'{headers = H}, payload = <<>>}
          end, [{<<"hash-header">>, longstr, <<"hashme">>}], Qs).

rnd() ->
    list_to_binary(integer_to_list(random:uniform(1000000))).

test0(MakeMethod, MakeMsg, DeclareArgs, [Q1, Q2, Q3, Q4] = Queues) ->
    Count = 10000,

    {ok, Conn} = amqp_connection:start(#amqp_params_network{}),
    {ok, Chan} = amqp_connection:open_channel(Conn),
    #'exchange.declare_ok'{} =
        amqp_channel:call(Chan,
                          #'exchange.declare' {
                            exchange = <<"e">>,
                            type = <<"x-consistent-hash">>,
                            auto_delete = true,
                            arguments = DeclareArgs
                           }),
    [#'queue.declare_ok'{} =
         amqp_channel:call(Chan, #'queue.declare' {
                             queue = Q, exclusive = true }) || Q <- Queues],
    [#'queue.bind_ok'{} =
         amqp_channel:call(Chan, #'queue.bind' { queue = Q,
                                                 exchange = <<"e">>,
                                                 routing_key = <<"10">> })
     || Q <- [Q1, Q2]],
    [#'queue.bind_ok'{} =
         amqp_channel:call(Chan, #'queue.bind' { queue = Q,
                                                 exchange = <<"e">>,
                                                 routing_key = <<"20">> })
     || Q <- [Q3, Q4]],
    #'tx.select_ok'{} = amqp_channel:call(Chan, #'tx.select'{}),
    [amqp_channel:call(Chan,
                       MakeMethod(),
                       MakeMsg()) || _ <- lists:duplicate(Count, const)],
    amqp_channel:call(Chan, #'tx.commit'{}),
    Counts =
        [begin
            #'queue.declare_ok'{message_count = M} =
                 amqp_channel:call(Chan, #'queue.declare' {queue     = Q,
                                                           exclusive = true }),
             M
         end || Q <- Queues],
    Count = lists:sum(Counts), %% All messages got routed
    [true = C > 0.01 * Count || C <- Counts], %% We are not *grossly* unfair
    amqp_channel:call(Chan, #'exchange.delete' { exchange = <<"e">> }),
    [amqp_channel:call(Chan, #'queue.delete' { queue = Q }) || Q <- Queues],
    amqp_channel:close(Chan),
    amqp_connection:close(Conn),
    ok.
