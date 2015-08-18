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
%% The Original Code is RabbitMQ Sharding Plugin
%%
%% The Initial Developer of the Original Code is GoPivotal, Inc.
%% Copyright (c) 2011-2015 GoPivotal, Inc.  All rights reserved.
%%

-module(rabbit_hash_exchange_test).

-include_lib("eunit/include/eunit.hrl").
-include_lib("amqp_client/include/amqp_client.hrl").

routed_to_zero_queue_test() ->
    test0(fun () ->
                  #'basic.publish'{exchange = <<"e">>, routing_key = rnd()}
          end,
          fun() ->
                  #amqp_msg{props = #'P_basic'{}, payload = <<>>}
          end, [], 5, 0).

routed_to_one_queue_test() ->
    test0(fun () ->
                  #'basic.publish'{exchange = <<"e">>, routing_key = rnd()}
          end,
          fun() ->
                  #amqp_msg{props = #'P_basic'{}, payload = <<>>}
          end, [<<"q1">>, <<"q2">>, <<"q3">>], 1, 1).

routed_to_many_queue_test() ->
    test0(fun () ->
                  #'basic.publish'{exchange = <<"e">>, routing_key = rnd()}
          end,
          fun() ->
                  #amqp_msg{props = #'P_basic'{}, payload = <<>>}
          end, [<<"q1">>, <<"q2">>, <<"q3">>], 5, 5).

test0(MakeMethod, MakeMsg, Queues, MsgCount, Count) ->
    {ok, Conn} = amqp_connection:start(#amqp_params_network{}),
    {ok, Chan} = amqp_connection:open_channel(Conn),
    #'exchange.declare_ok'{} =
        amqp_channel:call(Chan,
                          #'exchange.declare' {
                            exchange = <<"e">>,
                            type = <<"x-modulus-hash">>,
                            auto_delete = true
                           }),
    [#'queue.declare_ok'{} =
         amqp_channel:call(Chan, #'queue.declare' {
                             queue = Q, exclusive = true }) || Q <- Queues],
    [#'queue.bind_ok'{} =
         amqp_channel:call(Chan, #'queue.bind' { queue = Q,
                                                 exchange = <<"e">>,
                                                 routing_key = <<"">> })
     || Q <- Queues],

    amqp_channel:call(Chan, #'confirm.select'{}),

    [amqp_channel:call(Chan,
                       MakeMethod(),
                       MakeMsg()) || _ <- lists:duplicate(MsgCount, const)],

    % ensure that the messages have been delivered to the queues before asking
    % for the message count
    amqp_channel:wait_for_confirms_or_die(Chan),

    Counts =
        [begin
             #'queue.declare_ok'{message_count = M} =
                 amqp_channel:call(Chan, #'queue.declare' {queue     = Q,
                                                           exclusive = true }),
             M
         end || Q <- Queues],

    ?assertEqual(Count, lists:sum(Counts)),

    amqp_channel:call(Chan, #'exchange.delete' { exchange = <<"e">> }),
    [amqp_channel:call(Chan, #'queue.delete' { queue = Q }) || Q <- Queues],
    amqp_channel:close(Chan),
    amqp_connection:close(Conn),
    ok.

rnd() ->
    list_to_binary(integer_to_list(random:uniform(1000000))).
