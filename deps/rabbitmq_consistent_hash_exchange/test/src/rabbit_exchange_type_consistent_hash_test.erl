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
%% The Initial Developer of the Original Code is VMware, Inc.
%% Copyright (c) 2011-2011 VMware, Inc.  All rights reserved.
%%

-module(rabbit_exchange_type_consistent_hash_test).
-export([test/0]).
-include_lib("amqp_client/include/amqp_client.hrl").

%% Because the routing is probabilistic, we can't really test a great
%% deal here.

test() ->
    Count = 10000,

    {ok, Conn} = amqp_connection:start(#amqp_params_network{}),
    {ok, Chan} = amqp_connection:open_channel(Conn),
    Queues = [<<"q0">>, <<"q1">>, <<"q2">>, <<"q3">>],
    #'exchange.declare_ok'{} =
        amqp_channel:call(Chan,
                          #'exchange.declare' {
                            exchange = <<"e">>,
                            type = <<"x-consistent-hash">>,
                            auto_delete = true
                           }),
    [#'queue.declare_ok'{} =
         amqp_channel:call(Chan, #'queue.declare' {
                             queue = Q, exclusive = true }) || Q <- Queues],
    [#'queue.bind_ok'{} =
         amqp_channel:call(Chan, #'queue.bind' { queue = Q,
                                                 exchange = <<"e">>,
                                                 routing_key = <<"10">> })
     || Q <- [<<"q0">>, <<"q1">>]],
    [#'queue.bind_ok'{} =
         amqp_channel:call(Chan, #'queue.bind' { queue = Q,
                                                 exchange = <<"e">>,
                                                 routing_key = <<"20">> })
     || Q <- [<<"q2">>, <<"q3">>]],
    #'tx.select_ok'{} = amqp_channel:call(Chan, #'tx.select'{}),
    Msg = #amqp_msg { props = #'P_basic'{}, payload = <<>> },
    [amqp_channel:call(Chan,
                       #'basic.publish'{
                         exchange = <<"e">>,
                         routing_key = list_to_binary(
                                         integer_to_list(
                                           random:uniform(1000000)))
                       }, Msg) || _ <- lists:duplicate(Count, const)],
    amqp_channel:call(Chan, #'tx.commit'{}),
    Decls =
        [amqp_channel:call(Chan, #'queue.declare' {
                             queue = Q, exclusive = true }) || Q <- Queues],
    Count = lists:sum([Len || #'queue.declare_ok' { message_count = Len } <-
                                  Decls]), %% ASSERTION
    amqp_channel:call(Chan, #'exchange.delete' { exchange = <<"e">> }),
    [amqp_channel:call(Chan, #'queue.delete' { queue = Q }) || Q <- Queues],
    amqp_channel:close(Chan),
    amqp_connection:close(Conn),
    ok.
