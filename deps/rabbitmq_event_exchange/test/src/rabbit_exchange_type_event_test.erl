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
%% Copyright (c) 2007-2016 Pivotal Software, Inc.  All rights reserved.
%%

-module(rabbit_exchange_type_event_test).
-include_lib("eunit/include/eunit.hrl").

-include_lib("amqp_client/include/amqp_client.hrl").

%% Only really tests that we're not completely broken.
queue_created_test() ->
    Now = time_compat:os_system_time(seconds),
    {ok, Conn} = amqp_connection:start(#amqp_params_network{}),
    {ok, Ch} = amqp_connection:open_channel(Conn),
    #'queue.declare_ok'{queue = Q} =
        amqp_channel:call(Ch, #'queue.declare'{exclusive = true}),
    amqp_channel:call(Ch, #'queue.bind'{queue       = Q,
                                        exchange    = <<"amq.rabbitmq.event">>,
                                        routing_key = <<"queue.*">>}),
    amqp_channel:subscribe(Ch, #'basic.consume'{queue = Q, no_ack = true},
                           self()),
    receive
        #'basic.consume_ok'{} -> ok
    end,

    #'queue.declare_ok'{queue = Q2} =
        amqp_channel:call(Ch, #'queue.declare'{exclusive = true}),

    receive
        {#'basic.deliver'{routing_key = Key},
         #amqp_msg{props = #'P_basic'{headers = Headers, timestamp = TS}}} ->
            %% timestamp is within the last 5 seconds
            ?assert((TS - Now) =< 5),
            ?assertMatch(<<"queue.created">>, Key),
            ?assertMatch({longstr, Q2}, rabbit_misc:table_lookup(
                                          Headers, <<"name">>))
    end,

    amqp_connection:close(Conn),
    ok.


authentication_test() ->
    {ok, Conn} = amqp_connection:start(#amqp_params_network{}),
    {ok, Ch}   = amqp_connection:open_channel(Conn),

    #'queue.declare_ok'{queue = Q} =
        amqp_channel:call(Ch, #'queue.declare'{exclusive = true}),
    amqp_channel:call(Ch, #'queue.bind'{queue       = Q,
                                        exchange    = <<"amq.rabbitmq.event">>,
                                        routing_key = <<"user.#">>}),
    amqp_channel:call(Ch, #'queue.bind'{queue       = Q,
                                        exchange    = <<"amq.rabbitmq.event">>,
                                        routing_key = <<"connection.#">>}),

    {ok, Conn2} = amqp_connection:start(#amqp_params_network{}),
    amqp_channel:subscribe(Ch, #'basic.consume'{queue = Q, no_ack = true},
                           self()),
    receive
        #'basic.consume_ok'{} -> ok
    end,

    receive
        {#'basic.deliver'{routing_key = Key},
         #amqp_msg{props = #'P_basic'{headers = Headers}}} ->
            ?assertMatch(<<"user.authentication.success">>, Key),
            ?assertMatch(undefined,             rabbit_misc:table_lookup(
                                                  Headers, <<"vhost">>)),
            ?assertMatch({longstr, _PeerHost},  rabbit_misc:table_lookup(
                                                  Headers, <<"peer_host">>)),
            ?assertMatch({bool, false},         rabbit_misc:table_lookup(
                                                  Headers, <<"ssl">>))
    end,

    amqp_connection:close(Conn),
    amqp_connection:close(Conn2),
    ok.
