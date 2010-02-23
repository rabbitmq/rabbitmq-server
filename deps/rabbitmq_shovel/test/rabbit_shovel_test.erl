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
%%   The Original Code is RabbitMQ-shovel.
%%
%%   The Initial Developers of the Original Code are LShift Ltd.
%%
%%   Copyright (C) 2010 LShift Ltd.
%%
%%   All Rights Reserved.
%%
%%   Contributor(s): ______________________________________.
%%

-module(rabbit_shovel_test).
-export([test/0]).
-include_lib("amqp_client/include/amqp_client.hrl").

-define(EXCHANGE,    <<"test_exchange">>).
-define(TO_SHOVEL,   <<"to_the_shovel">>).
-define(FROM_SHOVEL, <<"from_the_shovel">>).
-define(UNSHOVELLED, <<"unshovelled">>).
-define(SHOVELLED,   <<"shovelled">>).
-define(TIMEOUT,     1000).

test() ->
    application:set_env(
      rabbit_shovel,
      shovels,
      [{test_shovel,
        [{sources,
          [{broker, "amqp://"},
           {declarations,
            [{'queue.declare',    [exclusive, auto_delete]},
             {'exchange.declare', [{exchange, ?EXCHANGE}, auto_delete]},
             {'queue.bind',       [{queue, <<>>}, {exchange, ?EXCHANGE},
                                   {routing_key, ?TO_SHOVEL}]}
            ]}]},
         {destinations,
          [{broker, "amqp://"}]},
         {queue, <<>>},
         {publish_fields, [{exchange, ?EXCHANGE}, {routing_key, ?FROM_SHOVEL}]},
         {publish_properties, [{content_type, ?SHOVELLED}]}
        ]}],
      infinity),
    ok = application:start(rabbit_shovel),

    Conn = amqp_connection:start_network(),
    Chan = amqp_connection:open_channel(Conn),

    #'queue.declare_ok'{ queue = Q } =
        amqp_channel:call(Chan, #'queue.declare' { exclusive = true }),
    #'queue.bind_ok'{} =
        amqp_channel:call(Chan, #'queue.bind' { queue = Q, exchange = ?EXCHANGE,
                                                routing_key = ?FROM_SHOVEL }),
    #'queue.bind_ok'{} =
        amqp_channel:call(Chan, #'queue.bind' { queue = Q, exchange = ?EXCHANGE,
                                                routing_key = ?TO_SHOVEL }),

    #'basic.consume_ok'{ consumer_tag = CTag } =
        amqp_channel:subscribe(Chan,
                               #'basic.consume' { queue = Q, exclusive = true },
                               self()),
    receive
        #'basic.consume_ok'{ consumer_tag = CTag } -> ok
    after ?TIMEOUT -> throw(timeout)
    end,

    ok = amqp_channel:call(Chan,
                           #'basic.publish' { exchange    = ?EXCHANGE,
                                              routing_key = ?TO_SHOVEL },
                           #amqp_msg { payload = <<42>>,
                                       props   = #'P_basic' {
                                         delivery_mode = 2,
                                         content_type  = ?UNSHOVELLED }
                                     }),

    receive
        {#'basic.deliver' { consumer_tag = CTag, delivery_tag = AckTag,
                            routing_key = ?FROM_SHOVEL },
         #amqp_msg { payload = <<42>>,
                     props   = #'P_basic' { delivery_mode = 2,
                                            content_type  = ?SHOVELLED }
                   }} ->
            ok = amqp_channel:call(Chan, #'basic.ack'{ delivery_tag = AckTag })
    after ?TIMEOUT -> throw(timeout)
    end,

    [{test_shovel,
      {running, {source, _Source}, {destination, _Destination}}, _Time}] =
        rabbit_shovel_status:status(),

    receive
        {#'basic.deliver' { consumer_tag = CTag, delivery_tag = AckTag1,
                            routing_key = ?TO_SHOVEL },
         #amqp_msg { payload = <<42>>,
                     props   = #'P_basic' { delivery_mode = 2,
                                            content_type  = ?UNSHOVELLED }
                   }} ->
            ok = amqp_channel:call(Chan, #'basic.ack'{ delivery_tag = AckTag1 })
    after ?TIMEOUT -> throw(timeout)
    end,

    amqp_channel:close(Chan),
    amqp_connection:close(Conn),

    ok = application:stop(rabbit_shovel),
    passed.
