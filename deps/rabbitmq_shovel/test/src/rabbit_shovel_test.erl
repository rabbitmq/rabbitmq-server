%%  The contents of this file are subject to the Mozilla Public License
%%  Version 1.1 (the "License"); you may not use this file except in
%%  compliance with the License. You may obtain a copy of the License
%%  at http://www.mozilla.org/MPL/
%%
%%  Software distributed under the License is distributed on an "AS IS"
%%  basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See
%%  the License for the specific language governing rights and
%%  limitations under the License.
%%
%%  The Original Code is RabbitMQ.
%%
%%  The Initial Developer of the Original Code is GoPivotal, Inc.
%%  Copyright (c) 2007-2013 GoPivotal, Inc.  All rights reserved.
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
    %% it may already be running. Stop if possible
    application:stop(rabbitmq_shovel),

    %% shovel can be started with zero shovels configured
    ok = application:start(rabbitmq_shovel),
    ok = application:stop(rabbitmq_shovel),

    %% various ways of breaking the config
    require_list_of_shovel_configurations =
        test_broken_shovel_configs(invalid_config),

    require_list_of_shovel_configurations =
        test_broken_shovel_configs([{test_shovel, invalid_shovel_config}]),

    Config = [{sources, [{broker, "amqp://"}]},
              {destinations, [{broker, "amqp://"}]},
              {queue, <<"">>}],

    {duplicate_shovel_definition, test_shovel} =
        test_broken_shovel_configs(
          [{test_shovel, Config}, {test_shovel, Config}]),

    {invalid_parameters, [{invalid, invalid, invalid}]} =
        test_broken_shovel_config([{invalid, invalid, invalid} | Config]),

    {duplicate_parameters, [queue]} =
        test_broken_shovel_config([{queue, <<"">>} | Config]),

    {missing_parameters, Missing} =
        test_broken_shovel_config([]),
    [destinations, queue, sources] = lists:sort(Missing),

    {unrecognised_parameters, [invalid]} =
        test_broken_shovel_config([{invalid, invalid} | Config]),

    {require_list, invalid} =
        test_broken_shovel_sources(invalid),

    {missing_endpoint_parameter, broker_or_brokers} =
        test_broken_shovel_sources([]),

    {expected_list, brokers, invalid} =
        test_broken_shovel_sources([{brokers, invalid}]),

    {expected_string_uri, 42} =
        test_broken_shovel_sources([{brokers, [42]}]),

    {{unexpected_uri_scheme, "invalid"}, "invalid://"} =
        test_broken_shovel_sources([{broker, "invalid://"}]),

    {{unable_to_parse_uri, no_scheme}, "invalid"} =
        test_broken_shovel_sources([{broker, "invalid"}]),

    {expected_list,declarations, invalid} =
        test_broken_shovel_sources([{broker, "amqp://"},
                                    {declarations, invalid}]),
    {unknown_method_name, 42} =
        test_broken_shovel_sources([{broker, "amqp://"},
                                    {declarations, [42]}]),

    {expected_method_field_list, 'queue.declare', 42} =
        test_broken_shovel_sources([{broker, "amqp://"},
                                    {declarations, [{'queue.declare', 42}]}]),

    {unknown_fields, 'queue.declare', [invalid]} =
        test_broken_shovel_sources(
          [{broker, "amqp://"},
           {declarations, [{'queue.declare', [invalid]}]}]),

    {{invalid_amqp_params_parameter, heartbeat, "text",
      [{"heartbeat", "text"}], {not_an_integer, "text"}}, _} =
        test_broken_shovel_sources(
          [{broker, "amqp://localhost/?heartbeat=text"}]),

    {{invalid_amqp_params_parameter, username, "text",
      [{"username", "text"}],
      {parameter_unconfigurable_in_query, username, "text"}}, _} =
        test_broken_shovel_sources([{broker, "amqp://?username=text"}]),

    {invalid_parameter_value, prefetch_count,
     {require_non_negative_integer, invalid}} =
        test_broken_shovel_config([{prefetch_count, invalid} | Config]),

    {invalid_parameter_value, ack_mode,
     {ack_mode_value_requires_one_of,
      {no_ack, on_publish, on_confirm}, invalid}} =
        test_broken_shovel_config([{ack_mode, invalid} | Config]),

    {invalid_parameter_value, queue,
     {require_binary, invalid}} =
        test_broken_shovel_config([{sources, [{broker, "amqp://"}]},
                                   {destinations, [{broker, "amqp://"}]},
                                   {queue, invalid}]),

    {invalid_parameter_value, publish_properties,
     {require_list, invalid}} =
        test_broken_shovel_config([{publish_properties, invalid} | Config]),

    {invalid_parameter_value, publish_properties,
     {unexpected_fields, [invalid], _}} =
        test_broken_shovel_config([{publish_properties, [invalid]} | Config]),

    {{invalid_ssl_parameter, fail_if_no_peer_cert, "42", _,
      {require_boolean, '42'}}, _} =
        test_broken_shovel_sources([{broker, "amqps://username:password@host:5673/vhost?cacertfile=/path/to/cacert.pem&certfile=/path/to/certfile.pem&keyfile=/path/to/keyfile.pem&verify=verify_peer&fail_if_no_peer_cert=42"}]),

    %% a working config
    application:set_env(
      rabbitmq_shovel,
      shovels,
      [{test_shovel,
        [{sources,
          [{broker, "amqp:///%2f?heartbeat=5"},
           {declarations,
            [{'queue.declare',    [exclusive, auto_delete]},
             {'exchange.declare', [{exchange, ?EXCHANGE}, auto_delete]},
             {'queue.bind',       [{queue, <<>>}, {exchange, ?EXCHANGE},
                                   {routing_key, ?TO_SHOVEL}]}
            ]}]},
         {destinations,
          [{broker, "amqp:///%2f"}]},
         {queue, <<>>},
         {ack_mode, on_confirm},
         {publish_fields, [{exchange, ?EXCHANGE}, {routing_key, ?FROM_SHOVEL}]},
         {publish_properties, [{content_type, ?SHOVELLED}]}
        ]}],
      infinity),

    ok = application:start(rabbitmq_shovel),

    {ok, Conn} = amqp_connection:start(#amqp_params_network{}),
    {ok, Chan} = amqp_connection:open_channel(Conn),

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
    after ?TIMEOUT -> throw(timeout_waiting_for_consume_ok)
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
    after ?TIMEOUT -> throw(timeout_waiting_for_deliver1)
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
    after ?TIMEOUT -> throw(timeout_waiting_for_deliver2)
    end,

    amqp_channel:close(Chan),
    amqp_connection:close(Conn),

    ok = application:stop(rabbitmq_shovel),
    ok.

test_broken_shovel_configs(Configs) ->
    application:set_env(rabbitmq_shovel, shovels, Configs),
    {error, {Error, _}} = application:start(rabbitmq_shovel),
    Error.

test_broken_shovel_config(Config) ->
    {invalid_shovel_configuration, test_shovel, Error} =
        test_broken_shovel_configs([{test_shovel, Config}]),
    Error.

test_broken_shovel_sources(Sources) ->
    {invalid_parameter_value, sources, Error} =
        test_broken_shovel_config([{sources, Sources},
                                   {destinations, [{broker, "amqp://"}]},
                                   {queue, <<"">>}]),
    Error.
