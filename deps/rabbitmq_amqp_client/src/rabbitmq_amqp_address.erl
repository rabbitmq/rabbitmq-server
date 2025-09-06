%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2025 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.

-module(rabbitmq_amqp_address).

-export([exchange/1,
         exchange/2,
         queue/1,
         from_map/1,
         to_map/1]).

-type address_map() :: #{queue := unicode:unicode_binary()} |
                       #{exchange := unicode:unicode_binary(),
                         routing_key => unicode:unicode_binary()}.

-spec exchange(unicode:unicode_binary()) ->
    unicode:unicode_binary().
exchange(ExchangeName) ->
    ExchangeNameQuoted = uri_string:quote(ExchangeName),
    <<"/exchanges/", ExchangeNameQuoted/binary>>.

-spec exchange(unicode:unicode_binary(), unicode:unicode_binary()) ->
    unicode:unicode_binary().
exchange(ExchangeName, RoutingKey) ->
    ExchangeNameQuoted = uri_string:quote(ExchangeName),
    RoutingKeyQuoted = uri_string:quote(RoutingKey),
    <<"/exchanges/", ExchangeNameQuoted/binary, "/", RoutingKeyQuoted/binary>>.

-spec queue(unicode:unicode_binary()) ->
    unicode:unicode_binary().
queue(QueueName) ->
    QueueNameQuoted = uri_string:quote(QueueName),
    <<"/queues/", QueueNameQuoted/binary>>.

-spec from_map(address_map()) ->
    unicode:unicode_binary().
from_map(#{exchange := Exchange, routing_key := RoutingKey}) ->
    exchange(Exchange, RoutingKey);
from_map(#{exchange := Exchange}) ->
    exchange(Exchange);
from_map(#{queue := Queue}) ->
    queue(Queue).

-spec to_map(unicode:unicode_binary()) ->
    {ok, address_map()} | error.
to_map(<<"/exchanges/", Rest/binary>>) ->
    case binary:split(Rest, <<"/">>, [global]) of
        [ExchangeQuoted]
          when ExchangeQuoted =/= <<>> ->
            Exchange = uri_string:unquote(ExchangeQuoted),
            {ok, #{exchange => Exchange}};
        [ExchangeQuoted, RoutingKeyQuoted]
          when ExchangeQuoted =/= <<>> ->
            Exchange = uri_string:unquote(ExchangeQuoted),
            RoutingKey = uri_string:unquote(RoutingKeyQuoted),
            {ok, #{exchange => Exchange,
                   routing_key => RoutingKey}};
        _ ->
            error
    end;
to_map(<<"/queues/">>) ->
    error;
to_map(<<"/queues/", QueueQuoted/binary>>) ->
    Queue = uri_string:unquote(QueueQuoted),
    {ok, #{queue => Queue}};
to_map(_) ->
    error.

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
address_test() ->
    M1 = #{queue => <<"my queue">>},
    M2 = #{queue => <<"🥕"/utf8>>},
    M3 = #{exchange => <<"my exchange">>},
    M4 = #{exchange => <<"🥕"/utf8>>},
    M5 = #{exchange => <<"my exchange">>,
           routing_key => <<"my routing key">>},
    M6 = #{exchange => <<"🥕"/utf8>>,
           routing_key => <<"🍰"/utf8>>},
    lists:foreach(fun(Map) ->
                          {ok, Map} = to_map(from_map(Map))
                  end, [M1, M2, M3, M4, M5, M6]),

    error = to_map(<<"/queues/">>),
    error = to_map(<<"/exchanges/">>),
    error = to_map(<<"/exchanges//key">>).
-endif.
