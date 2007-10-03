-module(amqp_network_client_test).

-include_lib("eunit/include/eunit.hrl").

basic_get_test() ->
    Connection = amqp_connection:start("guest", "guest", "localhost"),
    amqp_test_util:basic_get_test(Connection).

basic_consume_test() ->
    Connection = amqp_connection:start("guest", "guest", "localhost"),
    amqp_test_util:basic_consume_test(Connection).

lifecycle_test() ->
    Connection = amqp_connection:start("guest", "guest", "localhost"),
    amqp_test_util:lifecycle_test(Connection).

basic_ack_test() ->
    Connection = amqp_connection:start("guest", "guest", "localhost"),
    amqp_test_util:basic_ack_test(Connection).
