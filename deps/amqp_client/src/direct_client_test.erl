-module(direct_client_test).

-define(RPC_TIMEOUT, 10000).
-define(RPC_SLEEP, 500).

-export([test_wrapper/1, test_coverage/1]).

-include_lib("eunit/include/eunit.hrl").

basic_get_test() ->
    Connection = amqp_connection:start("guest", "guest"),
    test_util:basic_get_test(Connection).

basic_consume_test() ->
    Connection = amqp_connection:start("guest", "guest"),
    test_util:basic_consume_test(Connection).

lifecycle_test() ->
    Connection = amqp_connection:start("guest", "guest"),
    test_util:lifecycle_test(Connection).

basic_ack_test() ->
    Connection = amqp_connection:start("guest", "guest"),
    test_util:basic_ack_test(Connection).

test_wrapper(NodeName) ->
    Node = rabbit_misc:localnode(list_to_atom(NodeName)),
    rabbit_multi:get_pid(Node, 0),
    test().

test_coverage(NodeName) ->
    Node = rabbit_misc:localnode(list_to_atom(NodeName)),
    rabbit_multi:get_pid(Node, 0),
    rabbit_misc:enable_cover(),
    test(),
    rabbit_misc:report_cover().

