-module(network_integration_test).

-include_lib("eunit/include/eunit.hrl").

rpc_client_test() ->
    Connection = amqp_connection:start("guest", "guest", "localhost"),
    integration_test_util:rpc_client_test(Connection).
