-module(direct_integration_test).

-export([start_rabbit_management/0]).

start_rabbit_management() ->
    Connection = amqp_connection:start("guest", "guest"),
    ChannelPid = integration_test_util:start_rabbit_management(Connection).
