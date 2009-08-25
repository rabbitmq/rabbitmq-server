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
%%   The Original Code is the RabbitMQ Erlang Client.
%%
%%   The Initial Developers of the Original Code are LShift Ltd.,
%%   Cohesive Financial Technologies LLC., and Rabbit Technologies Ltd.
%%
%%   Portions created by LShift Ltd., Cohesive Financial
%%   Technologies LLC., and Rabbit Technologies Ltd. are Copyright (C)
%%   2007 LShift Ltd., Cohesive Financial Technologies LLC., and Rabbit
%%   Technologies Ltd.;
%%
%%   All Rights Reserved.
%%
%%   Contributor(s): Ben Hood <0x6e6562@gmail.com>.
%%
-module(negative_test_util).

-include("amqp_client.hrl").
-include_lib("eunit/include/eunit.hrl").

-compile(export_all).

non_existent_exchange_test(Connection) ->
    X = test_util:uuid(),
    RoutingKey = <<"a">>, 
    Payload = <<"foobar">>,
    Channel = amqp_connection:open_channel(Connection),
    amqp_channel:call(Channel, #'exchange.declare'{exchange = X}),
    %% Deliberately mix up the routingkey and exchange arguments
    Publish = #'basic.publish'{exchange = RoutingKey, routing_key = X},
    amqp_channel:call(Channel, Publish, #amqp_msg{payload = Payload}),
    wait_for_death(Channel),
    ?assertMatch(true, is_process_alive(Connection)),
    amqp_connection:close(Connection).

hard_error_test(Connection) ->
    Channel = amqp_connection:open_channel(Connection),
    ?assertExit(_, amqp_channel:call(Channel, #'basic.qos'{global = true})),
    wait_for_death(Channel),
    wait_for_death(Connection).

wait_for_death(Pid) ->
    Ref = erlang:monitor(process, Pid),
    receive {'DOWN', Ref, process, Pid, _Reason} -> ok
    after 1000 -> exit({timed_out_waiting_for_process_death, Pid})
    end.

non_existent_user_test() ->
    Params = #amqp_params{username = test_util:uuid(),
                          password = test_util:uuid()},
    ?assertError(_, amqp_connection:start_network(Params)).

invalid_password_test() ->
    Params = #amqp_params{username = <<"guest">>,
                          password = test_util:uuid()},
    ?assertError(_, amqp_connection:start_network(Params)).

non_existent_vhost_test() ->
    Params = #amqp_params{virtual_host = test_util:uuid()},
    ?assertError(_, amqp_connection:start_network(Params)).

no_permission_test() ->
    Params = #amqp_params{username = <<"test_user_no_perm">>,
                          password = <<"test_user_no_perm">>},
    ?assertError(_, amqp_connection:start_network(Params)).
