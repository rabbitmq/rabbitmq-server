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
    test_util:wait_for_death(Channel),
    ?assertMatch(true, is_process_alive(Connection)),
    amqp_connection:close(Connection).

bogus_rpc_test(Connection) ->
    X = test_util:uuid(),
    Q = test_util:uuid(),
    R = test_util:uuid(),
    Channel = amqp_connection:open_channel(Connection),
    amqp_channel:call(Channel, #'exchange.declare'{exchange = X}),
    %% Deliberately bind to a non-existent queue
    Bind = #'queue.bind'{exchange = X, queue = Q, routing_key = R},
    try amqp_channel:call(Channel, Bind) of
        _ -> exit(expected_to_exit)
    catch
        exit:{{server_initiated_close, Code, _},_} ->
            ?assertMatch(?NOT_FOUND, Code)
    end,
    test_util:wait_for_death(Channel),
    ?assertMatch(true, is_process_alive(Connection)),
    amqp_connection:close(Connection).

hard_error_test(Connection) ->
    Channel = amqp_connection:open_channel(Connection),
    Qos = #'basic.qos'{global = true},
    try amqp_channel:call(Channel, Qos) of
        _ -> exit(expected_to_exit)
    catch
        exit:{{server_initiated_close, Code, _Text}, _} ->
            ?assertMatch(?NOT_IMPLEMENTED, Code)
    end,
    test_util:wait_for_death(Channel),
    test_util:wait_for_death(Connection).

%% An error in a channel should result in the death of the entire connection.
%% The death of the channel is caused by an error in generating the frames
%% (writer dies) - only in the network case
channel_writer_death_test(Connection) ->
    Channel = amqp_connection:open_channel(Connection),
    Publish = #'basic.publish'{routing_key = <<>>, exchange = <<>>},
    Message = #amqp_msg{props = <<>>, payload = <<>>},
    ok = amqp_channel:call(Channel, Publish, Message),
    timer:sleep(300),
    ?assertNot(is_process_alive(Channel)),
    ?assertNot(is_process_alive(Connection)),
    ok.

%% An error in the channel process should result in the death of the entire
%% connection. The death of the channel is caused by making a call with an
%% invalid message to the channel process
channel_death_test(Connection) ->
    Channel = amqp_connection:open_channel(Connection),
    ?assertExit(_, amqp_channel:call(Channel, bogus_message)),
    timer:sleep(300),
    ?assertNot(is_process_alive(Channel)),
    ?assertNot(is_process_alive(Connection)),
    ok.

%% Attempting to send a shortstr longer than 255 bytes in a property field
%% should fail - this only applies to the network case
shortstr_overflow_property_test(Connection) ->
    Channel = amqp_connection:open_channel(Connection),
    SentString = << <<"k">> || _ <- lists:seq(1, 340)>>,
    Q = test_util:uuid(), X = test_util:uuid(), Key = test_util:uuid(),
    Payload = <<"foobar">>,
    test_util:setup_exchange(Channel, Q, X, Key),
    Publish = #'basic.publish'{exchange = X, routing_key = Key},
    PBasic = #'P_basic'{content_type = SentString},
    AmqpMsg = #amqp_msg{payload = Payload, props = PBasic},
    amqp_channel:call(Channel, Publish, AmqpMsg),
    timer:sleep(300),
    ?assertNot(is_process_alive(Channel)),
    ?assertNot(is_process_alive(Connection)),
    ok.

%% Attempting to send a shortstr longer than 255 bytes in a method's field
%% should fail - this only applies to the network case
shortstr_overflow_field_test(Connection) ->
    Channel = amqp_connection:open_channel(Connection),
    SentString = << <<"k">> || _ <- lists:seq(1, 340)>>,
    Q = test_util:uuid(), X = test_util:uuid(), Key = test_util:uuid(),
    test_util:setup_exchange(Channel, Q, X, Key),
    ?assertExit(_, amqp_channel:subscribe(
                       Channel, #'basic.consume'{queue = Q, no_ack = true,
                                                  consumer_tag = SentString},
                       self())),
    timer:sleep(300),
    ?assertNot(is_process_alive(Channel)),
    ?assertNot(is_process_alive(Connection)),
    ok.

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
