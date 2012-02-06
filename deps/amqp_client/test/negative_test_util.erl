%% The contents of this file are subject to the Mozilla Public License
%% Version 1.1 (the "License"); you may not use this file except in
%% compliance with the License. You may obtain a copy of the License at
%% http://www.mozilla.org/MPL/
%%
%% Software distributed under the License is distributed on an "AS IS"
%% basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See the
%% License for the specific language governing rights and limitations
%% under the License.
%%
%% The Original Code is RabbitMQ.
%%
%% The Initial Developer of the Original Code is VMware, Inc.
%% Copyright (c) 2007-2012 VMware, Inc.  All rights reserved.
%%

-module(negative_test_util).

-include("amqp_client.hrl").
-include_lib("eunit/include/eunit.hrl").

-compile(export_all).

non_existent_exchange_test() ->
    {ok, Connection} = test_util:new_connection(),
    X = test_util:uuid(),
    RoutingKey = <<"a">>,
    Payload = <<"foobar">>,
    {ok, Channel} = amqp_connection:open_channel(Connection),
    {ok, OtherChannel} = amqp_connection:open_channel(Connection),
    amqp_channel:call(Channel, #'exchange.declare'{exchange = X}),

    %% Deliberately mix up the routingkey and exchange arguments
    Publish = #'basic.publish'{exchange = RoutingKey, routing_key = X},
    amqp_channel:call(Channel, Publish, #amqp_msg{payload = Payload}),
    test_util:wait_for_death(Channel),

    %% Make sure Connection and OtherChannel still serve us and are not dead
    {ok, _} = amqp_connection:open_channel(Connection),
    #'exchange.declare_ok'{} =
        amqp_channel:call(OtherChannel,
                          #'exchange.declare'{exchange = test_util:uuid()}),
    amqp_connection:close(Connection).

bogus_rpc_test() ->
    {ok, Connection} = test_util:new_connection(),
    X = test_util:uuid(),
    Q = test_util:uuid(),
    R = test_util:uuid(),
    {ok, Channel} = amqp_connection:open_channel(Connection),
    amqp_channel:call(Channel, #'exchange.declare'{exchange = X}),
    %% Deliberately bind to a non-existent queue
    Bind = #'queue.bind'{exchange = X, queue = Q, routing_key = R},
    try amqp_channel:call(Channel, Bind) of
        _ -> exit(expected_to_exit)
    catch
        exit:{{shutdown, {server_initiated_close, Code, _}},_} ->
            ?assertMatch(?NOT_FOUND, Code)
    end,
    test_util:wait_for_death(Channel),
    ?assertMatch(true, is_process_alive(Connection)),
    amqp_connection:close(Connection).

hard_error_test() ->
    {ok, Connection} = test_util:new_connection(),
    {ok, Channel} = amqp_connection:open_channel(Connection),
    {ok, OtherChannel} = amqp_connection:open_channel(Connection),
    OtherChannelMonitor = erlang:monitor(process, OtherChannel),
    Qos = #'basic.qos'{global = true},
    try amqp_channel:call(Channel, Qos) of
        _ -> exit(expected_to_exit)
    catch
        exit:{{shutdown, {connection_closing,
                          {server_initiated_close, ?NOT_IMPLEMENTED, _}}}, _} ->
            ok
    end,
    receive
        {'DOWN', OtherChannelMonitor, process, OtherChannel, OtherExit} ->
            ?assertMatch({shutdown,
                          {connection_closing,
                           {server_initiated_close, ?NOT_IMPLEMENTED, _}}},
                         OtherExit)
    end,
    test_util:wait_for_death(Channel),
    test_util:wait_for_death(Connection).

%% An error in a channel should result in the death of the entire connection.
%% The death of the channel is caused by an error in generating the frames
%% (writer dies) - only in the network case
channel_writer_death_test() ->
    {ok, Connection} = test_util:new_connection(just_network),
    {ok, Channel} = amqp_connection:open_channel(Connection),
    Publish = #'basic.publish'{routing_key = <<>>, exchange = <<>>},
    Message = #amqp_msg{props = <<>>, payload = <<>>},
    ?assertExit(_, amqp_channel:call(Channel, Publish, Message)),
    test_util:wait_for_death(Channel),
    test_util:wait_for_death(Connection),
    ok.

%% An error in the channel process should result in the death of the entire
%% connection. The death of the channel is caused by making a call with an
%% invalid message to the channel process
channel_death_test() ->
    {ok, Connection} = test_util:new_connection(),
    {ok, Channel} = amqp_connection:open_channel(Connection),
    ?assertExit(_, amqp_channel:call(Channel, bogus_message)),
    test_util:wait_for_death(Channel),
    test_util:wait_for_death(Connection),
    ok.

%% Attempting to send a shortstr longer than 255 bytes in a property field
%% should fail - this only applies to the network case
shortstr_overflow_property_test() ->
    {ok, Connection} = test_util:new_connection(just_network),
    {ok, Channel} = amqp_connection:open_channel(Connection),
    SentString = << <<"k">> || _ <- lists:seq(1, 340)>>,
    Q = test_util:uuid(), X = test_util:uuid(), Key = test_util:uuid(),
    Payload = <<"foobar">>,
    test_util:setup_exchange(Channel, Q, X, Key),
    Publish = #'basic.publish'{exchange = X, routing_key = Key},
    PBasic = #'P_basic'{content_type = SentString},
    AmqpMsg = #amqp_msg{payload = Payload, props = PBasic},
    ?assertExit(_, amqp_channel:call(Channel, Publish, AmqpMsg)),
    test_util:wait_for_death(Channel),
    test_util:wait_for_death(Connection),
    ok.

%% Attempting to send a shortstr longer than 255 bytes in a method's field
%% should fail - this only applies to the network case
shortstr_overflow_field_test() ->
    {ok, Connection} = test_util:new_connection(just_network),
    {ok, Channel} = amqp_connection:open_channel(Connection),
    SentString = << <<"k">> || _ <- lists:seq(1, 340)>>,
    Q = test_util:uuid(), X = test_util:uuid(), Key = test_util:uuid(),
    test_util:setup_exchange(Channel, Q, X, Key),
    ?assertExit(_, amqp_channel:call(
                       Channel, #'basic.consume'{queue = Q,
                                                 no_ack = true,
                                                 consumer_tag = SentString})),
    test_util:wait_for_death(Channel),
    test_util:wait_for_death(Connection),
    ok.

%% Simulates a #'connection.open'{} method received on non-zero channel. The
%% connection is expected to send a '#connection.close{}' to the server with
%% reply code command_invalid
command_invalid_over_channel_test() ->
    {ok, Connection} = test_util:new_connection(),
    {ok, Channel} = amqp_connection:open_channel(Connection),
    MonitorRef = erlang:monitor(process, Connection),
    case amqp_connection:info(Connection, [type]) of
        [{type, direct}]  -> Channel ! {send_command, #'connection.open'{}};
        [{type, network}] -> gen_server:cast(Channel,
                                 {method, #'connection.open'{}, none, noflow})
    end,
    assert_down_with_error(MonitorRef, command_invalid),
    ?assertNot(is_process_alive(Channel)),
    ok.

%% Simulates a #'basic.ack'{} method received on channel zero. The connection
%% is expected to send a '#connection.close{}' to the server with reply code
%% command_invalid - this only applies to the network case
command_invalid_over_channel0_test() ->
    {ok, Connection} = test_util:new_connection(just_network),
    gen_server:cast(Connection, {method, #'basic.ack'{}, none, noflow}),
    MonitorRef = erlang:monitor(process, Connection),
    assert_down_with_error(MonitorRef, command_invalid),
    ok.

assert_down_with_error(MonitorRef, CodeAtom) ->
    receive
        {'DOWN', MonitorRef, process, _, Reason} ->
            {shutdown, {server_misbehaved, Code, _}} = Reason,
            ?assertMatch(CodeAtom, ?PROTOCOL:amqp_exception(Code))
    after 2000 ->
        exit(did_not_die)
    end.

non_existent_user_test() ->
    Params = [{username, test_util:uuid()}, {password, test_util:uuid()}],
    ?assertMatch({error, auth_failure}, test_util:new_connection(Params)).

invalid_password_test() ->
    Params = [{username, <<"guest">>}, {password, test_util:uuid()}],
    ?assertMatch({error, auth_failure},
                 test_util:new_connection(just_network, Params)).

non_existent_vhost_test() ->
    Params = [{virtual_host, test_util:uuid()}],
    ?assertMatch({error, access_refused}, test_util:new_connection(Params)).

no_permission_test() ->
    Params = [{username, <<"test_user_no_perm">>},
              {password, <<"test_user_no_perm">>}],
    ?assertMatch({error, access_refused}, test_util:new_connection(Params)).
