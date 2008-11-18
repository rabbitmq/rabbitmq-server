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

-module(test_util).

-include_lib("rabbitmq_server/include/rabbit.hrl").
-include_lib("rabbitmq_server/include/rabbit_framing.hrl").
-include_lib("eunit/include/eunit.hrl").
-include("amqp_client.hrl").

-compile([export_all]).

-record(publish,{q, x, routing_key, bind_key, payload,
                 mandatory = false, immediate = false}).

-define(Latch, 100).
-define(Wait, 200).

%%%%
%
% This is an example of how the client interaction should work
%
%   Connection = amqp_connection:start(User, Password, Host),
%   Channel = amqp_connection:open_channel(Connection),
%   %%...do something useful
%   ChannelClose = #'channel.close'{ %% set the appropriate fields },
%   amqp_channel:call(Channel, ChannelClose),
%   ConnectionClose = #'connection.close'{ %% set the appropriate fields },
%   amqp_connection:close(Connection, ConnectionClose).
%

lifecycle_test(Connection) ->
    X = <<"x">>,
    Channel = lib_amqp:start_channel(Connection),
    lib_amqp:declare_exchange(Channel, X, <<"topic">>),
    Parent = self(),
    [spawn(fun() -> queue_exchange_binding(Channel, X, Parent, Tag) end) || Tag <- lists:seq(1,?Latch)],
    latch_loop(?Latch),
    lib_amqp:delete_exchange(Channel, X),
    lib_amqp:teardown(Connection, Channel),
    ok.

queue_exchange_binding(Channel, X, Parent, Tag) ->
    receive
        nothing -> ok
    after (?Latch - Tag rem 7) * 10 ->
        ok
    end,
    Q = <<"a.b.c",Tag:32>>,
    Binding = <<"a.b.c.*">>,
    RoutingKey = <<"a.b.c.d">>,
    Payload = <<"foobar">>,
    Q1 = lib_amqp:declare_queue(Channel, Q),
    ?assertMatch(Q, Q1),
    lib_amqp:bind_queue(Channel, X, Q, Binding),
    lib_amqp:delete_queue(Channel, Q),
    Parent ! finished.

channel_lifecycle_test(Connection) ->
    Channel = lib_amqp:start_channel(Connection),
    lib_amqp:close_channel(Channel),
    Channel2 = lib_amqp:start_channel(Connection),
    lib_amqp:teardown(Connection, Channel2),
    ok.

% This is designed to exercize the internal queuing mechanism
% to ensure that commands are properly serialized
command_serialization_test(Connection) ->
    Channel = lib_amqp:start_channel(Connection),
    Parent = self(),
    [spawn(fun() -> 
                Q = uuid(),
                Q1 = lib_amqp:declare_queue(Channel, Q),
                ?assertMatch(Q, Q1),     
                Parent ! finished
           end) || Tag <- lists:seq(1,?Latch)],
    latch_loop(?Latch),
    lib_amqp:teardown(Connection, Channel).

queue_unbind_test(Connection) ->
    X = <<"eggs">>, Q = <<"foobar">>, Key = <<"quay">>,
    Payload = <<"foobar">>,
    Channel = lib_amqp:start_channel(Connection),
    lib_amqp:declare_exchange(Channel, X),
    lib_amqp:declare_queue(Channel, Q),
    lib_amqp:bind_queue(Channel, X, Q, Key),
    lib_amqp:publish(Channel, X, Key, Payload),
    get_and_assert_equals(Channel, Q, Payload),
    lib_amqp:unbind_queue(Channel, X, Q, Key),
    lib_amqp:publish(Channel, X, Key, Payload),
    get_and_assert_empty(Channel, Q),
    lib_amqp:teardown(Connection, Channel).

get_and_assert_empty(Channel, Q) ->
    BasicGetEmpty = lib_amqp:get(Channel, Q, false),
    ?assertMatch('basic.get_empty', BasicGetEmpty).
    
get_and_assert_equals(Channel, Q, Payload) ->
    Content = lib_amqp:get(Channel, Q),
    #content{class_id = ClassId,
             properties = Properties,
             properties_bin = PropertiesBin,
             payload_fragments_rev = PayloadFragments} = Content,
    ?assertMatch([Payload], PayloadFragments).

basic_get_test(Connection) ->
    Channel = lib_amqp:start_channel(Connection),
    {ok, Q} = setup_publish(Channel),
    % TODO: This could be refactored to use get_and_assert_equals,
    % get_and_assert_empty .... would require another bug though :-)
    Content = lib_amqp:get(Channel, Q),
    #content{class_id = ClassId,
             properties = Properties,
             properties_bin = PropertiesBin,
             payload_fragments_rev = PayloadFragments} = Content,
    ?assertMatch([<<"foobar">>], PayloadFragments),
    BasicGetEmpty = lib_amqp:get(Channel, Q, false),
    ?assertMatch('basic.get_empty', BasicGetEmpty),
    lib_amqp:teardown(Connection, Channel).

basic_return_test(Connection) ->
    X = uuid(),
    Q = uuid(),
    Key = uuid(),
    Payload = <<"qwerty">>,
    Channel = lib_amqp:start_channel(Connection),
    amqp_channel:register_return_handler(Channel, self()),
    lib_amqp:declare_exchange(Channel, X),
    lib_amqp:declare_queue(Channel, Q),
    lib_amqp:publish(Channel, X, Key, Payload, true),
    timer:sleep(200),
    receive
        {BasicReturn = #'basic.return'{}, Content} ->
            #'basic.return'{reply_code = ReplyCode,
                            reply_text = ReplyText,
                            exchange = X,
                            routing_key = RoutingKey} = BasicReturn,
            ?assertMatch(<<"unroutable">>, ReplyText),
            #content{class_id = ClassId,
                     properties = Props,
                     properties_bin = PropsBin,
                     payload_fragments_rev = Payload2} = Content,
            ?assertMatch([Payload], Payload2);
        WhatsThis ->
            %% TODO investigate where this comes from
            io:format(">>>Rec'd ~p/~p~n",[WhatsThis])
    after 2000 ->
        exit(no_return_received)
    end.

basic_ack_test(Connection) ->
    Channel = lib_amqp:start_channel(Connection),
    {ok, Q} = setup_publish(Channel),
    {DeliveryTag, Content} = lib_amqp:get(Channel, Q, false),
    lib_amqp:ack(Channel, DeliveryTag),
    lib_amqp:teardown(Connection, Channel).

basic_consume_test(Connection) ->
    Channel = lib_amqp:start_channel(Connection),
    {ok, Q} = setup_publish(Channel),
    Parent = self(),
    [spawn(fun() -> consume_loop(Channel, Q, Parent, <<Tag:32>>) end) || Tag <- lists:seq(1,?Latch)],
    latch_loop(?Latch),
    lib_amqp:teardown(Connection, Channel).

consume_loop(Channel, Q, Parent, Tag) ->
    {ok, Consumer} = gen_event:start_link(),
    gen_event:add_handler(Consumer, amqp_consumer , [] ),
    lib_amqp:subscribe(Channel, Q, Consumer, Tag),
    timer:sleep(?Wait div 4),
    lib_amqp:unsubscribe(Channel, Tag),
    gen_event:stop(Consumer),
    Parent ! finished.

basic_recover_test(Connection) ->
    Q = uuid(),
    Channel = lib_amqp:start_channel(Connection),
    lib_amqp:declare_queue(Channel, Q),
    Tag = lib_amqp:subscribe(Channel, Q, self(), false),
    receive
        #'basic.consume_ok'{consumer_tag = Tag} -> ok
    after 2000 ->
        exit(did_not_receive_subscription_message)
    end,
    lib_amqp:publish(Channel, <<>>, Q, <<"foobar">>),
    receive
        {#'basic.deliver'{delivery_tag = DeliveryTag}, Content} ->
            %% no_ack set to false, but don't send ack
            ok
    after 2000 ->
        exit(did_not_receive_first_message)
    end,
    BasicRecover = #'basic.recover'{requeue = true},
    amqp_channel:cast(Channel,BasicRecover),
    receive
        {#'basic.deliver'{delivery_tag = DeliveryTag2}, Content2} ->
            lib_amqp:ack(Channel, DeliveryTag2)
    after 2000 ->
        exit(did_not_receive_second_message)
    end,
    lib_amqp:teardown(Connection, Channel).

% QOS is not yet implemented in RabbitMQ
basic_qos_test(Connection) -> 
    % TODO - this is code duplication
    % Also, lib_amqp has an declare_queue function that returns
    % an auto-created name, but that is in a branch that is awaiting
    % QA
    Messages = 50,
    Workers = 5,
    Q = uuid(),
    Parent = self(),
    lib_amqp:declare_queue(lib_amqp:start_channel(Connection), Q),
    %Channel = lib_amqp:start_channel(Connection),
    Consumers = [spawn(fun() -> Channel = lib_amqp:start_channel(Connection),
                                lib_amqp:subscribe(Channel, Q, self(), false),
                                sleeping_consumer(Channel, Sleep, Parent) end)
                                || Sleep <- lists:seq(1, Workers)],
    latch_loop(Workers),
    producer_loop(lib_amqp:start_channel(Connection), Q, Messages),
    latch_loop(Messages),
    ok.

sleeping_consumer(Channel, Sleep, Parent) ->
    receive
        #'basic.consume_ok'{} -> 
            Parent ! finished,
            sleeping_consumer(Channel, Sleep, Parent);
        #'basic.cancel_ok'{}  -> ok;
        {#'basic.deliver'{delivery_tag = DeliveryTag}, _Content} ->
            io:format("Sleeper ~p got a msg~n",[Sleep]),
            timer:sleep(Sleep * 1000),
            lib_amqp:ack(Channel, DeliveryTag),
            Parent ! finished,
            sleeping_consumer(Channel, Sleep, Parent)
    end.

producer_loop(Channel, RoutingKey, 0) ->
    ok;

producer_loop(Channel, RoutingKey, N) ->
    lib_amqp:publish(Channel, <<>>, RoutingKey, <<"foobar">>),
    producer_loop(Channel, RoutingKey, N - 1).

% Reject is not yet implemented in RabbitMQ
basic_reject_test(Connection) -> ok.

setup_publish(Channel) ->
    Publish = #publish{routing_key = <<"a.b.c.d">>,
                       q = <<"a.b.c">>,
                       x = <<"x">>,
                       bind_key = <<"a.b.c.*">>,
                       payload = <<"foobar">>
                       },
    setup_publish(Channel, Publish).

setup_publish(Channel, #publish{routing_key = RoutingKey,
                                q = Q, x = X,
                                bind_key = BindKey, payload = Payload,
                                mandatory = Mandatory,
                                immediate = Immediate}) ->
    ok = setup_exchange(Channel, Q, X, BindKey),
    lib_amqp:publish(Channel, X, RoutingKey, Payload),
    {ok, Q}.

teardown_test(Connection = {ConnectionPid, Mode}) ->
    Channel = lib_amqp:start_channel(Connection),
    ?assertMatch(true, is_process_alive(Channel)),
    ?assertMatch(true, is_process_alive(ConnectionPid)),
    lib_amqp:teardown(Connection, Channel),
    ?assertMatch(false, is_process_alive(Channel)),
    ?assertMatch(false, is_process_alive(ConnectionPid)).

setup_exchange(Channel, Q, X, Binding) ->
    lib_amqp:declare_exchange(Channel, X, <<"topic">>),
    lib_amqp:declare_queue(Channel, Q),
    lib_amqp:bind_queue(Channel, X, Q, Binding),
    ok.

latch_loop(0) -> ok;
latch_loop(Latch) ->
    receive
        finished ->
            latch_loop(Latch - 1)
    after ?Latch * ?Wait ->
        exit(waited_too_long)
    end.

uuid() ->
    {A, B, C} = now(),
    <<A:32,B:32,C:32>>.

