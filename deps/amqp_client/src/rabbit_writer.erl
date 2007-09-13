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
%%   The Original Code is RabbitMQ.
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
%%   Contributor(s): ______________________________________.
%%

-module(rabbit_writer).
-include("rabbit.hrl").
-include("rabbit_framing.hrl").

-export([start/4, shutdown/1, mainloop/1]).
-export([send_command/2, send_command/3,
         deliver/6, get_ok/6,
         pause/1, unpause/2]).
-export([internal_send_command/3, internal_send_command/5]).

-import(gen_tcp).

-record(wstate, {sock, channel, connection, next_tag, tx}).

start(Sock, Channel, Connection, Tx) ->
    spawn_link(?MODULE, mainloop, [#wstate{sock = Sock,
                                           channel = Channel,
                                           connection = Connection,
                                           next_tag = 10000,
                                           tx = Tx}]).

mainloop(State) ->
    receive
        Message ->
            State1 = handle_message(Message, State),
            ?MODULE:mainloop(State1)
    end.

handle_message({send_command, MethodRecord},
               State = #wstate{sock = Sock, channel = Channel}) ->
    ok = internal_send_command(Sock, Channel, MethodRecord),
    State;
handle_message({send_command, MethodRecord, Content},
               State = #wstate{
                 sock = Sock,
                 channel = Channel,
                 connection = #connection{frame_max = FrameMax}}) ->
    ok = internal_send_command(Sock, Channel, MethodRecord, Content, FrameMax),
    State;
handle_message({deliver, ConsumerTag, AckRequired, QName, QPid,
                Message = #basic_message{exchange_name = ExchangeName,
                                         routing_key = RoutingKey,
                                         persistent_key = PersistentKey,
                                         redelivered = Redelivered,
                                         content = Content}},
               State = #wstate{
                 sock = Sock,
                 channel = Channel,
                 connection = #connection{frame_max = FrameMax},
                 next_tag = DeliveryTag,
                 tx = Tx}) ->
    maybe_lock_message(AckRequired, {Tx, DeliveryTag, ConsumerTag,
                                     QName, QPid, Message}),
    ok = internal_send_command(
           Sock,
           Channel,
           #'basic.deliver'{consumer_tag = ConsumerTag,
                            delivery_tag = DeliveryTag,
                            redelivered = Redelivered,
                            exchange = ExchangeName#resource.name,
                            routing_key = RoutingKey},
           Content,
           FrameMax),
    rabbit_amqqueue:notify_sent(QPid),
    ok = auto_acknowledge(AckRequired, QName, PersistentKey),
    State#wstate{next_tag = DeliveryTag + 1};
handle_message({get_ok, MessageCount, AckRequired, QName, QPid,
                Message = #basic_message{exchange_name = ExchangeName,
                                         routing_key = RoutingKey,
                                         persistent_key = PersistentKey,
                                         redelivered = Redelivered,
                                         content = Content}},
               State = #wstate{
                 sock = Sock,
                 channel = Channel,
                 connection = #connection{frame_max = FrameMax},
                 next_tag = DeliveryTag,
                 tx = Tx}) ->
    maybe_lock_message(AckRequired, {Tx, DeliveryTag, none,
                                     QName, QPid, Message}),
    ok = internal_send_command(
           Sock,
           Channel,
           #'basic.get_ok'{delivery_tag = DeliveryTag,
                           redelivered = Redelivered,
                           exchange = ExchangeName#resource.name,
                           routing_key = RoutingKey,
                           message_count = MessageCount},
           Content,
           FrameMax),
    ok = auto_acknowledge(AckRequired, QName, PersistentKey),
    State#wstate{next_tag = DeliveryTag + 1};
handle_message({pause, ReplyPid}, State) ->
    Cookie = make_ref(),
    ReplyPid ! {paused, Cookie},
    receive
        {unpause, C} when C == Cookie -> % "C is for Cookie..."
            ok
    end,
    State;
handle_message(shutdown, _State) ->
    exit(normal);
handle_message(Message, _State) ->
    exit({writer, message_not_understood, Message}).

%---------------------------------------------------------------------------

send_command(W, MethodRecord) ->
    W ! {send_command, MethodRecord},
    ok.

send_command(W, MethodRecord, Content) ->
    W ! {send_command, MethodRecord, Content},
    ok.

deliver(W, ConsumerTag, AckRequired, QName, QPid, Message) ->
    W !{deliver, ConsumerTag, AckRequired, QName, QPid, Message},
    ok.

get_ok(W, MessageCount, AckRequired, QName, QPid, Message) ->
    W !{get_ok, MessageCount, AckRequired, QName, QPid, Message},
    ok.

pause(W) ->
    W ! {pause, self()},
    receive
        {paused, Cookie} ->
            {ok, Cookie}
    end.

unpause(W, Cookie) ->
    W ! {unpause, Cookie},
    ok.

shutdown(W) ->
    W ! shutdown,
    ok.

%---------------------------------------------------------------------------

send(Sock, Data) ->
    ok = gen_tcp:send(Sock, Data).

maybe_lock_message(true, {Tx, DeliveryTag, ConsumerTag,
                          QName, QPid, Message}) ->
    rabbit_transaction:lock_message(Tx, DeliveryTag, ConsumerTag,
                                    QName, QPid, Message);
maybe_lock_message(false, _) ->
    ok.

auto_acknowledge(_AckRequired, _QName, none) ->
    ok;
auto_acknowledge(true, _QName, _PersistentKey) ->
    %% auto ack turned off if AckRequired set
    ok;
auto_acknowledge(false, QName, PersistentKey) ->
    rabbit_transaction:auto_acknowledge(QName, PersistentKey).

internal_send_command(Sock, Channel, MethodRecord) ->
    ?LOGMESSAGE(out, Channel, MethodRecord, none),
    Frame = rabbit_binary_generator:build_simple_method_frame(Channel, MethodRecord),
    ok = send(Sock, Frame).

internal_send_command(Sock, Channel, MethodRecord, Content, FrameMax) ->
    ?LOGMESSAGE(out, Channel, MethodRecord, Content),
    MethodName = rabbit_misc:method_record_type(MethodRecord),
    true = rabbit_framing:method_has_content(MethodName), % assertion
    MethodFrame = rabbit_binary_generator:build_simple_method_frame(Channel, MethodRecord),
    ContentFrames = rabbit_binary_generator:build_simple_content_frames(Channel,
                                                                 Content,
                                                                 FrameMax),
    ok = send(Sock, [MethodFrame | ContentFrames]),
    ok.
