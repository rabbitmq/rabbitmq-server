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

-module(amqp_direct_driver).

-include_lib("rabbitmq_server/include/rabbit.hrl").
-include("amqp_client.hrl").

-export([handshake/2, open_channel/3, close_connection/3]).
-export([acquire_lock/2, release_lock/2]).

%---------------------------------------------------------------------------
% Driver API Methods
%---------------------------------------------------------------------------

handshake(ConnectionPid, ConnectionState = #connection_state{username = User,
                                                             password = Pass,
                                                             vhostpath = VHostPath}) ->
    UserBin = amqp_util:binary(User),
    PassBin = amqp_util:binary(Pass),
    rabbit_access_control:user_pass_login(UserBin, PassBin),
    rabbit_access_control:check_vhost_access(#user{username = UserBin}, VHostPath),
    ConnectionState.

open_channel({Channel,OutOfBand}, ChannelPid, State = #connection_state{username = User,
                                                                       vhostpath = VHost}) ->
    %% Why must only the username be binary?
    %% I think this is because of the binary guard on rabbit_realm:access_request/3
    UserBin = amqp_util:binary(User),
    ReaderPid = WriterPid = ChannelPid,
    Peer = spawn_link(rabbit_channel, start, [Channel, ReaderPid, WriterPid, UserBin, VHost, fun read_method/0]),
    amqp_channel:register_direct_peer(ChannelPid, Peer).

read_method() ->
    receive
        {Sender, Method} ->
            {ok, Method, <<>>};
        {Sender, Method, Content} ->
            {ok, Method, Content}
    end.

close_connection(Close, From, State) ->
    ok.

acquire_lock(AckRequired, {Tx, DeliveryTag, ConsumerTag,QName, QPid, Message}) ->
    rabbit_writer:maybe_lock_message(AckRequired,{Tx, DeliveryTag, ConsumerTag,QName, QPid, Message}).

release_lock(AckRequired, {QName, QPid, PersistentKey}) ->
    rabbit_amqqueue:notify_sent(QPid),
    ok = rabbit_writer:auto_acknowledge(AckRequired, QName, PersistentKey).

