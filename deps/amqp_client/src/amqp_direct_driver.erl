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

-include_lib("rabbit.hrl").
-include_lib("rabbit_framing.hrl").
-include("amqp_client.hrl").

-export([handshake/1, open_channel/3, close_channel/1, close_connection/3]).
-export([do/2, do/3]).
-export([handle_broker_close/1]).

%---------------------------------------------------------------------------
% Driver API Methods
%---------------------------------------------------------------------------

handshake(ConnectionState = #connection_state{username = User,
                                              password = Pass,
                                              vhostpath = VHostPath}) ->
    case lists:keymember(rabbit, 1, application:which_applications()) of
        false -> throw(broker_not_found_in_vm);
        true  -> ok
    end,
    UserBin = amqp_util:binary(User),
    PassBin = amqp_util:binary(Pass),
    rabbit_access_control:user_pass_login(UserBin, PassBin),
    rabbit_access_control:check_vhost_access(#user{username = UserBin,
                                                   password = PassBin},
                                             VHostPath),
    ConnectionState.

open_channel({Channel, _OutOfBand}, ChannelPid,
             State = #connection_state{username = User,
                                       vhostpath = VHost}) ->
    UserBin = amqp_util:binary(User),
    ReaderPid = WriterPid = ChannelPid,
    Peer = rabbit_channel:start_link(Channel, ReaderPid, WriterPid,
                                     UserBin, VHost),
    amqp_channel:register_direct_peer(ChannelPid, Peer),
    State.

close_channel(_WriterPid) ->
    ok.

close_connection(_Close, From, _State) ->
    gen_server:reply(From, #'connection.close_ok'{}).

do(Writer, Method) ->
    rabbit_channel:do(Writer, Method).

do(Writer, Method, Content) ->
    rabbit_channel:do(Writer, Method, Content).

handle_broker_close(_State) ->
    ok.
