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

%% @private
-module(amqp_direct_driver).

-include("amqp_client.hrl").

-export([handshake/1, init_channel/2, terminate_channel/3]).
-export([all_channels_closed_event/3, terminate_connection/2]).
-export([do/3]).

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
    rabbit_access_control:user_pass_login(User, Pass),
    rabbit_access_control:check_vhost_access(#user{username = User,
                                                   password = Pass},
                                             VHostPath),
    ConnectionState.

init_channel(ChannelNumber,
             #connection_state{username = User, vhostpath = VHost}) ->
    Peer = rabbit_channel:start_link(ChannelNumber, self(), self(),
                                     User, VHost),
    {Peer, Peer}.

terminate_channel(_Reason, _WriterPid, _ReaderPid) ->
    ok.

all_channels_closed_event(Reason, _Close, _State) ->
    case Reason of
        server_initiated_close ->
            %% In the direct case, the connection should terminate right away
            %% in the case of a server initiated shutdown
            erlang:error(unexpected_situation);
        _ ->
            self() ! {'$gen_cast', {method, #'connection.close_ok'{}, none}},
            wait_close_ok
    end.

terminate_connection(_Reason, _State) ->
    ok.

do(Writer, Method, none) ->
    rabbit_channel:do(Writer, Method);
do(Writer, Method, Content) ->
    rabbit_channel:do(Writer, Method, Content).
