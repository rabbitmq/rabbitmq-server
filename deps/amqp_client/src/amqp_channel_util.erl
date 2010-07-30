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

%% @private
-module(amqp_channel_util).

-include("amqp_client.hrl").

-export([open_channel/4]).
-export([channel_infrastructure_children/4,
         terminate_channel_infrastructure/2]).
-export([do/4]).

%%---------------------------------------------------------------------------
%% Starting and terminating channels
%%---------------------------------------------------------------------------

%% Spawns a new channel supervision tree linked under the given connection
%% supervisor, starts monitoring the channel and registers it with the channel
%% manager
open_channel(Sup, ProposedNumber, Driver, InfraArgs) ->
    ChMgr = amqp_infra_sup:child(Sup, channels_manager),
    case amqp_channels_manager:new_number(ChMgr, ProposedNumber) of
        {ok, ChNumber} ->
            ChSupSup = amqp_infra_sup:child(Sup, channel_sup_sup),
            {ok, ChSup} = amqp_channel_sup_sup:start_channel_sup(
                                  ChSupSup, ChNumber, Driver, InfraArgs),
            Ch = amqp_infra_sup:child(ChSup, channel),
            %% Framing is undefined in the direct case
            Framing = amqp_infra_sup:child(ChSup, framing),
            case amqp_channels_manager:register(ChMgr, ChNumber, Ch, Framing) of
                ok     -> #'channel.open_ok'{} =
                              amqp_channel:call(Ch, #'channel.open'{}),
                          {ok, Ch};
                noproc -> {error, channel_died}
            end;
        {error, _} = Error ->
            Error
    end.

channel_infrastructure_children(network, ChannelPid, ChannelNumber,
                                [Sock, _MainReader]) ->
    FramingChild =
        {worker, framing, {rabbit_framing_channel, start_link, [ChannelPid]}},
    WriterChild =
        {worker, writer,
         {rabbit_writer, start_link, [Sock, ChannelNumber, ?FRAME_MIN_SIZE]}},
    [FramingChild, WriterChild];
channel_infrastructure_children(direct, ChannelPid, ChannelNumber,
                                [User, VHost, Collector]) ->
    RabbitChannelChild =
        {worker, rabbit_channel,
         {rabbit_channel, start_link,
          [ChannelNumber, ChannelPid, ChannelPid, User, VHost, Collector]}},
    [RabbitChannelChild].

terminate_channel_infrastructure(network, Sup) ->
    Writer = amqp_infra_sup:child(Sup, writer),
    rabbit_writer:flush(Writer),
    ok;
terminate_channel_infrastructure(direct, Sup) ->
    RChannel = amqp_infra_sup:child(Sup, rabbit_channel),
    rabbit_channel:shutdown(RChannel),
    ok.

%%---------------------------------------------------------------------------
%% Do
%%---------------------------------------------------------------------------

do(network, Sup, Method, Content) ->
    Writer = amqp_infra_sup:child(Sup, writer),
    case Content of
        none -> rabbit_writer:send_command_and_signal_back(Writer, Method,
                                                           self());
        _    -> rabbit_writer:send_command_and_signal_back(Writer, Method,
                                                           Content, self())
    end,
    receive
        rabbit_writer_send_command_signal -> ok
    end;
do(direct, Sup, Method, Content) ->
    RabbitChannel = amqp_infra_sup:child(Sup, rabbit_channel),
    case Content of
        none -> rabbit_channel:do(RabbitChannel, Method);
        _    -> rabbit_channel:do(RabbitChannel, Method, Content)
    end.
