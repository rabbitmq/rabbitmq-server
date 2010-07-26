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
%%   Contributor(s): ____________________.

%% @private
-module(amqp_channel_sup).

-include("amqp_client.hrl").

-export([start_link/3]).

%%---------------------------------------------------------------------------

start_link(ChannelNumber, Driver, InfraArgs) ->
    ChChild =
        {worker, channel, {amqp_channel, start_link, [ChannelNumber, Driver]}},
    {ok, Sup} = amqp_infra_sup:start_link([ChChild]),
    ChannelPid = amqp_infra_sup:child(Sup, channel),
    InfraChildren = amqp_channel_util:channel_infrastructure_children(
                            Driver, ChannelPid, ChannelNumber, InfraArgs),
    {all_ok, _} = amqp_infra_sup:start_children(Sup, InfraChildren),
    case Driver of
        direct  -> ok;
        network -> [_Sock, MainReader] = InfraArgs,
                   FramingPid = amqp_infra_sup:child(Sup, framing),
                   amqp_main_reader:register_framing_channel(
                           MainReader, ChannelNumber, FramingPid)
    end,
    #'channel.open_ok'{} = amqp_channel:call(ChannelPid, #'channel.open'{}),
    {ok, Sup}.
