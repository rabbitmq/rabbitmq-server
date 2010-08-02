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

-behaviour(supervisor2).

-export([start_link/3]).
-export([init/1]).

%%---------------------------------------------------------------------------
%% Interface
%%---------------------------------------------------------------------------

start_link(Driver, InfraArgs, ChNumber) ->
    {ok, Sup} = supervisor2:start_link(?MODULE, [Driver, InfraArgs, ChNumber]),
    case Driver of
        direct  -> ok;
        network -> [_, MainReader] = InfraArgs,
                   FramingPid = supervisor2:find_child(Sup, framing),
                   amqp_main_reader:register_framing_channel(
                           MainReader, ChNumber, FramingPid)
    end,
    [Channel] = supervisor2:find_child(Sup, channel),
    #'channel.open_ok'{} = amqp_channel:call(Channel, #'channel.open'{}),
    {ok, Sup}.

%%---------------------------------------------------------------------------
%% supervisor2 callbacks
%%---------------------------------------------------------------------------

init([Driver, InfraArgs, ChNumber]) ->
    Me = self(),
    GetChPid = fun() -> supervisor2:find_child(Me, channel) end,
    InfraChildren = amqp_channel_util:channel_infrastructure_children(
                        Driver, InfraArgs, GetChPid, ChNumber),
    {ok, {{one_for_all, 0, 1},
          [channel, {amqp_channel, start_link, [Driver, ChNumber]},
           permanent, ?MAX_WAIT, worker, [amqp_channel]] ++ InfraChildren}}.
