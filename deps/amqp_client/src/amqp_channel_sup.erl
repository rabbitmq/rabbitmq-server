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
    {ok, Sup} = supervisor2:start_link(?MODULE, []),
    {ok, ChPid} = supervisor2:start_child(Sup,
                      {channel, {amqp_channel, start_link, [Driver, ChNumber]},
                       permanent, ?MAX_WAIT, worker, [amqp_channel]}),
    start_infrastructure(Sup, Driver, InfraArgs, ChPid, ChNumber),
    {ok, Sup}.

%%---------------------------------------------------------------------------
%% Internal plumbing
%%---------------------------------------------------------------------------

start_infrastructure(Sup, direct, [User, VHost, Collector], ChPid, ChNumber) ->
    {ok, _} = supervisor2:start_child(Sup,
                  {rabbit_channel, {rabbit_channel, start_link,
                                    [ChNumber, ChPid, ChPid, User, VHost,
                                     Collector]},
                   permanent, ?MAX_WAIT, worker, [rabbit_channel]}),
    ok;
start_infrastructure(Sup, network, [Sock, MainReader], ChPid, ChNumber) ->
    {ok, Framing} = supervisor2:start_child(Sup,
                        {framing, {rabbit_framing_channel, start_link,
                                   [ChPid, ?PROTOCOL]},
                         permanent, ?MAX_WAIT, worker,
                         [rabbit_framing_channel]}),
    {ok, _} = supervisor2:start_child(Sup,
                  {writer, {rabbit_writer, start_link,
                            [Sock, ChNumber, ?FRAME_MIN_SIZE, ?PROTOCOL]},
                   permanent, ?MAX_WAIT, worker, [rabbit_writer]}),
    ok.

%%---------------------------------------------------------------------------
%% supervisor2 callbacks
%%---------------------------------------------------------------------------

init([]) ->
    {ok, {{one_for_all, 0, 1}, []}}.
