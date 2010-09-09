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
    SIF = start_infrastructure_fun(Sup, Driver, InfraArgs, ChNumber),
    {ok, ChPid} =
        supervisor2:start_child(
          Sup, {channel, {amqp_channel, start_link, [Driver, ChNumber, SIF]},
                intrinsic, brutal_kill, worker, [amqp_channel]}),
    {ok, Sup, ChPid}.

%%---------------------------------------------------------------------------
%% Internal plumbing
%%---------------------------------------------------------------------------

start_infrastructure_fun(Sup, direct, [User, VHost, Collector], ChNumber) ->
    fun () ->
            ChPid = self(),
            supervisor2:start_child(
              Sup,
              {rabbit_channel, {rabbit_channel, start_link,
                                [ChNumber, ChPid, ChPid, User, VHost,
                                 Collector, start_limiter_fun(Sup)]},
               transient, ?MAX_WAIT, worker, [rabbit_channel]})
    end;
start_infrastructure_fun(Sup, network, [Sock, MainReader], ChNumber) ->
    fun () ->
            ChPid = self(),
            {ok, Framing} =
                supervisor2:start_child(
                  Sup,
                  {framing, {rabbit_framing_channel, start_link,
                             [Sup, ChPid, ?PROTOCOL]},
                   intrinsic, ?MAX_WAIT, worker, [rabbit_framing_channel]}),
            {ok, Writer} =
                supervisor2:start_child(
                  Sup,
                  {writer, {rabbit_writer, start_link,
                            [Sock, ChNumber, ?FRAME_MIN_SIZE, ?PROTOCOL,
                             MainReader]},
                   intrinsic, ?MAX_WAIT, worker, [rabbit_writer]}),
            %% This call will disappear as part of bug 23024
            amqp_main_reader:register_framing_channel(MainReader, ChNumber,
                                                      Framing),
            {ok, Writer}
    end.

start_limiter_fun(Sup) ->
    fun (UnackedCount) ->
            Parent = self(),
            {ok, _} = supervisor2:start_child(
                        Sup,
                        {limiter, {rabbit_limiter, start_link,
                                   [Parent, UnackedCount]},
                         transient, ?MAX_WAIT, worker, [rabbit_limiter]})
    end.

%%---------------------------------------------------------------------------
%% supervisor2 callbacks
%%---------------------------------------------------------------------------

init([]) ->
    {ok, {{one_for_all, 0, 1}, []}}.
