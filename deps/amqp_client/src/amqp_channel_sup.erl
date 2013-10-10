%% The contents of this file are subject to the Mozilla Public License
%% Version 1.1 (the "License"); you may not use this file except in
%% compliance with the License. You may obtain a copy of the License at
%% http://www.mozilla.org/MPL/
%%
%% Software distributed under the License is distributed on an "AS IS"
%% basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See the
%% License for the specific language governing rights and limitations
%% under the License.
%%
%% The Original Code is RabbitMQ.
%%
%% The Initial Developer of the Original Code is GoPivotal, Inc.
%% Copyright (c) 2007-2013 GoPivotal, Inc.  All rights reserved.
%%

%% @private
-module(amqp_channel_sup).

-include("amqp_client_internal.hrl").

-behaviour(supervisor2).

-export([start_link/5]).
-export([init/1]).

%%---------------------------------------------------------------------------
%% Interface
%%---------------------------------------------------------------------------

start_link(Type, Connection, InfraArgs, ChNumber, Consumer = {_, _}) ->
    {ok, Sup} = supervisor2:start_link(?MODULE, [Consumer]),
    [{gen_consumer, ConsumerPid, _, _}] = supervisor2:which_children(Sup),
    {ok, ChPid} = supervisor2:start_child(
                    Sup, {channel,
                          {amqp_channel, start_link,
                           [Type, Connection, ChNumber, ConsumerPid]},
                          intrinsic, ?MAX_WAIT, worker, [amqp_channel]}),
    Writer = start_writer(Sup, Type, InfraArgs, ChNumber, ChPid),
    amqp_channel:set_writer(ChPid, Writer),
    {ok, AState} = init_command_assembler(Type),
    {ok, Sup, {ChPid, AState}}.

%%---------------------------------------------------------------------------
%% Internal plumbing
%%---------------------------------------------------------------------------

start_writer(_Sup, direct, [ConnPid, ConnName, Node, User, VHost, Collector],
             ChNumber, ChPid) ->
    {ok, RabbitCh} =
        rpc:call(Node, rabbit_direct, start_channel,
                 [ChNumber, ChPid, ConnPid, ConnName, ?PROTOCOL, User,
                  VHost, ?CLIENT_CAPABILITIES, Collector]),
    link(RabbitCh),
    RabbitCh;
start_writer(Sup, network, [Sock, FrameMax], ChNumber, ChPid) ->
    {ok, Writer} = supervisor2:start_child(
                     Sup,
                     {writer, {rabbit_writer, start_link,
                               [Sock, ChNumber, FrameMax, ?PROTOCOL, ChPid]},
                      intrinsic, ?MAX_WAIT, worker, [rabbit_writer]}),
    Writer.

init_command_assembler(direct)  -> {ok, none};
init_command_assembler(network) -> rabbit_command_assembler:init(?PROTOCOL).

%%---------------------------------------------------------------------------
%% supervisor2 callbacks
%%---------------------------------------------------------------------------

init([{ConsumerModule, ConsumerArgs}]) ->
    {ok, {{one_for_all, 0, 1},
          [{gen_consumer, {amqp_gen_consumer, start_link,
                           [ConsumerModule, ConsumerArgs]},
           intrinsic, ?MAX_WAIT, worker, [amqp_gen_consumer]}]}}.
