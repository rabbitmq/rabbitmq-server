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
%% The Initial Developer of the Original Code is VMware, Inc.
%% Copyright (c) 2007-2011 VMware, Inc.  All rights reserved.
%%

%% @private
-module(amqp_channel_sup).

-include("amqp_client.hrl").

-behaviour(supervisor2).

-export([start_link/3]).
-export([init/1]).

%%---------------------------------------------------------------------------
%% Interface
%%---------------------------------------------------------------------------

start_link(Type, InfraArgs, ChNumber) ->
    {ok, Sup} = supervisor2:start_link(?MODULE, []),
    {ok, ChPid} = supervisor2:start_child(
                    Sup, {channel, {amqp_channel, start_link,
                                    [Type, ChNumber,
                                     start_writer_fun(Sup, Type, InfraArgs,
                                                      ChNumber)]},
                          intrinsic, brutal_kill, worker, [amqp_channel]}),
    {ok, AState} = init_command_assembler(Type),
    {ok, Sup, {ChPid, AState}}.

%%---------------------------------------------------------------------------
%% Internal plumbing
%%---------------------------------------------------------------------------

start_writer_fun(Sup, direct, [User, VHost, Collector], ChNumber) ->
    fun() ->
        ChPid = self(),
        {ok, _} = supervisor2:start_child(
                    Sup,
                    {rabbit_channel, {rabbit_channel, start_link,
                                      [ChNumber, ChPid, ChPid, User, VHost,
                                       Collector, start_limiter_fun(Sup)]},
                     transient, ?MAX_WAIT, worker, [rabbit_channel]})
    end;
start_writer_fun(Sup, network, [Sock], ChNumber) ->
    fun() ->
        ChPid = self(),
        {ok, _} = supervisor2:start_child(
                    Sup,
                    {writer, {rabbit_writer, start_link,
                              [Sock, ChNumber, ?FRAME_MIN_SIZE, ?PROTOCOL,
                               ChPid]},
                     transient, ?MAX_WAIT, worker, [rabbit_writer]})
    end.

init_command_assembler(direct)  -> {ok, none};
init_command_assembler(network) -> rabbit_command_assembler:init(?PROTOCOL).

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
