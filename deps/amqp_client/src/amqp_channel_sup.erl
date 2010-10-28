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
%%   The Original Code is RabbitMQ Erlang Client.
%%
%%   The Initial Developers of the Original Code are LShift Ltd,
%%   Cohesive Financial Technologies LLC, and Rabbit Technologies Ltd.
%%
%%   Portions created before 22-Nov-2008 00:00:00 GMT by LShift Ltd,
%%   Cohesive Financial Technologies LLC, or Rabbit Technologies Ltd
%%   are Copyright (C) 2007-2008 LShift Ltd, Cohesive Financial
%%   Technologies LLC, and Rabbit Technologies Ltd.
%%
%%   Portions created by LShift Ltd are Copyright (C) 2007-2010 LShift
%%   Ltd. Portions created by Cohesive Financial Technologies LLC are
%%   Copyright (C) 2007-2010 Cohesive Financial Technologies
%%   LLC. Portions created by Rabbit Technologies Ltd are Copyright
%%   (C) 2007-2010 Rabbit Technologies Ltd.
%%
%%   All Rights Reserved.
%%
%%   Contributor(s): ______________________________________.
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
    {ok, Framing} = start_framing(Sup, Type, ChPid),
    {ok, Sup, {ChPid, Framing}}.

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

start_framing(_Sup, direct, _ChPid) ->
    {ok, none};
start_framing(Sup, network, ChPid) ->
    {ok, _} = supervisor2:start_child(
                Sup,
                {framing, {rabbit_framing_channel, start_link,
                           [ChPid, ChPid, ?PROTOCOL]},
                 transient, ?MAX_WAIT, worker, [rabbit_framing_channel]}).

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
