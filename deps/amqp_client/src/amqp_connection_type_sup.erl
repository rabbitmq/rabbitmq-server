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
-module(amqp_connection_type_sup).

-include("amqp_client.hrl").

-behaviour(supervisor2).

-export([start_link_direct/0, start_link_network/2, start_heartbeat_fun/1]).
-export([init/1]).

%%---------------------------------------------------------------------------
%% Interface
%%---------------------------------------------------------------------------

start_link_direct() ->
    {ok, Sup} = supervisor2:start_link(?MODULE, []),
    {ok, Collector} =
        supervisor2:start_child(
          Sup, {collector, {rabbit_queue_collector, start_link, []},
                intrinsic, ?MAX_WAIT, worker, [rabbit_queue_collector]}),
    {ok, Sup, Collector}.

start_link_network(Sock, ConnectionPid) ->
    {ok, Sup} = supervisor2:start_link(?MODULE, []),
    {ok, Framing} =
        supervisor2:start_child(
          Sup, {framing, {rabbit_framing_channel, start_link,
                          [Sup, ConnectionPid, ?PROTOCOL]},
                intrinsic, ?MAX_WAIT, worker, [rabbit_framing_channel]}),
    {ok, MainReader} =
        supervisor2:start_child(
          Sup, {main_reader, {amqp_main_reader, start_link,
                              [Sock, Framing, ConnectionPid]},
                intrinsic, ?MAX_WAIT, worker, [amqp_main_reader]}),
    {ok, Writer} =
        supervisor2:start_child(
          Sup, {writer, {rabbit_writer, start_link,
                         [Sock, 0, ?FRAME_MIN_SIZE, ?PROTOCOL, MainReader]},
                intrinsic, ?MAX_WAIT, worker, [rabbit_writer]}),
    {ok, Sup, {MainReader, Framing, Writer}}.

start_heartbeat_fun(Sup) ->
    fun (_Sock, 0) ->
            none;
        (Sock, Timeout) ->
            Connection = self(),
            {ok, Sender} =
                supervisor2:start_child(
                  Sup,
                  {heartbeat_sender, {rabbit_heartbeat, start_heartbeat_sender,
                                      [Connection, Sock, Timeout]},
                   transient, ?MAX_WAIT, worker, [rabbit_heartbeat]}),
            {ok, Receiver} =
                supervisor2:start_child(
                  Sup,
                  {heartbeat_receiver,
                   {rabbit_heartbeat, start_heartbeat_receiver,
                    [Connection, Sock, Timeout]},
                   transient, ?MAX_WAIT, worker, [rabbit_heartbeat]}),
            {Sender, Receiver}
    end.

%%---------------------------------------------------------------------------
%% supervisor2 callbacks
%%---------------------------------------------------------------------------

init([]) ->
    {ok, {{one_for_all, 0, 1}, []}}.
