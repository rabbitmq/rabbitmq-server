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
-module(amqp_connection_sup).

-include("amqp_client.hrl").

-behaviour(supervisor2).

-export([start_link/2]).
-export([init/1]).

%%---------------------------------------------------------------------------
%% Interface
%%---------------------------------------------------------------------------

start_link(Type, AmqpParams) ->
    {ok, Sup} = supervisor2:start_link(?MODULE, []),
    unlink(Sup),
    {ok, ChSupSup} = supervisor2:start_child(Sup,
                         {channel_sup_sup, {amqp_channel_sup_sup, start_link,
                                            [Type]},
                          intrinsic, infinity, supervisor,
                          [amqp_channel_sup_sup]}),
    {ok, Connection} = start_connection(Sup, Type, AmqpParams, ChSupSup,
                                        start_infrastructure_fun(Sup, Type)),
    {ok, Sup, Connection}.

%%---------------------------------------------------------------------------
%% Internal plumbing
%%---------------------------------------------------------------------------

start_connection(Sup, network, AmqpParams, ChSupSup, SIF) ->
    {ok, _} = supervisor2:start_child(
                Sup,
                {connection, {amqp_network_connection, start_link,
                              [AmqpParams, ChSupSup, SIF]},
                 intrinsic, brutal_kill, worker, [amqp_network_connection]});
start_connection(Sup, direct, AmqpParams, ChSupSup, SIF) ->
    {ok, _} = supervisor2:start_child(
                Sup,
                {connection, {amqp_direct_connection, start_link,
                              [AmqpParams, ChSupSup, SIF]},
                 intrinsic, brutal_kill, worker, [amqp_direct_connection]}).

start_infrastructure_fun(Sup, network) ->
    fun (Sock) ->
            Connection = self(),
            {ok, CTSup, {MainReader, Framing, Writer}} =
                supervisor2:start_child(
                  Sup,
                  {connection_type_sup,
                   {amqp_connection_type_sup, start_link_network,
                    [Sock, Connection]},
                   intrinsic, infinity, supervisor,
                   [amqp_connection_type_sup]}),
            {ok, {MainReader, Framing, Writer,
                  amqp_connection_type_sup:start_heartbeat_fun(CTSup)}}
    end;
start_infrastructure_fun(Sup, direct) ->
    fun () ->
            {ok, _CTSup, Collector} =
                supervisor2:start_child(
                  Sup,
                  {connection_type_sup,
                   {amqp_connection_type_sup, start_link_direct, []},
                   intrinsic, infinity, supervisor,
                   [amqp_connection_type_sup]}),
            {ok, Collector}
    end.

%%---------------------------------------------------------------------------
%% supervisor2 callbacks
%%---------------------------------------------------------------------------

init([]) ->
    {ok, {{one_for_all, 0, 1}, []}}.
