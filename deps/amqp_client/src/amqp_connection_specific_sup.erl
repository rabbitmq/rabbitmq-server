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
-module(amqp_connection_specific_sup).

-include("amqp_client.hrl").

-behaviour(supervisor2).

-export([start_link_direct/0, start_link_network/1]).
-export([init/1]).

%%---------------------------------------------------------------------------
%% Interface
%%---------------------------------------------------------------------------

start_link_direct() ->
    supervisor2:start_link(?MODULE, [direct, []]).

start_link_network(Sock) ->
    supervisor2:start_link(?MODULE, [network, [Sock]]).

%%---------------------------------------------------------------------------
%% supervisor2 callbacks
%%---------------------------------------------------------------------------

init([network, [Sock]]) ->
    Channel0InfraChildren = amqp_channel_util:channel_infrastructure_children(
                                network, [Sock, none], 0),
    {ok, {{one_for_all, 0, 1},
          Channel0InfraChildren ++
          [{main_reader, {amqp_main_reader, start_link, [Sock]},
           permanent, ?MAX_WAIT, worker, [amqp_main_reader]}]}};
init([direct, []]) ->
    {ok, {{one_for_all, 0, 1},
          [collector, {rabbit_queue_collector, start_link, []},
           permanent, ?MAX_WAIT, worker, [rabbit_queue_collector]]}}.
