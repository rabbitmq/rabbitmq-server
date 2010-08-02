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
-module(amqp_channel_sup_sup).

-include("amqp_client.hrl").

-behaviour(supervisor2).

-export([start_link/2, start_channel_sup/2]).
-export([init/1]).

%%---------------------------------------------------------------------------
%% Interface
%%---------------------------------------------------------------------------

start_link(Driver, InfraArgs) ->
    supervisor2:start_link(?MODULE, [Driver, InfraArgs]).

start_channel_sup(Sup, ChannelNumber) ->
    supervisor2:start_child(Sup, [ChannelNumber]).

%%---------------------------------------------------------------------------
%% supervisor2 callbacks
%%---------------------------------------------------------------------------

init([Driver, InfraArgs]) ->
    {ok, {{simple_one_for_one_terminate, 0, 1},
          [{channel_sup, {amqp_channel_sup, start_link, [Driver, InfraArgs]},
           temporary, infinity, supervisor, [amqp_channel_sup]}]}}.
