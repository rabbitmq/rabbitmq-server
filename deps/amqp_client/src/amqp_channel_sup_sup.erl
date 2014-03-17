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
%% Copyright (c) 2007-2014 GoPivotal, Inc.  All rights reserved.
%%

%% @private
-module(amqp_channel_sup_sup).

-include("amqp_client.hrl").

-behaviour(supervisor2).

-export([start_link/3, start_channel_sup/4]).
-export([init/1]).

%%---------------------------------------------------------------------------
%% Interface
%%---------------------------------------------------------------------------

start_link(Type, Connection, ConnName) ->
    supervisor2:start_link(?MODULE, [Type, Connection, ConnName]).

start_channel_sup(Sup, InfraArgs, ChannelNumber, Consumer) ->
    supervisor2:start_child(Sup, [InfraArgs, ChannelNumber, Consumer]).

%%---------------------------------------------------------------------------
%% supervisor2 callbacks
%%---------------------------------------------------------------------------

init([Type, Connection, ConnName]) ->
    {ok, {{simple_one_for_one, 0, 1},
          [{channel_sup,
            {amqp_channel_sup, start_link, [Type, Connection, ConnName]},
            temporary, brutal_kill, supervisor, [amqp_channel_sup]}]}}.
