%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2022 VMware, Inc. or its affiliates.  All rights reserved.
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
            temporary, infinity, supervisor, [amqp_channel_sup]}]}}.
