%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2023 VMware, Inc. or its affiliates.  All rights reserved.
%%

%% @private
-module(amqp_channel_sup_sup).

-include("amqp_client.hrl").

-behaviour(supervisor).

-export([start_link/3, start_channel_sup/4]).
-export([init/1]).

%%---------------------------------------------------------------------------
%% Interface
%%---------------------------------------------------------------------------

start_link(Type, Connection, ConnName) ->
    supervisor:start_link(?MODULE, [Type, Connection, ConnName]).

start_channel_sup(Sup, InfraArgs, ChannelNumber, Consumer) ->
    supervisor:start_child(Sup, [InfraArgs, ChannelNumber, Consumer]).

%%---------------------------------------------------------------------------
%% supervisor callbacks
%%---------------------------------------------------------------------------

init([Type, Connection, ConnName]) ->
    SupFlags = #{strategy => simple_one_for_one, intensity => 0, period => 1},
    ChildStartMFA = {amqp_channel_sup, start_link, [Type, Connection, ConnName]},
    ChildSpec = #{id => channel_sup,
                  start => ChildStartMFA,
                  restart => temporary,
                  shutdown => infinity,
                  type => supervisor,
                  modules => [amqp_channel_sup]},
    {ok, {SupFlags, [ChildSpec]}}.
