%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2023 VMware, Inc. or its affiliates.  All rights reserved.
%%

%% @private
-module(amqp_sup).

-include("amqp_client.hrl").

-behaviour(supervisor).

-export([start_link/0, is_ready/0, start_connection_sup/1]).
-export([init/1]).

%%---------------------------------------------------------------------------
%% Interface
%%---------------------------------------------------------------------------

start_link() ->
    supervisor:start_link({local, amqp_sup}, ?MODULE, []).

is_ready() ->
    whereis(amqp_sup) =/= undefined.

start_connection_sup(AmqpParams) ->
    supervisor:start_child(amqp_sup, [AmqpParams]).

%%---------------------------------------------------------------------------
%% supervisor callbacks
%%---------------------------------------------------------------------------

init([]) ->
    SupFlags = #{strategy => simple_one_for_one,
                 intensity => 0,
                 period => 1},
    ChildSpec = #{id => connection_sup,
                  start => {amqp_connection_sup, start_link, []},
                  restart => temporary,
                  shutdown => ?SUPERVISOR_WAIT,
                  type => supervisor,
                  modules => [amqp_connection_sup]},
    {ok, {SupFlags, [ChildSpec]}}.
