%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2021 VMware, Inc. or its affiliates.  All rights reserved.
%%

%% @private
-module(amqp_sup).

-include("amqp_client.hrl").

-behaviour(supervisor2).

-export([start_link/0, is_ready/0, start_connection_sup/1]).
-export([init/1]).

%%---------------------------------------------------------------------------
%% Interface
%%---------------------------------------------------------------------------

start_link() ->
    supervisor2:start_link({local, amqp_sup}, ?MODULE, []).

is_ready() ->
    whereis(amqp_sup) =/= undefined.

start_connection_sup(AmqpParams) ->
    supervisor2:start_child(amqp_sup, [AmqpParams]).

%%---------------------------------------------------------------------------
%% supervisor2 callbacks
%%---------------------------------------------------------------------------

init([]) ->
    {ok, {{simple_one_for_one, 0, 1},
          [{connection_sup, {amqp_connection_sup, start_link, []},
           temporary, ?SUPERVISOR_WAIT, supervisor, [amqp_connection_sup]}]}}.
