%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2021 VMware, Inc. or its affiliates.  All rights reserved.
%%

%% @private
-module(amqp_connection_sup).

-include("amqp_client.hrl").

-behaviour(supervisor2).

-export([start_link/1]).
-export([init/1]).

%%---------------------------------------------------------------------------
%% Interface
%%---------------------------------------------------------------------------

start_link(AMQPParams) ->
    {ok, Sup} = supervisor2:start_link(?MODULE, []),
    {ok, TypeSup}    = supervisor2:start_child(
                         Sup, {connection_type_sup,
                               {amqp_connection_type_sup, start_link, []},
                               transient, ?SUPERVISOR_WAIT, supervisor,
                               [amqp_connection_type_sup]}),
    {ok, Connection} = supervisor2:start_child(
                         Sup, {connection, {amqp_gen_connection, start_link,
                                            [TypeSup, AMQPParams]},
                               intrinsic, brutal_kill, worker,
                               [amqp_gen_connection]}),
    {ok, Sup, Connection}.

%%---------------------------------------------------------------------------
%% supervisor2 callbacks
%%---------------------------------------------------------------------------

init([]) ->
    {ok, {{one_for_all, 0, 1}, []}}.
