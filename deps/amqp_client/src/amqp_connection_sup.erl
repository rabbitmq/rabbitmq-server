%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2023 VMware, Inc. or its affiliates.  All rights reserved.
%%

%% @private
-module(amqp_connection_sup).

-include("amqp_client.hrl").

-behaviour(supervisor).

-export([start_link/1]).
-export([init/1]).

%%---------------------------------------------------------------------------
%% Interface
%%---------------------------------------------------------------------------

start_link(AMQPParams) ->
    {ok, Sup} = supervisor:start_link(?MODULE, []),

    StartMFA0 = {amqp_connection_type_sup, start_link, []},
    ChildSpec0 = #{id => connection_type_sup,
                   start => StartMFA0,
                   restart => transient,
                   shutdown => ?SUPERVISOR_WAIT,
                   type => supervisor,
                   modules => [amqp_connection_type_sup]},
    {ok, TypeSup} = supervisor:start_child(Sup, ChildSpec0),

    StartMFA1 = {amqp_gen_connection, start_link, [TypeSup, AMQPParams]},
    ChildSpec1 = #{id => connection,
                   start => StartMFA1,
                   restart => transient,
                   significant => true,
                   shutdown => brutal_kill,
                   type => worker,
                   modules => [amqp_gen_connection]},
    {ok, Connection} = supervisor:start_child(Sup, ChildSpec1),

    {ok, Sup, Connection}.

%%---------------------------------------------------------------------------
%% supervisor callbacks
%%---------------------------------------------------------------------------

init([]) ->
    SupFlags = #{strategy => one_for_all,
                 intensity => 0,
                 period => 1,
                 auto_shutdown => any_significant},
    {ok, {SupFlags, []}}.
