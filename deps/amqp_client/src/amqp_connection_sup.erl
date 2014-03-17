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
                               transient, infinity, supervisor,
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
