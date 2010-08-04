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
-module(amqp_connection_sup).

-include("amqp_client.hrl").

-behaviour(supervisor2).

-export([start_link/3]).
-export([init/1]).

%%---------------------------------------------------------------------------
%% Interface
%%---------------------------------------------------------------------------

start_link(Type, Module, AmqpParams) ->
    {ok, Sup} = supervisor2:start_link(?MODULE, []),
    {ok, ChSupSup} = supervisor2:start_child(Sup,
                         {channel_sup_sup, {amqp_channel_sup_sup, start_link,
                                            [Type]},
                          permanent, infinity, supervisor,
                          [amqp_channel_sup_sup]}),
    {ok, ChMgr} = supervisor2:start_child(Sup,
                      {channels_manager, {amqp_channels_manager, start_link,
                                          [ChSupSup]},
                       permanent, ?MAX_WAIT, worker, [amqp_channels_manager]}),
    {ok, _} = supervisor2:start_child(Sup,
                  {connection, {Module, start_link, [AmqpParams, ChMgr]},
                   permanent, ?MAX_WAIT, worker, [Module]}),
    {ok, Sup}.

%%---------------------------------------------------------------------------
%% supervisor2 callbacks
%%---------------------------------------------------------------------------

init([]) ->
    {ok, {{one_for_all, 0, 1}, []}}.
