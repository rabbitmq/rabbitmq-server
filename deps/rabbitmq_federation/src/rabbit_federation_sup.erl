%% The contents of this file are subject to the Mozilla Public License
%% Version 1.1 (the "License"); you may not use this file except in
%% compliance with the License. You may obtain a copy of the License
%% at http://www.mozilla.org/MPL/
%%
%% Software distributed under the License is distributed on an "AS IS"
%% basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See
%% the License for the specific language governing rights and
%% limitations under the License.
%%
%% The Original Code is RabbitMQ Federation.
%%
%% The Initial Developer of the Original Code is VMware, Inc.
%% Copyright (c) 2007-2011 VMware, Inc.  All rights reserved.
%%

-module(rabbit_federation_sup).

-behaviour(supervisor).

-export([start_link/0, start_child/2]).

-export([init/1]).

-define(SUPERVISOR, ?MODULE).

start_link() ->
    supervisor:start_link({local, ?SUPERVISOR}, ?MODULE, []).

start_child(Downstream, UpstreamURIs) ->
    supervisor:start_child(?SUPERVISOR,
                           {id(Downstream, UpstreamURIs),
                            {rabbit_federation_exchange, start_link,
                             [Downstream, UpstreamURIs]},
                            transient, brutal_kill, worker,
                            [rabbit_federation_exchange]}).

%%----------------------------------------------------------------------------

id(Downstream, UpstreamURIs) ->
    {Downstream, UpstreamURIs}.

init([]) ->
    {ok, {{one_for_one,3,10},[]}}.
