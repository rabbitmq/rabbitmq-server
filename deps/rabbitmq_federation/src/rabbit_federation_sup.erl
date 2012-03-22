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
%% Copyright (c) 2007-2012 VMware, Inc.  All rights reserved.
%%

-module(rabbit_federation_sup).

-behaviour(mirrored_supervisor).

%% Supervises everything. There is just one of these.

-include("rabbit_federation.hrl").
-include_lib("rabbit_common/include/rabbit.hrl").

-export([start_link/0, start_child/2, restart_child/1, stop_child/1]).

-export([init/1]).

%% This supervisor needs to be part of the rabbit application since
%% a) it needs to be in place when exchange recovery takes place
%% b) it needs to go up and down with rabbit

-rabbit_boot_step({rabbit_federation_supervisor,
                   [{description, "federation"},
                    {mfa,         {rabbit_sup, start_child, [?MODULE]}},
                    {requires,    kernel_ready},
                    {enables,     rabbit_federation_exchange}]}).

%%----------------------------------------------------------------------------

start_link() ->
    mirrored_supervisor:start_link({local, ?SUPERVISOR},
                                   ?SUPERVISOR, ?MODULE, []).

start_child(Id, Args) ->
    {ok, _Pid} = mirrored_supervisor:start_child(
                   ?SUPERVISOR,
                   {Id, {rabbit_federation_link_sup, start_link, [Args]},
                    transient, ?MAX_WAIT, supervisor,
                    [rabbit_federation_link_sup]}).

restart_child(Id) ->
    %% Could we come up with a nice shutdown protocol for links, (so
    %% we don't end up redelivering any messages)?
    ok = mirrored_supervisor:terminate_child(?SUPERVISOR, Id),
    {ok, _Pid} = mirrored_supervisor:restart_child(?SUPERVISOR, Id).

stop_child(Id) ->
    ok = mirrored_supervisor:terminate_child(?SUPERVISOR, Id),
    ok = mirrored_supervisor:delete_child(?SUPERVISOR, Id).

%%----------------------------------------------------------------------------

init([]) -> {ok, {{one_for_one, 3, 10},[]}}.
