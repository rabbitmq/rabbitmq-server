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
%% The Initial Developer of the Original Code is GoPivotal, Inc.
%% Copyright (c) 2007-2013 GoPivotal, Inc.  All rights reserved.
%%

-module(rabbit_federation_sup).

-behaviour(supervisor).

%% Supervises everything. There is just one of these.

-include_lib("rabbit_common/include/rabbit.hrl").
-define(SUPERVISOR, rabbit_federation_sup).

-export([start_link/0]).

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
    supervisor:start_link({local, ?SUPERVISOR}, ?MODULE, []).

%%----------------------------------------------------------------------------

init([]) ->
    Status = {status, {rabbit_federation_status, start_link, []},
              transient, ?MAX_WAIT, worker,
              [rabbit_federation_status]},
    XLinkSupSup = {x_links,
                   {rabbit_federation_exchange_link_sup_sup, start_link, []},
                   transient, ?MAX_WAIT, supervisor,
                   [rabbit_federation_exchange_link_sup_sup]},
    QLinkSupSup = {q_links,
                   {rabbit_federation_queue_link_sup_sup, start_link, []},
                  transient, ?MAX_WAIT, supervisor,
                  [rabbit_federation_queue_link_sup_sup]},
    {ok, {{one_for_one, 3, 10}, [Status, XLinkSupSup, QLinkSupSup]}}.
