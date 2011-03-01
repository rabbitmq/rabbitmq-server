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

-module(rabbit_federation_exchange_sup).

-behaviour(supervisor).

-include_lib("rabbit_common/include/rabbit.hrl").

%% Supervises an exchange. Supervises the upstream_sup, and also a
%% little process which is just around to hold information on the
%% upstream_sup before it starts.

-export([start_link/1]).
-export([init/1]).
-export([go/1]).

-define(SUPERVISOR, ?MODULE).

start_link(Args) ->
    supervisor:start_link(?MODULE, [Args]).

go(Pid) ->
    Args = rabbit_federation_exchange_info:args(info(Pid)),
    supervisor:start_child(
      Pid, {sup, {rabbit_federation_exchange_upstream_sup, start_link, [Args]},
            transient, ?MAX_WAIT, supervisor,
            [rabbit_federation_exchange_upstream_sup]}).

%%----------------------------------------------------------------------------

info(Pid) ->
    case supervisor:which_children(Pid) of
        [{_, Info, _, _}] -> Info;
        Children          -> exit({wrong_children, Children})
    end.

%%----------------------------------------------------------------------------

init(Args) ->
    {ok, {{one_for_all, 3, 10},
          [{info, {rabbit_federation_exchange_info, start_link, Args},
            transient, ?MAX_WAIT, worker,
            [rabbit_federation_exchange_info]}]}}.
