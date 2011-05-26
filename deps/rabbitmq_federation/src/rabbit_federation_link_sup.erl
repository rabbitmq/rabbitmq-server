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

-module(rabbit_federation_link_sup).

-behaviour(supervisor2).

-include_lib("rabbit_common/include/rabbit.hrl").
-include("rabbit_federation.hrl").

%% Supervises the upstream links for an exchange.

-export([start_link/1]).
-export([init/1]).

start_link(Args) -> supervisor2:start_link(?MODULE, Args).

%%----------------------------------------------------------------------------

init({Upstreams, X}) ->
    Specs = [spec(Upstream, X) || Upstream <- Upstreams],
    {ok, {{one_for_one, 2, 2}, Specs}}.

spec(Upstream = #upstream{reconnect_delay = Delay}, X) ->
    {Upstream, {rabbit_federation_link, start_link, [{Upstream, X}]},
     {transient, Delay}, ?MAX_WAIT, worker,
     [rabbit_federation_link]}.
