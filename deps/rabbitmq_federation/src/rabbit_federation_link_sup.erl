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

-module(rabbit_federation_link_sup).

-behaviour(supervisor2).

-include_lib("rabbit_common/include/rabbit.hrl").
-include("rabbit_federation.hrl").

%% Supervises the upstream links for an exchange.

-export([start_link/1]).
-export([init/1]).

start_link(Args) -> supervisor2:start_link(?MODULE, Args).

%%----------------------------------------------------------------------------

init({UpstreamSet, X}) ->
    %% We can't look this up in fed_exchange or fed_sup since
    %% mirrored_sup may fail us over to a different node with a
    %% different definition of the same upstream-set. This is the
    %% first point at which we know we're not switching nodes.
    %% TODO the above is no longer true - what should change?
    {ok, Upstreams} = rabbit_federation_upstream:from_set(UpstreamSet, X),
    %% 1, 1 so that the supervisor can give up and get into waiting
    %% for the reconnect_delay quickly.
    {ok, {{one_for_one, 1, 1},
          [spec(Upstream, X) || Upstream <- Upstreams]}}.

spec(Upstream = #upstream{reconnect_delay = Delay}, #exchange{name = XName}) ->
    {Upstream, {rabbit_federation_link, start_link, [{Upstream, XName}]},
     {transient, Delay}, ?MAX_WAIT, worker,
     [rabbit_federation_link]}.
