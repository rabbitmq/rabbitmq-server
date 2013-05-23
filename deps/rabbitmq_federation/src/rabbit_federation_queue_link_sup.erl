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
%% Copyright (c) 2007-2013 VMware, Inc.  All rights reserved.
%%

-module(rabbit_federation_queue_link_sup).

-include_lib("rabbit_common/include/rabbit.hrl").
-include("rabbit_federation.hrl").

%% Supervises the upstream links for a queue.

-export([start_link/1]).
-export([init/1]).

start_link(Q) ->
    supervisor2:start_link(?MODULE, Q).

%%----------------------------------------------------------------------------

init(Q) ->
    {ok, {{one_for_one, 1, 1}, specs(Q)}}.

specs(Q) ->
    %% TODO don't depend on an exchange here!
    FakeX = rabbit_exchange:lookup_or_die(
              rabbit_misc:r(<<"/">>, exchange, <<"">>)),
    Upstreams = rabbit_federation_upstream:from_set(<<"all">>, FakeX),
    [spec(Upstream, Q, FakeX) || Upstream <- Upstreams].

spec(Upstream = #upstream{reconnect_delay = Delay}, Q, FakeX) ->
    Params = rabbit_federation_upstream:to_params(Upstream, FakeX),
    {Upstream, {rabbit_federation_queue_link, start_link, [{Params, Q}]},
     {permanent, Delay}, ?MAX_WAIT, worker,
     [rabbit_federation_queue_link]}.
