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
%% The Original Code is RabbitMQ.
%%
%% The Initial Developer of the Original Code is GoPivotal, Inc.
%% Copyright (c) 2017 Pivotal Software, Inc.  All rights reserved.
%%

-module(rabbit_vhost_sup_wrapper).

-include("rabbit.hrl").

-behaviour(supervisor2).
-export([init/1]).
-export([start_link/1]).
-export([start_vhost_sup/1]).

start_link(VHost) ->
    supervisor2:start_link(?MODULE, [VHost]).

%% This module is a wrapper around vhost supervisor to
%% provide exactly once restart.

%% rabbit_vhost_sup supervisor children are added dynamically,
%% so one_for_all strategy cannot be used.

init([VHost]) ->
    {ok, {{one_for_all, 1, 10000000},
          [{rabbit_vhost_sup,
            {rabbit_vhost_sup_wrapper, start_vhost_sup, [VHost]},
             intrinsic, infinity, supervisor,
             [rabbit_vhost_sup]}]}}.

start_vhost_sup(VHost) ->
     case rabbit_vhost_sup:start_link(VHost) of
        {ok, Pid} ->
            %% Save vhost sup record with wrapper pid and vhost sup pid.
            ok = rabbit_vhost_sup_sup:save_vhost_sup(VHost, self(), Pid),
            %% We can start recover as soon as we have vhost_sup record saved
            ok = rabbit_vhost:recover(VHost),
            {ok, Pid};
        Other ->
            Other
    end.