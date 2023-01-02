%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2017-2023 VMware, Inc. or its affiliates.  All rights reserved.
%%

%% This module is a wrapper around vhost supervisor to
%% provide exactly once restart semantics.

-module(rabbit_vhost_sup_wrapper).

-include_lib("rabbit_common/include/rabbit.hrl").

-behaviour(supervisor).
-export([init/1]).
-export([start_link/1]).
-export([start_vhost_sup/1]).

start_link(VHost) ->
    case rabbit_vhost_sup_sup:get_vhost_sup(VHost) of
        {ok, Pid} ->
            {error, {already_started, Pid}};
        {error, _} ->
            supervisor:start_link(?MODULE, [VHost])
    end.

init([VHost]) ->
    %% 2 restarts in 5 minutes. One per message store.
    {ok,
        {#{strategy => one_for_all, intensity => 2, period => 300}, [
            %% rabbit_vhost_sup is an empty supervisor container for
            %% all data processes.
            #{
                id => rabbit_vhost_sup,
                start => {rabbit_vhost_sup_wrapper, start_vhost_sup, [VHost]},
                restart => permanent,
                shutdown => infinity,
                type => supervisor,
                modules => [rabbit_vhost_sup]
            },
            %% rabbit_vhost_process is a vhost identity process, which
            %% is responsible for data recovery and vhost aliveness status.
            %% See the module comments for more info.
            #{
                id => rabbit_vhost_process,
                start => {rabbit_vhost_process, start_link, [VHost]},
                restart => permanent,
                shutdown => ?WORKER_WAIT,
                type => worker,
                modules => [rabbit_vhost_process]
            }
        ]}}.

start_vhost_sup(VHost) ->
    case rabbit_vhost_sup:start_link(VHost) of
        {ok, Pid} ->
            %% Save vhost sup record with wrapper pid and vhost sup pid.
            ok = rabbit_vhost_sup_sup:save_vhost_sup(VHost, self(), Pid),
            {ok, Pid};
        Other ->
            Other
    end.
