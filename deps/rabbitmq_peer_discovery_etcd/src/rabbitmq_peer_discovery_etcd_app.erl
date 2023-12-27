%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2023 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries.  All rights reserved.
%%

-module(rabbitmq_peer_discovery_etcd_app).

%%
%% API
%%

-behaviour(application).
-export([start/2, stop/1, prep_stop/1]).

start(_Type, _StartArgs) ->
    %% The tree had been started earlier, see rabbit_peer_discovery_etcd:init/0. MK.
    rabbitmq_peer_discovery_etcd_sup:start_link().

prep_stop(_State) ->
    try
        rabbitmq_peer_discovery_etcd_v3_client:unregister()
    catch
        _:_ -> ok
    end.

stop(_State) ->
    ok.
