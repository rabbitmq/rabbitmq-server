%%   The contents of this file are subject to the Mozilla Public License
%%   Version 1.1 (the "License"); you may not use this file except in
%%   compliance with the License. You may obtain a copy of the License at
%%   http://www.mozilla.org/MPL/
%%
%%   Software distributed under the License is distributed on an "AS IS"
%%   basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See the
%%   License for the specific language governing rights and limitations
%%   under the License.
%%
%%   The Original Code is RabbitMQ Management Console.
%%
%%   The Initial Developer of the Original Code is GoPivotal, Inc.
%%   Copyright (c) 2007-2020 VMware, Inc. or its affiliates.  All rights reserved.
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
