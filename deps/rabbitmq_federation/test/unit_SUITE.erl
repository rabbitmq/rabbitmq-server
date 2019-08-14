%% The contents of this file are subject to the Mozilla Public License
%% Version 1.1 (the "License"); you may not use this file except in
%% compliance with the License. You may obtain a copy of the License at
%% https://www.mozilla.org/MPL/
%%
%% Software distributed under the License is distributed on an "AS IS"
%% basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See the
%% License for the specific language governing rights and limitations
%% under the License.
%%
%% The Original Code is RabbitMQ.
%%
%% The Initial Developer of the Original Code is GoPivotal, Inc.
%% Copyright (c) 2019 Pivotal Software, Inc.  All rights reserved.
%%

-module(unit_SUITE).
-include_lib("common_test/include/ct.hrl").
-include_lib("amqp_client/include/amqp_client.hrl").
-include("rabbit_federation.hrl").

-compile(export_all).
 
all() -> [obfuscate_upstream, obfuscate_upstream_params_network, obfuscate_upstream_params_direct].
 
init_per_suite(Config) ->
    application:ensure_all_started(credentials_obfuscation),
    Config.

end_per_suite(Config) ->
    Config.

obfuscate_upstream(_Config) ->
    Upstream = #upstream{uris = [<<"amqp://guest:password@localhost">>]},
    ObfuscatedUpstream = rabbit_federation_util:obfuscate_upstream(Upstream),
    Upstream = rabbit_federation_util:deobfuscate_upstream(ObfuscatedUpstream),
    ok.

obfuscate_upstream_params_network(_Config) ->
    UpstreamParams = #upstream_params{
        uri = <<"amqp://guest:password@localhost">>, 
        params = #amqp_params_network{password = <<"password">>}
    },
    ObfuscatedUpstreamParams = rabbit_federation_util:obfuscate_upstream_params(UpstreamParams),
    UpstreamParams = rabbit_federation_util:deobfuscate_upstream_params(ObfuscatedUpstreamParams),
    ok.

 obfuscate_upstream_params_direct(_Config) ->
    UpstreamParams = #upstream_params{
        uri = <<"amqp://guest:password@localhost">>, 
        params = #amqp_params_direct{password = <<"password">>}
    },
    ObfuscatedUpstreamParams = rabbit_federation_util:obfuscate_upstream_params(UpstreamParams),
    UpstreamParams = rabbit_federation_util:deobfuscate_upstream_params(ObfuscatedUpstreamParams),
    ok.