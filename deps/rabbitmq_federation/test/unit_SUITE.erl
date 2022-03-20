%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2019-2022 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(unit_SUITE).
-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("amqp_client/include/amqp_client.hrl").
-include("rabbit_federation.hrl").

-compile(export_all).

all() -> [
    obfuscate_upstream,
    obfuscate_upstream_params_network,
    obfuscate_upstream_params_network_with_char_list_password_value,
    obfuscate_upstream_params_direct
].

init_per_suite(Config) ->
    application:ensure_all_started(credentials_obfuscation),
    Config.

end_per_suite(Config) ->
    Config.

obfuscate_upstream(_Config) ->
    Upstream = #upstream{uris = [<<"amqp://guest:password@localhost">>]},
    ObfuscatedUpstream = rabbit_federation_util:obfuscate_upstream(Upstream),
    ?assertEqual(Upstream, rabbit_federation_util:deobfuscate_upstream(ObfuscatedUpstream)),
    ok.

obfuscate_upstream_params_network(_Config) ->
    UpstreamParams = #upstream_params{
        uri = <<"amqp://guest:password@localhost">>,
        params = #amqp_params_network{password = <<"password">>}
    },
    ObfuscatedUpstreamParams = rabbit_federation_util:obfuscate_upstream_params(UpstreamParams),
    ?assertEqual(UpstreamParams, rabbit_federation_util:deobfuscate_upstream_params(ObfuscatedUpstreamParams)),
    ok.

obfuscate_upstream_params_network_with_char_list_password_value(_Config) ->
    Input = #upstream_params{
        uri = <<"amqp://guest:password@localhost">>,
        params = #amqp_params_network{password = "password"}
    },
    Output = #upstream_params{
        uri = <<"amqp://guest:password@localhost">>,
        params = #amqp_params_network{password = <<"password">>}
    },
    ObfuscatedUpstreamParams = rabbit_federation_util:obfuscate_upstream_params(Input),
    ?assertEqual(Output, rabbit_federation_util:deobfuscate_upstream_params(ObfuscatedUpstreamParams)),
    ok.

 obfuscate_upstream_params_direct(_Config) ->
    UpstreamParams = #upstream_params{
        uri = <<"amqp://guest:password@localhost">>,
        params = #amqp_params_direct{password = <<"password">>}
    },
    ObfuscatedUpstreamParams = rabbit_federation_util:obfuscate_upstream_params(UpstreamParams),
    ?assertEqual(UpstreamParams, rabbit_federation_util:deobfuscate_upstream_params(ObfuscatedUpstreamParams)),
    ok.
