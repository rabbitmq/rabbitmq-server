%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2026 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
%%

-module(rabbit_peer_discovery_util_SUITE).

-compile(export_all).

-include_lib("eunit/include/eunit.hrl").

all() ->
    [
     redact_secrets_in_a_map,
     redact_secrets_in_a_proplist,
     redact_secrets_in_nested_cluster_formation_proplist,
     redact_ssl_options_in_nested_cluster_formation_proplist,
     redact_secrets_leaves_non_secret_values_untouched
    ].

redact_secrets_in_a_map(_Config) ->
    Map = #{consul_acl_token => "letmein",
            aws_secret_key   => "letmein",
            consul_host      => "localhost"},
    Redacted = rabbit_peer_discovery_util:redact_secrets(Map),
    ?assertEqual("...", maps:get(consul_acl_token, Redacted)),
    ?assertEqual("...", maps:get(aws_secret_key, Redacted)),
    ?assertEqual("localhost", maps:get(consul_host, Redacted)).

redact_secrets_in_a_proplist(_Config) ->
    Proplist = [{consul_host, "localhost"},
                {consul_acl_token, "letmein"},
                {etcd_password, "letmein"}],
    Redacted = rabbit_peer_discovery_util:redact_secrets(Proplist),
    ?assertEqual("localhost", proplists:get_value(consul_host, Redacted)),
    ?assertEqual("...", proplists:get_value(consul_acl_token, Redacted)),
    ?assertEqual("...", proplists:get_value(etcd_password, Redacted)).

redact_secrets_in_nested_cluster_formation_proplist(_Config) ->
    %% Shape of `application:get_env(rabbit, cluster_formation)`.
    ClusterFormation = [{peer_discovery_consul,
                         [{consul_host, "localhost"},
                          {consul_acl_token, "letmein"}]},
                        {peer_discovery_aws,
                         [{aws_access_key, "AKIA..."},
                          {aws_secret_key, "letmein"}]}],
    [{peer_discovery_consul, ConsulConfig},
     {peer_discovery_aws, AwsConfig}] =
        rabbit_peer_discovery_util:redact_secrets(ClusterFormation),
    ?assertEqual("localhost", proplists:get_value(consul_host, ConsulConfig)),
    ?assertEqual("...", proplists:get_value(consul_acl_token, ConsulConfig)),
    ?assertEqual("...", proplists:get_value(aws_access_key, AwsConfig)),
    ?assertEqual("...", proplists:get_value(aws_secret_key, AwsConfig)).

redact_ssl_options_in_nested_cluster_formation_proplist(_Config) ->
    %% Shape of `application:get_env(rabbit, cluster_formation)`.
    ClusterFormation = [{peer_discovery_etcd,
                         [{etcd_host, "localhost"},
                          {ssl_options, [{key, "private-key"},
                                         {password, "letmein"}]}]}],
    [{peer_discovery_etcd, EtcdConfig}] =
        rabbit_peer_discovery_util:redact_secrets(ClusterFormation),
    ?assertEqual("localhost", proplists:get_value(etcd_host, EtcdConfig)),
    ?assertEqual("...", proplists:get_value(ssl_options, EtcdConfig)).

redact_secrets_leaves_non_secret_values_untouched(_Config) ->
    %% List-valued config (e.g. Consul service tags) must not be mistaken
    %% for a nested proplist and must be returned unchanged.
    Proplist = [{consul_svc_tags, ["rabbitmq", "peer-discovery"]}],
    Redacted = rabbit_peer_discovery_util:redact_secrets(Proplist),
    ?assertEqual(["rabbitmq", "peer-discovery"],
                  proplists:get_value(consul_svc_tags, Redacted)).
