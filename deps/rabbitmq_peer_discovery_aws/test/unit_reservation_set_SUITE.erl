%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2025 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
%%

-module(unit_reservation_set_SUITE).

-compile(export_all).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").

all() ->
    [
     get_hostname_name_from_private_DNS,
     get_hostname_name_from_arbitrary_path,
     get_hostname_name_from_private_IP,
     get_hostname_name_from_private_IP_DNS_in_network_interface,
     multiple_paths_extraction,
     single_path_fallback,
     use_private_ip_override_with_multiple_paths,
     complex_path_extraction,
     hostname_paths_takes_precedence_over_hostname_path,
     empty_hostname_paths_falls_back_to_hostname_path,
     use_private_ip_with_both_hostname_path_and_hostname_paths,
     use_private_ip_with_fallback_to_hostname_path,
     out_of_bounds_index_should_not_crash,
     nested_out_of_bounds_should_not_crash,
     zero_index_should_not_crash,
     multiple_paths_with_graceful_degradation,
     empty_network_interface_set_should_not_crash,
     hostname_paths_helper_functions
    ].

init_per_testcase(_TestCase, Config) ->
    ok = reset(),
    Config.

end_per_testcase(_TestCase, _Config) ->
    ok = reset().

%%%
%%% Testcases
%%%

get_hostname_name_from_private_DNS(_Config) ->
    Expectation = ["ip-10-0-16-29.eu-west-1.compute.internal",
                    "ip-10-0-16-31.eu-west-1.compute.internal"],
    ?assertEqual(Expectation,
                    rabbit_peer_discovery_aws:get_hostname_name_from_reservation_set(
                    reservation_set(), [])).

get_hostname_name_from_arbitrary_path(_Config) ->
    os:putenv("AWS_HOSTNAME_PATH", "networkInterfaceSet,1,association,publicDnsName"),
    Expectation = ["ec2-203-0-113-11.eu-west-1.compute.amazonaws.com",
                    "ec2-203-0-113-21.eu-west-1.compute.amazonaws.com"],
    ?assertMatch(Expectation,
                    rabbit_peer_discovery_aws:get_hostname_name_from_reservation_set(
                    reservation_set(), [])).

get_hostname_name_from_private_IP(_Config) ->
    os:putenv("AWS_USE_PRIVATE_IP", "true"),
    Expectation = ["10.0.16.29", "10.0.16.31"],
    ?assertMatch(Expectation,
                    rabbit_peer_discovery_aws:get_hostname_name_from_reservation_set(
                    reservation_set(), [])).

get_hostname_name_from_private_IP_DNS_in_network_interface(_Config) ->
    os:putenv("AWS_HOSTNAME_PATH", "networkInterfaceSet,2,privateIpAddressesSet,1,privateDnsName"),
    Expectation = ["ip-10-0-15-100.eu-west-1.compute.internal",
                    "ip-10-0-16-31.eu-west-1.compute.internal"],
    ?assertMatch(Expectation,
                    rabbit_peer_discovery_aws:get_hostname_name_from_reservation_set(
                    reservation_set(), [])).

multiple_paths_extraction(_Config) ->
    %% Set up multiple hostname paths via application config
    application:set_env(rabbit, cluster_formation, [
        {peer_discovery_aws, [
            {aws_hostname_paths, [["privateDnsName"], ["privateIpAddress"]]}
        ]}
    ]),

    %% Test that multiple hostnames are extracted
    Expected = ["ip-10-0-16-29.eu-west-1.compute.internal", "10.0.16.29",
                "ip-10-0-16-31.eu-west-1.compute.internal", "10.0.16.31"],
    Result = rabbit_peer_discovery_aws:get_hostname_name_from_reservation_set(
                reservation_set(), []),
    ?assertMatch(Expected, Result).

single_path_fallback(_Config) ->
    %% Test fallback to single path when no multiple paths configured
    %% This should use the default single path behavior
    Expected = ["ip-10-0-16-29.eu-west-1.compute.internal",
                "ip-10-0-16-31.eu-west-1.compute.internal"],
    Result = rabbit_peer_discovery_aws:get_hostname_name_from_reservation_set(
                reservation_set(), []),
    ?assertMatch(Expected, Result).

use_private_ip_override_with_multiple_paths(_Config) ->
    %% Test that use_private_ip still works with multiple paths
    os:putenv("AWS_USE_PRIVATE_IP", "true"),
    application:set_env(rabbit, cluster_formation, [
        {peer_discovery_aws, [
            {aws_hostname_paths, [["privateDnsName"], ["privateIpAddress"]]},
            {aws_use_private_ip, true}
        ]}
    ]),

    %% Should get private IPs for privateDnsName path (overridden), plus privateIpAddress path
    %% After deduplication, should have unique IPs only
    Expected = ["10.0.16.29", "10.0.16.31"],
    Result = rabbit_peer_discovery_aws:get_hostname_name_from_reservation_set(
                reservation_set(), []),
    ?assertMatch(Expected, Result).

complex_path_extraction(_Config) ->
    %% Test extraction from network interface paths
    application:set_env(rabbit, cluster_formation, [
        {peer_discovery_aws, [
            {aws_hostname_paths, [["networkInterfaceSet", 1, "association", "publicDnsName"],
                                    ["privateDnsName"]]}
        ]}
    ]),

    %% Should extract from both paths
    Expected = ["ec2-203-0-113-11.eu-west-1.compute.amazonaws.com",
                "ip-10-0-16-29.eu-west-1.compute.internal",
                "ec2-203-0-113-21.eu-west-1.compute.amazonaws.com",
                "ip-10-0-16-31.eu-west-1.compute.internal"],
    Result = rabbit_peer_discovery_aws:get_hostname_name_from_reservation_set(
                reservation_set(), []),
    ?assertMatch(Expected, Result).

hostname_paths_takes_precedence_over_hostname_path(_Config) ->
    %% Test that aws_hostname_paths takes precedence over aws_hostname_path
    application:set_env(rabbit, cluster_formation, [
        {peer_discovery_aws, [
            {aws_hostname_path, ["privateIpAddress"]},  % This should be ignored
            {aws_hostname_paths, [["privateDnsName"]]}  % This should be used
        ]}
    ]),

    %% Should use hostname_paths (privateDnsName), not hostname_path (privateIpAddress)
    Expected = ["ip-10-0-16-29.eu-west-1.compute.internal",
                "ip-10-0-16-31.eu-west-1.compute.internal"],
    Result = rabbit_peer_discovery_aws:get_hostname_name_from_reservation_set(
                reservation_set(), []),
    ?assertMatch(Expected, Result).

empty_hostname_paths_falls_back_to_hostname_path(_Config) ->
    %% Test that empty aws_hostname_paths falls back to aws_hostname_path
    application:set_env(rabbit, cluster_formation, [
        {peer_discovery_aws, [
            {aws_hostname_path, ["privateIpAddress"]},  % This should be used
            {aws_hostname_paths, []}                    % Empty, should fall back
        ]}
    ]),

    %% Should fall back to hostname_path (privateIpAddress)
    Expected = ["10.0.16.29", "10.0.16.31"],
    Result = rabbit_peer_discovery_aws:get_hostname_name_from_reservation_set(
                reservation_set(), []),
    ?assertMatch(Expected, Result).

use_private_ip_with_both_hostname_path_and_hostname_paths(_Config) ->
    %% Test use_private_ip override when both configurations exist
    os:putenv("AWS_USE_PRIVATE_IP", "true"),
    application:set_env(rabbit, cluster_formation, [
        {peer_discovery_aws, [
            {aws_hostname_path, ["privateDnsName"]},                    % Should be ignored
            {aws_hostname_paths, [["privateDnsName"], ["privateIpAddress"]]}, % Should be used
            {aws_use_private_ip, true}
        ]}
    ]),

    %% Should use hostname_paths with use_private_ip override applied
    %% privateDnsName -> privateIpAddress, privateIpAddress stays the same
    %% After deduplication: only unique IPs
    Expected = ["10.0.16.29", "10.0.16.31"],
    Result = rabbit_peer_discovery_aws:get_hostname_name_from_reservation_set(
                reservation_set(), []),
    ?assertMatch(Expected, Result).

use_private_ip_with_fallback_to_hostname_path(_Config) ->
    %% Test use_private_ip override when falling back to single hostname_path
    os:putenv("AWS_USE_PRIVATE_IP", "true"),
    application:set_env(rabbit, cluster_formation, [
        {peer_discovery_aws, [
            {aws_hostname_path, ["privateDnsName"]},    % Should be used with override
            {aws_hostname_paths, []},                   % Empty, falls back
            {aws_use_private_ip, true}
        ]}
    ]),

    %% Should fall back to hostname_path with use_private_ip override
    %% privateDnsName -> privateIpAddress
    Expected = ["10.0.16.29", "10.0.16.31"],
    Result = rabbit_peer_discovery_aws:get_hostname_name_from_reservation_set(
                reservation_set(), []),
    ?assertMatch(Expected, Result).

out_of_bounds_index_should_not_crash(_Config) ->
    os:putenv("AWS_HOSTNAME_PATH", "networkInterfaceSet,3,privateIpAddress"),
    Result = rabbit_peer_discovery_aws:get_hostname_name_from_reservation_set(reservation_set(), []),
    ?assertMatch([], Result).

nested_out_of_bounds_should_not_crash(_Config) ->
    os:putenv("AWS_HOSTNAME_PATH", "networkInterfaceSet,2,privateIpAddressesSet,5,privateIpAddress"),
    Result = rabbit_peer_discovery_aws:get_hostname_name_from_reservation_set(reservation_set(), []),
    ?assertMatch([], Result).

zero_index_should_not_crash(_Config) ->
    os:putenv("AWS_HOSTNAME_PATH", "networkInterfaceSet,0,privateIpAddress"),
    Result = rabbit_peer_discovery_aws:get_hostname_name_from_reservation_set(reservation_set(), []),
    ?assertMatch([], Result).

multiple_paths_with_graceful_degradation(_Config) ->
    application:set_env(rabbit, cluster_formation, [
        {peer_discovery_aws, [
            {aws_hostname_paths, [
                ["privateDnsName"],                              % Valid
                ["networkInterfaceSet", 10, "privateIpAddress"], % Out of bounds
                ["privateIpAddress"]                             % Valid
            ]}
        ]}
    ]),
    Expected = ["ip-10-0-16-29.eu-west-1.compute.internal", "10.0.16.29",
                "ip-10-0-16-31.eu-west-1.compute.internal", "10.0.16.31"],
    Result = rabbit_peer_discovery_aws:get_hostname_name_from_reservation_set(reservation_set(), []),
    ?assertMatch(Expected, Result).

empty_network_interface_set_should_not_crash(_Config) ->
    EmptyNetworkReservationSet = [
        {"item", [{"reservationId","r-test"},
                    {"instancesSet",
                    [{"item",
                    [{"instanceId","i-test"},
                        {"privateDnsName", "test-host.compute.internal"},
                        {"networkInterfaceSet", []},
                        {"privateIpAddress","10.0.1.1"}]}]}]}
    ],
    os:putenv("AWS_HOSTNAME_PATH", "networkInterfaceSet,1,privateIpAddress"),
    Result = rabbit_peer_discovery_aws:get_hostname_name_from_reservation_set(EmptyNetworkReservationSet, []),
    ?assertMatch([], Result).

hostname_paths_helper_functions(_Config) ->
    %% Test extract_unique_hostnames function
    Paths = [["privateDnsName"], ["privateIpAddress"]],
    %% Extract items correctly from reservation set structure - reservation_set() returns a list
    ReservationSet = reservation_set(),
    [{"item", FirstReservation}] = lists:sublist(ReservationSet, 1),
    InstancesSet = proplists:get_value("instancesSet", FirstReservation),
    Items = [Item || {"item", Item} <- InstancesSet],

    Hostnames = rabbit_peer_discovery_aws:extract_unique_hostnames(Paths, Items),
    ?assertEqual(2, length(Hostnames)), % Should extract 2 unique hostnames for the first instance

    %% Test with duplicates - should be automatically removed
    DuplicatePaths = [["privateDnsName"], ["privateDnsName"], ["privateIpAddress"]],
    UniqueHostnames = rabbit_peer_discovery_aws:extract_unique_hostnames(DuplicatePaths, Items),
    ?assertEqual(2, length(UniqueHostnames)), % Should still be 2 unique hostnames

    %% Test empty path handling - should return empty string gracefully
    EmptyPath = [],
    EmptyResult = rabbit_peer_discovery_aws:get_hostname(EmptyPath, [{"test", "value"}]),
    ?assertMatch("", EmptyResult),

    %% Test invalid path handling - should return empty string gracefully
    InvalidPath = [invalid_atom],
    InvalidResult = rabbit_peer_discovery_aws:get_hostname(InvalidPath, [{"test", "value"}]),
    ?assertMatch("", InvalidResult),

    %% Test get_hostname_paths function behavior
    %% Test default behavior (no configuration)
    DefaultPaths = rabbit_peer_discovery_aws:get_hostname_paths(),
    ?assertMatch([["privateDnsName"]], DefaultPaths),

    %% Test with multiple paths configured
    application:set_env(rabbit, cluster_formation, [
        {peer_discovery_aws, [
            {aws_hostname_paths, [["privateDnsName"], ["privateIpAddress"]]}
        ]}
    ]),
    MultiplePaths = rabbit_peer_discovery_aws:get_hostname_paths(),
    ?assertMatch([["privateDnsName"], ["privateIpAddress"]], MultiplePaths).

%%%
%%% Implementation
%%%

reset() ->
    ok = application:unset_env(rabbit, cluster_formation),
    true = os:unsetenv("AWS_HOSTNAME_PATH"),
    true = os:unsetenv("AWS_HOSTNAME_PATHS"),
    true = os:unsetenv("AWS_USE_PRIVATE_IP"),
    ok.

reservation_set() ->
    [{"item", [{"reservationId","r-006cfdbf8d04c5f01"},
               {"ownerId","248536293561"},
               {"groupSet",[]},
               {"instancesSet",
                [{"item",
                  [{"instanceId","i-0c6d048641f09cad2"},
                   {"imageId","ami-ef4c7989"},
                   {"instanceState",
                    [{"code","16"},{"name","running"}]},
                   {"privateDnsName",
                    "ip-10-0-16-29.eu-west-1.compute.internal"},
                   {"dnsName",[]},
                   {"instanceType","c4.large"},
                   {"launchTime","2017-04-07T12:05:10"},
                   {"subnetId","subnet-61ff660"},
                   {"vpcId","vpc-4fe1562b"},
                   {"networkInterfaceSet", [
                    {"item",
                    [{"attachment", [{"deviceIndex", "1"}]},
                     {"association",
                     [{"publicIp","203.0.113.12"},
                      {"publicDnsName",
                       "ec2-203-0-113-12.eu-west-1.compute.amazonaws.com"},
                      {"ipOwnerId","amazon"}]},
                     {"privateIpAddressesSet", [
                        {"item", [
                            {"privateIpAddress", "10.0.15.101"},
                            {"privateDnsName", "ip-10-0-15-101.eu-west-1.compute.internal"},
                            {"primary", "false"}
                        ]},
                        {"item", [
                            {"privateIpAddress", "10.0.15.100"},
                            {"privateDnsName", "ip-10-0-15-100.eu-west-1.compute.internal"},
                            {"primary", "true"}
                        ]}
                     ]}]},
                    {"item",
                    [{"attachment", [{"deviceIndex", "0"}]},
                     {"association",
                     [{"publicIp","203.0.113.11"},
                      {"publicDnsName",
                       "ec2-203-0-113-11.eu-west-1.compute.amazonaws.com"},
                      {"ipOwnerId","amazon"}]}]}]},
                   {"privateIpAddress","10.0.16.29"}]}]}]},
     {"item", [{"reservationId","r-006cfdbf8d04c5f01"},
               {"ownerId","248536293561"},
               {"groupSet",[]},
               {"instancesSet",
                [{"item",
                  [{"instanceId","i-1c6d048641f09cad2"},
                   {"imageId","ami-af4c7989"},
                   {"instanceState",
                    [{"code","16"},{"name","running"}]},
                   {"privateDnsName",
                    "ip-10-0-16-31.eu-west-1.compute.internal"},
                   {"dnsName",[]},
                   {"instanceType","c4.large"},
                   {"launchTime","2017-04-07T12:05:10"},
                   {"subnetId","subnet-61ff660"},
                   {"vpcId","vpc-4fe1562b"},
                   {"networkInterfaceSet", [
                    {"item",
                    [{"attachment", [{"deviceIndex", "0"}]},
                     {"association",
                     [{"publicIp","203.0.113.21"},
                      {"publicDnsName",
                       "ec2-203-0-113-21.eu-west-1.compute.amazonaws.com"},
                      {"ipOwnerId","amazon"}]}]},
                    {"item",
                    [{"attachment", [{"deviceIndex", "1"}]},
                     {"association",
                     [{"publicIp","203.0.113.22"},
                      {"publicDnsName",
                       "ec2-203-0-113-22.eu-west-1.compute.amazonaws.com"},
                      {"ipOwnerId","amazon"}]},
                     {"privateIpAddressesSet", [
                        {"item", [
                            {"privateIpAddress", "10.0.16.31"},
                            {"privateDnsName", "ip-10-0-16-31.eu-west-1.compute.internal"},
                            {"primary", "true"}
                        ]}
                     ]}]}]},
                   {"privateIpAddress","10.0.16.31"}]}]}]}].
