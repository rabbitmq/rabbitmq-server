%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2026 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
%%

-module(rabbit_mgmt_http_vhost_deletion_protection_SUITE).

-include("rabbit_mgmt.hrl").
-include_lib("amqp_client/include/amqp_client.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("rabbitmq_ct_helpers/include/rabbit_mgmt_test.hrl").

-import(rabbit_mgmt_test_util, [http_get/3, http_delete/3, http_post_json/4]).

-compile(nowarn_export_all).
-compile(export_all).

all() ->
    [
     {group, cluster_size_3},
     {group, single_node}
    ].

groups() ->
    [
     {cluster_size_3, [], all_tests()},
     {single_node,    [], all_tests()}
    ].

all_tests() -> [
                protected_virtual_host_cannot_be_deleted,
                virtual_host_can_be_deleted_after_protection_removal,
                protected_virtual_host_is_marked_as_such_in_definition_export
               ].

%% -------------------------------------------------------------------
%% Testsuite setup/teardown.
%% -------------------------------------------------------------------

init_per_group(Group, Config0) ->
    rabbit_ct_helpers:log_environment(),
    inets:start(),
    ClusterSize = case Group of
                      cluster_size_3 -> 3;
                      cluster_size_5 -> 5;
                      single_node -> 1
                  end,
    NodeConf = [{rmq_nodename_suffix, Group},
                {rmq_nodes_count, ClusterSize},
                {tcp_ports_base}],
    Config1 = rabbit_ct_helpers:set_config(Config0, NodeConf),
    rabbit_ct_helpers:run_setup_steps(
        Config1,
      rabbit_ct_broker_helpers:setup_steps() ++
      rabbit_ct_client_helpers:setup_steps()).

end_per_group(_, Config) ->
    inets:stop(),
    Teardown0 = rabbit_ct_client_helpers:teardown_steps(),
    Teardown1 = rabbit_ct_broker_helpers:teardown_steps(),
    Steps = Teardown0 ++ Teardown1,
    rabbit_ct_helpers:run_teardown_steps(Config, Steps).

init_per_testcase(Testcase, Config) ->
    case rabbit_ct_helpers:is_mixed_versions() of
        true ->
            {skip, "not mixed versions compatible"};
        _ ->
            rabbit_ct_helpers:testcase_started(Config, Testcase)
    end.

end_per_testcase(Testcase, Config) ->
    rabbit_ct_helpers:testcase_finished(Config, Testcase).

%% -------------------------------------------------------------------
%% Test cases
%% -------------------------------------------------------------------

-define(DELETION_PROTECTION_KEY, protected_from_deletion).

protected_virtual_host_cannot_be_deleted(Config) ->
    VH = rabbit_data_coercion:to_binary(?FUNCTION_NAME),

    MetaWhenLocked = #{
        ?DELETION_PROTECTION_KEY => true,
        tags => [VH, "locked"]
    },

    MetaWhenUnlocked = #{
        ?DELETION_PROTECTION_KEY => false,
        tags => [VH]
    },

    %% extra care needs to be taken to a delete a potentially protected virtual host
    rabbit_ct_broker_helpers:update_vhost_metadata(Config, VH, MetaWhenUnlocked),
    rabbit_ct_broker_helpers:delete_vhost(Config, VH),
    rabbit_ct_broker_helpers:add_vhost(Config, VH),
    rabbit_ct_broker_helpers:set_full_permissions(Config, VH),

    %% protect the virtual host from deletion
    rabbit_ct_broker_helpers:update_vhost_metadata(Config, VH, MetaWhenLocked),

    %% deletion fails with 412 Precondition Failed
    Path = io_lib:format("/vhosts/~ts", [VH]),
    http_delete(Config, Path, 412),

    rabbit_ct_broker_helpers:update_vhost_metadata(Config, VH, MetaWhenUnlocked),
    http_delete(Config, Path, {group, '2xx'}),

    passed.

virtual_host_can_be_deleted_after_protection_removal(Config) ->
    VH = rabbit_data_coercion:to_binary(?FUNCTION_NAME),

    rabbit_ct_broker_helpers:disable_vhost_protection_from_deletion(Config, VH),
    rabbit_ct_broker_helpers:delete_vhost(Config, VH),
    rabbit_ct_broker_helpers:add_vhost(Config, VH),
    rabbit_ct_broker_helpers:set_full_permissions(Config, VH),

    rabbit_ct_broker_helpers:enable_vhost_protection_from_deletion(Config, VH),

    %% deletion fails with 412 Precondition Failed
    Path = io_lib:format("/vhosts/~ts", [VH]),
    http_delete(Config, Path, 412),

    %% lift the protection
    rabbit_ct_broker_helpers:disable_vhost_protection_from_deletion(Config, VH),
    %% deletion succeeds
    http_delete(Config, Path, {group, '2xx'}),
    %% subsequent deletion responds with a 404 Not Found
    http_delete(Config, Path, ?NOT_FOUND),

    passed.

protected_virtual_host_is_marked_as_such_in_definition_export(Config) ->
    Name = rabbit_data_coercion:to_binary(?FUNCTION_NAME),

    %% extra care needs to be taken to a delete a potentially protected virtual host
    rabbit_ct_broker_helpers:disable_vhost_protection_from_deletion(Config, Name),
    rabbit_ct_broker_helpers:delete_vhost(Config, Name),

    rabbit_ct_broker_helpers:add_vhost(Config, Name),
    rabbit_ct_broker_helpers:set_full_permissions(Config, Name),

    %% protect the virtual host from deletion
    rabbit_ct_broker_helpers:enable_vhost_protection_from_deletion(Config, Name),

    %% Get the definitions
    Definitions = http_get(Config, "/definitions", ?OK),
    ct:pal("Exported definitions:~n~tp~tn", [Definitions]),

    %% Check if vhost definition is correct
    VHosts = maps:get(vhosts, Definitions),
    {value, VHost} = lists:search(fun(VHost) ->
                        maps:get(name, VHost) =:= Name
                     end, VHosts),

    Metadata = maps:get(metadata, VHost),
    ?assertEqual(Name, maps:get(name, VHost)),
    ?assertEqual(Metadata, #{
        description             => <<>>,
        tags                    => [],
        protected_from_deletion => true,
        default_queue_type      => <<"classic">>
    }),

    rabbit_ct_broker_helpers:disable_vhost_protection_from_deletion(Config, Name),
    rabbit_ct_broker_helpers:delete_vhost(Config, Name),

    passed.