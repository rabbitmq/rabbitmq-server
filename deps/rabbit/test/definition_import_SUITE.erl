%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2022 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(definition_import_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("rabbit_common/include/rabbit.hrl").
-include_lib("eunit/include/eunit.hrl").

-compile(export_all).

all() ->
    [
     %% uses rabbit.load_definitions
     {group, boot_time_import_using_classic_source},
     %% uses rabbit.definitions with import_backend set to local_filesystem
     {group, boot_time_import_using_modern_local_filesystem_source},
     {group, boot_time_import_using_public_https_source},
     {group, skip_if_unchanged},
     {group, roundtrip},
     {group, import_on_a_running_node}
    ].

groups() ->
    [
     {import_on_a_running_node, [], [
                               %% Note: to make it easier to see which case failed,
                               %% these are intentionally not folded into a single case.
                               %% If generation becomes an alternative worth considering for these tests,
                               %% we'll just add a case that drives PropEr.
                               import_case1,
                               import_case2,
                               import_case3,
                               import_case4,
                               import_case5,
                               import_case6,
                               import_case7,
                               import_case8,
                               import_case9,
                               import_case10,
                               import_case11,
                               import_case12,
                               import_case13,
                               import_case14,
                               import_case15,
                               import_case16,
                               import_case17,
                               import_case18,
                               import_case19
                              ]},
        
        {boot_time_import_using_classic_source, [], [
            import_on_a_booting_node_using_classic_local_source
        ]},

        {boot_time_import_using_modern_local_filesystem_source, [], [
            import_on_a_booting_node_using_modern_local_filesystem_source
        ]},

        {boot_time_import_using_public_https_source, [], [
            import_on_a_booting_node_using_public_https_source
        ]},

        {roundtrip, [], [
            export_import_round_trip_case1,
            export_import_round_trip_case2
        ]},

        {skip_if_unchanged, [], [
            %% these all must import the same definition file
            import_on_a_booting_node_using_skip_if_unchanged,
            import_case5
        ]}
    ].

%% -------------------------------------------------------------------
%% Test suite setup/teardown.
%% -------------------------------------------------------------------

init_per_suite(Config) ->
    rabbit_ct_helpers:log_environment(),
    inets:start(),
    Config.
end_per_suite(Config) ->
    Config.

init_per_group(boot_time_import_using_classic_source = Group, Config) ->
    CasePath = filename:join(?config(data_dir, Config), "case5.json"),
    Config1 = rabbit_ct_helpers:set_config(Config, [
        {rmq_nodename_suffix, Group},
        {rmq_nodes_count, 1}
      ]),
    Config2 = rabbit_ct_helpers:merge_app_env(Config1,
      {rabbit, [
          {load_definitions, CasePath}
      ]}),
    rabbit_ct_helpers:run_setup_steps(Config2, rabbit_ct_broker_helpers:setup_steps());
%% same as the classic source semantically but uses a different configuration structure
init_per_group(boot_time_import_using_modern_local_filesystem_source = Group, Config) ->
    CasePath = filename:join(?config(data_dir, Config), "case5.json"),
    Config1 = rabbit_ct_helpers:set_config(Config, [
        {rmq_nodename_suffix, Group},
        {rmq_nodes_count, 1}
      ]),
    Config2 = rabbit_ct_helpers:merge_app_env(Config1,
      {rabbit, [
          {definitions, [
              {import_backend, rabbit_definitions_import_local_filesystem},
              {local_path,     CasePath}
          ]}
      ]}),
    rabbit_ct_helpers:run_setup_steps(Config2, rabbit_ct_broker_helpers:setup_steps());
%% same as the classic source semantically, uses skip_if_unchanged
init_per_group(skip_if_unchanged = Group, Config) ->
    CasePath = filename:join(?config(data_dir, Config), "case5.json"),
    Config1 = rabbit_ct_helpers:set_config(Config, [
        {rmq_nodename_suffix, Group},
        {rmq_nodes_count, 1}
      ]),
    Config2 = rabbit_ct_helpers:merge_app_env(Config1,
      {rabbit, [
          {definitions, [
              {import_backend,    rabbit_definitions_import_local_filesystem},
              {local_path,        CasePath},
              {skip_if_unchanged, true}
          ]}
      ]}),
    rabbit_ct_helpers:run_setup_steps(Config2, rabbit_ct_broker_helpers:setup_steps());
init_per_group(boot_time_import_using_public_https_source = Group, Config) ->
    Config1 = rabbit_ct_helpers:set_config(Config, [
        {rmq_nodename_suffix, Group},
        {rmq_nodes_count, 1}
      ]),
    Config2 = rabbit_ct_helpers:merge_app_env(Config1,
      {rabbit, [
          {definitions, [
              {import_backend, rabbit_definitions_import_https},
              {url,             "https://gist.githubusercontent.com/michaelklishin/e73b0114728d9391425d0644304f264a/raw/f15642771f099c60b6fa93f75d46a4246bb47c45/upstream.definitions.json"},
              {ssl_options,  [
                   {log_level, error},
                   {secure_renegotiate, true},
                   {versions, ['tlsv1.2']},
                   {ciphers, [
                    "ECDHE-ECDSA-AES256-GCM-SHA384",
                    "ECDHE-RSA-AES256-GCM-SHA384",
                    "ECDH-ECDSA-AES256-GCM-SHA384",
                    "ECDH-RSA-AES256-GCM-SHA384",
                    "DHE-RSA-AES256-GCM-SHA384",
                    "DHE-DSS-AES256-GCM-SHA384",
                    "ECDHE-ECDSA-AES128-GCM-SHA256",
                    "ECDHE-RSA-AES128-GCM-SHA256",
                    "ECDH-ECDSA-AES128-GCM-SHA256",
                    "ECDH-RSA-AES128-GCM-SHA256",
                    "DHE-RSA-AES128-GCM-SHA256",
                    "DHE-DSS-AES128-GCM-SHA256"
                    ]}
            ]}
          ]}
      ]}),
    rabbit_ct_helpers:run_setup_steps(Config2, rabbit_ct_broker_helpers:setup_steps());
init_per_group(Group, Config) ->
    Config1 = rabbit_ct_helpers:set_config(Config, [
                                                    {rmq_nodename_suffix, Group}
                                                   ]),
    rabbit_ct_helpers:run_setup_steps(Config1, rabbit_ct_broker_helpers:setup_steps()).

end_per_group(_, Config) ->
    rabbit_ct_helpers:run_teardown_steps(Config, rabbit_ct_broker_helpers:teardown_steps()).

init_per_testcase(Testcase, Config) ->
    rabbit_ct_helpers:testcase_started(Config, Testcase).

end_per_testcase(Testcase, Config) ->
    rabbit_ct_helpers:testcase_finished(Config, Testcase).

%%
%% Tests
%%

import_case1(Config) -> import_file_case(Config, "case1").
import_case2(Config) -> import_file_case(Config, "case2").
import_case3(Config) -> import_file_case(Config, "case3").
import_case4(Config) -> import_file_case(Config, "case4").
import_case6(Config) -> import_file_case(Config, "case6").
import_case7(Config) -> import_file_case(Config, "case7").
import_case8(Config) -> import_file_case(Config, "case8").

import_case9(Config) -> import_from_directory_case(Config, "case9").

import_case10(Config) -> import_from_directory_case_fails(Config, "case10").

import_case5(Config) ->
    import_file_case(Config, "case5"),
    ?assertEqual(rabbit_ct_broker_helpers:rpc(Config, 0,
                                              rabbit_runtime_parameters, value_global,
                                              [mqtt_port_to_vhost_mapping]),
                 %% expect a proplist, see rabbitmq/rabbitmq-management#528
                 [{<<"1883">>,<<"/">>},
                  {<<"1884">>,<<"vhost2">>}]).

import_case11(Config) -> import_file_case(Config, "case11").
import_case12(Config) -> import_invalid_file_case(Config, "failing_case12").

import_case13(Config) ->
    case rabbit_ct_broker_helpers:enable_feature_flag(Config, quorum_queue) of
        ok ->
            import_file_case(Config, "case13"),
            VHost = <<"/">>,
            QueueName = <<"definitions.import.case13.qq.1">>,
            QueueIsImported =
            fun () ->
                    case queue_lookup(Config, VHost, QueueName) of
                        {ok, _} -> true;
                        _       -> false
                    end
            end,
            rabbit_ct_helpers:await_condition(QueueIsImported, 20000),
            {ok, Q} = queue_lookup(Config, VHost, QueueName),

            %% see rabbitmq/rabbitmq-server#2400, rabbitmq/rabbitmq-server#2426
            ?assert(amqqueue:is_quorum(Q)),
            ?assertEqual([{<<"x-max-length">>, long, 991},
                          {<<"x-queue-type">>, longstr, <<"quorum">>}],
                         amqqueue:get_arguments(Q));
        Skip ->
            Skip
    end.

import_case14(Config) -> import_file_case(Config, "case14").
%% contains a user with tags as a list
import_case15(Config) -> import_file_case(Config, "case15").
%% contains a virtual host with tags
import_case16(Config) ->
    case rabbit_ct_helpers:is_mixed_versions() of
      false ->
        case rabbit_ct_broker_helpers:enable_feature_flag(Config, virtual_host_metadata) of
            ok ->
                import_file_case(Config, "case16"),
                VHost = <<"tagged">>,
                VHostIsImported =
                fun () ->
                        case vhost_lookup(Config, VHost) of
                            {error, {no_such_vhosts, _}} -> false;
                            _       -> true
                        end
                end,
                rabbit_ct_helpers:await_condition(VHostIsImported, 20000),
                VHostRec = vhost_lookup(Config, VHost),
                ?assertEqual(<<"A case16 description">>, vhost:get_description(VHostRec)),
                ?assertEqual([multi_dc_replication,ab,cde], vhost:get_tags(VHostRec)),

                ok;
            Skip ->
                Skip
        end;
      _ ->
        %% skip the test in mixed version mode
        {skip, "Should not run in mixed version environments"}
    end.

import_case17(Config) -> import_invalid_file_case(Config, "failing_case17").

import_case18(Config) ->
    case rabbit_ct_helpers:is_mixed_versions() of
      false ->
        case rabbit_ct_broker_helpers:enable_feature_flag(Config, user_limits) of
            ok ->
                import_file_case(Config, "case18"),
                User = <<"limited_guest">>,
                UserIsImported =
                    fun () ->
                            case user_lookup(Config, User) of
                                {error, not_found} -> false;
                                _       -> true
                            end
                    end,
                rabbit_ct_helpers:await_condition(UserIsImported, 20000),
                {ok, UserRec} = user_lookup(Config, User),
                ?assertEqual(#{<<"max-connections">> => 2}, internal_user:get_limits(UserRec)),
                ok;
            Skip ->
                Skip
        end;
      _ ->
        %% skip the test in mixed version mode
        {skip, "Should not run in mixed version environments"}
    end.

import_case19(Config) ->
    case rabbit_ct_helpers:is_mixed_versions() of
        false ->
            import_invalid_file_case(Config, "failing_case19");
        true ->
            %% skip the test in mixed version mode
            {skip, "Should not run in mixed version environments"}
    end.

export_import_round_trip_case1(Config) ->
    case rabbit_ct_helpers:is_mixed_versions() of
      false ->
        %% case 6 has runtime parameters that do not depend on any plugins
        import_file_case(Config, "case6"),
        Defs = export(Config),
        import_raw(Config, rabbit_json:encode(Defs));
      _ ->
        %% skip the test in mixed version mode
        {skip, "Should not run in mixed version environments"}
    end.

export_import_round_trip_case2(Config) ->
    case rabbit_ct_helpers:is_mixed_versions() of
      false ->
        import_file_case(Config, "case9", "case9a"),
        Defs = export(Config),
        import_parsed(Config, Defs);
      _ ->
        %% skip the test in mixed version mode
        {skip, "Should not run in mixed version environments"}
    end.

import_on_a_booting_node_using_classic_local_source(Config) ->
    %% see case5.json
    VHost = <<"vhost2">>,
    %% verify that vhost2 eventually starts
    case rabbit_ct_broker_helpers:rpc(Config, 0, rabbit_vhost, await_running_on_all_nodes, [VHost, 3000]) of
        ok -> ok;
        {error, timeout} -> ct:fail("virtual host ~p was not imported on boot", [VHost])
    end.

import_on_a_booting_node_using_modern_local_filesystem_source(Config) ->
    %% see case5.json
    VHost = <<"vhost2">>,
    %% verify that vhost2 eventually starts
    case rabbit_ct_broker_helpers:rpc(Config, 0, rabbit_vhost, await_running_on_all_nodes, [VHost, 3000]) of
        ok -> ok;
        {error, timeout} -> ct:fail("virtual host ~p was not imported on boot", [VHost])
    end.

import_on_a_booting_node_using_public_https_source(Config) ->
    VHost = <<"bunny_testbed">>,
    %% verify that virtual host eventually starts
    case rabbit_ct_broker_helpers:rpc(Config, 0, rabbit_vhost, await_running_on_all_nodes, [VHost, 3000]) of
        ok -> ok;
        {error, timeout} -> ct:fail("virtual host ~p was not imported on boot", [VHost])
    end.

import_on_a_booting_node_using_skip_if_unchanged(Config) ->
    %% see case5.json
    VHost = <<"vhost2">>,
    %% verify that vhost2 eventually starts
    case rabbit_ct_broker_helpers:rpc(Config, 0, rabbit_vhost, await_running_on_all_nodes, [VHost, 3000]) of
        ok -> ok;
        {error, timeout} -> ct:fail("virtual host ~p was not imported on boot", [VHost])
    end.

%%
%% Implementation
%%

import_file_case(Config, CaseName) ->
    CasePath = filename:join([
        ?config(data_dir, Config),
        CaseName ++ ".json"
    ]),
    rabbit_ct_broker_helpers:rpc(Config, 0, ?MODULE, run_import_case, [CasePath]),
    ok.

import_file_case(Config, Subdirectory, CaseName) ->
    CasePath = filename:join([
        ?config(data_dir, Config),
        Subdirectory,
        CaseName ++ ".json"
    ]),
    rabbit_ct_broker_helpers:rpc(Config, 0, ?MODULE, run_import_case, [CasePath]),
    ok.

import_invalid_file_case(Config, CaseName) ->
    CasePath = filename:join(?config(data_dir, Config), CaseName ++ ".json"),
    rabbit_ct_broker_helpers:rpc(Config, 0, ?MODULE, run_invalid_import_case, [CasePath]),
    ok.

import_from_directory_case(Config, CaseName) ->
    import_from_directory_case_expect(Config, CaseName, ok).

import_from_directory_case_fails(Config, CaseName) ->
    import_from_directory_case_expect(Config, CaseName, error).

import_from_directory_case_expect(Config, CaseName, Expected) ->
    CasePath = filename:join(?config(data_dir, Config), CaseName),
    ?assert(filelib:is_dir(CasePath)),
    rabbit_ct_broker_helpers:rpc(Config, 0,
                                 ?MODULE, run_directory_import_case,
                                 [CasePath, Expected]),
    ok.

import_raw(Config, Body) ->
    case rabbit_ct_broker_helpers:rpc(Config, 0, rabbit_definitions, import_raw, [Body]) of
        ok -> ok;
        {error, E} ->
            ct:pal("Import of JSON definitions ~p failed: ~p~n", [Body, E]),
            ct:fail({failure, Body, E})
    end.

import_parsed(Config, Body) ->
    case rabbit_ct_broker_helpers:rpc(Config, 0, rabbit_definitions, import_parsed, [Body]) of
        ok -> ok;
        {error, E} ->
            ct:pal("Import of parsed definitions ~p failed: ~p~n", [Body, E]),
            ct:fail({failure, Body, E})
    end.

export(Config) ->
    rabbit_ct_broker_helpers:rpc(Config, 0, ?MODULE, run_export, []).

run_export() ->
    rabbit_definitions:all_definitions().

run_directory_import_case(Path, Expected) ->
    ct:pal("Will load definitions from files under ~p~n", [Path]),
    Result = rabbit_definitions:maybe_load_definitions_from(true, Path),
    case Expected of
        ok ->
            ok = Result;
        error ->
            ?assertMatch({error, {failed_to_import_definitions, _, _}}, Result)
    end.

run_import_case(Path) ->
   {ok, Body} = file:read_file(Path),
   ct:pal("Successfully loaded a definition to import from ~p~n", [Path]),
   case rabbit_definitions:import_raw(Body) of
     ok -> ok;
     {error, E} ->
       ct:pal("Import case ~p failed: ~p~n", [Path, E]),
       ct:fail({failure, Path, E})
   end.

run_invalid_import_case(Path) ->
   {ok, Body} = file:read_file(Path),
   ct:pal("Successfully loaded a definition to import from ~p~n", [Path]),
   case rabbit_definitions:import_raw(Body) of
     ok ->
       ct:pal("Expected import case ~p to fail~n", [Path]),
       ct:fail({failure, Path});
     {error, _E} -> ok
   end.

queue_lookup(Config, VHost, Name) ->
    rabbit_ct_broker_helpers:rpc(Config, 0, rabbit_amqqueue, lookup, [rabbit_misc:r(VHost, queue, Name)]).

vhost_lookup(Config, VHost) ->
    rabbit_ct_broker_helpers:rpc(Config, 0, rabbit_vhost, lookup, [VHost]).

user_lookup(Config, User) ->
    rabbit_ct_broker_helpers:rpc(Config, 0, rabbit_auth_backend_internal, lookup_user, [User]).
