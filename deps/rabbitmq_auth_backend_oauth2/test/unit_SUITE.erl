%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2024 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
%%
-module(unit_SUITE).

-compile(export_all).

-include_lib("rabbit_common/include/rabbit.hrl").
-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").


all() ->
    [
        test_own_scope,
        test_validate_payload_with_scope_prefix,
        test_validate_payload,
        test_validate_payload_without_scope,
        test_validate_payload_when_verify_aud_false,

        test_unsuccessful_access_without_scopes,
        test_successful_access_with_a_token_with_variables_in_scopes,
        test_successful_access_with_a_parsed_token,
        test_successful_access_with_a_token_that_has_tag_scopes,
        test_unsuccessful_access_with_a_bogus_token,
        test_restricted_vhost_access_with_a_valid_token,
        test_insufficient_permissions_in_a_valid_token,
        test_token_expiration,
        test_invalid_signature,
        test_incorrect_kid,
        test_post_process_token_payload,
        test_post_process_token_payload_keycloak,
        test_post_process_payload_rich_auth_request,
        test_post_process_payload_rich_auth_request_using_regular_expression_with_cluster,
        test_unsuccessful_access_with_a_token_that_uses_missing_scope_alias_in_scope_field,
        test_unsuccessful_access_with_a_token_that_uses_missing_scope_alias_in_extra_scope_source_field,
        test_username_from,
        {group, with_rabbitmq_node}
    ].
groups() ->
    [
      {with_rabbitmq_node, [], [
          test_command_json,
          test_command_pem,
          test_command_pem_no_kid
        ]
      },
      {with_resource_server_id, [], [
          test_successful_access_with_a_token,
          test_validate_payload_resource_server_id_mismatch,
          test_successful_access_with_a_token_that_uses_single_scope_alias_in_scope_field,
          test_successful_access_with_a_token_that_uses_multiple_scope_aliases_in_scope_field,
          test_successful_authorization_without_scopes,
          test_successful_authentication_without_scopes,
          test_successful_access_with_a_token_that_uses_single_scope_alias_in_extra_scope_source_field,
          test_successful_access_with_a_token_that_uses_multiple_scope_aliases_in_extra_scope_source_field,
          test_post_process_token_payload_complex_claims,
          test_successful_access_with_a_token_that_uses_single_scope_alias_in_scope_field_and_custom_scope_prefix
      ]}
    ].

init_per_suite(Config) ->
    application:load(rabbitmq_auth_backend_oauth2),
    Env = application:get_all_env(rabbitmq_auth_backend_oauth2),
    Config1 = rabbit_ct_helpers:set_config(Config, {env, Env}),
    rabbit_ct_helpers:run_setup_steps(Config1, []).

end_per_suite(Config) ->
    Env = ?config(env, Config),
    lists:foreach(
        fun({K, V}) ->
            application:set_env(rabbitmq_auth_backend_oauth2, K, V)
        end,
        Env),
    rabbit_ct_helpers:run_teardown_steps(Config).

init_per_group(with_rabbitmq_node, Config) ->
  Config1 = rabbit_ct_helpers:set_config(Config, [
      {rmq_nodename_suffix, signing_key_group},
      {rmq_nodes_count, 1}
  ]),
  Config2 = rabbit_ct_helpers:merge_app_env(
             Config1, {rabbitmq_auth_backend_oauth2, [
              {resource_server_id, <<"rabbitmq">>},
              {key_config, [{default_key, <<"token-key">>}]}
             ]}),
  rabbit_ct_helpers:run_steps(Config2, rabbit_ct_broker_helpers:setup_steps());

init_per_group(with_resource_server_id, Config) ->
  application:set_env(rabbitmq_auth_backend_oauth2, resource_server_id, <<"rabbitmq">>),
  Config;

init_per_group(_, Config) ->
  Config.

end_per_group(with_rabbitmq_node, Config) ->
  rabbit_ct_helpers:run_steps(Config, rabbit_ct_broker_helpers:teardown_steps());

end_per_group(_, Config) ->
  application:unset_env(rabbitmq_auth_backend_oauth2, resource_server_id),
  Config.

init_per_testcase(test_post_process_token_payload_complex_claims, Config) ->
  application:set_env(rabbitmq_auth_backend_oauth2, extra_scopes_source, <<"additional_rabbitmq_scopes">>),
  application:set_env(rabbitmq_auth_backend_oauth2, resource_server_id, <<"rabbitmq-resource">>),
  Config;

init_per_testcase(test_validate_payload_when_verify_aud_false, Config) ->
  application:set_env(rabbitmq_auth_backend_oauth2, verify_aud, false),
  Config;



init_per_testcase(test_post_process_payload_rich_auth_request, Config) ->
  application:set_env(rabbitmq_auth_backend_oauth2, resource_server_type, <<"rabbitmq-type">>),
  Config;

init_per_testcase(test_post_process_payload_rich_auth_request_using_regular_expression_with_cluster, Config) ->
  application:set_env(rabbitmq_auth_backend_oauth2, resource_server_type, <<"rabbitmq-type">>),
  application:set_env(rabbitmq_auth_backend_oauth2, resource_server_id, <<"rabbitmq-test">>),
  Config;

init_per_testcase(_, Config) ->
  Config.

end_per_testcase(test_post_process_token_payload_complex_claims, Config) ->
  application:set_env(rabbitmq_auth_backend_oauth2, extra_scopes_source, undefined),
  application:set_env(rabbitmq_auth_backend_oauth2, resource_server_id, undefined),
  Config;

end_per_testcase(_, Config) ->
  Config.


%%
%% Test Cases
%%

-define(UTIL_MOD, rabbit_auth_backend_oauth2_test_util).
-define(RESOURCE_SERVER_ID, <<"rabbitmq">>).
-define(RESOURCE_SERVER_TYPE, <<"rabbitmq-type">>).
-define(DEFAULT_SCOPE_PREFIX, <<"rabbitmq.">>).

test_post_process_token_payload(_) ->
    ArgumentsExpections = [
        {{[<<"rabbitmq">>, <<"hare">>], [<<"read">>, <<"write">>, <<"configure">>]},
         {[<<"rabbitmq">>, <<"hare">>], [<<"read">>, <<"write">>, <<"configure">>]}},
        {{<<"rabbitmq hare">>, <<"read write configure">>},
         {[<<"rabbitmq">>, <<"hare">>], [<<"read">>, <<"write">>, <<"configure">>]}},
        {{<<"rabbitmq">>, <<"read">>},
         {[<<"rabbitmq">>], [<<"read">>]}}
    ],
    lists:foreach(
        fun({{Aud, Scope}, {ExpectedAud, ExpectedScope}}) ->
            Payload = post_process_token_payload(Aud, Scope),
            ?assertEqual(ExpectedAud, maps:get(<<"aud">>, Payload)),
            ?assertEqual(ExpectedScope, maps:get(<<"scope">>, Payload))
        end, ArgumentsExpections).

post_process_token_payload(Audience, Scopes) ->
    Jwk = ?UTIL_MOD:fixture_jwk(),
    Token = maps:put(<<"aud">>, Audience, ?UTIL_MOD:fixture_token_with_scopes(Scopes)),
    {_, EncodedToken} = ?UTIL_MOD:sign_token_hs(Token, Jwk),
    case rabbit_oauth2_config:find_audience_in_resource_server_ids(Audience) of
      {ok, TargetResourceServerId} ->
        {true, Payload} = uaa_jwt_jwt:decode_and_verify(TargetResourceServerId, Jwk, EncodedToken),
        rabbit_auth_backend_oauth2:post_process_payload(TargetResourceServerId, Payload);
      {error, _} = Error -> Error
    end.

test_post_process_token_payload_keycloak(_) ->
    Pairs = [
        %% common case
        {
          #{<<"permissions">> =>
                [#{<<"rsid">> => <<"2c390fe4-02ad-41c7-98a2-cebb8c60ccf1">>,
                   <<"rsname">> => <<"allvhost">>,
                   <<"scopes">> => [<<"rabbitmq-resource.read:*/*">>]},
                 #{<<"rsid">> => <<"e7f12e94-4c34-43d8-b2b1-c516af644cee">>,
                   <<"rsname">> => <<"vhost1">>,
                   <<"scopes">> => [<<"rabbitmq-resource-read">>]},
                 #{<<"rsid">> => <<"12ac3d1c-28c2-4521-8e33-0952eff10bd9">>,
                   <<"rsname">> => <<"Default Resource">>}]},
          [<<"rabbitmq-resource.read:*/*">>, <<"rabbitmq-resource-read">>]
        },

        %% one scopes field with a string instead of an array
        {
          #{<<"permissions">> =>
                [#{<<"rsid">> => <<"2c390fe4-02ad-41c7-98a2-cebb8c60ccf1">>,
                   <<"rsname">> => <<"allvhost">>,
                   <<"scopes">> => <<"rabbitmq-resource.read:*/*">>},
                 #{<<"rsid">> => <<"e7f12e94-4c34-43d8-b2b1-c516af644cee">>,
                   <<"rsname">> => <<"vhost1">>,
                   <<"scopes">> => [<<"rabbitmq-resource-read">>]},
                 #{<<"rsid">> => <<"12ac3d1c-28c2-4521-8e33-0952eff10bd9">>,
                   <<"rsname">> => <<"Default Resource">>}]},
          [<<"rabbitmq-resource.read:*/*">>, <<"rabbitmq-resource-read">>]
        },

        %% no scopes field in permissions
        {
          #{<<"permissions">> =>
                [#{<<"rsid">> => <<"2c390fe4-02ad-41c7-98a2-cebb8c60ccf1">>,
                   <<"rsname">> => <<"allvhost">>},
                 #{<<"rsid">> => <<"e7f12e94-4c34-43d8-b2b1-c516af644cee">>,
                   <<"rsname">> => <<"vhost1">>},
                 #{<<"rsid">> => <<"12ac3d1c-28c2-4521-8e33-0952eff10bd9">>,
                   <<"rsname">> => <<"Default Resource">>}]},
          []
        },

        %% no permissions
        {
          #{<<"permissions">> => []},
          []
        },
        %% missing permissions key
        {#{}, []}
    ],
    lists:foreach(
        fun({Authorization, ExpectedScope}) ->
            Payload = post_process_payload_with_keycloak_authorization(Authorization),
            ?assertEqual(ExpectedScope, maps:get(<<"scope">>, Payload))
        end, Pairs).

post_process_payload_with_keycloak_authorization(Authorization) ->
    Jwk = ?UTIL_MOD:fixture_jwk(),
    Token = maps:put(<<"authorization">>, Authorization, ?UTIL_MOD:fixture_token_with_scopes([])),
    {_, EncodedToken} = ?UTIL_MOD:sign_token_hs(Token, Jwk),
    {true, Payload} = uaa_jwt_jwt:decode_and_verify(<<"rabbitmq">>, Jwk, EncodedToken),
    rabbit_auth_backend_oauth2:post_process_payload(<<"rabbitmq">>, Payload).

test_post_process_payload_rich_auth_request_using_regular_expression_with_cluster(_) ->

  Pairs = [

  { "should filter out those permisions whose locations do not refer to cluster : {resource_server_id}",
    [ #{<<"type">> => ?RESOURCE_SERVER_TYPE,
        <<"locations">> => [<<"cluster:rabbitmq-test">>],
        <<"actions">> => [<<"read">>]
       },
      #{<<"type">> => ?RESOURCE_SERVER_TYPE,
        <<"locations">> => [<<"cluster:rabbitmq-other">>],
        <<"actions">> => [<<"read">>]
      }
    ],
    [<<"rabbitmq-test.read:*/*/*">> ]
  },

  { "can use regular expression on any location's attribute ",
    [ #{<<"type">> => ?RESOURCE_SERVER_TYPE,
        <<"locations">> => [<<"cluster:rabbitmq-*/vhost:^finance-*">> ],
        <<"actions">> => [<<"read">>]
        }
    ],
    [<<"rabbitmq-test.read:^finance-*/*/*">> ]
  },

  { "should filter out any location which does not match the cluster's pattern ",
    [ #{<<"type">> => ?RESOURCE_SERVER_TYPE,
        <<"locations">> => [<<"cluster:rabbitmq-t-*/vhost:^finance-*">>,
          <<"cluster:^rabbitmq$/vhost:^finance-*">>  ],
        <<"actions">> => [<<"read">>]
        }
    ],
    []
  }
  ],

  lists:foreach(
      fun({Case, Permissions, ExpectedScope}) ->
          Payload = post_process_payload_with_rich_auth_request(<<"rabbitmq-test">>, Permissions),
          ?assertEqual(lists:sort(ExpectedScope), lists:sort(maps:get(<<"scope">>, Payload)), Case)
      end, Pairs).

test_post_process_payload_rich_auth_request(_) ->

  Pairs = [
  { "should merge all permissions for the current cluster",
    [
      #{<<"type">> => ?RESOURCE_SERVER_TYPE,
        <<"locations">> => [<<"cluster:finance/vhost:primary-*">>],
        <<"actions">> => [<<"configure">>]
      },
      #{<<"type">> => ?RESOURCE_SERVER_TYPE,
        <<"locations">> => [<<"cluster:rabbitmq">>],
        <<"actions">> => [<<"management">> ]
       },
      #{<<"type">> => ?RESOURCE_SERVER_TYPE,
       <<"locations">> => [<<"cluster:rabbitmq">>],
       <<"actions">> => [<<"administrator">> ]
      }
    ],
    [ <<"rabbitmq.tag:management">>, <<"rabbitmq.tag:administrator">> ]
  },
  { "should filter out those permisions whose type does not match <resource_server_type>",
    [ #{<<"type">> => ?RESOURCE_SERVER_TYPE,
        <<"locations">> => [<<"cluster:rabbitmq">>],
        <<"actions">> => [<<"read">>]
      },
      #{<<"type">> => <<"unknown">>,
        <<"locations">> => [<<"cluster:rabbitmq">>],
        <<"actions">> => [<<"read">>]
      }
    ],
    [<<"rabbitmq.read:*/*/*">> ]
  },
  { "should filter out those permisions whose type is the empty string",
    [
      #{<<"type">> => <<>>,
        <<"locations">> => [<<"cluster:rabbitmq">>],
        <<"actions">> => [<<"read">>]
      }
    ],
    [ ]
  },
  { "should filter out those permisions with empty string action",
    [
      #{<<"type">> => ?RESOURCE_SERVER_TYPE,
        <<"locations">> => [<<"cluster:rabbitmq">>],
        <<"actions">> => <<>>
      }
    ],
    [ ]
  },
  { "should filter out those permisions whose locations do not refer to cluster : {resource_server_id}",
    [ #{<<"type">> => ?RESOURCE_SERVER_TYPE,
        <<"locations">> => [<<"cluster:rabbitmq">>],
        <<"actions">> => [<<"read">>]
       },
      #{<<"type">> => ?RESOURCE_SERVER_TYPE,
        <<"locations">> => [<<"cluster:rabbitmq-other">>],
        <<"actions">> => [<<"read">>]
      }
    ],
    [<<"rabbitmq.read:*/*/*">> ]
  },
  { "should filter out those permisions whose locations' regexpr do not match the cluster : {resource_server_id} ",
    [ #{<<"type">> => ?RESOURCE_SERVER_TYPE,
        <<"locations">> => [<<"cluster:rabbit*">>],
        <<"actions">> => [<<"read">>]
       },
       #{<<"type">> => ?RESOURCE_SERVER_TYPE,
       <<"locations">> => [<<"cluster:*">>],
       <<"actions">> => [<<"write">>]
      },
      #{<<"type">> => ?RESOURCE_SERVER_TYPE,
        <<"locations">> => [<<"cluster:rabbitmq-other">>],
        <<"actions">> => [<<"configure">>]
      }
    ],
    [<<"rabbitmq.read:*/*/*">>, <<"rabbitmq.write:*/*/*">>  ]
  },

  { "should ignore permissions without actions",
    [ #{<<"type">> => ?RESOURCE_SERVER_TYPE,
        <<"locations">> => [<<"cluster:rabbitmq">>]
      },
      #{<<"type">> => ?RESOURCE_SERVER_TYPE,
        <<"locations">> => [<<"cluster:rabbit*">>],
        <<"actions">> => [<<"read">>]
      }
    ],
    [<<"rabbitmq.read:*/*/*">>]
  },
  { "should ignore permissions without locations",
    [ #{<<"type">> => ?RESOURCE_SERVER_TYPE,
        <<"actions">> => [<<"read">>]
        }
    ]
    ,[]
  },
  { "should ignore unknown actions",
    [ #{<<"type">> => ?RESOURCE_SERVER_TYPE,
        <<"locations">> => [<<"cluster:rabbitmq">>],
        <<"actions">> => [<<"read2">>, <<"read">>]
       }
    ]
    ,[<<"rabbitmq.read:*/*/*">> ]
  },
  { "should filter out locations with permissions not meant for {resource_server_id}",
    [ #{<<"type">> => ?RESOURCE_SERVER_TYPE,
       <<"locations">> => [<<"cluster:rabbitmq">>, <<"cluster:unknown">> ],
       <<"actions">> => [<<"read">>]
       }
    ],
    [<<"rabbitmq.read:*/*/*">> ]
  },
  { "should produce a scope for every (action, location) permutation for all locations meant for {resource_server_id}",
    [ #{<<"type">> => ?RESOURCE_SERVER_TYPE,
        <<"locations">> => [<<"cluster:rabbitmq/vhost:a">>, <<"cluster:rabbitmq/vhost:b">> ],
        <<"actions">> => [<<"read">>]
       }
    ],
    [<<"rabbitmq.read:a/*/*">>, <<"rabbitmq.read:b/*/*">> ]
  },
  { "should support all known user tags ",
    [ #{<<"type">> => ?RESOURCE_SERVER_TYPE,
        <<"locations">> => [<<"cluster:rabbitmq/vhost:a">>, <<"cluster:rabbitmq/vhost:b">>, <<"cluster:other">>  ],
        <<"actions">> => [<<"management">>, <<"policymaker">>, <<"management">>, <<"monitoring">>]
      }
    ],
    [<<"rabbitmq.tag:management">>, <<"rabbitmq.tag:policymaker">>, <<"rabbitmq.tag:management">>, <<"rabbitmq.tag:monitoring">>  ]
  },
  { "should produce a scope for every user tag action but only for the clusters that match {resource_server_id}",
    [ #{<<"type">> => ?RESOURCE_SERVER_TYPE,
        <<"locations">> => [<<"cluster:rabbitmq/vhost:a">>, <<"cluster:rabbitmq/vhost:b">>, <<"cluster:other">>  ],
        <<"actions">> => [<<"management">>, <<"policymaker">>]
      }
    ],
    [<<"rabbitmq.tag:management">>, <<"rabbitmq.tag:policymaker">> ]
  },

  { "should produce as scope for every location meant for {resource_server_id} multiplied by actions",
    [ #{<<"type">> => ?RESOURCE_SERVER_TYPE,
        <<"locations">> => [<<"cluster:rabbitmq/vhost:a">>, <<"cluster:rabbitmq/vhost:b">> ],
        <<"actions">> => [<<"read">>, <<"write">>]
       }
    ],
    [<<"rabbitmq.read:a/*/*">>, <<"rabbitmq.read:b/*/*">>, <<"rabbitmq.write:a/*/*">>, <<"rabbitmq.write:b/*/*">> ]
  },
  { "should accept single value locations",
    [ #{<<"type">> => ?RESOURCE_SERVER_TYPE,
        <<"locations">> => <<"cluster:rabbitmq">>,
        <<"actions">> => [<<"read">>]
       }
    ],
    [<<"rabbitmq.read:*/*/*">> ]
  },
  { "should accept single value actions",
    [ #{<<"type">> => ?RESOURCE_SERVER_TYPE,
        <<"locations">> => <<"cluster:rabbitmq">>,
        <<"actions">> => <<"read">>
      }
    ],
    [<<"rabbitmq.read:*/*/*">> ]
  },
  { "should merge all scopes produced by each permission",
    [ #{<<"type">> => ?RESOURCE_SERVER_TYPE,
        <<"locations">> => [<<"cluster:rabbitmq/vhost:a">> ],
        <<"actions">> => [<<"read">>]
      },
      #{<<"type">> => ?RESOURCE_SERVER_TYPE,
        <<"locations">> => [<<"cluster:rabbitmq/vhost:b">> ],
        <<"actions">> => [<<"write">>]
      }
    ],
    [<<"rabbitmq.read:a/*/*">>, <<"rabbitmq.write:b/*/*">> ]
  },
  { "can grant permission to a queue in any virtual host",
    [ #{<<"type">> => ?RESOURCE_SERVER_TYPE,
        <<"locations">> => [<<"cluster:rabbitmq/queue:b">> ],
        <<"actions">> => [<<"read">>]
      }
    ],
    [<<"rabbitmq.read:*/b/*">> ]
  },
  { "can grant permission to an exchange in any virtual host",
    [ #{<<"type">> => ?RESOURCE_SERVER_TYPE,
        <<"locations">> => [<<"cluster:rabbitmq/exchange:b">> ],
        <<"actions">> => [<<"read">>]
        }
    ],
    [<<"rabbitmq.read:*/b/*">> ]
  },
  { "cannot specify both exchange and queue unless they have the same value",
    [ #{<<"type">> => ?RESOURCE_SERVER_TYPE,
        <<"locations">> => [<<"cluster:rabbitmq/queue:b/exchange:c">> ],
        <<"actions">> => [<<"read">>]
        }
    ],
    []
  },
  { "can specify exchange and queue when have same value",
    [ #{<<"type">> => ?RESOURCE_SERVER_TYPE,
        <<"locations">> => [<<"cluster:rabbitmq/queue:*/exchange:*">> ],
        <<"actions">> => [<<"read">>]
      }
    ],
    [ <<"rabbitmq.read:*/*/*">> ]
  },
  { "can specify routing-key only -> on any vhost and on any queue if that makes sense ",
    [ #{<<"type">> => ?RESOURCE_SERVER_TYPE,
        <<"locations">> => [<<"cluster:rabbitmq/routing-key:b">> ],
        <<"actions">> => [<<"read">>]
      }
    ],
    [<<"rabbitmq.read:*/*/b">> ]
  },
  { "can specify vhost, queue or exchange and routing-key that combine fixed values and wildcards",
    [ #{<<"type">> => ?RESOURCE_SERVER_TYPE,
        <<"locations">> => [<<"cluster:rabbitmq/vhost:finance-*/queue:*-invoice/routing-key:r-*">> ],
        <<"actions">> => [<<"read">>]
      }
    ],
    [<<"rabbitmq.read:finance-*/*-invoice/r-*">> ]
  },
  { "should ignore any location's attribute other than the supported ones",
    [ #{<<"type">> => ?RESOURCE_SERVER_TYPE,
        <<"locations">> => [<<"cluster:rabbitmq/unknown:finance-*/queue:*-invoice/routing-key:r-*">> ],
        <<"actions">> => [<<"read">>]
      }
    ],
    [<<"rabbitmq.read:*/*-invoice/r-*">> ]
  },
  { "should not matter the location's attributes order",
    [ #{<<"type">> => ?RESOURCE_SERVER_TYPE,
        <<"locations">> => [<<"cluster:rabbitmq/queue:invoices/vhost:finance/routing-key:r-*">> ],
        <<"actions">> => [<<"read">>]
      }
    ],
    [<<"rabbitmq.read:finance/invoices/r-*">> ]
  },
  { "should ignore locations like //",
    [ #{<<"type">> => ?RESOURCE_SERVER_TYPE,
        <<"locations">> => [<<"cluster:rabbitmq//routing-key:r-*">> ],
        <<"actions">> => [<<"read">>]
      }
    ],
    [<<"rabbitmq.read:*/*/r-*">> ]
  },
  { "should default to wildcard those attributes with empty value",
    [ #{<<"type">> => ?RESOURCE_SERVER_TYPE,
        <<"locations">> => [<<"cluster:rabbitmq/queue:/vhost:/routing-key:r-*">> ],
        <<"actions">> => [<<"read">>]
      }
    ],
    [<<"rabbitmq.read:*/*/r-*">> ]
  },
  { "should ignore any location path element which is not compliant with <key>:<value> format",
    [ #{<<"type">> => ?RESOURCE_SERVER_TYPE,
        <<"locations">> => [<<"some-prefix-value/cluster:rabbitmq/vhost:finance-*/queue:*-invoice/routing-key:r-*">> ],
        <<"actions">> => [<<"read">>]
      }
    ],
    [<<"rabbitmq.read:finance-*/*-invoice/r-*">> ]
  },
  { "can use regular expression on any location's attribute",
    [ #{<<"type">> => ?RESOURCE_SERVER_TYPE,
        <<"locations">> => [<<"cluster:rabbitmq/vhost:^finance-*">> ],
        <<"actions">> => [<<"read">>]
        }
    ],
    [<<"rabbitmq.read:^finance-*/*/*">> ]
  },
  { "can use single string value for location",
    [ #{<<"type">> => ?RESOURCE_SERVER_TYPE,
        <<"locations">> => <<"cluster:rabbitmq/vhost:^finance-*">>,
        <<"actions">> => [<<"read">>]
        }
    ],
    [<<"rabbitmq.read:^finance-*/*/*">> ]
  },
  { "can use single string value for action",
    [ #{<<"type">> => ?RESOURCE_SERVER_TYPE,
        <<"locations">> => <<"cluster:rabbitmq/vhost:^finance-*">>,
        <<"actions">> => <<"read">>
        }
    ],
    [<<"rabbitmq.read:^finance-*/*/*">> ]
  },
  { "should ignore empty permission lists",
    [],
    []
  }
  ],

  lists:foreach(
      fun({Case, Permissions, ExpectedScope}) ->
          Payload = post_process_payload_with_rich_auth_request(<<"rabbitmq">>, Permissions),
          ?assertEqual(lists:sort(ExpectedScope), lists:sort(maps:get(<<"scope">>, Payload)), Case)
      end, Pairs).

post_process_payload_with_rich_auth_request(ResourceServerId, Permissions) ->
    Jwk = ?UTIL_MOD:fixture_jwk(),
    Token = maps:put(<<"authorization_details">>, Permissions, ?UTIL_MOD:plain_token_without_scopes_and_aud()),
    {_, EncodedToken} = ?UTIL_MOD:sign_token_hs(Token, Jwk),
    {true, Payload} = uaa_jwt_jwt:decode_and_verify(<<"rabbitmq">>, Jwk, EncodedToken),
    rabbit_auth_backend_oauth2:post_process_payload(ResourceServerId, Payload).

test_post_process_token_payload_complex_claims(_) ->
    Pairs = [
        %% claims in form of binary
        {
          <<"rabbitmq.rabbitmq-resource.read:*/* rabbitmq.rabbitmq-resource-read">>,
          [<<"rabbitmq.rabbitmq-resource.read:*/*">>, <<"rabbitmq.rabbitmq-resource-read">>]
        },
        %% claims in form of binary - empty result
        {<<>>, []},
        %% claims in form of list
        {
          [<<"rabbitmq.rabbitmq-resource.read:*/*">>,
           <<"rabbitmq2.rabbitmq-resource-read">>],
          [<<"rabbitmq.rabbitmq-resource.read:*/*">>, <<"rabbitmq2.rabbitmq-resource-read">>]
        },
        %% claims in form of list - empty result
        {[], []},
        %% claims are map with list content
        {
          #{<<"rabbitmq">> =>
                [<<"rabbitmq-resource.read:*/*">>,
                 <<"rabbitmq-resource-read">>],
            <<"rabbitmq3">> =>
                [<<"rabbitmq-resource.write:*/*">>,
                 <<"rabbitmq-resource-write">>]},
          [<<"rabbitmq.rabbitmq-resource.read:*/*">>, <<"rabbitmq.rabbitmq-resource-read">>]
        },
        %% claims are map with list content - empty result
        {
          #{<<"rabbitmq2">> =>
                 [<<"rabbitmq-resource.read:*/*">>,
                  <<"rabbitmq-resource-read">>]},
          []
        },
        %% claims are map with binary content
        {
          #{<<"rabbitmq">> => <<"rabbitmq-resource.read:*/* rabbitmq-resource-read">>,
           <<"rabbitmq3">> => <<"rabbitmq-resource.write:*/* rabbitmq-resource-write">>},
          [<<"rabbitmq.rabbitmq-resource.read:*/*">>, <<"rabbitmq.rabbitmq-resource-read">>]
        },
        %% claims are map with binary content - empty result
        {
          #{<<"rabbitmq2">> => <<"rabbitmq-resource.read:*/* rabbitmq-resource-read">>}, []
        },
        %% claims are map with empty binary content - empty result
        {
          #{<<"rabbitmq">> => <<>>}, []
        },
        %% claims are map with empty list content - empty result
        {
          #{<<"rabbitmq">> => []}, []
        },
        %% no extra claims provided
        {[], []},
        %% no extra claims provided
        {#{}, []}
    ],
    lists:foreach(
        fun({Authorization, ExpectedScope}) ->
            Payload = post_process_payload_with_complex_claim_authorization(<<"rabbitmq-resource">>, Authorization),
            ?assertEqual(ExpectedScope, maps:get(<<"scope">>, Payload))
        end, Pairs).

post_process_payload_with_complex_claim_authorization(ResourceServerId, Authorization) ->
    Jwk = ?UTIL_MOD:fixture_jwk(),
    Token =  maps:put(<<"additional_rabbitmq_scopes">>, Authorization, ?UTIL_MOD:fixture_token_with_scopes([])),
    {_, EncodedToken} = ?UTIL_MOD:sign_token_hs(Token, Jwk),
    {true, Payload} = uaa_jwt_jwt:decode_and_verify(Jwk, EncodedToken),
    rabbit_auth_backend_oauth2:post_process_payload(ResourceServerId, Payload).

test_successful_authentication_without_scopes(_) ->
  Jwk = ?UTIL_MOD:fixture_jwk(),
  UaaEnv = [{signing_keys, #{<<"token-key">> => {map, Jwk}}}],
  application:set_env(rabbitmq_auth_backend_oauth2, key_config, UaaEnv),

  Username = <<"username">>,
  Token    = ?UTIL_MOD:sign_token_hs(?UTIL_MOD:token_with_sub(?UTIL_MOD:fixture_token(), Username), Jwk),

  {ok, #auth_user{username = Username} } =
      rabbit_auth_backend_oauth2:user_login_authentication(Username, [{password, Token}]).

test_successful_authorization_without_scopes(_) ->
  Jwk = ?UTIL_MOD:fixture_jwk(),
  UaaEnv = [{signing_keys, #{<<"token-key">> => {map, Jwk}}}],
  application:set_env(rabbitmq_auth_backend_oauth2, key_config, UaaEnv),

  Username = <<"username">>,
  Token    = ?UTIL_MOD:sign_token_hs(?UTIL_MOD:token_with_sub(?UTIL_MOD:fixture_token(), Username), Jwk),

  {ok, _ } =
      rabbit_auth_backend_oauth2:user_login_authorization(Username, [{password, Token}]).

test_successful_access_with_a_token(_) ->
    %% Generate a token with JOSE
    %% Check authorization with the token
    %% Check user access granted by token
    Jwk = ?UTIL_MOD:fixture_jwk(),
    UaaEnv = [{signing_keys, #{<<"token-key">> => {map, Jwk}}}],
    application:set_env(rabbitmq_auth_backend_oauth2, key_config, UaaEnv),

    VHost    = <<"vhost">>,
    Username = <<"username">>,
    Token    = ?UTIL_MOD:sign_token_hs(?UTIL_MOD:token_with_sub(?UTIL_MOD:fixture_token(), Username), Jwk),

    {ok, #auth_user{username = Username} = User} =
        rabbit_auth_backend_oauth2:user_login_authentication(Username, [{password, Token}]),
%    {ok, #auth_user{username = Username} = User} =
%        rabbit_auth_backend_oauth2:user_login_authentication(Username, #{password => Token}),

    ?assertEqual(true, rabbit_auth_backend_oauth2:check_vhost_access(User, <<"vhost">>, none)),
    assert_resource_access_granted(User, VHost, <<"foo">>, configure),
    assert_resource_access_granted(User, VHost, <<"foo">>, write),
    assert_resource_access_granted(User, VHost, <<"bar">>, read),
    assert_resource_access_granted(User, VHost, custom, <<"bar">>, read),

    assert_topic_access_granted(User, VHost, <<"bar">>, read, #{routing_key => <<"#/foo">>}).

test_successful_access_with_a_token_with_variables_in_scopes(_) ->
    %% Generate a token with JOSE
    %% Check authorization with the token
    %% Check user access granted by token
    Jwk = ?UTIL_MOD:fixture_jwk(),
    UaaEnv = [{signing_keys, #{<<"token-key">> => {map, Jwk}}}],
    application:set_env(rabbitmq_auth_backend_oauth2, key_config, UaaEnv),

    VHost    = <<"my-vhost">>,
    Username = <<"username">>,
    Token    = ?UTIL_MOD:sign_token_hs(
        ?UTIL_MOD:token_with_sub(?UTIL_MOD:fixture_token([<<"rabbitmq.read:{vhost}/*/{sub}">>]), Username),
      Jwk),
    {ok, #auth_user{username = Username} = User} =
      rabbit_auth_backend_oauth2:user_login_authentication(Username, #{password => Token}),

    assert_topic_access_granted(User, VHost, <<"bar">>, read, #{routing_key => Username}).

test_successful_access_with_a_parsed_token(_) ->
    Jwk = ?UTIL_MOD:fixture_jwk(),
    UaaEnv = [{signing_keys, #{<<"token-key">> => {map, Jwk}}}],
    application:set_env(rabbitmq_auth_backend_oauth2, key_config, UaaEnv),

    Username = <<"username">>,
    Token    = ?UTIL_MOD:sign_token_hs(?UTIL_MOD:token_with_sub(?UTIL_MOD:fixture_token(), Username), Jwk),
    {ok, #auth_user{impl = Impl} } =
        rabbit_auth_backend_oauth2:user_login_authentication(Username, [{password, Token}]),

    {ok, _ } =
        rabbit_auth_backend_oauth2:user_login_authentication(Username, [{rabbit_auth_backend_oauth2, Impl}]).


test_successful_access_with_a_token_that_has_tag_scopes(_) ->
    Jwk = ?UTIL_MOD:fixture_jwk(),
    UaaEnv = [{signing_keys, #{<<"token-key">> => {map, Jwk}}}],
    application:set_env(rabbitmq_auth_backend_oauth2, key_config, UaaEnv),
    Username = <<"username">>,
    Token    = ?UTIL_MOD:sign_token_hs(?UTIL_MOD:token_with_sub(?UTIL_MOD:fixture_token(
        [<<"rabbitmq.tag:management">>, <<"rabbitmq.tag:policymaker">>]), Username), Jwk),

    {ok, #auth_user{username = Username, tags = [management, policymaker]}} =
        rabbit_auth_backend_oauth2:user_login_authentication(Username, [{password, Token}]).

test_successful_access_with_a_token_that_uses_single_scope_alias_in_scope_field(_) ->
    Jwk = ?UTIL_MOD:fixture_jwk(),
    UaaEnv = [{signing_keys, #{<<"token-key">> => {map, Jwk}}}],
    application:set_env(rabbitmq_auth_backend_oauth2, key_config, UaaEnv),
    Alias = <<"client-alias-1">>,
    application:set_env(rabbitmq_auth_backend_oauth2, scope_aliases, #{
        Alias => [
            <<"rabbitmq.configure:vhost/one">>,
            <<"rabbitmq.write:vhost/two">>,
            <<"rabbitmq.read:vhost/one">>,
            <<"rabbitmq.read:vhost/two">>,
            <<"rabbitmq.read:vhost/two/abc">>,
            <<"rabbitmq.tag:management">>,
            <<"rabbitmq.tag:custom">>
        ]
    }),

    VHost = <<"vhost">>,
    Username = <<"username">>,
    Token    = ?UTIL_MOD:sign_token_hs(?UTIL_MOD:token_with_sub(
      ?UTIL_MOD:token_with_scope_alias_in_scope_field(Alias), Username), Jwk),

    {ok, #auth_user{username = Username} = AuthUser} =
        rabbit_auth_backend_oauth2:user_login_authentication(Username, [{password, Token}]),
    assert_vhost_access_granted(AuthUser, VHost),
    assert_vhost_access_denied(AuthUser, <<"some-other-vhost">>),

    assert_resource_access_granted(AuthUser, VHost, <<"one">>, configure),
    assert_resource_access_granted(AuthUser, VHost, <<"one">>, read),
    assert_resource_access_granted(AuthUser, VHost, <<"two">>, read),
    assert_resource_access_granted(AuthUser, VHost, <<"two">>, write),
    assert_resource_access_denied(AuthUser, VHost, <<"three">>, configure),
    assert_resource_access_denied(AuthUser, VHost, <<"three">>, read),
    assert_resource_access_denied(AuthUser, VHost, <<"three">>, write),

    application:unset_env(rabbitmq_auth_backend_oauth2, scope_aliases),
    application:unset_env(rabbitmq_auth_backend_oauth2, key_config),
    application:unset_env(rabbitmq_auth_backend_oauth2, resource_server_id).


test_successful_access_with_a_token_that_uses_single_scope_alias_in_scope_field_and_custom_scope_prefix(_) ->
    Jwk = ?UTIL_MOD:fixture_jwk(),
    UaaEnv = [{signing_keys, #{<<"token-key">> => {map, Jwk}}}],
    application:set_env(rabbitmq_auth_backend_oauth2, key_config, UaaEnv),
    application:set_env(rabbitmq_auth_backend_oauth2, scope_prefix, <<>>),
    Alias = <<"client-alias-1">>,
    application:set_env(rabbitmq_auth_backend_oauth2, scope_aliases, #{
        Alias => [
            <<"configure:vhost/one">>,
            <<"write:vhost/two">>,
            <<"read:vhost/one">>,
            <<"read:vhost/two">>,
            <<"read:vhost/two/abc">>,
            <<"tag:management">>,
            <<"tag:custom">>
        ]
    }),

    VHost = <<"vhost">>,
    Username = <<"username">>,
    Token    = ?UTIL_MOD:sign_token_hs(?UTIL_MOD:token_with_sub(
      ?UTIL_MOD:token_with_scope_alias_in_scope_field(Alias), Username), Jwk),

    {ok, #auth_user{username = Username} = AuthUser} =
        rabbit_auth_backend_oauth2:user_login_authentication(Username, [{password, Token}]),
    assert_vhost_access_granted(AuthUser, VHost),
    assert_vhost_access_denied(AuthUser, <<"some-other-vhost">>),

    assert_resource_access_granted(AuthUser, VHost, <<"one">>, configure),
    assert_resource_access_granted(AuthUser, VHost, <<"one">>, read),
    assert_resource_access_granted(AuthUser, VHost, <<"two">>, read),
    assert_resource_access_granted(AuthUser, VHost, <<"two">>, write),
    assert_resource_access_denied(AuthUser, VHost, <<"three">>, configure),
    assert_resource_access_denied(AuthUser, VHost, <<"three">>, read),
    assert_resource_access_denied(AuthUser, VHost, <<"three">>, write),

    application:unset_env(rabbitmq_auth_backend_oauth2, scope_aliases),
    application:unset_env(rabbitmq_auth_backend_oauth2, key_config),
    application:unset_env(rabbitmq_auth_backend_oauth2, scope_prefix),
    application:unset_env(rabbitmq_auth_backend_oauth2, resource_server_id).

test_successful_access_with_a_token_that_uses_multiple_scope_aliases_in_scope_field(_) ->
    Jwk = ?UTIL_MOD:fixture_jwk(),
    UaaEnv = [{signing_keys, #{<<"token-key">> => {map, Jwk}}}],
    application:set_env(rabbitmq_auth_backend_oauth2, key_config, UaaEnv),
    Role1 = <<"client-aliases-1">>,
    Role2 = <<"client-aliases-2">>,
    Role3 = <<"client-aliases-3">>,
    application:set_env(rabbitmq_auth_backend_oauth2, scope_aliases, #{
        Role1 => [
            <<"rabbitmq.configure:vhost/one">>,
            <<"rabbitmq.tag:management">>
        ],
        Role2 => [
            <<"rabbitmq.write:vhost/two">>
        ],
        Role3 => [
            <<"rabbitmq.read:vhost/one">>,
            <<"rabbitmq.read:vhost/two">>,
            <<"rabbitmq.read:vhost/two/abc">>,
            <<"rabbitmq.tag:custom">>
        ]
    }),

    VHost = <<"vhost">>,
    Username = <<"username">>,
    Token    = ?UTIL_MOD:sign_token_hs(?UTIL_MOD:token_with_sub(
    ?UTIL_MOD:token_with_scope_alias_in_scope_field([Role1, Role2, Role3]), Username), Jwk),

    {ok, #auth_user{username = Username}  = AuthUser} =
        rabbit_auth_backend_oauth2:user_login_authentication(Username, [{password, Token}]),
    assert_vhost_access_granted(AuthUser, VHost),
    assert_vhost_access_denied(AuthUser, <<"some-other-vhost">>),

    assert_resource_access_granted(AuthUser, VHost, <<"one">>, configure),
    assert_resource_access_granted(AuthUser, VHost, <<"one">>, read),
    assert_resource_access_granted(AuthUser, VHost, <<"two">>, read),
    assert_resource_access_granted(AuthUser, VHost, <<"two">>, write),
    assert_resource_access_denied(AuthUser, VHost, <<"three">>, configure),
    assert_resource_access_denied(AuthUser, VHost, <<"three">>, read),
    assert_resource_access_denied(AuthUser, VHost, <<"three">>, write),

    application:unset_env(rabbitmq_auth_backend_oauth2, scope_aliases),
    application:unset_env(rabbitmq_auth_backend_oauth2, key_config),
    application:unset_env(rabbitmq_auth_backend_oauth2, resource_server_id).

test_unsuccessful_access_with_a_token_that_uses_missing_scope_alias_in_scope_field(_) ->
    Jwk = ?UTIL_MOD:fixture_jwk(),
    UaaEnv = [{signing_keys, #{<<"token-key">> => {map, Jwk}}}],
    application:set_env(rabbitmq_auth_backend_oauth2, key_config, UaaEnv),
    application:set_env(rabbitmq_auth_backend_oauth2, resource_server_id, <<"rabbitmq">>),
    Alias = <<"client-alias-33">>,
    application:set_env(rabbitmq_auth_backend_oauth2, scope_aliases, #{
        <<"non-existent-alias-23948sdkfjsdof8">> => [
            <<"rabbitmq.configure:vhost/one">>,
            <<"rabbitmq.write:vhost/two">>,
            <<"rabbitmq.read:vhost/one">>,
            <<"rabbitmq.read:vhost/two">>,
            <<"rabbitmq.read:vhost/two/abc">>
        ]
    }),

    VHost = <<"vhost">>,
    Username = <<"username">>,
    Token    = ?UTIL_MOD:sign_token_hs(?UTIL_MOD:token_with_sub(
      ?UTIL_MOD:token_with_scope_alias_in_scope_field(Alias), Username), Jwk),

    {ok, AuthUser} = rabbit_auth_backend_oauth2:user_login_authentication(Username, [{password, Token}]),
    assert_vhost_access_denied(AuthUser, VHost),
    assert_vhost_access_denied(AuthUser, <<"some-other-vhost">>),

    assert_resource_access_denied(AuthUser, VHost, <<"one">>, configure),
    assert_resource_access_denied(AuthUser, VHost, <<"one">>, read),
    assert_resource_access_denied(AuthUser, VHost, <<"two">>, read),
    assert_resource_access_denied(AuthUser, VHost, <<"two">>, write),
    assert_resource_access_denied(AuthUser, VHost, <<"three">>, configure),
    assert_resource_access_denied(AuthUser, VHost, <<"three">>, read),
    assert_resource_access_denied(AuthUser, VHost, <<"three">>, write),

    application:unset_env(rabbitmq_auth_backend_oauth2, scope_aliases),
    application:unset_env(rabbitmq_auth_backend_oauth2, key_config),
    application:unset_env(rabbitmq_auth_backend_oauth2, resource_server_id).

test_successful_access_with_a_token_that_uses_single_scope_alias_in_extra_scope_source_field(_) ->
    Jwk = ?UTIL_MOD:fixture_jwk(),
    UaaEnv = [{signing_keys, #{<<"token-key">> => {map, Jwk}}}],
    application:set_env(rabbitmq_auth_backend_oauth2, key_config, UaaEnv),
    application:set_env(rabbitmq_auth_backend_oauth2, extra_scopes_source, <<"claims">>),
    Alias = <<"client-alias-1">>,
    application:set_env(rabbitmq_auth_backend_oauth2, scope_aliases, #{
        Alias => [
            <<"rabbitmq.configure:vhost/one">>,
            <<"rabbitmq.write:vhost/two">>,
            <<"rabbitmq.read:vhost/one">>,
            <<"rabbitmq.read:vhost/two">>,
            <<"rabbitmq.read:vhost/two/abc">>
        ]
    }),

    VHost = <<"vhost">>,
    Username = <<"username">>,
    Token    = ?UTIL_MOD:sign_token_hs(?UTIL_MOD:token_with_sub(
        ?UTIL_MOD:token_with_scope_alias_in_claim_field(Alias, [<<"unrelated">>]), Username), Jwk),

    {ok, AuthUser} = rabbit_auth_backend_oauth2:user_login_authentication(Username, [{password, Token}]),
    assert_vhost_access_granted(AuthUser, VHost),
    assert_vhost_access_denied(AuthUser, <<"some-other-vhost">>),

    assert_resource_access_granted(AuthUser, VHost, <<"one">>, configure),
    assert_resource_access_granted(AuthUser, VHost, <<"one">>, read),
    assert_resource_access_granted(AuthUser, VHost, <<"two">>, read),
    assert_resource_access_granted(AuthUser, VHost, <<"two">>, write),
    assert_resource_access_denied(AuthUser, VHost, <<"three">>, configure),
    assert_resource_access_denied(AuthUser, VHost, <<"three">>, read),
    assert_resource_access_denied(AuthUser, VHost, <<"three">>, write),

    application:unset_env(rabbitmq_auth_backend_oauth2, scope_aliases),
    application:unset_env(rabbitmq_auth_backend_oauth2, key_config),
    application:unset_env(rabbitmq_auth_backend_oauth2, resource_server_id).

test_successful_access_with_a_token_that_uses_multiple_scope_aliases_in_extra_scope_source_field(_) ->
    Jwk = ?UTIL_MOD:fixture_jwk(),
    UaaEnv = [{signing_keys, #{<<"token-key">> => {map, Jwk}}}],
    application:set_env(rabbitmq_auth_backend_oauth2, key_config, UaaEnv),
    application:set_env(rabbitmq_auth_backend_oauth2, extra_scopes_source, <<"claims">>),
    Role1 = <<"client-aliases-1">>,
    Role2 = <<"client-aliases-2">>,
    Role3 = <<"client-aliases-3">>,
    application:set_env(rabbitmq_auth_backend_oauth2, scope_aliases, #{
        Role1 => [
            <<"rabbitmq.configure:vhost/one">>
        ],
        Role2 => [
            <<"rabbitmq.write:vhost/two">>
        ],
        Role3 => [
            <<"rabbitmq.read:vhost/one">>,
            <<"rabbitmq.read:vhost/two">>,
            <<"rabbitmq.read:vhost/two/abc">>
        ]
    }),

    VHost = <<"vhost">>,
    Username = <<"username">>,
    Claims   = [Role1, Role2, Role3],
    Token    = ?UTIL_MOD:sign_token_hs(?UTIL_MOD:token_with_sub(
        ?UTIL_MOD:token_with_scope_alias_in_claim_field(Claims, [<<"unrelated">>]), Username), Jwk),

    {ok, AuthUser} = rabbit_auth_backend_oauth2:user_login_authentication(Username, [{password, Token}]),
    assert_vhost_access_granted(AuthUser, VHost),
    assert_vhost_access_denied(AuthUser, <<"some-other-vhost">>),

    assert_resource_access_granted(AuthUser, VHost, <<"one">>, configure),
    assert_resource_access_granted(AuthUser, VHost, <<"one">>, read),
    assert_resource_access_granted(AuthUser, VHost, <<"two">>, read),
    assert_resource_access_granted(AuthUser, VHost, <<"two">>, write),
    assert_resource_access_denied(AuthUser, VHost, <<"three">>, configure),
    assert_resource_access_denied(AuthUser, VHost, <<"three">>, read),
    assert_resource_access_denied(AuthUser, VHost, <<"three">>, write),

    application:unset_env(rabbitmq_auth_backend_oauth2, scope_aliases),
    application:unset_env(rabbitmq_auth_backend_oauth2, key_config),
    application:unset_env(rabbitmq_auth_backend_oauth2, resource_server_id).

test_unsuccessful_access_with_a_token_that_uses_missing_scope_alias_in_extra_scope_source_field(_) ->
    Jwk = ?UTIL_MOD:fixture_jwk(),
    UaaEnv = [{signing_keys, #{<<"token-key">> => {map, Jwk}}}],
    application:set_env(rabbitmq_auth_backend_oauth2, key_config, UaaEnv),
    application:set_env(rabbitmq_auth_backend_oauth2, extra_scopes_source, <<"claims">>),
    application:set_env(rabbitmq_auth_backend_oauth2, resource_server_id, <<"rabbitmq">>),
    Alias = <<"client-alias-11">>,
    application:set_env(rabbitmq_auth_backend_oauth2, scope_aliases, #{
        <<"non-existent-client-alias-9238923789">> => [
            <<"rabbitmq.configure:vhost/one">>,
            <<"rabbitmq.write:vhost/two">>,
            <<"rabbitmq.read:vhost/one">>,
            <<"rabbitmq.read:vhost/two">>,
            <<"rabbitmq.read:vhost/two/abc">>
        ]
    }),

    VHost = <<"vhost">>,
    Username = <<"username">>,
    Token    = ?UTIL_MOD:sign_token_hs(?UTIL_MOD:token_with_sub(
      ?UTIL_MOD:token_with_scope_alias_in_claim_field(Alias, [<<"unrelated">>]), Username), Jwk),

    {ok, AuthUser} = rabbit_auth_backend_oauth2:user_login_authentication(Username, [{password, Token}]),
    assert_vhost_access_denied(AuthUser, VHost),
    assert_vhost_access_denied(AuthUser, <<"some-other-vhost">>),

    assert_resource_access_denied(AuthUser, VHost, <<"one">>, configure),
    assert_resource_access_denied(AuthUser, VHost, <<"one">>, read),
    assert_resource_access_denied(AuthUser, VHost, <<"two">>, read),
    assert_resource_access_denied(AuthUser, VHost, <<"two">>, write),
    assert_resource_access_denied(AuthUser, VHost, <<"three">>, configure),
    assert_resource_access_denied(AuthUser, VHost, <<"three">>, read),
    assert_resource_access_denied(AuthUser, VHost, <<"three">>, write),

    application:unset_env(rabbitmq_auth_backend_oauth2, scope_aliases),
    application:unset_env(rabbitmq_auth_backend_oauth2, key_config),
    application:unset_env(rabbitmq_auth_backend_oauth2, resource_server_id).

test_unsuccessful_access_with_a_bogus_token(_) ->
    Username = <<"username">>,
    application:set_env(rabbitmq_auth_backend_oauth2, resource_server_id, <<"rabbitmq">>),

    Jwk0 = ?UTIL_MOD:fixture_jwk(),
    Jwk  = Jwk0#{<<"k">> => <<"bm90b2tlbmtleQ">>},
    UaaEnv = [{signing_keys, #{<<"token-key">> => {map, Jwk}}}],
    application:set_env(rabbitmq_auth_backend_oauth2, key_config, UaaEnv),

    ?assertMatch({refused, _, _},
                 rabbit_auth_backend_oauth2:user_login_authentication(Username, [{password, <<"not a token">>}])).

test_unsuccessful_access_without_scopes(_) ->
    Username = <<"username">>,
    application:set_env(rabbitmq_auth_backend_oauth2, resource_server_id, <<"rabbitmq">>),

    Jwk   = ?UTIL_MOD:fixture_jwk(),
    Token = ?UTIL_MOD:sign_token_hs(?UTIL_MOD:token_with_sub(?UTIL_MOD:token_without_scopes(), Username), Jwk),
    UaaEnv = [{signing_keys, #{<<"token-key">> => {map, Jwk}}}],
    application:set_env(rabbitmq_auth_backend_oauth2, key_config, UaaEnv),

    {ok, #auth_user{username = Username, tags = [], impl = _CredentialsFun } = AuthUser} =
        rabbit_auth_backend_oauth2:user_login_authentication(Username, [{password, Token}]),

    assert_vhost_access_denied(AuthUser, <<"vhost">>).

test_restricted_vhost_access_with_a_valid_token(_) ->
    Username = <<"username">>,
    application:set_env(rabbitmq_auth_backend_oauth2, resource_server_id, <<"rabbitmq">>),

    Jwk   = ?UTIL_MOD:fixture_jwk(),
    Token = ?UTIL_MOD:sign_token_hs(?UTIL_MOD:token_with_sub(?UTIL_MOD:fixture_token(), Username), Jwk),
    UaaEnv = [{signing_keys, #{<<"token-key">> => {map, Jwk}}}],
    application:set_env(rabbitmq_auth_backend_oauth2, key_config, UaaEnv),

    %% this user can authenticate successfully and access certain vhosts
    {ok, #auth_user{username = Username, tags = []} = User} =
        rabbit_auth_backend_oauth2:user_login_authentication(Username, [{password, Token}]),

    %% access to a different vhost
    ?assertEqual(false, rabbit_auth_backend_oauth2:check_vhost_access(User, <<"different vhost">>, none)).

test_insufficient_permissions_in_a_valid_token(_) ->
    VHost = <<"vhost">>,
    Username = <<"username">>,
    application:set_env(rabbitmq_auth_backend_oauth2, resource_server_id, <<"rabbitmq">>),

    Jwk   = ?UTIL_MOD:fixture_jwk(),
    Token = ?UTIL_MOD:sign_token_hs(?UTIL_MOD:token_with_sub(?UTIL_MOD:fixture_token(), Username), Jwk),
    UaaEnv = [{signing_keys, #{<<"token-key">> => {map, Jwk}}}],
    application:set_env(rabbitmq_auth_backend_oauth2, key_config, UaaEnv),

    {ok, #auth_user{username = Username} = User} =
        rabbit_auth_backend_oauth2:user_login_authentication(Username, [{password, Token}]),

    %% access to these resources is not granted
    assert_resource_access_denied(User, VHost, <<"foo1">>, configure),
    assert_resource_access_denied(User, VHost, <<"bar">>, write),
    assert_topic_access_refused(User, VHost, <<"bar">>, read, #{routing_key => <<"foo/#">>}).

test_invalid_signature(_) ->
    Username = <<"username">>,
    Jwk = ?UTIL_MOD:fixture_jwk(),
    WrongJwk = ?UTIL_MOD:fixture_jwk("wrong", <<"GawgguFyGrWKav7AX4VKUg">>),
    UaaEnv = [{signing_keys, #{<<"token-key">> => {map, WrongJwk}}}],
    application:set_env(rabbitmq_auth_backend_oauth2, key_config, UaaEnv),
    application:set_env(rabbitmq_auth_backend_oauth2, resource_server_id, <<"rabbitmq">>),
    TokenData =  ?UTIL_MOD:token_with_sub(?UTIL_MOD:expirable_token(), Username),
    Token     = ?UTIL_MOD:sign_token_hs(TokenData, Jwk),
    ?assertMatch({refused, _, [signature_invalid]},
                 rabbit_auth_backend_oauth2:user_login_authentication(Username, [{password, Token}])).

test_token_expiration(_) ->
    VHost = <<"vhost">>,
    Username = <<"username">>,
    Jwk = ?UTIL_MOD:fixture_jwk(),
    UaaEnv = [{signing_keys, #{<<"token-key">> => {map, Jwk}}}],
    application:set_env(rabbitmq_auth_backend_oauth2, key_config, UaaEnv),
    application:set_env(rabbitmq_auth_backend_oauth2, resource_server_id, <<"rabbitmq">>),
    TokenData =  ?UTIL_MOD:token_with_sub(?UTIL_MOD:expirable_token(), Username),
    Token     = ?UTIL_MOD:sign_token_hs(TokenData, Jwk),
    {ok, #auth_user{username = Username} = User} =
        rabbit_auth_backend_oauth2:user_login_authentication(Username, [{password, Token}]),

    assert_resource_access_granted(User, VHost, <<"foo">>, configure),
    assert_resource_access_granted(User, VHost, <<"foo">>, write),
    Now = os:system_time(seconds),
    ExpiryTs = rabbit_auth_backend_oauth2:expiry_timestamp(User),
    ?assert(ExpiryTs > (Now - 10)),
    ?assert(ExpiryTs < (Now + 10)),

    ?UTIL_MOD:wait_for_token_to_expire(),
    #{<<"exp">> := Exp} = TokenData,
    ExpectedError = "Provided JWT token has expired at timestamp " ++ integer_to_list(Exp) ++ " (validated at " ++ integer_to_list(Exp) ++ ")",
    assert_resource_access_errors(ExpectedError, User, VHost, <<"foo">>, configure),

    ?assertMatch({refused, _, _},
                 rabbit_auth_backend_oauth2:user_login_authentication(Username, [{password, Token}])).

test_incorrect_kid(_) ->
    AltKid   = <<"other-token-key">>,
    Username = <<"username">>,
    Jwk      = ?UTIL_MOD:fixture_jwk(),
    application:set_env(rabbitmq_auth_backend_oauth2, resource_server_id, <<"rabbitmq">>),
    Token = ?UTIL_MOD:sign_token_hs(?UTIL_MOD:token_with_sub(?UTIL_MOD:fixture_token(), Username), Jwk, AltKid),
    ?assertMatch({refused, "Authentication using an OAuth 2/JWT token failed: ~tp", [{error,{missing_oauth_provider_attributes, [issuer]}}]},
                 rabbit_auth_backend_oauth2:user_login_authentication(Username, #{password => Token})).

login_and_check_vhost_access(Username, Token, Vhost) ->
  {ok, #auth_user{username = Username} = User} =
      rabbit_auth_backend_oauth2:user_login_authentication(Username, #{password => Token}),

  ?assertEqual(true, rabbit_auth_backend_oauth2:check_vhost_access(User, <<"vhost">>, Vhost)).

test_command_json(Config) ->
    Username = <<"username">>,
    Jwk      = ?UTIL_MOD:fixture_jwk(),
    Json     = rabbit_json:encode(Jwk),

    'Elixir.RabbitMQ.CLI.Ctl.Commands.AddUaaKeyCommand':run(
        [<<"token-key">>],
        #{node => rabbit_ct_broker_helpers:get_node_config(Config, 0, nodename), json => Json}),
    Token = ?UTIL_MOD:sign_token_hs(?UTIL_MOD:token_with_sub(?UTIL_MOD:fixture_token(), Username), Jwk),
    rabbit_ct_broker_helpers:rpc(Config,  0, unit_SUITE, login_and_check_vhost_access, [Username, Token, none]).

test_username_from(_) ->
    Pairs = [
      { <<"resolve username from 1st claim in the array of configured claims ">>,
        [<<"user_name">>, <<"email">>],
        #{
          <<"user_name">> => <<"rabbit_user">>,
          <<"email">> => <<"rabbit_user@">>
         },
        <<"rabbit_user">>
      },
      { <<"resolve username from 2nd claim in the array of configured claims">>,
        [<<"user_name">>, <<"email">>],
        #{
          <<"email">> => <<"rabbit_user">>
         },
        <<"rabbit_user">>
      },
      { <<"resolve username from configured string claim ">>,
        [<<"email">>],
        #{
          <<"email">> => <<"rabbit_user">>
         },
        <<"rabbit_user">>
      },
      { <<"unresolve username">>,
        [<<"user_name">>, <<"email">>],
        #{
          <<"email2">> => <<"rabbit_user">>
         },
        <<"unknown">>
      }
    ],
    lists:foreach(
        fun(
          {Comment, PreferredUsernameClaims, Token, ExpectedUsername}) ->
            ActualUsername = rabbit_auth_backend_oauth2:username_from(PreferredUsernameClaims, Token),
            ?assertEqual(ExpectedUsername, ActualUsername, Comment)
            end,
          Pairs).

test_command_pem_file(Config) ->
    Username = <<"username">>,
    CertsDir = ?config(rmq_certsdir, Config),
    Keyfile = filename:join([CertsDir, "client", "key.pem"]),
    Jwk = jose_jwk:from_pem_file(Keyfile),

    PublicJwk  = jose_jwk:to_public(Jwk),
    PublicKeyFile = filename:join([CertsDir, "client", "public.pem"]),
    jose_jwk:to_pem_file(PublicKeyFile, PublicJwk),

    'Elixir.RabbitMQ.CLI.Ctl.Commands.AddUaaKeyCommand':run(
        [<<"token-key">>],
        #{node => rabbit_ct_broker_helpers:get_node_config(Config, 0, nodename), pem_file => PublicKeyFile}),

    Token = ?UTIL_MOD:sign_token_rsa(?UTIL_MOD:fixture_token(), Jwk, <<"token-key">>),
    rabbit_ct_broker_helpers:rpc(Config,  0, unit_SUITE, login_and_check_vhost_access, [Username, Token, none]).


test_command_pem(Config) ->
    Username = <<"username">>,
    CertsDir = ?config(rmq_certsdir, Config),
    Keyfile = filename:join([CertsDir, "client", "key.pem"]),
    Jwk = jose_jwk:from_pem_file(Keyfile),

    Pem = jose_jwk:to_pem(jose_jwk:to_public(Jwk)),

    'Elixir.RabbitMQ.CLI.Ctl.Commands.AddUaaKeyCommand':run(
        [<<"token-key">>],
        #{node => rabbit_ct_broker_helpers:get_node_config(Config, 0, nodename), pem => Pem}),

    Token = ?UTIL_MOD:sign_token_rsa(?UTIL_MOD:token_with_sub(?UTIL_MOD:fixture_token(), Username), Jwk, <<"token-key">>),
    rabbit_ct_broker_helpers:rpc(Config,  0, unit_SUITE, login_and_check_vhost_access, [Username, Token, none]).

test_command_pem_no_kid(Config) ->
    Username = <<"username">>,
    CertsDir = ?config(rmq_certsdir, Config),
    Keyfile = filename:join([CertsDir, "client", "key.pem"]),
    Jwk = jose_jwk:from_pem_file(Keyfile),

    Pem = jose_jwk:to_pem(jose_jwk:to_public(Jwk)),

    'Elixir.RabbitMQ.CLI.Ctl.Commands.AddUaaKeyCommand':run(
        [<<"token-key">>],
        #{node => rabbit_ct_broker_helpers:get_node_config(Config, 0, nodename), pem => Pem}),

    Token = ?UTIL_MOD:sign_token_no_kid(?UTIL_MOD:token_with_sub(?UTIL_MOD:fixture_token(), Username), Jwk),
    rabbit_ct_broker_helpers:rpc(Config,  0, unit_SUITE, login_and_check_vhost_access, [Username, Token, none]).


test_own_scope(_) ->
    Examples = [
        {<<"foo.">>, [<<"foo">>, <<"foo.bar">>, <<"bar.foo">>,
                     <<"one.two">>, <<"foobar">>, <<"foo.other.third">>],
                    [<<"bar">>, <<"other.third">>]},
        {<<"foo.">>, [], []},
        {<<"foo.">>, [<<"foo">>, <<"other.foo.bar">>], []},
        {<<"">>, [<<"foo">>, <<"bar">>], [<<"foo">>, <<"bar">>]}
    ],
    lists:map(
        fun({ScopePrefix, Src, Dest}) ->
            Dest = rabbit_auth_backend_oauth2:filter_scopes(Src, ScopePrefix)
        end,
        Examples).

test_validate_payload_resource_server_id_mismatch(_) ->
    NoKnownResourceServerId = #{<<"aud">>   => [<<"foo">>, <<"bar">>],
                                <<"scope">> => [<<"foo">>, <<"foo.bar">>,
                                                <<"bar.foo">>, <<"one.two">>,
                                                <<"foobar">>, <<"foo.other.third">>]},
    EmptyAud = #{<<"aud">>   => [],
                 <<"scope">> => [<<"foo.bar">>, <<"bar.foo">>]},

    ?assertEqual({refused, {invalid_aud, {resource_id_not_found_in_aud, ?RESOURCE_SERVER_ID,
                                          [<<"foo">>,<<"bar">>]}}},
                 rabbit_auth_backend_oauth2:validate_payload(?RESOURCE_SERVER_ID, NoKnownResourceServerId, ?DEFAULT_SCOPE_PREFIX)),

    ?assertEqual({refused, {invalid_aud, {resource_id_not_found_in_aud, ?RESOURCE_SERVER_ID, []}}},
                 rabbit_auth_backend_oauth2:validate_payload(?RESOURCE_SERVER_ID, EmptyAud, ?DEFAULT_SCOPE_PREFIX)).

test_validate_payload_with_scope_prefix(_) ->
    Scenarios = [ { <<>>,
                #{<<"aud">>   => [?RESOURCE_SERVER_ID],
                  <<"scope">> => [<<"foo">>, <<"foo.bar">>, <<"foo.other.third">> ]},
                [<<"foo">>, <<"foo.bar">>, <<"foo.other.third">> ]
                },
                { <<"some-prefix::">>,
                  #{<<"aud">>   => [?RESOURCE_SERVER_ID],
                    <<"scope">> => [<<"some-prefix::foo">>, <<"foo.bar">>, <<"some-prefix::other.third">> ]},
                 [<<"foo">>, <<"other.third">>]
                 }

               ],

  lists:map(fun({ ScopePrefix, Token, ExpectedScopes}) ->
      ?assertEqual({ok, #{<<"aud">>   => [?RESOURCE_SERVER_ID], <<"scope">> => ExpectedScopes } },
        rabbit_auth_backend_oauth2:validate_payload(?RESOURCE_SERVER_ID, Token, ScopePrefix))
      end
    , Scenarios).

test_validate_payload(_) ->
    KnownResourceServerId = #{<<"aud">>   => [?RESOURCE_SERVER_ID],
                              <<"scope">> => [<<"foo">>, <<"rabbitmq.bar">>,
                                              <<"bar.foo">>, <<"one.two">>,
                                              <<"foobar">>, <<"rabbitmq.other.third">>]},
    ?assertEqual({ok, #{<<"aud">>   => [?RESOURCE_SERVER_ID],
                        <<"scope">> => [<<"bar">>, <<"other.third">>]}},
                 rabbit_auth_backend_oauth2:validate_payload(?RESOURCE_SERVER_ID, KnownResourceServerId, ?DEFAULT_SCOPE_PREFIX)).

test_validate_payload_without_scope(_) ->
    KnownResourceServerId = #{<<"aud">>   => [?RESOURCE_SERVER_ID]
                              },
    ?assertEqual({ok, #{<<"aud">>   => [?RESOURCE_SERVER_ID] }},
                 rabbit_auth_backend_oauth2:validate_payload(?RESOURCE_SERVER_ID, KnownResourceServerId, ?DEFAULT_SCOPE_PREFIX)).

test_validate_payload_when_verify_aud_false(_) ->
    WithoutAud = #{
                              <<"scope">> => [<<"foo">>, <<"rabbitmq.bar">>,
                                              <<"bar.foo">>, <<"one.two">>,
                                              <<"foobar">>, <<"rabbitmq.other.third">>]},
    ?assertEqual({ok, #{
                        <<"scope">> => [<<"bar">>, <<"other.third">>]}},
                 rabbit_auth_backend_oauth2:validate_payload(?RESOURCE_SERVER_ID, WithoutAud, ?DEFAULT_SCOPE_PREFIX)),

    WithAudWithUnknownResourceId = #{
                              <<"aud">>   => [<<"unknown">>],
                              <<"scope">> => [<<"foo">>, <<"rabbitmq.bar">>,
                                              <<"bar.foo">>, <<"one.two">>,
                                              <<"foobar">>, <<"rabbitmq.other.third">>]},
    ?assertEqual({ok, #{<<"aud">>   => [<<"unknown">>],
                        <<"scope">> => [<<"bar">>, <<"other.third">>]}},
                 rabbit_auth_backend_oauth2:validate_payload(?RESOURCE_SERVER_ID, WithAudWithUnknownResourceId, ?DEFAULT_SCOPE_PREFIX)).

%%
%% Helpers
%%

assert_vhost_access_granted(AuthUser, VHost) ->
    assert_vhost_access_response(true, AuthUser, VHost).

assert_vhost_access_denied(AuthUser, VHost) ->
    assert_vhost_access_response(false, AuthUser, VHost).

assert_vhost_access_response(ExpectedResult, AuthUser, VHost) ->
    ?assertEqual(ExpectedResult,
        rabbit_auth_backend_oauth2:check_vhost_access(AuthUser, VHost, none)).

assert_resource_access_granted(AuthUser, VHost, ResourceName, PermissionKind) ->
        assert_resource_access_response(true, AuthUser, VHost, ResourceName, PermissionKind).

assert_resource_access_denied(AuthUser, VHost, ResourceName, PermissionKind) ->
        assert_resource_access_response(false, AuthUser, VHost, ResourceName, PermissionKind).

assert_resource_access_errors(ExpectedError, AuthUser, VHost, ResourceName, PermissionKind) ->
    assert_resource_access_response({error, ExpectedError}, AuthUser, VHost, ResourceName, PermissionKind).

assert_resource_access_response(ExpectedResult, AuthUser, VHost, ResourceName, PermissionKind) ->
    ?assertEqual(ExpectedResult,
            rabbit_auth_backend_oauth2:check_resource_access(
                          AuthUser,
                          rabbit_misc:r(VHost, queue, ResourceName),
                          PermissionKind, #{})).

assert_resource_access_granted(AuthUser, VHost, ResourceKind, ResourceName, PermissionKind) ->
        assert_resource_access_response(true, AuthUser, VHost, ResourceKind, ResourceName, PermissionKind).

assert_resource_access_denied(AuthUser, VHost, ResourceKind, ResourceName, PermissionKind) ->
        assert_resource_access_response(false, AuthUser, VHost, ResourceKind, ResourceName, PermissionKind).

assert_resource_access_errors(ExpectedError, AuthUser, VHost, ResourceKind, ResourceName, PermissionKind) ->
    assert_resource_access_response({error, ExpectedError}, AuthUser, VHost, ResourceKind, ResourceName, PermissionKind).

assert_resource_access_response(ExpectedResult, AuthUser, VHost, ResourceKind, ResourceName, PermissionKind) ->
    ?assertEqual(ExpectedResult,
            rabbit_auth_backend_oauth2:check_resource_access(
                          AuthUser,
                          rabbit_misc:r(VHost, ResourceKind, ResourceName),
                          PermissionKind, #{})).

assert_topic_access_granted(AuthUser, VHost, ResourceName, PermissionKind, AuthContext) ->
    assert_topic_access_response(true, AuthUser, VHost, ResourceName, PermissionKind, AuthContext).

assert_topic_access_refused(AuthUser, VHost, ResourceName, PermissionKind, AuthContext) ->
    assert_topic_access_response(false, AuthUser, VHost, ResourceName, PermissionKind, AuthContext).

assert_topic_access_response(ExpectedResult, AuthUser, VHost, ResourceName, PermissionKind, AuthContext) ->
        ?assertEqual(ExpectedResult, rabbit_auth_backend_oauth2:check_topic_access(
                         AuthUser,
                         #resource{virtual_host = VHost,
                                   kind = topic,
                                   name = ResourceName},
                         PermissionKind,
                         AuthContext)).
