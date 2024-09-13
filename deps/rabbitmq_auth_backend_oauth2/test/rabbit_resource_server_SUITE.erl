%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2024 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
%%

-module(rabbit_resource_server_SUITE).

-compile(export_all).
-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("oauth2_client/include/oauth2_client.hrl").

-define(RABBITMQ,<<"rabbitmq">>).
-define(RABBITMQ_RESOURCE_ONE,<<"rabbitmq1">>).
-define(RABBITMQ_RESOURCE_TWO,<<"rabbitmq2">>).
-define(OAUTH_PROVIDER_A,<<"A">>).
-define(OAUTH_PROVIDER_B,<<"B">>).

-import(oauth2_client, [get_oauth_provider/2]).
-import(rabbit_resource_server, [
    resolve_resource_server_id_from_audience/1,
    get_resource_server/1
]).


all() -> [
    {group, without_resource_server_id},
    {group, with_rabbitmq_as_resource_server_id},
    {group, with_two_resource_servers},
    {group, with_two_resource_servers_and_rabbitmq_as_resource_server_id}
].
groups() -> [

    {verify_get_rabbitmq_server_configuration, [], [
        rabbitmq_verify_aud_is_true,
        {with_verify_aud_false, [], [
            rabbitmq_verify_aud_is_false
        ]},
        rabbitmq_has_no_scope_prefix,
        {with_scope_prefix, [], [
            rabbitmq_has_scope_prefix
        ]},
        {with_empty_scope_prefix, [], [
            rabbitmq_has_empty_scope_prefix
        ]},
        rabbitmq_oauth_provider_id_is_root,
        {with_default_oauth_provider_A, [], [
            rabbitmq_oauth_provider_id_is_A
        ]},
        rabbitmq_has_no_additional_scopes_key,
        {with_additional_scopes_key, [], [
            rabbitmq_has_additional_scopes_key
        ]},
        rabbitmq_has_no_preferred_username_claims_but_gets_default,
        {with_preferred_username_claims, [], [
            rabbitmq_has_preferred_username_claims_plus_default
        ]},
        rabbitmq_has_no_scope_aliases,
        {with_scope_aliases, [], [
            rabbitmq_has_scope_aliases
        ]}
    ]},
    {with_rabbitmq_as_resource_server_id, [], [
        resolve_resource_server_for_rabbitmq_audience,
        resolve_resource_server_for_rabbitmq_plus_unknown_audience,
        resolve_resource_server_for_none_audience_returns_error,
        resolve_resource_server_for_unknown_audience_returns_error,
        {with_verify_aud_false, [], [
            resolve_resource_server_for_none_audience_returns_rabbitmq,
            resolve_resource_server_for_unknown_audience_returns_rabbitmq
        ]},
        {group, verify_get_rabbitmq_server_configuration}
    ]},
    {without_resource_server_id, [], [
        resolve_resource_server_id_for_any_audience_returns_error
    ]},
    {verify_configuration_inheritance_with_rabbitmq2, [], [
        rabbitmq2_verify_aud_is_true,
        {with_verify_aud_false, [], [
            rabbitmq2_verify_aud_is_false
        ]},
        rabbitmq2_has_no_scope_prefix,
        {with_scope_prefix, [], [
            rabbitmq2_has_scope_prefix
        ]},
        rabbitmq2_oauth_provider_id_is_root,
        {with_default_oauth_provider_A, [], [
            rabbitmq2_oauth_provider_id_is_A
        ]},
        rabbitmq2_has_no_additional_scopes_key,
        {with_additional_scopes_key, [], [
            rabbitmq2_has_additional_scopes_key
        ]},
        rabbitmq2_has_no_preferred_username_claims_but_gets_default,
        {with_preferred_username_claims, [], [
            rabbitmq2_has_preferred_username_claims_plus_default
        ]},
        rabbitmq2_has_no_scope_aliases,
        {with_scope_aliases, [], [
            rabbitmq2_has_scope_aliases
        ]}
    ]},
    {with_two_resource_servers, [], [
        resolve_resource_server_id_for_rabbitmq1,
        resolve_resource_server_id_for_rabbitmq2,
        resolve_resource_server_id_for_both_resources_returns_error,
        resolve_resource_server_for_none_audience_returns_error,
        resolve_resource_server_for_unknown_audience_returns_error,
        {with_verify_aud_false, [], [
            resolve_resource_server_for_none_audience_returns_rabbitmq1,
            resolve_resource_server_for_unknown_audience_returns_rabbitmq1,
            {with_rabbitmq1_verify_aud_false, [], [
                resolve_resource_server_for_none_audience_returns_error
            ]}
        ]},
        {group, verify_rabbitmq1_server_configuration},
        {group, verify_configuration_inheritance_with_rabbitmq2},
        {with_rabbitmq_as_resource_server_id, [], [
            resolve_resource_server_for_rabbitmq_audience,
            resolve_resource_server_id_for_rabbitmq1,
            resolve_resource_server_id_for_rabbitmq2
        ]}
    ]}
].

init_per_suite(Config) ->
    rabbit_ct_helpers:log_environment(),
    rabbit_ct_helpers:run_setup_steps(Config).

end_per_suite(Config) ->
    rabbit_ct_helpers:run_teardown_steps(Config).

init_per_group(with_jwks_url, Config) ->
    KeyConfig = get_env(key_config, []),
    set_env(key_config, KeyConfig ++
        [{jwks_url,build_url_to_oauth_provider(<<"/keys">>)}]),
    [{key_config_before_group_with_jwks_url, KeyConfig} | Config];

init_per_group(with_default_oauth_provider_A, Config) ->
    set_env(default_oauth_provider, ?OAUTH_PROVIDER_A),
    Config;

init_per_group(with_default_oauth_provider_B, Config) ->
    set_env(default_oauth_provider, ?OAUTH_PROVIDER_B),
    Config;

init_per_group(with_rabbitmq_as_resource_server_id, Config) ->
    set_env(resource_server_id, ?RABBITMQ),
    Config;

init_per_group(with_scope_prefix, Config) ->
    Prefix = <<"some-prefix:">>,
    set_env(scope_prefix, Prefix),
    [{scope_prefix, Prefix} | Config];

init_per_group(with_empty_scope_prefix, Config) ->
    Prefix = <<"">>,
    set_env(scope_prefix, Prefix),
    Config;

init_per_group(with_additional_scopes_key, Config) ->
    Key = <<"roles">>,
    set_env(additional_scopes_key, Key),
    [{additional_scopes_key, Prefix} | Config;

init_per_group(with_preferred_username_claims, Config) ->
    Claims = [<<"new-user">>, <<"new-email">>],
    set_env(preferred_username_claims, Key),
    [{preferred_username_claims, Claims} | Config;


init_per_group(with_scope_aliases, Config) ->
    Aliases = #{
        <<"admin">> -> [<<"rabbitmq.tag:administrator">>]
    },
    set_env(scope_aliases, Aliases),
    [{scope_aliases, Aliases} | Config;

init_per_group(with_empty_scope_prefix_for_resource_one, Config) ->
    ResourceServers = get_env(resource_servers, #{}),
    Proplist = maps:get(?RABBITMQ_RESOURCE_ONE, ResourceServers, []),
     set_env(resource_servers, maps:put(?RABBITMQ_RESOURCE_ONE,
        [{scope_prefix, <<"">>} | proplists:delete(scope_prefix, Proplist)],
            ResourceServers)),
    Config;

init_per_group(with_verify_aud_false, Config) ->
    set_env(verify_aud, false),
    Config;

init_per_group(with_rabbitmq2_verify_aud_false, Config) ->
    ResourceServers = get_env(resource_servers, #{}),
    Proplist = maps:get(?RABBITMQ_RESOURCE_TWO, ResourceServers, []),
    set_env(resource_servers, maps:put(?RABBITMQ_RESOURCE_TWO,
        [{verify_aud, false} | proplists:delete(verify_aud, Proplist)],
            ResourceServers)),
    Config;

init_per_group(with_two_resource_servers_and_rabbitmq_as_resource_server_id, Config) ->
    set_env(resource_server_id, ?RABBITMQ),
    set_env(key_config, [{jwks_url,<<"https://oauth-for-rabbitmq">> }]),
    set_env(resource_servers,
        #{?RABBITMQ_RESOURCE_ONE =>  [
            { key_config, [
                {jwks_url,<<"https://oauth-for-rabbitmq1">> }
            ]}

        ],
          ?RABBITMQ_RESOURCE_TWO =>  [
            { key_config, [
                {jwks_url,<<"https://oauth-for-rabbitmq2">> }
            ]}
          ]
        }),
    Config;

init_per_group(with_different_oauth_provider_for_each_resource, Config) ->
    {ok, ResourceServers} = get_env(resource_servers),
    Rabbit1 = maps:get(?RABBITMQ_RESOURCE_ONE, ResourceServers) ++
        [ {oauth_provider_id, <<"A">>} ],
    Rabbit2 = maps:get(?RABBITMQ_RESOURCE_TWO, ResourceServers) ++
        [ {oauth_provider_id, <<"B">>} ],
    ResourceServers1 = maps:update(?RABBITMQ_RESOURCE_ONE, Rabbit1, ResourceServers),
    set_env(resource_servers, maps:update(?RABBITMQ_RESOURCE_TWO, Rabbit2,
        ResourceServers1)),
    Config;

init_per_group(with_two_resource_servers, Config) ->
    RabbitMQ1 = [
        {id, ?RABBITMQ_RESOURCE_ONE},
        {resource_server_type, <<"some-type">>},
        {verify_aud, false},
        {scope_prefix, <<"some-prefix">>},
        {additional_scopes_key, <<"roles">>},
        {preferred_username_claims, [<<"x-username">>, <<"x-email">>]},
        {scope_aliases, #{ <<"admin">> -> [<<"rabbitmq.tag:administrator"]},
        {oauth_provider_id, ?OAUTH_PROVIDER_A}
    ],
    RabbitMQ2 = [
        {id, ?RABBITMQ_RESOURCE_ONE}
    ],
    set_env(resource_servers, #{
        ?RABBITMQ_RESOURCE_ONE => RabbitMQ1,
        ?RABBITMQ_RESOURCE_TWO => RabbitMQ2
    }),
    [{?RABBITMQ_RESOURCE_ONE, RabbitMQ1} | {?RABBITMQ_RESOURCE_TWO, RabbitMQ2}
        | Config;

init_per_group(inheritance_group, Config) ->
    set_env(resource_server_id, ?RABBITMQ),
    set_env(resource_server_type, <<"rabbitmq-type">>),
    set_env(scope_prefix, <<"some-prefix-">>),
    set_env(extra_scopes_source, <<"roles">>),
    set_env(scope_aliases, #{}),

    set_env(key_config, [ {jwks_url,<<"https://oauth-for-rabbitmq">> } ]),

    set_env(resource_servers,
        #{?RABBITMQ_RESOURCE_ONE =>  [
            { extra_scopes_source, <<"extra-scope-1">>},
            { verify_aud, false},
            { preferred_username_claims, [<<"email-address">>] },
            { scope_prefix, <<"my-prefix:">> },
            { resource_server_type, <<"my-type">> },
            { scope_aliases, #{} }
        ],
        ?RABBITMQ_RESOURCE_TWO =>  [ {id, ?RABBITMQ_RESOURCE_TWO  } ]
        }
    ),
    Config;

init_per_group(_any, Config) ->
    Config.

end_per_group(with_empty_scope_prefix, Config) ->
    unset_env(scope_prefix),
    Config;

end_per_group(with_resource_server_id, Config) ->
    unset_env(resource_server_id),
    Config;

end_per_group(with_verify_aud_false, Config) ->
    unset_env(verify_aud),
    Config;

end_per_group(with_verify_aud_false_for_resource_two, Config) ->
    ResourceServers = get_env(rabbitmq_auth_backend_oauth2, resource_servers, #{}),
    Proplist = maps:get(?RABBITMQ_RESOURCE_TWO, ResourceServers, []),
     set_env(resource_servers,
        maps:put(?RABBITMQ_RESOURCE_TWO, proplists:delete(verify_aud, Proplist), ResourceServers)),
    Config;

end_per_group(with_empty_scope_prefix_for_resource_one, Config) ->
    ResourceServers = get_env(rabbitmq_auth_backend_oauth2, resource_servers, #{}),
    Proplist = maps:get(?RABBITMQ_RESOURCE_ONE, ResourceServers, []),
     set_env(resource_servers,
        maps:put(?RABBITMQ_RESOURCE_ONE, proplists:delete(scope_prefix, Proplist), ResourceServers)),
    Config;

end_per_group(with_two_resource_servers, Config) ->
    unset_env(resource_servers),
    Config;

end_per_group(with_different_oauth_provider_for_each_resource, Config) ->
    {ok, ResourceServers} = get_env(resource_servers),
    Rabbit1 = proplists:delete(oauth_provider_id,
        maps:get(?RABBITMQ_RESOURCE_ONE, ResourceServers)),
    Rabbit2 = proplists:delete(oauth_provider_id,
        maps:get(?RABBITMQ_RESOURCE_TWO, ResourceServers)),
    ResourceServers1 = maps:update(?RABBITMQ_RESOURCE_ONE, Rabbit1,
        ResourceServers),
    set_env(resource_servers, maps:update(?RABBITMQ_RESOURCE_TWO, Rabbit2,
        ResourceServers1)),
    Config;

end_per_group(inheritance_group, Config) ->
    unset_env(resource_server_id),
    unset_env(scope_prefix),
    unset_env(extra_scopes_source),
    unset_env(key_config),
    unset_env(resource_servers),
    Config;

end_per_group(with_scope_prefix, Config) ->
    unset_env(scope_prefix),
    Config;

end_per_group(_any, Config) ->
    Config.


%% --- Test cases

resolve_resource_server_for_rabbitmq_audience(_ ->
    ?RABBITMQ = resolve_resource_server_id_for_audience(?RABBITMQ).

resolve_resource_server_for_rabbitmq_plus_unknown_audience(_) ->
    ?RABBITMQ = resolve_resource_server_id_for_audience([?RABBITMQ,
        <<"unknown">>]).

resolve_resource_server_for_none_audience_returns_error(_) ->
    {error, missing_audience_in_token} =
        resolve_resource_server_id_for_audience(none).

resolve_resource_server_for_unknown_audience_returns_error(_) ->
    {error, no_matching_aud_found} =
        resolve_resource_server_id_for_audience(<<"unknown">>).

resolve_resource_server_for_none_audience_returns_rabbitmq(_) ->
    ?RABBITMQ = resolve_resource_server_id_for_audience(none).

resolve_resource_server_for_unknown_audience_returns_rabbitmq(_) ->
    ?RABBITMQ = resolve_resource_server_id_for_audience(<<"unknown">>).

resolve_resource_server_id_for_any_audience_returns_error(_) ->
    {error, no_matching_aud_found} =
        resolve_resource_server_id_for_audience(?RABBITMQ),
    {error, no_matching_aud_found} =
        resolve_resource_server_id_for_audience(<<"unknown">>),

resolve_resource_server_id_for_rabbitmq1(_) ->
    ?RABBITMQ_RESOURCE_ONE = resolve_resource_server_id_for_audience(
        ?RABBITMQ_RESOURCE_ONE).

resolve_resource_server_id_for_rabbitmq2(_) ->
    ?RABBITMQ_RESOURCE_TWO = resolve_resource_server_id_for_audience(
        ?RABBITMQ_RESOURCE_TWO).

resolve_resource_server_id_for_both_resources_returns_error(_) ->
    {error, only_one_resource_server_as_audience_found_many} =
        resolve_resource_server_id_for_audience([?RABBITMQ_RESOURCE_TWO,
            ?RABBITMQ_RESOURCE_ONE]).

rabbitmq_verify_aud_is_true(_) ->
    #resource_server{verify_aud = true} =
        resolve_resource_server_id_for_audience(?RABBITMQ).

rabbitmq_verify_aud_is_false(_) ->
    #resource_server{verify_aud = false} =
        resolve_resource_server_id_for_audience(?RABBITMQ).

rabbitmq2_verify_aud_is_true(_) ->
    #resource_server{verify_aud = true} =
        resolve_resource_server_id_for_audience(?RABBITMQ_RESOURCE_TWO).

both_resources_oauth_provider_id_is_root(_) ->
    #resource_server{oauth_provider_id = root} =
        resolve_resource_server_id_for_audience(?RABBITMQ_RESOURCE_ONE),
    #resource_server{oauth_provider_id = root} =
        resolve_resource_server_id_for_audience(?RABBITMQ_RESOURCE_TWO).

rabbitmq2_verify_aud_is_false(_) ->
    #resource_server{verify_aud = false} =
        resolve_resource_server_id_for_audience(?RABBITMQ_RESOURCE_TWO).

rabbitmq2_has_no_scope_prefix(_) ->
    #resource_server{scope_prefix = undefined} =
        resolve_resource_server_id_for_audience(?RABBITMQ_RESOURCE_TWO).

rabbitmq2_has_scope_prefix(Config) ->
    #resource_server{scope_prefix = ScopePrefix} =
        resolve_resource_server_id_for_audience(?RABBITMQ_RESOURCE_TWO),
    ?assertEqual(?config(scope_prefix, Config), ScopePrefix).

rabbitmq2_oauth_provider_id_is_root(_) ->
    #resource_server{oauth_provider_id = root} =
        resolve_resource_server_id_for_audience(?RABBITMQ_RESOURCE_TWO).

rabbitmq2_oauth_provider_id_is_A(_) ->
    #resource_server{oauth_provider_id = ?OAUTH_PROVIDER_A} =
        resolve_resource_server_id_for_audience(?RABBITMQ_RESOURCE_TWO).

rabbitmq2_has_no_additional_scopes_key(_) ->
    #resource_server{additional_scopes_key = undefined} =
        resolve_resource_server_id_for_audience(?RABBITMQ_RESOURCE_TWO).

rabbitmq2_has_additional_scopes_key(Config) ->
    #resource_server{additional_scopes_key = ScopesKey} =
        resolve_resource_server_id_for_audience(?RABBITMQ_RESOURCE_TWO),
    ?assertEqual(?config(additional_scopes_key, Config), ScopesKey).

rabbitmq2_has_no_preferred_username_claims_but_gets_default(_) ->
    #resource_server{preferred_username_claims = Claims} =
        resolve_resource_server_id_for_audience(?RABBITMQ_RESOURCE_TWO),
    ?assertEqual(?DEFAULT_PREFERRED_USERNAME_CLAIMS, Claims).

rabbitmq2_has_preferred_username_claims_plus_default(Config) ->
    #resource_server{preferred_username_claims = Claims} =
        resolve_resource_server_id_for_audience(?RABBITMQ_RESOURCE_TWO),
    ?assertEqual(?config(preferred_username_claims, Config)
        ++ ?DEFAULT_PREFERRED_USERNAME_CLAIMS, Claims).

rabbitmq2_has_no_scope_aliases(_) ->
    #resource_server{scope_aliases = undefined} =
        resolve_resource_server_id_for_audience(?RABBITMQ_RESOURCE_TWO).

rabbitmq2_has_scope_aliases(_) ->
    #resource_server{scope_aliases = Aliases} =
        resolve_resource_server_id_for_audience(?RABBITMQ_RESOURCE_TWO),
    ?assertEqual(?config(scope_aliases, Config), Aliases).

rabbitmq_oauth_provider_id_is_root(_) ->
    #resource_server{oauth_provider_id = root} =
        resolve_resource_server_id_for_audience(?RABBITMQ).

rabbitmq_oauth_provider_id_is_A(_) ->
    #resource_server{oauth_provider_id = ?OAUTH_PROVIDER_A} =
        resolve_resource_server_id_for_audience(?RABBITMQ).

rabbitmq_has_no_scope_prefix(_) ->
    #resource_server{scope_prefix = undefined} =
        resolve_resource_server_id_for_audience(?RABBITMQ),

rabbitmq_has_scope_prefix(Config) ->
    #resource_server{scope_prefix = ScopePrefix} =
        resolve_resource_server_id_for_audience (?RABBITMQ),
    ?assertEqual(?config(scope_prefix, Config), ScopePrefix).

rabbitmq_has_empty_scope_prefix() ->
    #resource_server{scope_prefix = <<"">>} =
        resolve_resource_server_id_for_audience (?RABBITMQ).

rabbitmq_has_no_additional_scopes_key(_) ->
    #resource_server{additional_scopes_key = undefined} =
        resolve_resource_server_id_for_audience(?RABBITMQ),

rabbitmq_has_additional_scopes_key(Config) ->
    #resource_server{additional_scopes_key = AdditionalScopesKey} =
        resolve_resource_server_id_for_audience (?RABBITMQ),
    ?assertEqual(?config(additional_scopes_key, Config), AdditionalScopesKey).

rabbitmq_has_no_preferred_username_claims_but_gets_default(_) ->
    #resource_server{preferred_username_claims = ?DEFAULT_PREFERRED_USERNAME_CLAIMS} =
        resolve_resource_server_id_for_audience(?RABBITMQ).

rabbitmq_has_preferred_username_claims_plus_default(Config) ->
    #resource_server{additional_scopes_key = AdditionalScopesKey} =
        resolve_resource_server_id_for_audience (?RABBITMQ),
    ?assertEqual(?config(preferred_username_claims, Config) ++
        ?DEFAULT_PREFERRED_USERNAME_CLAIMS, AdditionalScopesKey).

rabbitmq_has_no_scope_aliases(_) ->
    #resource_server{scope_aliases = undefined} =
        resolve_resource_server_id_for_audience(?RABBITMQ),

rabbitmq_has_scope_aliases(Config) ->
    #resource_server{scope_aliases = Aliases} =
        resolve_resource_server_id_for_audience (?RABBITMQ),
    ?assertEqual(?config(scope_aliases, Config), Aliases).


verify_rabbitmq1_server_configuration(Config) ->
    ConfigRabbitMQ = ?config(?RABBITMQ_RESOURCE_ONE, Config),
    ActualRabbitMQ = get_resource_server(?RABBITMQ_RESOURCE_ONE),
    ?assertEqual(ConfigRabbitMQ#resource_server.id,
        ActualRabbitMQ#resource_server.id),
    ?assertEqual(ConfigRabbitMQ#resource_server.resource_server_type,
        ActualRabbitMQ#resource_server.resource_server_type),
    ?assertEqual(ConfigRabbitMQ#resource_server.verify_aud,
        ActualRabbitMQ#resource_server.verify_aud),
    ?assertEqual(ConfigRabbitMQ#resource_server.scope_prefix,
        ActualRabbitMQ#resource_server.scope_prefix),
    ?assertEqual(ConfigRabbitMQ#resource_server.additional_scopes_key,
        ActualRabbitMQ#resource_server.additional_scopes_key),
    ?assertEqual(ConfigRabbitMQ#resource_server.preferred_username_claims ++
        ?DEFAULT_PREFERRED_USERNAME_CLAIMS,
        ActualRabbitMQ#resource_server.preferred_username_claims),
    ?assertEqual(ConfigRabbitMQ#resource_server.scope_aliases,
        ActualRabbitMQ#resource_server.scope_aliases),
    ?assertEqual(ConfigRabbitMQ#resource_server.oauth_provider_id,
        ActualRabbitMQ#resource_server.oauth_provider_id).

%% -----

get_env(Par) ->
    application:get_env(rabbitmq_auth_backend_oauth2, Par).
get_env(Par, Def) ->
    application:get_env(rabbitmq_auth_backend_oauth2, Par, Def).
set_env(Par, Val) ->
    application:set_env(rabbitmq_auth_backend_oauth2, Par, Val).
unset_env(Par) ->
    unset_env(rabbitmq_auth_backend_oauth2, Par).
