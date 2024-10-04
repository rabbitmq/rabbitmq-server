%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2024 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
%%

-module(rabbit_oauth2_resource_server_SUITE).

-compile(export_all).
-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").
-include("oauth2.hrl").

-define(RABBITMQ,<<"rabbitmq">>).
-define(RABBITMQ_RESOURCE_ONE,<<"rabbitmq1">>).
-define(RABBITMQ_RESOURCE_TWO,<<"rabbitmq2">>).
-define(OAUTH_PROVIDER_A,<<"A">>).
-define(OAUTH_PROVIDER_B,<<"B">>).

-import(oauth2_client, [get_oauth_provider/2]).
-import(rabbit_oauth2_resource_server, [resolve_resource_server_from_audience/1]).


all() -> [
    {group, without_resource_server_id},
    {group, with_rabbitmq_as_resource_server_id},
    {group, with_two_resource_servers}
    %{group, with_two_resource_servers_and_rabbitmq_as_resource_server_id}
].
groups() -> [
    {with_rabbitmq_as_resource_server_id, [], [
        resolve_resource_server_for_rabbitmq_audience,
        resolve_resource_server_for_rabbitmq_plus_unknown_audience,
        resolve_resource_server_for_none_audience_returns_no_aud_found,
        resolve_resource_server_for_unknown_audience_returns_no_matching_aud_found,
        {with_verify_aud_false, [], [
            resolve_resource_server_for_none_audience_returns_rabbitmq,
            resolve_resource_server_for_unknown_audience_returns_rabbitmq
        ]},
        {verify_get_rabbitmq_server_configuration, [],
            verify_get_rabbitmq_server_configuration()}
    ]},
    {without_resource_server_id, [], [
        resolve_resource_server_id_for_any_audience_returns_no_matching_aud_found
    ]},

    {with_two_resource_servers, [], [
        resolve_resource_server_id_for_rabbitmq1,
        resolve_resource_server_id_for_rabbitmq2,
        resolve_resource_server_id_for_both_resources_returns_error,
        resolve_resource_server_for_none_audience_returns_no_aud_found,
        resolve_resource_server_for_unknown_audience_returns_no_matching_aud_found,
        {with_verify_aud_false, [], [
            resolve_resource_server_for_none_audience_returns_rabbitmq2,
            resolve_resource_server_for_unknown_audience_returns_rabbitmq2,
            {with_rabbitmq1_verify_aud_false, [], [
                resolve_resource_server_for_none_audience_returns_error
            ]}
        ]},
        verify_rabbitmq1_server_configuration,
        {verify_configuration_inheritance_with_rabbitmq2, [],
            verify_configuration_inheritance_with_rabbitmq2()},
        {with_rabbitmq_as_resource_server_id, [], [
            resolve_resource_server_for_rabbitmq_audience,
            resolve_resource_server_id_for_rabbitmq1,
            resolve_resource_server_id_for_rabbitmq2
        ]}
    ]}
].

verify_get_rabbitmq_server_configuration() -> [
    rabbitmq_verify_aud_is_true,
    {with_verify_aud_false, [], [
        rabbitmq_verify_aud_is_false
    ]},
    rabbitmq_has_default_scope_prefix,
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
        rabbitmq_has_preferred_username_claims
    ]},
    rabbitmq_has_no_scope_aliases,
    {with_scope_aliases, [], [
        rabbitmq_has_scope_aliases
    ]}
].

verify_configuration_inheritance_with_rabbitmq2() -> [
    rabbitmq2_verify_aud_is_true,
    {with_verify_aud_false, [], [
        rabbitmq2_verify_aud_is_false
    ]},
    rabbitmq2_has_default_scope_prefix,
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
].

init_per_suite(Config) ->
    rabbit_ct_helpers:log_environment(),
    rabbit_ct_helpers:run_setup_steps(Config).

end_per_suite(Config) ->
    rabbit_ct_helpers:run_teardown_steps(Config).

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
    set_env(extra_scopes_source, Key),
    [{additional_scopes_key, Key} | Config];

init_per_group(with_preferred_username_claims, Config) ->
    Claims = [<<"new-user">>, <<"new-email">>],
    set_env(preferred_username_claims, Claims),
    [{preferred_username_claims, Claims} | Config];

init_per_group(with_scope_aliases, Config) ->
    Aliases = #{
        <<"admin">> => [<<"rabbitmq.tag:administrator">>]
    },
    set_env(scope_aliases, Aliases),
    [{scope_aliases, Aliases} | Config];

init_per_group(with_verify_aud_false, Config) ->
    set_env(verify_aud, false),
    Config;

init_per_group(with_rabbitmq1_verify_aud_false, Config) ->
    RabbitMQServers = get_env(resource_servers, #{}),
    Resource0 = maps:get(?RABBITMQ_RESOURCE_ONE, RabbitMQServers, []),
    Resource = [{verify_aud, false} | Resource0],
    set_env(resource_servers, maps:put(?RABBITMQ_RESOURCE_ONE, Resource,
        RabbitMQServers)),
    Config;

init_per_group(with_two_resource_servers, Config) ->
    RabbitMQ1 = [
        {id, ?RABBITMQ_RESOURCE_ONE},
        {resource_server_type, <<"some-type">>},
        {verify_aud, true},
        {scope_prefix, <<"some-prefix">>},
        {additional_scopes_key, <<"roles">>},
        {preferred_username_claims, [<<"x-username">>, <<"x-email">>]},
        {scope_aliases, #{ <<"admin">> => [<<"rabbitmq.tag:administrator">>]}},
        {oauth_provider_id, ?OAUTH_PROVIDER_A}
    ],
    RabbitMQ2 = [
        {id, ?RABBITMQ_RESOURCE_TWO}
    ],
    set_env(resource_servers, #{
        ?RABBITMQ_RESOURCE_ONE => RabbitMQ1,
        ?RABBITMQ_RESOURCE_TWO => RabbitMQ2
    }),
    [{?RABBITMQ_RESOURCE_ONE, RabbitMQ1}, {?RABBITMQ_RESOURCE_TWO, RabbitMQ2}]
        ++ Config;

init_per_group(_any, Config) ->
    Config.

end_per_group(with_default_oauth_provider_A, Config) ->
    unset_env(default_oauth_provider),
    Config;

end_per_group(with_default_oauth_provider_B, Config) ->
    unset_env(default_oauth_provider),
    Config;

end_per_group(with_rabbitmq_as_resource_server_id, Config) ->
    unset_env(resource_server_id),
    Config;

end_per_group(with_empty_scope_prefix, Config) ->
    unset_env(scope_prefix),
    Config;

end_per_group(with_verify_aud_false, Config) ->
    unset_env(verify_aud),
    Config;

end_per_group(with_two_resource_servers, Config) ->
    unset_env(resource_servers),
    Config;

end_per_group(with_scope_prefix, Config) ->
    unset_env(scope_prefix),
    Config;

end_per_group(with_rabbitmq1_verify_aud_false, Config) ->
    RabbitMQServers = get_env(resource_servers, #{}),
    Resource = maps:get(?RABBITMQ_RESOURCE_ONE, RabbitMQServers, []),
    set_env(resource_servers, maps:put(?RABBITMQ_RESOURCE_ONE,
        proplists:delete(verify_aud, Resource),
        RabbitMQServers)),
    Config;

end_per_group(with_additional_scopes_key, Config) ->
    unset_env(extra_scopes_source),
    Config;

end_per_group(with_preferred_username_claims, Config) ->
    unset_env(preferred_username_claims),
    Config;

end_per_group(with_scope_aliases, Config) ->
    unset_env(scope_aliases),
    Config;

end_per_group(_any, Config) ->
    Config.


%% --- Test cases

resolve_resource_server_for_rabbitmq_audience(_) ->
    assert_resource_server_id(?RABBITMQ, ?RABBITMQ).

resolve_resource_server_for_rabbitmq_plus_unknown_audience(_) ->
    assert_resource_server_id(?RABBITMQ, [?RABBITMQ, <<"unknown">>]).

resolve_resource_server_for_none_audience_returns_no_aud_found(_) ->
    assert_resource_server_id({error, no_aud_found}, none).

resolve_resource_server_for_none_audience_returns_rabbitmq2(_) ->
    assert_resource_server_id(?RABBITMQ_RESOURCE_TWO, none).

resolve_resource_server_for_unknown_audience_returns_no_matching_aud_found(_) ->
    assert_resource_server_id({error, no_matching_aud_found}, <<"unknown">>).

resolve_resource_server_for_none_audience_returns_rabbitmq(_) ->
    assert_resource_server_id(?RABBITMQ, none).

resolve_resource_server_for_unknown_audience_returns_rabbitmq(_) ->
    assert_resource_server_id(?RABBITMQ, <<"unknown">>).

resolve_resource_server_for_unknown_audience_returns_rabbitmq2(_) ->
    assert_resource_server_id(?RABBITMQ_RESOURCE_TWO, <<"unknown">>).

resolve_resource_server_for_none_audience_returns_error(_) ->
    assert_resource_server_id(
        {error, no_aud_found_cannot_pick_one_from_too_many_resource_servers},
        none).
resolve_resource_server_id_for_any_audience_returns_no_matching_aud_found(_) ->
    assert_resource_server_id({error, no_matching_aud_found}, ?RABBITMQ),
    assert_resource_server_id({error, no_matching_aud_found}, <<"unknown">>).

resolve_resource_server_id_for_rabbitmq1(_) ->
    assert_resource_server_id(?RABBITMQ_RESOURCE_ONE, ?RABBITMQ_RESOURCE_ONE).

resolve_resource_server_id_for_rabbitmq2(_) ->
    assert_resource_server_id(?RABBITMQ_RESOURCE_TWO, ?RABBITMQ_RESOURCE_TWO).

resolve_resource_server_id_for_both_resources_returns_error(_) ->
    assert_resource_server_id({error, aud_matched_many_resource_servers_only_one_allowed},
        [?RABBITMQ_RESOURCE_TWO, ?RABBITMQ_RESOURCE_ONE]).

rabbitmq_verify_aud_is_true(_) ->
    assert_verify_aud(true, ?RABBITMQ).

rabbitmq_verify_aud_is_false(_) ->
    assert_verify_aud(false, ?RABBITMQ).

rabbitmq2_verify_aud_is_true(_) ->
    assert_verify_aud(true, ?RABBITMQ_RESOURCE_TWO).

both_resources_oauth_provider_id_is_root(_) ->
    assert_oauth_provider_id(root, ?RABBITMQ_RESOURCE_ONE),
    assert_oauth_provider_id(root, ?RABBITMQ_RESOURCE_TWO).

rabbitmq2_verify_aud_is_false(_) ->
    assert_verify_aud(false, ?RABBITMQ_RESOURCE_TWO).

rabbitmq2_has_default_scope_prefix(_) ->
    assert_scope_prefix(erlang:iolist_to_binary([?RABBITMQ_RESOURCE_TWO, <<".">>]),
        ?RABBITMQ_RESOURCE_TWO).

rabbitmq2_has_scope_prefix(Config) ->
    assert_scope_prefix(?config(scope_prefix, Config), ?RABBITMQ_RESOURCE_TWO).

rabbitmq2_oauth_provider_id_is_root(_) ->
    assert_oauth_provider_id(root, ?RABBITMQ_RESOURCE_TWO).

rabbitmq2_oauth_provider_id_is_A(_) ->
    assert_oauth_provider_id(?OAUTH_PROVIDER_A, ?RABBITMQ_RESOURCE_TWO).

rabbitmq2_has_no_additional_scopes_key(_) ->
    assert_additional_scopes_key(undefined, ?RABBITMQ_RESOURCE_TWO).

rabbitmq2_has_additional_scopes_key(Config) ->
    assert_additional_scopes_key(?config(additional_scopes_key, Config),
        ?RABBITMQ_RESOURCE_TWO).

rabbitmq2_has_no_preferred_username_claims_but_gets_default(_) ->
    assert_preferred_username_claims(?DEFAULT_PREFERRED_USERNAME_CLAIMS,
        ?RABBITMQ_RESOURCE_TWO).

rabbitmq2_has_preferred_username_claims_plus_default(Config) ->
    assert_preferred_username_claims(?config(preferred_username_claims, Config)
        , ?RABBITMQ_RESOURCE_TWO).

rabbitmq2_has_no_scope_aliases(_) ->
    assert_scope_aliases(undefined, ?RABBITMQ_RESOURCE_TWO).

rabbitmq2_has_scope_aliases(Config) ->
    assert_scope_aliases(?config(scope_aliases, Config), ?RABBITMQ_RESOURCE_TWO).

rabbitmq_oauth_provider_id_is_root(_) ->
    assert_oauth_provider_id(root, ?RABBITMQ).

rabbitmq_oauth_provider_id_is_A(_) ->
    assert_oauth_provider_id(?OAUTH_PROVIDER_A, ?RABBITMQ).

rabbitmq_has_default_scope_prefix(_) ->
    assert_scope_prefix(erlang:iolist_to_binary([?RABBITMQ, <<".">>]), ?RABBITMQ).

rabbitmq_has_scope_prefix(Config) ->
    assert_scope_prefix(?config(scope_prefix, Config), ?RABBITMQ).

rabbitmq_has_empty_scope_prefix(_) ->
    assert_scope_prefix(<<"">>, ?RABBITMQ).

rabbitmq_has_no_additional_scopes_key(_) ->
    assert_additional_scopes_key(undefined, ?RABBITMQ).

rabbitmq_has_additional_scopes_key(Config) ->
    assert_additional_scopes_key(?config(additional_scopes_key, Config),
        ?RABBITMQ).

rabbitmq_has_no_preferred_username_claims_but_gets_default(_) ->
    assert_preferred_username_claims(?DEFAULT_PREFERRED_USERNAME_CLAIMS, ?RABBITMQ).

rabbitmq_has_preferred_username_claims(Config) ->
    assert_preferred_username_claims(?config(preferred_username_claims, Config),
        ?RABBITMQ).

rabbitmq_has_no_scope_aliases(_) ->
    assert_scope_aliases(undefined, ?RABBITMQ).

rabbitmq_has_scope_aliases(Config) ->
    assert_scope_aliases(?config(scope_aliases, Config), ?RABBITMQ).

verify_rabbitmq1_server_configuration(Config) ->
    ConfigRabbitMQ = ?config(?RABBITMQ_RESOURCE_ONE, Config),
    {ok, ActualRabbitMQ} = resolve_resource_server_from_audience(?RABBITMQ_RESOURCE_ONE),
    ?assertEqual(proplists:get_value(id, ConfigRabbitMQ),
        ActualRabbitMQ#resource_server.id),
    ?assertEqual(proplists:get_value(resource_server_type, ConfigRabbitMQ),
        ActualRabbitMQ#resource_server.resource_server_type),
    ?assertEqual(proplists:get_value(verify_aud, ConfigRabbitMQ),
        ActualRabbitMQ#resource_server.verify_aud),
    ?assertEqual(proplists:get_value(scope_prefix, ConfigRabbitMQ),
        ActualRabbitMQ#resource_server.scope_prefix),
    ?assertEqual(proplists:get_value(extract_scopes_source, ConfigRabbitMQ),
        ActualRabbitMQ#resource_server.additional_scopes_key),
    ?assertEqual(proplists:get_value(preferred_username_claims, ConfigRabbitMQ),
        ActualRabbitMQ#resource_server.preferred_username_claims),
    ?assertEqual(proplists:get_value(scope_aliases, ConfigRabbitMQ),
        ActualRabbitMQ#resource_server.scope_aliases),
    ?assertEqual(proplists:get_value(oauth_provider_id, ConfigRabbitMQ),
        ActualRabbitMQ#resource_server.oauth_provider_id).

%% -----

assert_resource_server_id({error, ExpectedError}, Audience) ->
    {error, ExpectedError} = resolve_resource_server_from_audience(Audience);
assert_resource_server_id(Expected, Audience) ->
    {ok, Actual} = resolve_resource_server_from_audience(Audience),
    ?assertEqual(Expected, Actual#resource_server.id).

assert_verify_aud(Expected, Audience) ->
    {ok, Actual} = resolve_resource_server_from_audience(Audience),
    ?assertEqual(Expected, Actual#resource_server.verify_aud).

assert_oauth_provider_id(Expected, Audience) ->
    {ok, Actual} = resolve_resource_server_from_audience(Audience),
    ct:log("Actual:~p", [Actual]),
    ?assertEqual(Expected, Actual#resource_server.oauth_provider_id).

assert_scope_prefix(Expected, Audience) ->
    {ok, Actual} = resolve_resource_server_from_audience(Audience),
    ?assertEqual(Expected, Actual#resource_server.scope_prefix).

assert_additional_scopes_key(Expected, Audience) ->
    {ok, Actual} = resolve_resource_server_from_audience(Audience),
    ?assertEqual(Expected, Actual#resource_server.additional_scopes_key).

assert_preferred_username_claims(Expected, Audience) ->
    {ok, Actual} = resolve_resource_server_from_audience(Audience),
    ?assertEqual(Expected, Actual#resource_server.preferred_username_claims).

assert_scope_aliases(Expected, Audience) ->
    {ok, Actual} = resolve_resource_server_from_audience(Audience),
    ?assertEqual(Expected, Actual#resource_server.scope_aliases).

get_env(Par) ->
    application:get_env(rabbitmq_auth_backend_oauth2, Par).
get_env(Par, Def) ->
    application:get_env(rabbitmq_auth_backend_oauth2, Par, Def).
set_env(Par, Val) ->
    application:set_env(rabbitmq_auth_backend_oauth2, Par, Val).
unset_env(Par) ->
    application:unset_env(rabbitmq_auth_backend_oauth2, Par).
