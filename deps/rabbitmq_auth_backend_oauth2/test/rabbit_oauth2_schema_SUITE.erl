%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2024 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
%%
-module(rabbit_oauth2_schema_SUITE).

-compile(export_all).

-include_lib("rabbit_common/include/rabbit.hrl").
-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").

-import(rabbit_oauth2_schema, [
    translate_endpoint_params/2, 
    translate_oauth_providers/1,
    translate_resource_servers/1,
    translate_scope_aliases/1
]).

all() ->
    [
        test_without_oauth_providers,
        test_with_one_oauth_provider,
        test_with_many_oauth_providers,
        test_oauth_providers_attributes,
        test_oauth_providers_attributes_with_invalid_uri,
        test_oauth_providers_algorithms,
        test_oauth_providers_https,
        test_oauth_providers_https_with_missing_cacertfile,
        test_oauth_providers_signing_keys,
        test_without_endpoint_params,
        test_with_endpoint_params,
        test_with_invalid_endpoint_params,
        test_without_resource_servers,
        test_with_one_resource_server,
        test_with_many_resource_servers,
        test_resource_servers_attributes,
        test_invalid_oauth_providers_endpoint_params,
        test_without_oauth_providers_with_endpoint_params,
        test_scope_aliases_configured_as_list_of_properties,
        test_scope_aliases_configured_as_map,
        test_scope_aliases_configured_as_list_of_missing_properties
    ].


test_without_oauth_providers(_) ->
    #{} = translate_oauth_providers([]).

test_without_resource_servers(_) ->
    #{} = translate_resource_servers([]).

test_without_endpoint_params(_) ->
    [] = translate_endpoint_params("oauth_discovery_endpoint_params", []).

test_with_invalid_endpoint_params(_) ->
    try translate_endpoint_params("discovery_endpoint_params", [
            {["auth_oauth2","discovery_endpoint_params"], "some-value1"}]) of
        _ -> {throw, should_have_failed}
    catch
        _ -> ok
    end.

test_with_endpoint_params(_) ->
    Conf = [
        {["auth_oauth2","discovery_endpoint_params","param1"], "some-value1"},
        {["auth_oauth2","discovery_endpoint_params","param2"], "some-value2"}
    ],
    [ {<<"param1">>, <<"some-value1">>}, {<<"param2">>, <<"some-value2">>} ] =
        translate_endpoint_params("discovery_endpoint_params", Conf).

test_invalid_oauth_providers_endpoint_params(_) ->
    try translate_oauth_providers([
            {["auth_oauth2","oauth_providers", "X", "discovery_endpoint_params"], ""}]) of
        _ -> {throw, should_have_failed}
    catch
        _ -> ok
    end.

test_without_oauth_providers_with_endpoint_params(_) ->
    Conf = [
        {["auth_oauth2","oauth_providers", "A", "discovery_endpoint_params","param1"], 
            "some-value1"},
        {["auth_oauth2","oauth_providers", "A", "discovery_endpoint_params","param2"], 
            "some-value2"},
        {["auth_oauth2","oauth_providers", "B", "discovery_endpoint_params","param3"], 
            "some-value3"}
    ],
    #{
        <<"A">> := [{discovery_endpoint_params, [
                      {<<"param1">>, <<"some-value1">>},
                      {<<"param2">>, <<"some-value2">>}
                    ]}],
        <<"B">> := [{discovery_endpoint_params, [
                      {<<"param3">>, <<"some-value3">>}
                    ]}]

    } = translate_oauth_providers(Conf).

test_with_one_oauth_provider(_) ->
    Conf = [
        {["auth_oauth2","oauth_providers","keycloak","issuer"],
            "https://rabbit"}
    ],
    #{<<"keycloak">> := [
        {issuer, "https://rabbit"}]
    } = translate_oauth_providers(Conf).

test_with_one_resource_server(_) ->
    Conf = [
        {["auth_oauth2","resource_servers","rabbitmq1","id"],"rabbitmq1"}
    ],
    #{<<"rabbitmq1">> := [{id, <<"rabbitmq1">>}]
    } = translate_resource_servers(Conf).

test_with_many_oauth_providers(_) ->
    Conf = [
        {["auth_oauth2","oauth_providers","keycloak","issuer"],
            "https://keycloak"},
        {["auth_oauth2","oauth_providers","uaa","issuer"],
            "https://uaa"},
        {["auth_oauth2","oauth_providers","uaa","discovery_endpoint_path"],
            "/some-path"}
    ],
    #{<<"keycloak">> := [{issuer, "https://keycloak"}
                        ],
      <<"uaa">> := [{issuer, "https://uaa"},
                    {discovery_endpoint_path, "/some-path"}
                    ]
    } = translate_oauth_providers(Conf).


test_with_many_resource_servers(_) ->
    Conf = [
        {["auth_oauth2","resource_servers","rabbitmq1","id"],
            "rabbitmq1"},
        {["auth_oauth2","resource_servers","rabbitmq2","id"],
            "rabbitmq2"}
    ],
    #{<<"rabbitmq1">> := [{id, <<"rabbitmq1">>}
                        ],
      <<"rabbitmq2">> := [{id, <<"rabbitmq2">>}
                    ]
    } = translate_resource_servers(Conf).

test_oauth_providers_attributes(_) ->
    Conf = [
        {["auth_oauth2","oauth_providers","keycloak","issuer"],
            "https://keycloak"},
        {["auth_oauth2","oauth_providers","keycloak","default_key"],
            "token-key"}
    ],
    #{<<"keycloak">> := [{default_key, <<"token-key">>},
                         {issuer, "https://keycloak"}
                        ]
    } = sort_settings(translate_oauth_providers(Conf)).

test_resource_servers_attributes(_) ->
    Conf = [
        {["auth_oauth2","resource_servers","rabbitmq1","id"],
            "rabbitmq1xxx"},
        {["auth_oauth2","resource_servers","rabbitmq1","scope_prefix"],
            "somescope."},
        {["auth_oauth2","resource_servers","rabbitmq1","additional_scopes_key"],
            "roles"},
        {["auth_oauth2","resource_servers","rabbitmq1","preferred_username_claims","1"],
            "userid"},
        {["auth_oauth2","resource_servers","rabbitmq1","preferred_username_claims","2"],
            "groupid"}
    ],
    #{<<"rabbitmq1xxx">> := [{additional_scopes_key, <<"roles">>},
                          {id, <<"rabbitmq1xxx">>},
                          {preferred_username_claims, [<<"userid">>, <<"groupid">>]},
                          {scope_prefix, <<"somescope.">>}
                        ]
    } = sort_settings(translate_resource_servers(Conf)),

    Conf2 = [
        {["auth_oauth2","resource_servers","rabbitmq1","scope_prefix"],
            "somescope."},
        {["auth_oauth2","resource_servers","rabbitmq1","additional_scopes_key"],
            "roles"},
        {["auth_oauth2","resource_servers","rabbitmq1","preferred_username_claims","1"],
            "userid"},
        {["auth_oauth2","resource_servers","rabbitmq1","preferred_username_claims","2"],
            "groupid"}
    ],
    #{<<"rabbitmq1">> := [{additional_scopes_key, <<"roles">>},
                          {id, <<"rabbitmq1">>},
                          {preferred_username_claims, [<<"userid">>, <<"groupid">>]},
                          {scope_prefix, <<"somescope.">>}
                        ]
    } = sort_settings(translate_resource_servers(Conf2)).

test_oauth_providers_attributes_with_invalid_uri(_) ->
    Conf = [
        {["auth_oauth2","oauth_providers","keycloak","issuer"],
            "http://keycloak"},
        {["auth_oauth2","oauth_providers","keycloak","default_key"],
            "token-key"}
    ],
    try sort_settings(translate_oauth_providers(Conf)) of
        _ -> {throw, should_have_failed}
    catch
        _ -> ok
    end.

test_oauth_providers_algorithms(_) ->
    Conf = [
        {["auth_oauth2","oauth_providers","keycloak","issuer"],
            "https://keycloak"},
        {["auth_oauth2","oauth_providers","keycloak","algorithms","2"],
            "HS256"},
        {["auth_oauth2","oauth_providers","keycloak","algorithms","1"],
            "RS256"}
    ],
    #{<<"keycloak">> := [{algorithms, [<<"RS256">>, <<"HS256">>]},
                         {issuer, "https://keycloak"}
                         ]
    } = sort_settings(translate_oauth_providers(Conf)).

test_oauth_providers_https(Conf) ->

    CuttlefishConf = [
        {["auth_oauth2","oauth_providers","keycloak","issuer"],
            "https://keycloak"},
        {["auth_oauth2","oauth_providers","keycloak","https","verify"],
            verify_none},
        {["auth_oauth2","oauth_providers","keycloak","https","peer_verification"],
            verify_peer},
        {["auth_oauth2","oauth_providers","keycloak","https","depth"],
            2},
        {["auth_oauth2","oauth_providers","keycloak","https","hostname_verification"],
            wildcard},
        {["auth_oauth2","oauth_providers","keycloak","https","crl_check"],
            false},
        {["auth_oauth2","oauth_providers","keycloak","https","fail_if_no_peer_cert"],
            true},
        {["auth_oauth2","oauth_providers","keycloak","https","cacertfile"],
            cert_filename(Conf)}
    ],
    #{<<"keycloak">> := [{https, [{verify, verify_none},
                                  {peer_verification, verify_peer},
                                  {depth, 2},
                                  {hostname_verification, wildcard},
                                  {crl_check, false},
                                  {fail_if_no_peer_cert, true},
                                  {cacertfile, _CaCertFile}
                                  ]},
                         {issuer, "https://keycloak"}
                         ]
    } = sort_settings(translate_oauth_providers(CuttlefishConf)).

test_oauth_providers_https_with_missing_cacertfile(_) ->

    Conf = [
        {["auth_oauth2","oauth_providers","keycloak","issuer"],
            "https://keycloak"},
        {["auth_oauth2","oauth_providers","keycloak","https","cacertfile"],
            "/non-existent.pem"}
    ],
    try sort_settings(translate_oauth_providers(Conf)) of
        _ -> {throw, should_have_failed}
    catch
        _ -> ok
    end.

test_oauth_providers_signing_keys(Conf) ->
    CuttlefishConf = [
        {["auth_oauth2","oauth_providers","keycloak","issuer"],
            "https://keycloak"},
        {["auth_oauth2","oauth_providers","keycloak","signing_keys","2"], 
            cert_filename(Conf)},
        {["auth_oauth2","oauth_providers","keycloak","signing_keys","1"], 
            cert_filename(Conf)}
    ],
    #{<<"keycloak">> := [{issuer, "https://keycloak"},
                         {signing_keys, SigningKeys}
                         ]
    } = sort_settings(translate_oauth_providers(CuttlefishConf)),
    ct:log("SigningKey: ~p", [SigningKeys]),
    #{<<"1">> := {pem, <<"I'm not a certificate">>},
      <<"2">> := {pem, <<"I'm not a certificate">>}
    } = SigningKeys.

test_scope_aliases_configured_as_list_of_properties(_) ->
    CuttlefishConf = [
        {["auth_oauth2","scope_aliases","1","alias"],
            "admin"},
        {["auth_oauth2","scope_aliases","1","scope"], 
            "rabbitmq.tag:administrator"},
        {["auth_oauth2","scope_aliases","2","alias"],
            "developer"},
        {["auth_oauth2","scope_aliases","2","scope"], 
            "rabbitmq.tag:management rabbitmq.read:*/*"}        
    ],
    #{
        <<"admin">> := [<<"rabbitmq.tag:administrator">>],
        <<"developer">> := [<<"rabbitmq.tag:management">>, <<"rabbitmq.read:*/*">>]                         
    } = translate_scope_aliases(CuttlefishConf).
    
test_scope_aliases_configured_as_list_of_missing_properties(_) ->
    CuttlefishConf = [
        {["auth_oauth2","scope_aliases","1","alias"],
            "admin"}
    ],
    #{} = rabbit_oauth2_schema:translate_scope_aliases(CuttlefishConf),

    CuttlefishConf2 = [
        {["auth_oauth2","scope_aliases","1","scope"],
            "rabbitmq.tag:management rabbitmq.read:*/*"}
    ],
    #{} = rabbit_oauth2_schema:translate_scope_aliases(CuttlefishConf2).

        
test_scope_aliases_configured_as_map(_) ->
    CuttlefishConf = [
        {["auth_oauth2","scope_aliases","admin"], 
            "rabbitmq.tag:administrator"},
        {["auth_oauth2","scope_aliases","developer"], 
            "rabbitmq.tag:management rabbitmq.read:*/*"}        
    ],
    #{
        <<"admin">> := [<<"rabbitmq.tag:administrator">>],
        <<"developer">> := [<<"rabbitmq.tag:management">>, <<"rabbitmq.read:*/*">>]                         
    } = rabbit_oauth2_schema:translate_scope_aliases(CuttlefishConf).
    

cert_filename(Conf) ->
    string:concat(?config(data_dir, Conf), "certs/cert.pem").

sort_settings(MapOfListOfSettings) ->
    maps:map(fun(_K,List) ->
        lists:sort(fun({K1,_}, {K2,_}) -> K1 < K2 end, List) end, 
            MapOfListOfSettings).
