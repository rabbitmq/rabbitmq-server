%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2024 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
%%
-module(oauth2_schema_SUITE).

-compile(export_all).

-include_lib("rabbit_common/include/rabbit.hrl").
-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").


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
        test_without_resource_servers,
        test_with_one_resource_server,
        test_with_many_resource_servers,
        test_resource_servers_attributes

    ].


test_without_oauth_providers(_) ->
    #{} = oauth2_schema:translate_oauth_providers([]).

test_without_resource_servers(_) ->
    #{} = oauth2_schema:translate_resource_servers([]).

test_with_one_oauth_provider(_) ->
    Conf = [{["auth_oauth2","oauth_providers","keycloak","issuer"],"https://rabbit"}
            ],
    #{<<"keycloak">> := [{issuer, <<"https://rabbit">>}]
    } = oauth2_schema:translate_oauth_providers(Conf).

test_with_one_resource_server(_) ->
    Conf = [{["auth_oauth2","resource_servers","rabbitmq1","id"],"rabbitmq1"}
            ],
    #{<<"rabbitmq1">> := [{id, <<"rabbitmq1">>}]
    } = oauth2_schema:translate_resource_servers(Conf).

test_with_many_oauth_providers(_) ->
    Conf = [{["auth_oauth2","oauth_providers","keycloak","issuer"],"https://keycloak"},
            {["auth_oauth2","oauth_providers","uaa","issuer"],"https://uaa"}
            ],
    #{<<"keycloak">> := [{issuer, <<"https://keycloak">>}
                        ],
      <<"uaa">> := [{issuer, <<"https://uaa">>}
                    ]
    } = oauth2_schema:translate_oauth_providers(Conf).


test_with_many_resource_servers(_) ->
    Conf = [{["auth_oauth2","resource_servers","rabbitmq1","id"],"rabbitmq1"},
            {["auth_oauth2","resource_servers","rabbitmq2","id"],"rabbitmq2"}
            ],
    #{<<"rabbitmq1">> := [{id, <<"rabbitmq1">>}
                        ],
      <<"rabbitmq2">> := [{id, <<"rabbitmq2">>}
                    ]
    } = oauth2_schema:translate_resource_servers(Conf).

test_oauth_providers_attributes(_) ->
    Conf = [{["auth_oauth2","oauth_providers","keycloak","issuer"],"https://keycloak"},
            {["auth_oauth2","oauth_providers","keycloak","default_key"],"token-key"}
            ],
    #{<<"keycloak">> := [{default_key, <<"token-key">>},
                         {issuer, <<"https://keycloak">>}
                        ]
    } = sort_settings(oauth2_schema:translate_oauth_providers(Conf)).

test_resource_servers_attributes(_) ->
    Conf = [{["auth_oauth2","resource_servers","rabbitmq1","id"],"rabbitmq1xxx"},
            {["auth_oauth2","resource_servers","rabbitmq1","scope_prefix"],"somescope."},
            {["auth_oauth2","resource_servers","rabbitmq1","additional_scopes_key"],"roles"},
            {["auth_oauth2","resource_servers","rabbitmq1","preferred_username_claims","1"],"userid"},
            {["auth_oauth2","resource_servers","rabbitmq1","preferred_username_claims","2"],"groupid"}
            ],
    #{<<"rabbitmq1xxx">> := [{additional_scopes_key, <<"roles">>},
                          {id, <<"rabbitmq1xxx">>},
                          {preferred_username_claims, [<<"userid">>, <<"groupid">>]},
                          {scope_prefix, <<"somescope.">>}
                        ]
    } = sort_settings(oauth2_schema:translate_resource_servers(Conf)),

    Conf2 = [
            {["auth_oauth2","resource_servers","rabbitmq1","scope_prefix"],"somescope."},
            {["auth_oauth2","resource_servers","rabbitmq1","additional_scopes_key"],"roles"},
            {["auth_oauth2","resource_servers","rabbitmq1","preferred_username_claims","1"],"userid"},
            {["auth_oauth2","resource_servers","rabbitmq1","preferred_username_claims","2"],"groupid"}
            ],
    #{<<"rabbitmq1">> := [{additional_scopes_key, <<"roles">>},
                          {id, <<"rabbitmq1">>},
                          {preferred_username_claims, [<<"userid">>, <<"groupid">>]},
                          {scope_prefix, <<"somescope.">>}
                        ]
    } = sort_settings(oauth2_schema:translate_resource_servers(Conf2)).

test_oauth_providers_attributes_with_invalid_uri(_) ->
    Conf = [{["auth_oauth2","oauth_providers","keycloak","issuer"],"http://keycloak"},
            {["auth_oauth2","oauth_providers","keycloak","default_key"],"token-key"}
            ],
    try sort_settings(oauth2_schema:translate_oauth_providers(Conf)) of
        _ -> {throw, should_have_failed}
    catch
        _ -> ok
    end.

test_oauth_providers_algorithms(_) ->
    Conf = [{["auth_oauth2","oauth_providers","keycloak","issuer"],"https://keycloak"},
            {["auth_oauth2","oauth_providers","keycloak","algorithms","2"],"HS256"},
            {["auth_oauth2","oauth_providers","keycloak","algorithms","1"],"RS256"}
            ],
    #{<<"keycloak">> := [{algorithms, [<<"RS256">>, <<"HS256">>]},
                         {issuer, <<"https://keycloak">>}
                         ]
    } = sort_settings(oauth2_schema:translate_oauth_providers(Conf)).

test_oauth_providers_https(Conf) ->

    CuttlefishConf = [{["auth_oauth2","oauth_providers","keycloak","issuer"],"https://keycloak"},
            {["auth_oauth2","oauth_providers","keycloak","https","verify"],verify_none},
            {["auth_oauth2","oauth_providers","keycloak","https","peer_verification"],verify_peer},
            {["auth_oauth2","oauth_providers","keycloak","https","depth"],2},
            {["auth_oauth2","oauth_providers","keycloak","https","hostname_verification"],wildcard},
            {["auth_oauth2","oauth_providers","keycloak","https","crl_check"],false},
            {["auth_oauth2","oauth_providers","keycloak","https","fail_if_no_peer_cert"],true},
            {["auth_oauth2","oauth_providers","keycloak","https","cacertfile"],cert_filename(Conf)}
            ],
    #{<<"keycloak">> := [{https, [{verify, verify_none},
                                  {peer_verification, verify_peer},
                                  {depth, 2},
                                  {hostname_verification, wildcard},
                                  {crl_check, false},
                                  {fail_if_no_peer_cert, true},
                                  {cacertfile, _CaCertFile}
                                  ]},
                         {issuer, <<"https://keycloak">>}
                         ]
    } = sort_settings(oauth2_schema:translate_oauth_providers(CuttlefishConf)).

test_oauth_providers_https_with_missing_cacertfile(_) ->

    Conf = [{["auth_oauth2","oauth_providers","keycloak","issuer"],"https://keycloak"},
            {["auth_oauth2","oauth_providers","keycloak","https","cacertfile"],"/non-existent.pem"}
            ],
    try sort_settings(oauth2_schema:translate_oauth_providers(Conf)) of
        _ -> {throw, should_have_failed}
    catch
        _ -> ok
    end.

test_oauth_providers_signing_keys(Conf) ->
    CuttlefishConf = [{["auth_oauth2","oauth_providers","keycloak","issuer"],"https://keycloak"},
            {["auth_oauth2","oauth_providers","keycloak","signing_keys","2"], cert_filename(Conf)},
            {["auth_oauth2","oauth_providers","keycloak","signing_keys","1"], cert_filename(Conf)}
            ],
    #{<<"keycloak">> := [{issuer, <<"https://keycloak">>},
                         {signing_keys, SigningKeys}
                         ]
    } = sort_settings(oauth2_schema:translate_oauth_providers(CuttlefishConf)),
    ct:log("SigningKey: ~p", [SigningKeys]),
    #{<<"1">> := {pem, <<"I'm not a certificate">>},
      <<"2">> := {pem, <<"I'm not a certificate">>}
      } = SigningKeys.

cert_filename(Conf) ->
    string:concat(?config(data_dir, Conf), "certs/cert.pem").

sort_settings(MapOfListOfSettings) ->
    maps:map(fun(_K,List) ->
        lists:sort(fun({K1,_}, {K2,_}) -> K1 < K2 end, List) end, MapOfListOfSettings).
