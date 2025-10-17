%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2025 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
%%

-module(system_SUITE).

-compile([export_all]).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("rabbitmq_ct_helpers/include/rabbit_mgmt_test.hrl").
-include_lib("rabbitmq_ct_helpers/include/rabbit_ldap_test.hrl").

-import(rabbit_mgmt_test_util, [http_put/4]).

%%--------------------------------------------------------------------

all() ->
    [{group, non_parallel_tests}].

groups() ->
    [{non_parallel_tests, [], tests()}].

suite() ->
    [{timetrap, {minutes, 2}}].

tests() ->
    [validate_ldap_configuration_via_api].

%% -------------------------------------------------------------------
%% Testsuite setup/teardown.
%% -------------------------------------------------------------------

init_per_suite(Config) ->
    rabbit_ct_helpers:log_environment(),
    rabbit_ct_helpers:run_setup_steps(Config, [fun rabbit_ct_ldap_utils:init_slapd/1]).

end_per_suite(Config) ->
    rabbit_ct_helpers:run_teardown_steps(Config, [fun rabbit_ct_ldap_utils:stop_slapd/1]).

init_per_group(Group, Config) ->
    rabbit_ct_ldap_utils:init_per_group(Group, Config).

end_per_group(Group, Config) ->
    rabbit_ct_ldap_utils:end_per_group(Group, Config).

init_per_testcase(validate_ldap_configuration_via_api = Testcase, Config) ->
    _ = application:start(inets),
    rabbit_ct_helpers:testcase_started(Config, Testcase);
init_per_testcase(Testcase, Config) ->
    rabbit_ct_helpers:testcase_started(Config, Testcase).

end_per_testcase(validate_ldap_configuration_via_api = Testcase, Config) ->
    _ = application:stop(inets),
    rabbit_ct_helpers:testcase_finished(Config, Testcase);
end_per_testcase(Testcase, Config) ->
    rabbit_ct_helpers:testcase_finished(Config, Testcase).

%% -------------------------------------------------------------------
%% Testsuite cases
%% -------------------------------------------------------------------

%% NOTE:
%% All strings in the JSON body MUST be binaries!
validate_ldap_configuration_via_api(Config) ->
    CertsDir = ?config(rmq_certsdir, Config),
    CaCertfile = filename:join([CertsDir, "testca", "cacert.pem"]),

    %% {user_dn_pattern, "cn=${username},ou=People,dc=rabbitmq,dc=com"},
    UserDNFmt = "cn=~ts,ou=People,dc=rabbitmq,dc=com",
    AliceUserDN = rabbit_data_coercion:to_utf8_binary(io_lib:format(UserDNFmt, [?ALICE_NAME])),
    InvalidUserDN = rabbit_data_coercion:to_utf8_binary(io_lib:format(UserDNFmt, ["NOBODY"])),
    Password = rabbit_data_coercion:to_utf8_binary("password"),

    % NOTE:
    % If you don't do this, the charlist "localhost" will be JSON-encoded as
    % [108,111,99,97,108,104,111,115,116]
    % which obviously isn't what will happen in the real world
    LocalHost = rabbit_data_coercion:to_utf8_binary("localhost"),
    EmptyString = <<"">>,

    LdapPort = ?config(ldap_port, Config),
    LdapTlsPort = ?config(ldap_tls_port, Config),

    %% NB: bad resource name
    http_put(Config, "/ldap/validate/bad-bind-name",
        #{
            'user_dn' => AliceUserDN,
            'password' => Password,
            'servers' => [LocalHost],
            'port' => LdapPort
        }, ?METHOD_NOT_ALLOWED),
    %% Invalid JSON should return 400 Bad Request
    rabbit_mgmt_test_util:http_put_raw(Config, "/ldap/validate/simple-bind",
        "{invalid json", ?BAD_REQUEST),

    %% HTTP Method coverage tests
    %% GET method - should return 405 Method Not Allowed
    ?assertMatch({ok, {{_, ?METHOD_NOT_ALLOWED, _}, _Headers, _ResBody}},
        rabbit_mgmt_test_util:req(Config, 0, get, "/ldap/validate/simple-bind",
                                  [rabbit_mgmt_test_util:auth_header("guest", "guest")])),

    %% HEAD method - should return 405 Method Not Allowed (same as GET)
    ?assertMatch({ok, {{_, ?METHOD_NOT_ALLOWED, _}, _Headers, _ResBody}},
        rabbit_mgmt_test_util:req(Config, 0, head, "/ldap/validate/simple-bind",
                                  [rabbit_mgmt_test_util:auth_header("guest", "guest")])),

    %% POST method - should return 405 Method Not Allowed
    ?assertMatch({ok, {{_, ?METHOD_NOT_ALLOWED, _}, _Headers, _ResBody}},
        rabbit_mgmt_test_util:req(Config, 0, post, "/ldap/validate/simple-bind",
                                  [rabbit_mgmt_test_util:auth_header("guest", "guest")], "{}")),

    %% DELETE method - should return 405 Method Not Allowed
    ?assertMatch({ok, {{_, ?METHOD_NOT_ALLOWED, _}, _Headers, _ResBody}},
        rabbit_mgmt_test_util:req(Config, 0, delete, "/ldap/validate/simple-bind",
                                  [rabbit_mgmt_test_util:auth_header("guest", "guest")])),

    %% OPTIONS method - should return 200 with Allow header showing only PUT, OPTIONS
    {ok, {{_, OptionsCode, _}, OptionsHeaders, _OptionsResBody}} =
        rabbit_mgmt_test_util:req(Config, 0, options, "/ldap/validate/simple-bind",
                                  [rabbit_mgmt_test_util:auth_header("guest", "guest")]),
    ?assertEqual(?OK, OptionsCode),
    AllowHeader = proplists:get_value("allow", OptionsHeaders),
    ?assert(string:str(string:to_upper(AllowHeader), "PUT") > 0),
    ?assert(string:str(string:to_upper(AllowHeader), "OPTIONS") > 0),
    %% Should NOT contain GET or HEAD
    ?assertEqual(0, string:str(string:to_upper(AllowHeader), "GET")),
    ?assertEqual(0, string:str(string:to_upper(AllowHeader), "HEAD")),

    %% Missing required fields tests
    %% Empty servers array - connection failure (400)
    http_put(Config, "/ldap/validate/simple-bind",
        #{
            'user_dn' => AliceUserDN,
            'password' => Password,
            'servers' => [],
            'port' => LdapPort
        }, ?BAD_REQUEST),

    %% Missing servers field entirely - defaults to [], same as above (400)
    http_put(Config, "/ldap/validate/simple-bind",
        #{
            'user_dn' => AliceUserDN,
            'password' => Password,
            'port' => LdapPort
        }, ?BAD_REQUEST),

    %% Missing user_dn field entirely - empty DN fails credential validation (422)
    http_put(Config, "/ldap/validate/simple-bind",
        #{
            'password' => Password,
            'servers' => [LocalHost],
            'port' => LdapPort
        }, ?UNPROCESSABLE_ENTITY),

    %% Missing password field entirely - empty password fails credential validation (422)
    http_put(Config, "/ldap/validate/simple-bind",
        #{
            'user_dn' => AliceUserDN,
            'servers' => [LocalHost],
            'port' => LdapPort
        }, ?UNPROCESSABLE_ENTITY),

    %% Invalid field values tests
    %% Invalid port - string instead of number
    http_put(Config, "/ldap/validate/simple-bind",
        #{
            'user_dn' => AliceUserDN,
            'password' => Password,
            'servers' => [LocalHost],
            'port' => "not_a_number"
        }, ?BAD_REQUEST),

    %% Invalid port - negative number
    http_put(Config, "/ldap/validate/simple-bind",
        #{
            'user_dn' => AliceUserDN,
            'password' => Password,
            'servers' => [LocalHost],
            'port' => -1
        }, ?BAD_REQUEST),

    %% Invalid boolean - string instead of boolean
    http_put(Config, "/ldap/validate/simple-bind",
        #{
            'user_dn' => AliceUserDN,
            'password' => Password,
            'servers' => [LocalHost],
            'port' => LdapPort,
            'use_ssl' => <<"maybe">>
        }, ?BAD_REQUEST),

    %% Invalid servers - non-list value
    http_put(Config, "/ldap/validate/simple-bind",
        #{
            'user_dn' => AliceUserDN,
            'password' => Password,
            'servers' => <<"not_a_list">>,
            'port' => LdapPort
        }, ?BAD_REQUEST),

    %% Network/Infrastructure scenarios
    %% Non-existent server
    http_put(Config, "/ldap/validate/simple-bind",
        #{
            'user_dn' => AliceUserDN,
            'password' => Password,
            'servers' => [<<"nonexistent.example.com">>],
            'port' => LdapPort
        }, ?BAD_REQUEST),

    %% Invalid hostname format
    http_put(Config, "/ldap/validate/simple-bind",
        #{
            'user_dn' => AliceUserDN,
            'password' => Password,
            'servers' => [<<"not..a..valid..hostname">>],
            'port' => LdapPort
        }, ?BAD_REQUEST),

    %% Edge case credentials tests
    %% Empty password - should be 422 (credential validation failure)
    {ok, {{_, 422, _}, _Headers1, EmptyPasswordBody}} =
        rabbit_mgmt_test_util:req(Config, 0, put, "/ldap/validate/simple-bind",
                                  [rabbit_mgmt_test_util:auth_header("guest", "guest")],
                                  rabbit_mgmt_test_util:format_for_upload(#{
                                      'user_dn' => AliceUserDN,
                                      'password' => EmptyString,
                                      'servers' => [LocalHost],
                                      'port' => LdapPort
                                  })),
    EmptyPasswordJson = rabbit_json:decode(EmptyPasswordBody),
    ?assertEqual(<<"unprocessable_entity">>, maps:get(<<"error">>, EmptyPasswordJson)),
    ?assertEqual(<<"anonymous_auth">>, maps:get(<<"reason">>, EmptyPasswordJson)),

    %% Empty user DN - should be 422 (credential validation failure)
    {ok, {{_, 422, _}, _Headers2, EmptyUserDnBody}} =
        rabbit_mgmt_test_util:req(Config, 0, put, "/ldap/validate/simple-bind",
                                  [rabbit_mgmt_test_util:auth_header("guest", "guest")],
                                  rabbit_mgmt_test_util:format_for_upload(#{
                                      'user_dn' => EmptyString,
                                      'password' => Password,
                                      'servers' => [LocalHost],
                                      'port' => LdapPort
                                  })),
    EmptyUserDnJson = rabbit_json:decode(EmptyUserDnBody),
    ?assertEqual(<<"unprocessable_entity">>, maps:get(<<"error">>, EmptyUserDnJson)),
    ?assertEqual(<<"anonymous_auth">>, maps:get(<<"reason">>, EmptyUserDnJson)),

    %% Very long user DN (test size limits)
    {ok, {{_, 422, _}, _Headers3, LongUserDnBody}} =
        rabbit_mgmt_test_util:req(Config, 0, put, "/ldap/validate/simple-bind",
                                  [rabbit_mgmt_test_util:auth_header("guest", "guest")],
                                  rabbit_mgmt_test_util:format_for_upload(#{
                                      'user_dn' => binary:copy(<<"x">>, 10000),
                                      'password' => Password,
                                      'servers' => [LocalHost],
                                      'port' => LdapPort
                                  })),
    LongUserDnJson = rabbit_json:decode(LongUserDnBody),
    ?assertEqual(<<"unprocessable_entity">>, maps:get(<<"error">>, LongUserDnJson)),
    ?assertEqual(<<"invalid LDAP credentials: DN syntax invalid / too long">>,
                 maps:get(<<"reason">>, LongUserDnJson)),

    %% Very long password (test size limits)
    {ok, {{_, 422, _}, _Headers4, LongPasswordBody}} =
        rabbit_mgmt_test_util:req(Config, 0, put, "/ldap/validate/simple-bind",
                                  [rabbit_mgmt_test_util:auth_header("guest", "guest")],
                                  rabbit_mgmt_test_util:format_for_upload(#{
                                      'user_dn' => AliceUserDN,
                                      'password' => binary:copy(<<"x">>, 10000),
                                      'servers' => [LocalHost],
                                      'port' => LdapPort
                                  })),
    LongPasswordJson = rabbit_json:decode(LongPasswordBody),
    ?assertEqual(<<"unprocessable_entity">>, maps:get(<<"error">>, LongPasswordJson)),
    ?assertEqual(<<"invalid LDAP credentials: authentication failure">>,
                 maps:get(<<"reason">>, LongPasswordJson)),

    %% SSL/TLS Edge Cases
    %% Both use_ssl and use_starttls set to true - TLS configuration error
    {ok, {{_, 422, _}, _Headers5, BothTlsBody}} =
        rabbit_mgmt_test_util:req(Config, 0, put, "/ldap/validate/simple-bind",
                                  [rabbit_mgmt_test_util:auth_header("guest", "guest")],
                                  rabbit_mgmt_test_util:format_for_upload(#{
                                      'user_dn' => AliceUserDN,
                                      'password' => Password,
                                      'servers' => [LocalHost],
                                      'port' => LdapTlsPort,
                                      'use_ssl' => true,
                                      'use_starttls' => true,
                                      'ssl_options' => #{
                                          'cacertfile' => CaCertfile
                                      }
                                  })),
    BothTlsJson = rabbit_json:decode(BothTlsBody),
    ?assertEqual(<<"unprocessable_entity">>, maps:get(<<"error">>, BothTlsJson)),
    ?assertEqual(<<"TLS configuration error: cannot use StartTLS on an SSL connection (use_ssl and use_starttls cannot both be true)">>,
                 maps:get(<<"reason">>, BothTlsJson)),

    %% Invalid certificate file path
    http_put(Config, "/ldap/validate/simple-bind",
        #{
            'user_dn' => AliceUserDN,
            'password' => Password,
            'servers' => [LocalHost],
            'port' => LdapTlsPort,
            'use_ssl' => true,
            'ssl_options' => #{
                'cacertfile' => <<"/nonexistent/path/cert.pem">>
            }
        }, ?BAD_REQUEST),

    %% Invalid PEM data - should now return 400 Bad Request
    http_put(Config, "/ldap/validate/simple-bind",
        #{
            'user_dn' => AliceUserDN,
            'password' => Password,
            'servers' => [LocalHost],
            'port' => LdapTlsPort,
            'use_ssl' => true,
            'ssl_options' => #{
                'cacert_pem_data' => [<<"not-valid-pem-data">>]
            }
        }, ?BAD_REQUEST),

    %% Invalid SSL options structure - not a map
    http_put(Config, "/ldap/validate/simple-bind",
        #{
            'user_dn' => AliceUserDN,
            'password' => Password,
            'servers' => [LocalHost],
            'port' => LdapTlsPort,
            'use_ssl' => true,
            'ssl_options' => <<"not_a_map">>
        }, ?BAD_REQUEST),

    %% Invalid TLS versions
    http_put(Config, "/ldap/validate/simple-bind",
        #{
            'user_dn' => AliceUserDN,
            'password' => Password,
            'servers' => [LocalHost],
            'port' => LdapTlsPort,
            'use_ssl' => true,
            'ssl_options' => #{
                'versions' => [<<"invalid_version">>, <<"tlsv1.2">>],
                'cacertfile' => CaCertfile
            }
        }, ?BAD_REQUEST),

    %% Invalid verify option
    http_put(Config, "/ldap/validate/simple-bind",
        #{
            'user_dn' => AliceUserDN,
            'password' => Password,
            'servers' => [LocalHost],
            'port' => LdapTlsPort,
            'use_ssl' => true,
            'ssl_options' => #{
                'verify' => <<"invalid_verify_option">>,
                'cacertfile' => CaCertfile
            }
        }, ?BAD_REQUEST),

    %% Invalid depth value - string instead of integer
    http_put(Config, "/ldap/validate/simple-bind",
        #{
            'user_dn' => AliceUserDN,
            'password' => Password,
            'servers' => [LocalHost],
            'port' => LdapTlsPort,
            'use_ssl' => true,
            'ssl_options' => #{
                'depth' => <<"not_a_number">>,
                'cacertfile' => CaCertfile
            }
        }, ?BAD_REQUEST),

    %% Invalid server_name_indication - integer instead of string
    http_put(Config, "/ldap/validate/simple-bind",
        #{
            'user_dn' => AliceUserDN,
            'password' => Password,
            'servers' => [LocalHost],
            'port' => LdapTlsPort,
            'use_ssl' => true,
            'ssl_options' => #{
                'server_name_indication' => 123,
                'cacertfile' => CaCertfile
            }
        }, ?BAD_REQUEST),

    %% Invalid server_name_indication - boolean instead of string
    http_put(Config, "/ldap/validate/simple-bind",
        #{
            'user_dn' => AliceUserDN,
            'password' => Password,
            'servers' => [LocalHost],
            'port' => LdapTlsPort,
            'use_ssl' => true,
            'ssl_options' => #{
                'server_name_indication' => true,
                'cacertfile' => CaCertfile
            }
        }, ?BAD_REQUEST),
    http_put(Config, "/ldap/validate/simple-bind",
        #{
            'user_dn' => AliceUserDN,
            'password' => Password,
            'servers' => [LocalHost],
            'port' => LdapPort
        }, ?NO_CONTENT),
    http_put(Config, "/ldap/validate/simple-bind",
        #{
            'user_dn' => InvalidUserDN,
            'password' => Password,
            'servers' => [LocalHost],
            'port' => LdapPort
        }, ?UNPROCESSABLE_ENTITY),
    http_put(Config, "/ldap/validate/simple-bind",
        #{
            'user_dn' => AliceUserDN,
            'password' => Password,
            'servers' => [LocalHost],
            'port' => LdapTlsPort,
            'use_ssl' => true,
            'ssl_options' => #{
                'cacertfile' => CaCertfile
            }
        }, ?NO_CONTENT),
    http_put(Config, "/ldap/validate/simple-bind",
        #{
            'user_dn' => AliceUserDN,
            'password' => Password,
            'servers' => [LocalHost],
            'port' => LdapTlsPort,
            'use_ssl' => true,
            'ssl_options' => #{
                'server_name_indication' => LocalHost,
                'cacertfile' => CaCertfile
            }
        }, ?NO_CONTENT),
    http_put(Config, "/ldap/validate/simple-bind",
        #{
            'user_dn' => AliceUserDN,
            'password' => Password,
            'servers' => [LocalHost],
            'port' => LdapPort,
            'use_ssl' => false,
            'use_starttls' => true,
            'ssl_options' => #{
                'server_name_indication' => LocalHost,
                'cacertfile' => CaCertfile
            }
        }, ?NO_CONTENT),
    {ok, CaCertfileContent} = file:read_file(CaCertfile),
    http_put(Config, "/ldap/validate/simple-bind",
        #{
            'user_dn' => AliceUserDN,
            'password' => Password,
            'servers' => [LocalHost],
            'port' => LdapTlsPort,
            'use_ssl' => true,
            'ssl_options' => #{
                'versions' => [<<"tlsv1.2">>, <<"tlsv1.3">>],
                'depth' => 8,
                'verify' => <<"verify_peer">>,
                'cacert_pem_data' => [CaCertfileContent]
            }
        }, ?NO_CONTENT),
    http_put(Config, "/ldap/validate/simple-bind",
        #{
            'user_dn' => AliceUserDN,
            'password' => Password,
            'servers' => [LocalHost],
            'port' => LdapTlsPort,
            'use_ssl' => true,
            'ssl_options' => #{
                'versions' => [<<"tlsfoobar">>, <<"tlsv1.3">>],
                'depth' => 8,
                'verify' => <<"verify_peer">>,
                'cacert_pem_data' => [CaCertfileContent, CaCertfileContent]
            }
        }, ?BAD_REQUEST),
    http_put(Config, "/ldap/validate/simple-bind",
        #{
            'user_dn' => AliceUserDN,
            'password' => Password,
            'servers' => [LocalHost],
            'port' => LdapTlsPort,
            'use_ssl' => true,
            'ssl_options' => #{
                'verify' => <<"verify_peer">>,
                'cacertfile' => CaCertfile,
                'ssl_hostname_verification' => <<"wildcard">>
            }
        }, ?NO_CONTENT).
