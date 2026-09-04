%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2026 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
%%

%% Covers the trust store's effect on a listener that configures its own
%% TLS options rather than reading `rabbit`'s `ssl_options` (see
%% `rabbit_networking:fix_ssl_options/1`). `rabbitmq_web_mqtt` stands in
%% for every listener in that position.
-module(web_mqtt_SUITE).

-compile([export_all, nowarn_export_all]).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").

%% The exact alert the client receives during peer verification depends
%% on negotiation details outside this test's control (the TLS version used,
%% renegotiation options), so both types are accepted.
-define(IS_SERVER_REJECTION(Alert),
        (element(1, Alert) =:= unknown_ca orelse element(1, Alert) =:= handshake_failure)).

all() ->
    [
      {group, tests}
    ].

groups() ->
    [
      {tests, [], [
          whitelisted_certificate_accepted_over_web_mqtt_tls,
          non_whitelisted_certificate_rejected_over_web_mqtt_tls
        ]}
    ].

suite() ->
    [{timetrap, {seconds, 60}}].

init_per_suite(Config) ->
    {ok, _} = application:ensure_all_started(ssl),
    rabbit_ct_helpers:log_environment(),
    Config1 = rabbit_ct_helpers:set_config(Config, [{rmq_nodename_suffix, ?MODULE}]),
    Config2 = rabbit_ct_helpers:run_setup_steps(Config1),
    {rmq_certsdir, CertsDir} = proplists:lookup(rmq_certsdir, Config2),
    WhitelistDir = filename:join([CertsDir, "trust_store", "web_mqtt_SUITE"]),
    ok = filelib:ensure_dir(WhitelistDir),
    ok = file:make_dir(WhitelistDir),
    Config3 = rabbit_ct_helpers:merge_app_env(
                Config2,
                {rabbitmq_trust_store,
                 [{directory, WhitelistDir},
                  {providers, [rabbit_trust_store_file_provider]}]}),
    Config4 = rabbit_ct_helpers:merge_app_env(
                Config3,
                {rabbitmq_web_mqtt,
                 [{ssl_config,
                   [{cacertfile, filename:join([CertsDir, "testca", "cacert.pem"])},
                    {certfile, filename:join([CertsDir, "server", "cert.pem"])},
                    {keyfile, filename:join([CertsDir, "server", "key.pem"])},
                    {verify, verify_peer},
                    {fail_if_no_peer_cert, true},
                    {versions, ['tlsv1.2']},
                    %% Hard coded to match the port `rabbit_ct_broker_helpers`
                    %% pre-assigns to `tcp_port_web_mqtt_tls`, since the
                    %% broker isn't up yet to compute it for us.
                    {port, 21010}
                   ]}]}),
    case rabbit_ct_helpers:run_setup_steps(Config4, rabbit_ct_broker_helpers:setup_steps()) of
        {skip, _} = Error -> Error;
        Config5 -> rabbit_ct_helpers:set_config(Config5, {whitelist_dir, WhitelistDir})
    end.

end_per_suite(Config) ->
    rabbit_ct_helpers:run_teardown_steps(Config, rabbit_ct_broker_helpers:teardown_steps()).

init_per_testcase(Testcase, Config) ->
    rabbit_ct_helpers:testcase_started(Config, Testcase).

end_per_testcase(Testcase, Config) ->
    rabbit_ct_helpers:testcase_finished(Config, Testcase).

%% -------------------------------------------------------------------
%% Testsuite cases
%% -------------------------------------------------------------------

whitelisted_certificate_accepted_over_web_mqtt_tls(Config) ->
    {_RootCerts, Cert, Key} = ct_helper:make_certs(),
    whitelist(Config, "whitelisted", Cert),
    ok = rabbit_ct_broker_helpers:rpc(Config, 0, rabbit_trust_store, refresh, []),

    {ok, Socket} = ssl:connect(host(Config), port(Config), client_tls_opts(Cert, Key), 5000),
    ok = ssl:close(Socket).

non_whitelisted_certificate_rejected_over_web_mqtt_tls(Config) ->
    {_RootCerts, Cert, Key} = ct_helper:make_certs(),

    {error, {tls_alert, Alert}} =
        ssl:connect(host(Config), port(Config), client_tls_opts(Cert, Key), 5000),
    ?assert(?IS_SERVER_REJECTION(Alert)).

%% -------------------------------------------------------------------
%% Internal helpers
%% -------------------------------------------------------------------

host(Config) ->
    rabbit_ct_helpers:get_config(Config, rmq_hostname).

port(Config) ->
    rabbit_ct_broker_helpers:get_node_config(Config, 0, tcp_port_web_mqtt_tls).

client_tls_opts(Cert, Key) ->
    [{cert, Cert}, {key, Key}, {verify, verify_none}, {versions, ['tlsv1.2']}].

whitelist(Config, Name, Certificate) ->
    Path = ?config(whitelist_dir, Config),
    ok = file:write_file(filename:join(Path, Name ++ ".pem"),
                         public_key:pem_encode([{'Certificate', Certificate, not_encrypted}])).
