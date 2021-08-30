%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2021 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(system_SUITE).
-compile([export_all]).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("amqp_client/include/amqp_client.hrl").

-define(SERVER_REJECT_CLIENT, {tls_alert, "unknown ca"}).
-define(SERVER_REJECT_CLIENT_NEW, {tls_alert, {unknown_ca, _}}).
-define(SERVER_REJECT_CLIENT_ERLANG24,
        {tls_alert,
         {handshake_failure,
          "TLS client: In state cipher received SERVER ALERT: Fatal - "
          "Handshake Failure\n"}}).
-define(SERVER_REJECT_CONNECTION_ERLANG23,
        {{socket_error,
          {tls_alert,
           {handshake_failure,
            "TLS client: In state connection received SERVER ALERT: Fatal - "
            "Handshake Failure\n"}}},
         {expecting,'connection.start'}}).

all() ->
    [
      {group, http_provider_tests},
      {group, file_provider_tests}
    ].

groups() ->
    CommonTests = [
        validate_chain,
        validate_longer_chain,
        validate_chain_without_whitelisted,
        whitelisted_certificate_accepted_from_AMQP_client_regardless_of_validation_to_root,
        removed_certificate_denied_from_AMQP_client,
        installed_certificate_accepted_from_AMQP_client,
        whitelist_directory_DELTA,
        replaced_whitelisted_certificate_should_be_accepted,
        ensure_configuration_using_binary_strings_is_handled,
        ignore_corrupt_cert,
        ignore_same_cert_with_different_name,
        list],
    [
      {file_provider_tests, [], [
                                 library,
                                 invasive_SSL_option_change,
                                 validation_success_for_AMQP_client,
                                 validation_failure_for_AMQP_client,
                                 disabled_provider_removes_certificates,
                                 enabled_provider_adds_cerificates |
                                 CommonTests
                               ]},
      {http_provider_tests, [], CommonTests}
    ].

suite() ->
    [{timetrap, {seconds, 180}}].

%% -------------------------------------------------------------------
%% Testsuite setup/teardown.
%% -------------------------------------------------------------------

set_up_node(Config) ->
    rabbit_ct_helpers:log_environment(),
    Config1 = rabbit_ct_helpers:set_config(Config, [
        {rmq_nodename_suffix, ?MODULE},
        {rmq_extra_tcp_ports, [tcp_port_amqp_tls_extra]}
      ]),
    rabbit_ct_helpers:run_setup_steps(Config1,
      rabbit_ct_broker_helpers:setup_steps() ++
      rabbit_ct_client_helpers:setup_steps()).

tear_down_node(Config) ->
    rabbit_ct_helpers:run_teardown_steps(Config,
      rabbit_ct_client_helpers:teardown_steps() ++
      rabbit_ct_broker_helpers:teardown_steps()).

init_per_group(file_provider_tests, Config) ->
    case set_up_node(Config) of
        {skip, _} = Error -> Error;
        Config1           ->
            WhitelistDir = filename:join([?config(rmq_certsdir, Config1),
                                          "trust_store", "file_provider_tests"]),
            Config2 = init_whitelist_dir(Config1, WhitelistDir),
            ok = rabbit_ct_broker_helpers:rpc(Config2, 0,
                                              ?MODULE,  change_configuration,
                                              [rabbitmq_trust_store, [{directory, WhitelistDir},
                                                                      {refresh_interval, interval()},
                                                                      {providers, [rabbit_trust_store_file_provider]}]]),
            Config2
    end;

init_per_group(http_provider_tests, Config) ->
    case set_up_node(Config) of
        {skip, _} = Error -> Error;
        Config1           ->
            WhitelistDir = filename:join([?config(rmq_certsdir, Config1),
                                          "trust_store", "http_provider_tests"]),
            Config2 = init_whitelist_dir(Config1, WhitelistDir),
            Config3 = init_provider_server(Config2, WhitelistDir),
            Url = ?config(trust_store_server_url, Config3),

            ok = rabbit_ct_broker_helpers:rpc(Config3, 0,
                                              ?MODULE,  change_configuration,
                                              [rabbitmq_trust_store, [{url, Url},
                                                                      {refresh_interval, interval()},
                                                                      {providers, [rabbit_trust_store_http_provider]}]]),
            Config3
    end.

init_provider_server(Config, WhitelistDir) ->
    %% Assume we don't have more than 100 ports allocated for tests
    PortBase = rabbit_ct_broker_helpers:get_node_config(Config, 0, tcp_ports_base),

    CertServerPort = PortBase + 100,
    Url = "http://127.0.0.1:" ++ integer_to_list(CertServerPort) ++ "/",
    application:load(trust_store_http),
    ok = application:set_env(trust_store_http, directory, WhitelistDir),
    ok = application:set_env(trust_store_http, port, CertServerPort),
    ok = application:unset_env(trust_store_http, ssl_options),
    application:ensure_all_started(trust_store_http),
    rabbit_ct_helpers:set_config(Config, [{trust_store_server_port, CertServerPort},
                                          {trust_store_server_url, Url}]).

end_per_group(file_provider_tests, Config) ->
    Config1 = tear_down_node(Config),
    tear_down_whitelist_dir(Config1),
    Config;
end_per_group(http_provider_tests, Config) ->
    Config1 = tear_down_node(Config),
    application:stop(trust_store_http),
    Config1;
end_per_group(_, Config) ->
    tear_down_node(Config).

init_whitelist_dir(Config, WhitelistDir) ->
    ok = filelib:ensure_dir(WhitelistDir),
    ok = file:make_dir(WhitelistDir),
    rabbit_ct_helpers:set_config(Config, {whitelist_dir, WhitelistDir}).

tear_down_whitelist_dir(Config) ->
    WhitelistDir = ?config(whitelist_dir, Config),
    ok = rabbit_file:recursive_delete([WhitelistDir]).

init_per_testcase(Testcase, Config) ->
    WhitelistDir = ?config(whitelist_dir, Config),
    ok = rabbit_file:recursive_delete([WhitelistDir]),
    ok = file:make_dir(WhitelistDir),
    ok = rabbit_ct_broker_helpers:rpc(Config, 0,
           ?MODULE,  change_configuration,
           [rabbitmq_trust_store, []]),
    rabbit_ct_helpers:testcase_started(Config, Testcase).

end_per_testcase(Testcase, Config) ->
    rabbit_ct_helpers:testcase_finished(Config, Testcase).


%% -------------------------------------------------------------------
%% Testsuite cases
%% -------------------------------------------------------------------

library(_) ->
     %% Given: Makefile.
     {_Root, _Certificate, _Key} = ct_helper:make_certs(),
     ok.

invasive_SSL_option_change(Config) ->
    ok = rabbit_ct_broker_helpers:rpc(Config, 0,
          ?MODULE, invasive_SSL_option_change1, []).

invasive_SSL_option_change1() ->
    %% Given: Rabbit is started with the boot-steps in the
    %% Trust-Store's OTP Application file.

    %% When: we get Rabbit's SSL options.
    {ok, Options} = application:get_env(rabbit, ssl_options),

    %% Then: all necessary settings are correct.
    verify_peer             = proplists:get_value(verify, Options),
    true                    = proplists:get_value(fail_if_no_peer_cert, Options),
    {Verifyfun, _UserState} = proplists:get_value(verify_fun, Options),

    {module, rabbit_trust_store} = erlang:fun_info(Verifyfun, module),
    ok.

validation_success_for_AMQP_client(Config) ->
    ok = rabbit_ct_broker_helpers:rpc(Config, 0,
           ?MODULE, validation_success_for_AMQP_client1, [Config]).

validation_success_for_AMQP_client1(Config) ->
    %% This test intentionally doesn't whitelist any certificates.
    %% Both the client and the server use certificate/key pairs signed by
    %% the same root CA. This exercises a verify_fun clause that no ther tests hit.
    %% Note that when this test is executed together with the HTTP provider group
    %% it runs into unexpected interference and fails, even if TLS app PEM cache is force
    %% cleared. That's why originally each group was made to use a separate node.
    AuthorityInfo = {Root, _AuthorityKey} = erl_make_certs:make_cert([]),
    {Certificate, Key} = chain(AuthorityInfo),
    {Certificate2, Key2} = chain(AuthorityInfo),
    Port = port(Config),
    Host = rabbit_ct_helpers:get_config(Config, rmq_hostname),
    %% When: Rabbit accepts just this one authority's certificate
    %% (i.e. these are options that'd be in the configuration
    %% file).
    catch rabbit_networking:stop_tcp_listener(Port),
    ok = rabbit_networking:start_ssl_listener(Port, [{cacerts, [Root]},
                                                     {cert, Certificate2},
                                                     {key, Key2} | cfg()], 1, 1),

    %% Then: a client presenting a certifcate rooted at the same
    %% authority connects successfully.
    {ok, Con} = amqp_connection:start(#amqp_params_network{host = Host,
                                                           port = Port,
                                                           ssl_options = [{verify, verify_none},
                                                                          {cert, Certificate},
                                                                          {key, Key}]}),

    %% Clean: client & server TLS/TCP.
    ok = amqp_connection:close(Con),
    ok = rabbit_networking:stop_tcp_listener(Port).


validation_failure_for_AMQP_client(Config) ->
    ok = rabbit_ct_broker_helpers:rpc(Config, 0,
           ?MODULE, validation_failure_for_AMQP_client1, [Config]).

validation_failure_for_AMQP_client1(Config) ->
    %% Given: a root certificate and a certificate rooted with another
    %% authority.
    {Root, Cert, Key}      = ct_helper:make_certs(),
    {_,  CertOther, KeyOther}    = ct_helper:make_certs(),

    Port = port(Config),
    Host = rabbit_ct_helpers:get_config(Config, rmq_hostname),

    %% When: Rabbit accepts certificates rooted with just one
    %% particular authority.
    catch rabbit_networking:stop_tcp_listener(Port),
    ok = rabbit_networking:start_ssl_listener(Port, [{cacerts, [Root]},
                                                     {cert, Cert},
                                                     {key, Key} | cfg()], 1, 1),

    %% Then: a client presenting a certificate rooted with another
    %% authority is REJECTED.
    {error, Error} = amqp_connection:start(
              #amqp_params_network{host = Host,
                                   port = Port,
                                   ssl_options = [{verify, verify_none},
                                                  {cert, CertOther},
                                                  {key, KeyOther}]}),
    case Error of
        %% Expected error from amqp_client.
        ?SERVER_REJECT_CLIENT -> ok;
        ?SERVER_REJECT_CLIENT_NEW -> ok;
        ?SERVER_REJECT_CLIENT_ERLANG24 -> ok;
        ?SERVER_REJECT_CONNECTION_ERLANG23 -> ok;

        %% With Erlang 18.3, there is a regression which causes the SSL
        %% connection to crash with the following exception:
        %% ** {badarg,[{ets,update_counter,[1507362,#Ref<0.0.3.9>,-1],[]},
        %%             {ssl_pkix_db,ref_count,3,...
        %%
        %% When this exception reaches the connection process before the
        %% expected TLS error, amqp_connection:start() returns {error,
        %% closed} instead.
        closed -> expected_erlang_18_ssl_regression;

        %% ssl:setopts/2 hangs indefinitely on occasion
        {timeout, {gen_server,call,[_,post_init|_]}} -> ssl_setopts_hangs_occassionally
    end,

    %% Clean: server TLS/TCP.
    ok = rabbit_networking:stop_tcp_listener(Port).

validate_chain(Config) ->
    ok = rabbit_ct_broker_helpers:rpc(Config, 0,
           ?MODULE, validate_chain1, [Config]).

validate_chain1(Config) ->
    %% Given: a whitelisted certificate `CertTrusted` AND a CA `RootTrusted`
    {Root, Cert, Key} = ct_helper:make_certs(),
    {RootTrusted, CertTrusted, KeyTrusted} = ct_helper:make_certs(),

    Port = port(Config),
    Host = rabbit_ct_helpers:get_config(Config, rmq_hostname),

    ok = whitelist(Config, "alice", CertTrusted,  KeyTrusted),
    rabbit_trust_store:refresh(),

    catch rabbit_networking:stop_tcp_listener(Port),
    ok = rabbit_networking:start_ssl_listener(Port, [{cacerts, [Root]},
                                                     {cert, Cert},
                                                     {key, Key} | cfg()], 1, 1),

    %% When: a client connects and present `RootTrusted` as well as the `CertTrusted`
    %% Then: the connection is successful.
    {ok, Con} = amqp_connection:start(#amqp_params_network{host = Host,
                                                           port = Port,
                                                           ssl_options = [{cacerts, [RootTrusted]},
                                                                          {cert, CertTrusted},
                                                                          {key, KeyTrusted},
                                                                          {verify, verify_none},
                                                                          {server_name_indication, disable}]}),
    %% Clean: client & server TLS/TCP
    ok = amqp_connection:close(Con),
    ok = rabbit_networking:stop_tcp_listener(Port).

validate_longer_chain(Config) ->
    ok = rabbit_ct_broker_helpers:rpc(Config, 0,
           ?MODULE, validate_longer_chain1, [Config]).

validate_longer_chain1(Config) ->

    {Root, Cert, Key} = ct_helper:make_certs(),

    %% Given: a whitelisted certificate `CertTrusted`
    %% AND a certificate `CertUntrusted` that is not whitelisted with the same root as `CertTrusted`
    %% AND `CertInter` intermediate CA
    %% AND `RootTrusted` CA
    AuthorityInfo = {RootCA, _AuthorityKey} = erl_make_certs:make_cert([]),
    Inter = {CertInter, {KindInter, KeyDataInter, _}} = erl_make_certs:make_cert([{issuer, AuthorityInfo}]),
    KeyInter = {KindInter, KeyDataInter},
    {CertUntrusted, {KindUntrusted, KeyDataUntrusted, _}} = erl_make_certs:make_cert([{issuer, Inter}]),
    KeyUntrusted = {KindUntrusted, KeyDataUntrusted},
    {CertTrusted, {Kind, KeyData, _}} = erl_make_certs:make_cert([{issuer, Inter}]),
    KeyTrusted = {Kind, KeyData},

    Port = port(Config),
    Host = rabbit_ct_helpers:get_config(Config, rmq_hostname),

    ok = whitelist(Config, "alice", CertTrusted,  KeyTrusted),
    rabbit_trust_store:refresh(),

    catch rabbit_networking:stop_tcp_listener(Port),
    ok = rabbit_networking:start_ssl_listener(Port, [{cacerts, [Root]},
                                                     {cert, Cert},
                                                     {key, Key} | cfg()], 1, 1),

    %% When: a client connects and present `CertInter` as well as the `CertTrusted`
    %% Then: the connection is successful.
    {ok, Con} = amqp_connection:start(#amqp_params_network{host = Host,
                                                           port = Port,
                                                           ssl_options = [{cacerts, [CertInter]},
                                                                          {cert, CertTrusted},
                                                                          {key, KeyTrusted},
                                                                          {verify, verify_none}]}),

    %% When: a client connects and present `RootTrusted` and `CertInter` as well as the `CertTrusted`
    %% Then: the connection is successful.
    {ok, Con2} = amqp_connection:start(#amqp_params_network{host = Host,
                                                            port = Port,
                                                            ssl_options = [{cacerts, [RootCA, CertInter]},
                                                                           {cert, CertTrusted},
                                                                           {key, KeyTrusted},
                                                                           {verify, verify_none}]}),

    %% When: a client connects and present `CertInter` and `RootCA` as well as the `CertTrusted`
    %% Then: the connection is successful.
    {ok, Con3} = amqp_connection:start(#amqp_params_network{host = Host,
                                                            port = Port,
                                                            ssl_options = [{cacerts, [CertInter, RootCA]},
                                                                           {cert, CertTrusted},
                                                                           {key, KeyTrusted},
                                                                           {verify, verify_none}]}),

    % %% When: a client connects and present `CertInter` and `RootCA` but NOT `CertTrusted`
    % %% Then: the connection is not succcessful
    {error, Error1} = amqp_connection:start(
               #amqp_params_network{host = Host,
                                    port = Port,
                                    ssl_options = [{cacerts, [RootCA]},
                                                   {cert, CertInter},
                                                   {key, KeyInter},
                                                   {verify, verify_none}]}),
    case Error1 of
        %% Expected error from amqp_client.
        ?SERVER_REJECT_CLIENT -> ok;
        ?SERVER_REJECT_CLIENT_NEW -> ok;
        ?SERVER_REJECT_CLIENT_ERLANG24 -> ok;
        ?SERVER_REJECT_CONNECTION_ERLANG23 -> ok;

        %% See previous comment in validation_failure_for_AMQP_client1/1.
        closed -> expected_erlang_18_ssl_regression;

        %% ssl:setopts/2 hangs indefinitely on occasion
        {timeout, {gen_server,call,[_,post_init|_]}} -> ssl_setopts_hangs_occassionally
    end,

    %% When: a client connects and present `CertUntrusted` and `RootCA` and `CertInter`
    %% Then: the connection is not succcessful
    %% TODO: for some reason this returns `bad certifice` rather than `unknown ca`
    {error, Error2} = amqp_connection:start(
               #amqp_params_network{host = Host,
                                    port = Port,
                                    ssl_options = [{cacerts, [RootCA, CertInter]},
                                                   {cert, CertUntrusted},
                                                   {key, KeyUntrusted},
                                                   {verify, verify_none}]}),
    case Error2 of
        %% Expected error from amqp_client.
        {tls_alert, "bad certificate"} -> ok;
        {tls_alert, {bad_certificate, _}} -> ok;
        ?SERVER_REJECT_CLIENT_ERLANG24 -> ok;
        ?SERVER_REJECT_CONNECTION_ERLANG23 -> ok;

        %% See previous comment in validation_failure_for_AMQP_client1/1.
        closed -> expected_erlang_18_ssl_regression;

        %% ssl:setopts/2 hangs indefinitely on occasion
        {timeout, {gen_server,call,[_,post_init|_]}} -> ssl_setopts_hangs_occassionally
    end,

    %% Clean: client & server TLS/TCP
    ok = amqp_connection:close(Con),
    ok = amqp_connection:close(Con2),
    ok = amqp_connection:close(Con3),
    ok = rabbit_networking:stop_tcp_listener(Port).

validate_chain_without_whitelisted(Config) ->
    ok = rabbit_ct_broker_helpers:rpc(Config, 0,
           ?MODULE, validate_chain_without_whitelisted1, [Config]).

validate_chain_without_whitelisted1(Config) ->
    %% Given: a certificate `CertUntrusted` that is NOT whitelisted.
    {Root, Cert, Key} = ct_helper:make_certs(),
    {RootUntrusted,  CertUntrusted, KeyUntrusted} = ct_helper:make_certs(),

    Port = port(Config),
    Host = rabbit_ct_helpers:get_config(Config, rmq_hostname),
    rabbit_trust_store:refresh(),

    catch rabbit_networking:stop_tcp_listener(Port),
    ok = rabbit_networking:start_ssl_listener(Port, [{cacerts, [Root]},
                                                     {cert, Cert},
                                                     {key, Key} | cfg()], 1, 1),

    %% When: Rabbit validates paths
    %% Then: a client presenting the non-whitelisted certificate `CertUntrusted` and `RootUntrusted`
    %% is rejected
    {error, Error} = amqp_connection:start(
              #amqp_params_network{host = Host,
                                   port = Port,
                                   ssl_options = [{cacerts, [RootUntrusted]},
                                                  {cert, CertUntrusted},
                                                  {key, KeyUntrusted},
                                                  {verify, verify_none}]}),
    case Error of
        %% Expected error from amqp_client.
        ?SERVER_REJECT_CLIENT -> ok;
        ?SERVER_REJECT_CLIENT_NEW -> ok;
        ?SERVER_REJECT_CLIENT_ERLANG24 -> ok;
        ?SERVER_REJECT_CONNECTION_ERLANG23 -> ok;

        %% See previous comment in validation_failure_for_AMQP_client1/1.
        closed -> expected_erlang_18_ssl_regression;

        %% ssl:setopts/2 hangs indefinitely on occasion
        {timeout, {gen_server,call,[_,post_init|_]}} -> ssl_setopts_hangs_occassionally
    end,

    ok = rabbit_networking:stop_tcp_listener(Port).

whitelisted_certificate_accepted_from_AMQP_client_regardless_of_validation_to_root(Config) ->
    ok = rabbit_ct_broker_helpers:rpc(Config, 0,
           ?MODULE, whitelisted_certificate_accepted_from_AMQP_client_regardless_of_validation_to_root1, [Config]).

whitelisted_certificate_accepted_from_AMQP_client_regardless_of_validation_to_root1(Config) ->
    %% Given: a certificate `CertTrusted` AND that it is whitelisted.
    {Root, Cert, Key} = ct_helper:make_certs(),
    {_,  CertTrusted, KeyTrusted} = ct_helper:make_certs(),

    Port = port(Config),
    Host = rabbit_ct_helpers:get_config(Config, rmq_hostname),

    ok = whitelist(Config, "alice", CertTrusted,  KeyTrusted),
    rabbit_trust_store:refresh(),

    %% When: Rabbit validates paths with a different root `R` than
    %% that of the certificate `CertTrusted`.
    catch rabbit_networking:stop_tcp_listener(Port),
    ok = rabbit_networking:start_ssl_listener(Port, [{cacerts, [Root]},
                                                     {cert, Cert},
                                                     {key, Key} | cfg()], 1, 1),

    %% Then: a client presenting the whitelisted certificate `C`
    %% is allowed.
    {ok, Con} = amqp_connection:start(#amqp_params_network{host = Host,
                                                           port = Port,
                                                           ssl_options = [{cert, CertTrusted},
                                                                          {key, KeyTrusted},
                                                                          {verify, verify_none}]}),
    %% Clean: client & server TLS/TCP
    ok = amqp_connection:close(Con),
    ok = rabbit_networking:stop_tcp_listener(Port).


removed_certificate_denied_from_AMQP_client(Config) ->
    ok = rabbit_ct_broker_helpers:rpc(Config, 0,
           ?MODULE, removed_certificate_denied_from_AMQP_client1, [Config]).

removed_certificate_denied_from_AMQP_client1(Config) ->
    %% Given: a certificate `CertOther` AND that it is whitelisted.
    {Root, Cert, Key} = ct_helper:make_certs(),
    {_,  CertOther, KeyOther} = ct_helper:make_certs(),

    Port = port(Config),
    Host = rabbit_ct_helpers:get_config(Config, rmq_hostname),
    ok = whitelist(Config, "bob", CertOther,  KeyOther),
    rabbit_trust_store:refresh(),

    %% When: we wait for at least one second (the accuracy of the
    %% file system's time), remove the whitelisted certificate,
    %% then wait for the trust-store to refresh the whitelist.
    catch rabbit_networking:stop_tcp_listener(Port),
    ok = rabbit_networking:start_ssl_listener(Port, [{cacerts, [Root]},
                                                      {cert, Cert},
                                                      {key, Key} | cfg()], 1, 1),

    wait_for_file_system_time(),
    ok = delete("bob.pem", Config),
    wait_for_trust_store_refresh(),

    %% Then: a client presenting the removed whitelisted
    %% certificate `CertOther` is denied.
    {error, Error} = amqp_connection:start(
              #amqp_params_network{host = Host,
                                   port = Port,
                                   ssl_options = [{cert, CertOther},
                                                  {key, KeyOther},
                                                  {verify, verify_none}]}),
    case Error of
        %% Expected error from amqp_client.
        ?SERVER_REJECT_CLIENT -> ok;
        ?SERVER_REJECT_CLIENT_NEW -> ok;
        ?SERVER_REJECT_CLIENT_ERLANG24 -> ok;
        ?SERVER_REJECT_CONNECTION_ERLANG23 -> ok;

        %% See previous comment in validation_failure_for_AMQP_client1/1.
        closed -> expected_erlang_18_ssl_regression;

        %% ssl:setopts/2 hangs indefinitely on occasion
        {timeout, {gen_server,call,[_,post_init|_]}} -> ssl_setopts_hangs_occassionally
    end,

    %% Clean: server TLS/TCP
    ok = rabbit_networking:stop_tcp_listener(Port).


installed_certificate_accepted_from_AMQP_client(Config) ->
    ok = rabbit_ct_broker_helpers:rpc(Config, 0,
           ?MODULE, installed_certificate_accepted_from_AMQP_client1, [Config]).

installed_certificate_accepted_from_AMQP_client1(Config) ->
    %% Given: a certificate `CertOther` which is NOT yet whitelisted.
    {Root, Cert, Key} = ct_helper:make_certs(),
    {_,  CertOther, KeyOther} = ct_helper:make_certs(),

    Port = port(Config),
    Host = rabbit_ct_helpers:get_config(Config, rmq_hostname),
    rabbit_trust_store:refresh(),

    %% When: we wait for at least one second (the accuracy of the
    %% file system's time), add a certificate to the directory,
    %% then wait for the trust-store to refresh the whitelist.
    catch rabbit_networking:stop_tcp_listener(Port),
    ok = rabbit_networking:start_ssl_listener(Port, [{cacerts, [Root]},
                                                      {cert, Cert},
                                                      {key, Key} | cfg()], 1, 1),

    wait_for_file_system_time(),
    ok = whitelist(Config, "charlie", CertOther,  KeyOther),
    wait_for_trust_store_refresh(),

    %% Then: a client presenting the whitelisted certificate `CertOther`
    %% is allowed.
    {ok, Con} = amqp_connection:start(#amqp_params_network{host = Host,
                                                           port = Port,
                                                           ssl_options = [{cert, CertOther},
                                                                          {key, KeyOther},
                                                                          {verify, verify_none}]}),

    %% Clean: Client & server TLS/TCP
    ok = amqp_connection:close(Con),
    ok = rabbit_networking:stop_tcp_listener(Port).


whitelist_directory_DELTA(Config) ->
    ok = rabbit_ct_broker_helpers:rpc(Config, 0,
           ?MODULE, whitelist_directory_DELTA1, [Config]).

whitelist_directory_DELTA1(Config) ->
    %% Given: a certificate `Root` which Rabbit can use as a
    %% root certificate to validate agianst AND three
    %% certificates which clients can present (the first two
    %% of which are whitelisted).
    Port = port(Config),
    Host = rabbit_ct_helpers:get_config(Config, rmq_hostname),
    {Root, Cert, Key} = ct_helper:make_certs(),

    {_,  CertListed1, KeyListed1} = ct_helper:make_certs(),
    {_,  CertRevoked, KeyRevoked} = ct_helper:make_certs(),
    {_,  CertListed2, KeyListed2} = ct_helper:make_certs(),

    ok = whitelist(Config, "foo", CertListed1,  KeyListed1),
    ok = whitelist(Config, "bar", CertRevoked,  KeyRevoked),
    rabbit_trust_store:refresh(),

    %% When: we wait for at least one second (the accuracy
    %% of the file system's time), delete a certificate and
    %% a certificate to the directory, then wait for the
    %% trust-store to refresh the whitelist.
    catch rabbit_networking:stop_tcp_listener(Port),
    ok = rabbit_networking:start_ssl_listener(Port, [{cacerts, [Root]},
                                                      {cert, Cert},
                                                      {key, Key} | cfg()], 1, 1),

    wait_for_file_system_time(),
    ok = delete("bar.pem", Config),
    ok = whitelist(Config, "baz", CertListed2,  KeyListed2),
    wait_for_trust_store_refresh(),

    %% Then: connectivity to Rabbit is as it should be.
    {ok, Conn1} = amqp_connection:start(#amqp_params_network{host = Host,
                                                             port = Port,
                                                             ssl_options = [{cert, CertListed1},
                                                                            {key, KeyListed1},
                                                                            {verify, verify_none}]}),
    {error, Error} = amqp_connection:start(
              #amqp_params_network{host = Host,
                                   port = Port,
                                   ssl_options = [{cert, CertRevoked},
                                                  {key, KeyRevoked},
                                                  {verify, verify_none}]}),
    case Error of
        %% Expected error from amqp_client.
        ?SERVER_REJECT_CLIENT -> ok;
        ?SERVER_REJECT_CLIENT_NEW -> ok;
        ?SERVER_REJECT_CLIENT_ERLANG24 -> ok;
        ?SERVER_REJECT_CONNECTION_ERLANG23 -> ok;

        %% See previous comment in validation_failure_for_AMQP_client1/1.
        closed -> expected_erlang_18_ssl_regression;

        %% ssl:setopts/2 hangs indefinitely on occasion
        {timeout, {gen_server,call,[_,post_init|_]}} -> ssl_setopts_hangs_occassionally
    end,

    {ok, Conn2} = amqp_connection:start(#amqp_params_network{host = Host,
                                                             port = Port,
                                                             ssl_options = [{cert, CertListed2},
                                                                            {key, KeyListed2},
                                                                            {verify, verify_none}]}),
    %% Clean: delete certificate file, close client & server
    %% TLS/TCP
    ok = amqp_connection:close(Conn1),
    ok = amqp_connection:close(Conn2),

    ok = rabbit_networking:stop_tcp_listener(Port).

replaced_whitelisted_certificate_should_be_accepted(Config) ->
    ok = rabbit_ct_broker_helpers:rpc(Config, 0,
           ?MODULE, replaced_whitelisted_certificate_should_be_accepted1, [Config]).

replaced_whitelisted_certificate_should_be_accepted1(Config) ->
    %% Given: a root certificate and a 2 other certificates
    {Root, Cert, Key}      = ct_helper:make_certs(),
    {_,  CertFirst, KeyFirst}    = ct_helper:make_certs(),
    {_,  CertUpdated, KeyUpdated}    = ct_helper:make_certs(),

    Port = port(Config),
    Host = rabbit_ct_helpers:get_config(Config, rmq_hostname),

    catch rabbit_networking:stop_tcp_listener(Port),
    ok = rabbit_networking:start_ssl_listener(Port, [{cacerts, [Root]},
                                                    {cert, Cert},
                                                    {key, Key} | cfg()], 1, 1),
    %% And: the first certificate has been whitelisted
    ok = whitelist(Config, "bart", CertFirst,  KeyFirst),
    rabbit_trust_store:refresh(),

    wait_for_trust_store_refresh(),

    %% verify that the first cert can be used to connect
    {ok, Con} =
     amqp_connection:start(#amqp_params_network{host = Host,
                                                port = Port,
                                                ssl_options = [{cert, CertFirst},
                                                               {key, KeyFirst},
                                                               {verify, verify_none}]}),
    %% verify the other certificate is not accepted
    {error, Error1} = amqp_connection:start(
               #amqp_params_network{host = Host,
                                    port = Port,
                                    ssl_options = [{cert, CertUpdated},
                                                   {key, KeyUpdated},
                                                   {verify, verify_none}]}),
    case Error1 of
        %% Expected error from amqp_client.
        ?SERVER_REJECT_CLIENT -> ok;
        ?SERVER_REJECT_CLIENT_NEW -> ok;
        ?SERVER_REJECT_CLIENT_ERLANG24 -> ok;
        ?SERVER_REJECT_CONNECTION_ERLANG23 -> ok;

        %% See previous comment in validation_failure_for_AMQP_client1/1.
        closed -> expected_erlang_18_ssl_regression;

        %% ssl:setopts/2 hangs indefinitely on occasion
        {timeout, {gen_server,call,[_,post_init|_]}} -> ssl_setopts_hangs_occassionally
    end,
    ok = amqp_connection:close(Con),

    %% When: a whitelisted certicate is replaced with one with the same name
    ok = whitelist(Config, "bart", CertUpdated,  KeyUpdated),

    wait_for_trust_store_refresh(),

    %% Then: the first certificate should be rejected
    {error, Error2} = amqp_connection:start(
               #amqp_params_network{host = Host,
                                    port = Port,
                                    ssl_options = [{cert, CertFirst},
                                                   %% disable ssl session caching
                                                   %% as this ensures the cert
                                                   %% will be re-verified by the
                                                   %% server
                                                   {reuse_sessions, false},
                                                   {key, KeyFirst},
                                                   {verify, verify_none}]}),
    case Error2 of
        %% Expected error from amqp_client.
        ?SERVER_REJECT_CLIENT -> ok;
        ?SERVER_REJECT_CLIENT_NEW -> ok;
        ?SERVER_REJECT_CLIENT_ERLANG24 -> ok;
        ?SERVER_REJECT_CONNECTION_ERLANG23 -> ok;

        %% See previous comment in validation_failure_for_AMQP_client1/1.
        closed -> expected_erlang_18_ssl_regression
    end,

    %% And: the updated certificate should allow the user to connect
    {ok, Con2} =
     amqp_connection:start(#amqp_params_network{host = Host,
                                                port = Port,
                                                ssl_options = [{cert, CertUpdated},
                                                               {reuse_sessions, false},
                                                               {key, KeyUpdated},
                                                               {verify, verify_none}]}),
    ok = amqp_connection:close(Con2),
    %% Clean: server TLS/TCP.
    ok = rabbit_networking:stop_tcp_listener(Port).


ensure_configuration_using_binary_strings_is_handled(Config) ->
    ok = rabbit_ct_broker_helpers:rpc(Config, 0,
           ?MODULE, ensure_configuration_using_binary_strings_is_handled1, [Config]).

ensure_configuration_using_binary_strings_is_handled1(Config) ->
    ok = change_configuration(rabbitmq_trust_store, [{directory, list_to_binary(whitelist_dir(Config))},
                                                    {refresh_interval,
                                                        {seconds, interval()}}]).

ignore_corrupt_cert(Config) ->
    ok = rabbit_ct_broker_helpers:rpc(Config, 0,
           ?MODULE, ignore_corrupt_cert1, [Config]).

ignore_corrupt_cert1(Config) ->
    %% Given: a certificate `CertTrusted` AND that it is whitelisted.
    %% Given: a corrupt certificate.

    Port = port(Config),
    Host = rabbit_ct_helpers:get_config(Config, rmq_hostname),
    {Root, Cert, Key} = ct_helper:make_certs(),
    {_,  CertTrusted, KeyTrusted} = ct_helper:make_certs(),

    rabbit_trust_store:refresh(),
    ok = whitelist(Config, "alice", CertTrusted,  KeyTrusted),

    %% When: Rabbit tries to whitelist the corrupt certificate.
    ok = whitelist(Config, "corrupt", <<48>>,  KeyTrusted),
    rabbit_trust_store:refresh(),

    catch rabbit_networking:stop_tcp_listener(Port),
    ok = rabbit_networking:start_ssl_listener(Port, [{cacerts, [Root]},
                                                      {cert, Cert},
                                                      {key, Key} | cfg()], 1, 1),

    %% Then: the trust store should keep functioning
    %% And: a client presenting the whitelisted certificate `CertTrusted`
    %% is allowed.
    {ok, Con} = amqp_connection:start(#amqp_params_network{host = Host,
                                                           port = Port,
                                                           ssl_options = [{cert, CertTrusted},
                                                                          {key, KeyTrusted},
                                                                          {verify, verify_none}]}),
    %% Clean: client & server TLS/TCP
    ok = amqp_connection:close(Con),
    ok = rabbit_networking:stop_tcp_listener(Port).

ignore_same_cert_with_different_name(Config) ->
    ok = rabbit_ct_broker_helpers:rpc(Config, 0,
           ?MODULE, ignore_same_cert_with_different_name1, [Config]).

ignore_same_cert_with_different_name1(Config) ->
    %% Given: a certificate `CertTrusted` AND that it is whitelisted.
    %% Given: the same certificate saved with a different filename.

    Host = rabbit_ct_helpers:get_config(Config, rmq_hostname),
    Port = port(Config),
    {Root, Cert, Key} = ct_helper:make_certs(),
    {_,  CertTrusted, KeyTrusted} = ct_helper:make_certs(),

    rabbit_trust_store:refresh(),
    ok = whitelist(Config, "alice", CertTrusted,  KeyTrusted),
    %% When: Rabbit tries to insert the duplicate certificate
    ok = whitelist(Config, "malice", CertTrusted,  KeyTrusted),
    rabbit_trust_store:refresh(),

    catch rabbit_networking:stop_tcp_listener(Port),
    ok = rabbit_networking:start_ssl_listener(Port, [{cacerts, [Root]},
                                                      {cert, Cert},
                                                      {key, Key} | cfg()], 1, 1),

    %% Then: the trust store should keep functioning.
    %% And: a client presenting the whitelisted certificate `CertTrusted`
    %% is allowed.
    {ok, Con} = amqp_connection:start(#amqp_params_network{host = Host,
                                                           port = Port,
                                                           ssl_options = [{cert, CertTrusted},
                                                                          {key, KeyTrusted},
                                                                          {verify, verify_none}]}),
    %% Clean: client & server TLS/TCP
    ok = amqp_connection:close(Con),
    ok = rabbit_networking:stop_tcp_listener(Port).

list(Config) ->
    %% FIXME: The file provider calls stat(2) on the certificate
    %% directory to detect any new certificates. Unfortunately, the
    %% modification time has a resolution of one second. Thus, it can
    %% miss certificates added within the same second after a refresh.
    %% To workaround this, we force a refresh, wait for two seconds,
    %% write the test certificate and call refresh again.
    %%
    %% Once this is fixed, the two lines below can be removed.
    %%
    %% See rabbitmq/rabbitmq-trust-store#58.
    ok = rabbit_ct_broker_helpers:rpc(Config, 0, rabbit_trust_store, refresh, []),
    timer:sleep(2000),

    {_Root,  Cert, Key}    = ct_helper:make_certs(),
    ok = whitelist(Config, "alice", Cert,  Key),
    % wait_for_trust_store_refresh(),
    ok = rabbit_ct_broker_helpers:rpc(Config, 0, rabbit_trust_store, refresh, []),
    Certs = rabbit_ct_broker_helpers:rpc(Config, 0,
           rabbit_trust_store, list, []),
    % only really tests it isn't totally broken.
    {match, _} = re:run(Certs, ".*alice\.pem.*").

disabled_provider_removes_certificates(Config) ->
    {_Root,  Cert, Key}    = ct_helper:make_certs(),
    ok = whitelist(Config, "alice", Cert,  Key),
    ok = rabbit_ct_broker_helpers:rpc(Config, 0, rabbit_trust_store, refresh, []),

    %% Certificate is there
    Certs = rabbit_ct_broker_helpers:rpc(Config, 0,
           rabbit_trust_store, list, []),
    {match, _} = re:run(Certs, ".*alice\.pem.*"),


    rabbit_ct_broker_helpers:rpc(Config, 0, application, set_env,
                                 [rabbitmq_trust_store, providers, []]),
    wait_for_trust_store_refresh(),

    %% Certificate is not there anymore
    CertsAfterDelete = rabbit_ct_broker_helpers:rpc(Config, 0,
           rabbit_trust_store, list, []),
    nomatch = re:run(CertsAfterDelete, ".*alice\.pem.*").

enabled_provider_adds_cerificates(Config) ->
    {_Root,  Cert, Key}    = ct_helper:make_certs(),
    ok = whitelist(Config, "alice", Cert,  Key),
    ok = rabbit_ct_broker_helpers:rpc(Config, 0,
           ?MODULE,  change_configuration,
           [rabbitmq_trust_store, [{directory, whitelist_dir(Config)},
                                   {providers, []}]]),

    %% Certificate is not there yet
    Certs = rabbit_ct_broker_helpers:rpc(Config, 0,
           rabbit_trust_store, list, []),
    nomatch = re:run(Certs, ".*alice\.pem.*"),


    rabbit_ct_broker_helpers:rpc(Config, 0, application, set_env,
                                 [rabbitmq_trust_store, providers, [rabbit_trust_store_file_provider]]),
    wait_for_trust_store_refresh(),

    %% Certificate is there
    CertsAfterAdd = rabbit_ct_broker_helpers:rpc(Config, 0,
           rabbit_trust_store, list, []),
    {match, _} = re:run(CertsAfterAdd, ".*alice\.pem.*").


%% Test Constants

port(Config) ->
    rabbit_ct_broker_helpers:get_node_config(Config, 0, tcp_port_amqp_tls_extra).

whitelist_dir(Config) ->
    ?config(whitelist_dir, Config).

interval() ->
    1.

wait_for_file_system_time() ->
    timer:sleep(timer:seconds(1)).

wait_for_trust_store_refresh() ->
    timer:sleep(5 * timer:seconds(interval())).

cfg() ->
    {ok, Cfg} = application:get_env(rabbit, ssl_options),
    Cfg.

%% Ancillary

chain(Issuer) ->
    %% Theses are DER encoded.
    {Certificate, {Kind, Key, _}} = erl_make_certs:make_cert([{issuer, Issuer}]),
    {Certificate, {Kind, Key}}.

change_configuration(App, Props) ->
    ok = application:stop(App),
    ok = change_cfg(App, Props),
    application:start(App).

change_cfg(_, []) ->
    ok;
change_cfg(App, [{Name,Value}|Rest]) ->
    ok = application:set_env(App, Name, Value),
    change_cfg(App, Rest).

whitelist(Config, Filename, Certificate, {A, B} = _Key) ->
    Path = whitelist_dir(Config),
    ok = erl_make_certs:write_pem(Path, Filename, {Certificate, {A, B, not_encrypted}}),
    [file:delete(filename:join(Path, K)) || K <- filelib:wildcard("*_key.pem", Path)],
    ok.

delete(Name, Config) ->
    F = filename:join([whitelist_dir(Config), Name]),
    file:delete(F).
