-module(system_SUITE).
-compile([export_all]).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("amqp_client/include/amqp_client.hrl").

-define(SERVER_REJECT_CLIENT, {tls_alert, "unknown ca"}).
all() ->
    [
      {group, non_parallel_tests}
    ].

groups() ->
    [
      {non_parallel_tests, [], [
                                 library,
                                 invasive_SSL_option_change,
                                 validation_success_for_AMQP_client,
                                 validation_failure_for_AMQP_client,
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
                                 list
                               ]}
    ].

suite() ->
    [{timetrap, {seconds, 60}}].

%% -------------------------------------------------------------------
%% Testsuite setup/teardown.
%% -------------------------------------------------------------------

init_per_suite(Config) ->
    rabbit_ct_helpers:log_environment(),
    Config1 = rabbit_ct_helpers:set_config(Config, [
        {rmq_nodename_suffix, ?MODULE},
        {rmq_extra_tcp_ports, [tcp_port_amqp_tls_extra]}
      ]),
    rabbit_ct_helpers:run_setup_steps(Config1,
      rabbit_ct_broker_helpers:setup_steps() ++
      rabbit_ct_client_helpers:setup_steps()).

end_per_suite(Config) ->
    rabbit_ct_helpers:run_teardown_steps(Config,
      rabbit_ct_client_helpers:teardown_steps() ++
      rabbit_ct_broker_helpers:teardown_steps()).

init_per_group(_, Config) ->
    Config.

end_per_group(_, Config) ->
    Config.

init_per_testcase(Testcase, Config) ->
    TestCaseDir = rabbit_ct_helpers:config_to_testcase_name(Config, Testcase),
    WhitelistDir = filename:join([?config(rmq_certsdir, Config), "trust_store", TestCaseDir]),
    ok = filelib:ensure_dir(WhitelistDir),
    ok = file:make_dir(WhitelistDir),
    Config1 = rabbit_ct_helpers:set_config(Config, {whitelist_dir, WhitelistDir}),
    rabbit_ct_helpers:testcase_started(Config1, Testcase).

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
    {name,   whitelisted}        = erlang:fun_info(Verifyfun, name),
    ok.

validation_success_for_AMQP_client(Config) ->
    ok = rabbit_ct_broker_helpers:rpc(Config, 0,
           ?MODULE, validation_success_for_AMQP_client1, [Config]).

validation_success_for_AMQP_client1(Config) ->
    AuthorityInfo = {Root, _AuthorityKey} = erl_make_certs:make_cert([{key, dsa}]),
    {Certificate, Key} = chain(AuthorityInfo),
    {Certificate2, Key2} = chain(AuthorityInfo),
    Port = port(Config),
    Host = rabbit_ct_helpers:get_config(Config, rmq_hostname),
    %% When: Rabbit accepts just this one authority's certificate
    %% (i.e. these are options that'd be in the configuration
    %% file).
    ok = rabbit_networking:start_ssl_listener(Port, [{cacerts, [Root]},
                                                     {cert, Certificate2},
                                                     {key, Key2} | cfg()], 1),

    %% Then: a client presenting a certifcate rooted at the same
    %% authority connects successfully.
    {ok, Con} = amqp_connection:start(#amqp_params_network{host = Host,
                                                           port = Port,
                                                           ssl_options = [{cert, Certificate},
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
    ok = rabbit_networking:start_ssl_listener(Port, [{cacerts, [Root]},
                                                     {cert, Cert},
                                                     {key, Key} | cfg()], 1),

    %% Then: a client presenting a certificate rooted with another
    %% authority is REJECTED.
    {error, ?SERVER_REJECT_CLIENT} =
     amqp_connection:start(#amqp_params_network{host = Host,
                                                port = Port,
                                                ssl_options = [{cert, CertOther},
                                                               {key, KeyOther}]}),

    %% Clean: server TLS/TCP.
    ok = rabbit_networking:stop_tcp_listener(Port).

validate_chain(Config) ->
    ok = rabbit_ct_broker_helpers:rpc(Config, 0,
           ?MODULE, validate_chain1, [Config]).

validate_chain1(Config) ->
    %% Given: a whitelisted certificate `CertTrusted` AND a CA `RootTrusted`
    {Root, Cert, Key} = ct_helper:make_certs(),
    {RootTrusted,  CertTrusted, KeyTrusted} = ct_helper:make_certs(),

    Port = port(Config),
    Host = rabbit_ct_helpers:get_config(Config, rmq_hostname),

    ok = whitelist(Config, "alice", CertTrusted,  KeyTrusted),
    ok = change_configuration(rabbitmq_trust_store, [{directory, whitelist_dir(Config)}]),

    ok = rabbit_networking:start_ssl_listener(Port, [{cacerts, [Root]},
                                                     {cert, Cert},
                                                     {key, Key} | cfg()], 1),

    %% When: a client connects and present `RootTrusted` as well as the `CertTrusted`
    %% Then: the connection is successful.
    {ok, Con} = amqp_connection:start(#amqp_params_network{host = Host,
                                                           port = Port,
                                                           ssl_options = [{cacerts, [RootTrusted]},
                                                                          {cert, CertTrusted},
                                                                          {key, KeyTrusted}]}),

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
    AuthorityInfo = {RootCA, _AuthorityKey} = erl_make_certs:make_cert([{key, dsa}]),
    Inter = {CertInter, {KindInter, KeyDataInter, _}} = erl_make_certs:make_cert([{key, dsa}, {issuer, AuthorityInfo}]),
    KeyInter = {KindInter, KeyDataInter},
    {CertUntrusted, {KindUntrusted, KeyDataUntrusted, _}} = erl_make_certs:make_cert([{key, dsa}, {issuer, Inter}]),
    KeyUntrusted = {KindUntrusted, KeyDataUntrusted},
    {CertTrusted, {Kind, KeyData, _}} = erl_make_certs:make_cert([{key, dsa}, {issuer, Inter}]),
    KeyTrusted = {Kind, KeyData},

    Port = port(Config),
    Host = rabbit_ct_helpers:get_config(Config, rmq_hostname),

    ok = whitelist(Config, "alice", CertTrusted,  KeyTrusted),
    ok = change_configuration(rabbitmq_trust_store, [{directory, whitelist_dir(Config)}]),

    ok = rabbit_networking:start_ssl_listener(Port, [{cacerts, [Root]},
                                                     {cert, Cert},
                                                     {key, Key} | cfg()], 1),

    %% When: a client connects and present `CertInter` as well as the `CertTrusted`
    %% Then: the connection is successful.
    {ok, Con} = amqp_connection:start(#amqp_params_network{host = Host,
                                                           port = Port,
                                                           ssl_options = [{cacerts, [CertInter]},
                                                                          {cert, CertTrusted},
                                                                          {key, KeyTrusted}]}),

    %% When: a client connects and present `RootTrusted` and `CertInter` as well as the `CertTrusted`
    %% Then: the connection is successful.
    {ok, Con2} = amqp_connection:start(#amqp_params_network{host = Host,
                                                            port = Port,
                                                            ssl_options = [{cacerts, [RootCA, CertInter]},
                                                                           {cert, CertTrusted},
                                                                           {key, KeyTrusted}]}),

    %% When: a client connects and present `CertInter` and `RootCA` as well as the `CertTrusted`
    %% Then: the connection is successful.
    {ok, Con3} = amqp_connection:start(#amqp_params_network{host = Host,
                                                            port = Port,
                                                            ssl_options = [{cacerts, [CertInter, RootCA]},
                                                                           {cert, CertTrusted},
                                                                           {key, KeyTrusted}]}),

    % %% When: a client connects and present `CertInter` and `RootCA` but NOT `CertTrusted`
    % %% Then: the connection is not succcessful
    {error, ?SERVER_REJECT_CLIENT} =
        amqp_connection:start(#amqp_params_network{host = Host,
                                                   port = Port,
                                                   ssl_options = [{cacerts, [RootCA]},
                                                                  {cert, CertInter},
                                                                  {key, KeyInter}]}),

    %% When: a client connects and present `CertUntrusted` and `RootCA` and `CertInter`
    %% Then: the connection is not succcessful
    %% TODO: for some reason this returns `bad certifice` rather than `unknown ca`
    {error, {tls_alert, "bad certificate"}} =
        amqp_connection:start(#amqp_params_network{host = Host,
                                                   port = Port,
                                                   ssl_options = [{cacerts, [RootCA, CertInter]},
                                                                  {cert, CertUntrusted},
                                                                  {key, KeyUntrusted}]}),
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

    ok = change_configuration(rabbitmq_trust_store, [{directory, whitelist_dir(Config)}]),

    ok = rabbit_networking:start_ssl_listener(Port, [{cacerts, [Root]},
                                                     {cert, Cert},
                                                     {key, Key} | cfg()], 1),

    %% When: Rabbit validates paths
    %% Then: a client presenting the non-whitelisted certificate `CertUntrusted` and `RootUntrusted`
    %% is rejected 
    {error, ?SERVER_REJECT_CLIENT} =
        amqp_connection:start(#amqp_params_network{host = Host,
                                                   port = Port,
                                                   ssl_options = [{cacerts, [RootUntrusted]},
                                                                  {cert, CertUntrusted},
                                                                  {key, KeyUntrusted}]}),

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
    ok = change_configuration(rabbitmq_trust_store, [{directory, whitelist_dir(Config)}]),

    %% When: Rabbit validates paths with a different root `R` than
    %% that of the certificate `CertTrusted`.
    ok = rabbit_networking:start_ssl_listener(Port, [{cacerts, [Root]},
                                                      {cert, Cert},
                                                      {key, Key} | cfg()], 1),

    %% Then: a client presenting the whitelisted certificate `C`
    %% is allowed.
    {ok, Con} = amqp_connection:start(#amqp_params_network{host = Host,
                                                          port = Port,
                                                          ssl_options = [{cert, CertTrusted},
                                                                         {key, KeyTrusted}]}),

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
    ok = change_configuration(rabbitmq_trust_store, [{directory, whitelist_dir(Config)},
                                                     {refresh_interval,
                                                        {seconds, interval()}}]),

    %% When: we wait for at least one second (the accuracy of the
    %% file system's time), remove the whitelisted certificate,
    %% then wait for the trust-store to refresh the whitelist.
    ok = rabbit_networking:start_ssl_listener(Port, [{cacerts, [Root]},
                                                      {cert, Cert},
                                                      {key, Key} | cfg()], 1),

    wait_for_file_system_time(),
    ok = delete("bob.pem", Config),
    wait_for_trust_store_refresh(),

    %% Then: a client presenting the removed whitelisted
    %% certificate `CertOther` is denied.
    {error, ?SERVER_REJECT_CLIENT} =
       amqp_connection:start(#amqp_params_network{host = Host,
                                                  port = Port,
                                                  ssl_options = [{cert, CertOther},
                                                                 {key, KeyOther}]}),

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

    ok = change_configuration(rabbitmq_trust_store, [{directory, whitelist_dir(Config)},
                                                    {refresh_interval,
                                                        {seconds, interval()}}]),

    %% When: we wait for at least one second (the accuracy of the
    %% file system's time), add a certificate to the directory,
    %% then wait for the trust-store to refresh the whitelist.
    ok = rabbit_networking:start_ssl_listener(Port, [{cacerts, [Root]},
                                                      {cert, Cert},
                                                      {key, Key} | cfg()], 1),

    wait_for_file_system_time(),
    ok = whitelist(Config, "charlie", CertOther,  KeyOther),
    wait_for_trust_store_refresh(),

    %% Then: a client presenting the whitelisted certificate `CertOther`
    %% is allowed.
    {ok, Con} = amqp_connection:start(#amqp_params_network{host = Host,
                                                          port = Port,
                                                          ssl_options = [{cert, CertOther},
                                                                         {key, KeyOther}]}),

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
    ok = change_configuration(rabbitmq_trust_store, [{directory, whitelist_dir(Config)},
                                                    {refresh_interval,
                                                     {seconds, interval()}}]),

    %% When: we wait for at least one second (the accuracy
    %% of the file system's time), delete a certificate and
    %% a certificate to the directory, then wait for the
    %% trust-store to refresh the whitelist.
    ok = rabbit_networking:start_ssl_listener(Port, [{cacerts, [Root]},
                                                      {cert, Cert},
                                                      {key, Key} | cfg()], 1),

    wait_for_file_system_time(),
    ok = delete("bar.pem", Config),
    ok = whitelist(Config, "baz", CertListed2,  KeyListed2),
    wait_for_trust_store_refresh(),

    %% Then: connectivity to Rabbit is as it should be.
    {ok, Conn1} = amqp_connection:start(#amqp_params_network{host = Host,
                                                            port = Port,
                                                            ssl_options = [{cert, CertListed1},
                                                                           {key, KeyListed1}]}),
    {error, ?SERVER_REJECT_CLIENT} =
        amqp_connection:start(#amqp_params_network{host = Host,
                                                   port = Port,
                                                   ssl_options = [{cert, CertRevoked},
                                                                  {key, KeyRevoked}]}),

    {ok, Conn2} = amqp_connection:start(#amqp_params_network{host = Host,
                                                            port = Port,
                                                            ssl_options = [{cert, CertListed2},
                                                                           {key, KeyListed2}]}),

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

    ok = rabbit_networking:start_ssl_listener(Port, [{cacerts, [Root]},
                                                    {cert, Cert},
                                                    {key, Key} | cfg()], 1),
    %% And: the first certificate has been whitelisted
    ok = whitelist(Config, "bart", CertFirst,  KeyFirst),
    ok = change_configuration(rabbitmq_trust_store, [{directory, whitelist_dir(Config)},
                                                  {refresh_interval, {seconds, interval()}}]),

    wait_for_trust_store_refresh(),

    %% verify that the first cert can be used to connect
    {ok, Con} =
     amqp_connection:start(#amqp_params_network{host = Host,
                                                port = Port,
                                                ssl_options = [{cert, CertFirst},
                                                               {key, KeyFirst} ]}),
    %% verify the other certificate is not accepted
    {error, ?SERVER_REJECT_CLIENT} =
     amqp_connection:start(#amqp_params_network{host = Host,
                                                port = Port,
                                                ssl_options = [{cert, CertUpdated},
                                                               {key, KeyUpdated} ]}),
    ok = amqp_connection:close(Con),

    %% When: a whitelisted certicate is replaced with one with the same name
    ok = whitelist(Config, "bart", CertUpdated,  KeyUpdated),

    wait_for_trust_store_refresh(),

    %% Then: the first certificate should be rejected
    {error, ?SERVER_REJECT_CLIENT} =
     amqp_connection:start(#amqp_params_network{host = Host,
                                                port = Port,
                                                ssl_options = [{cert, CertFirst},
                                                               %% disable ssl session caching
                                                               %% as this ensures the cert
                                                               %% will be re-verified by the
                                                               %% server
                                                               {reuse_sessions, false},
                                                               {key, KeyFirst} ]}),

    %% And: the updated certificate should allow the user to connect
    {ok, Con2} =
     amqp_connection:start(#amqp_params_network{host = Host,
                                                port = Port,
                                                ssl_options = [{cert, CertUpdated},
                                                               {reuse_sessions, false},
                                                               {key, KeyUpdated} ]}),
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

    WhitelistDir = whitelist_dir(Config),
    ok = change_configuration(rabbitmq_trust_store, [{directory, WhitelistDir}]),
    ok = whitelist(Config, "alice", CertTrusted,  KeyTrusted),

    %% When: Rabbit tries to whitelist the corrupt certificate.
    ok = whitelist(Config, "corrupt", <<48>>,  KeyTrusted),
    ok = change_configuration(rabbitmq_trust_store, [{directory, WhitelistDir}]),

    ok = rabbit_networking:start_ssl_listener(Port, [{cacerts, [Root]},
                                                      {cert, Cert},
                                                      {key, Key} | cfg()], 1),

    %% Then: the trust store should keep functioning
    %% And: a client presenting the whitelisted certificate `CertTrusted`
    %% is allowed.
    {ok, Con} = amqp_connection:start(#amqp_params_network{host = Host,
                                                           port = Port,
                                                           ssl_options = [{cert, CertTrusted},
                                                                          {key, KeyTrusted}]}),

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

    WhitelistDir = whitelist_dir(Config),

    ok = change_configuration(rabbitmq_trust_store, [{directory, WhitelistDir}]),
    ok = whitelist(Config, "alice", CertTrusted,  KeyTrusted),
    %% When: Rabbit tries to insert the duplicate certificate
    ok = whitelist(Config, "malice", CertTrusted,  KeyTrusted),
    ok = change_configuration(rabbitmq_trust_store, [{directory, WhitelistDir}]),

    ok = rabbit_networking:start_ssl_listener(Port, [{cacerts, [Root]},
                                                      {cert, Cert},
                                                      {key, Key} | cfg()], 1),

    %% Then: the trust store should keep functioning.
    %% And: a client presenting the whitelisted certificate `CertTrusted`
    %% is allowed.
    {ok, Con} = amqp_connection:start(#amqp_params_network{host = Host,
                                                           port = Port,
                                                           ssl_options = [{cert, CertTrusted},
                                                                          {key, KeyTrusted}]}),

    %% Clean: client & server TLS/TCP
    ok = amqp_connection:close(Con),
    ok = rabbit_networking:stop_tcp_listener(Port).

list(Config) ->
    {_Root,  Cert, Key}    = ct_helper:make_certs(),
    ok = whitelist(Config, "alice", Cert,  Key),
    ok = rabbit_ct_broker_helpers:rpc(Config, 0,
           ?MODULE,  change_configuration, [rabbitmq_trust_store, [{directory, whitelist_dir(Config)}]]),
    Certs = rabbit_ct_broker_helpers:rpc(Config, 0,
           rabbit_trust_store, list, []),
    % only really tests it isn't totally broken.
    {match, _} = re:run(Certs, ".*alice\.pem.*").

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
    timer:sleep(2 * timer:seconds(interval())).

cfg() ->
    {ok, Cfg} = application:get_env(rabbit, ssl_options),
    Cfg.

%% Ancillary

chain(Issuer) ->
    %% Theses are DER encoded.
    {Certificate, {Kind, Key, _}} = erl_make_certs:make_cert([{key, dsa}, {issuer, Issuer}]),
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
