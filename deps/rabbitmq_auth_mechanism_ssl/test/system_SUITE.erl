%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2023 VMware, Inc. or its affiliates.  All rights reserved.

-module(system_SUITE).

-compile([export_all,
          nowarn_export_all]).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").

all() ->
    [{group, tests}].

groups() ->
    [
     {tests, [shuffle],
      [amqp]
     }
    ].

init_per_suite(Config) ->
    {ok, _} = application:ensure_all_started(amqp10_client),
    rabbit_ct_helpers:log_environment(),
    Config.

end_per_suite(Config) ->
    Config.

init_per_group(_Group, Config0) ->
    %% Command `deps/rabbitmq_ct_helpers/tools/tls-certs$ make`
    %% will put our hostname as common name in the client cert.
    Config1 = rabbit_ct_helpers:merge_app_env(
                Config0,
                {rabbit,
                 [
                  {auth_mechanisms, ['EXTERNAL']},
                  {ssl_cert_login_from, common_name}
                 ]}),
    Config = rabbit_ct_helpers:run_setup_steps(
               Config1,
               rabbit_ct_broker_helpers:setup_steps() ++
               rabbit_ct_client_helpers:setup_steps()),
    {ok, UserString} = inet:gethostname(),
    User = unicode:characters_to_binary(UserString),
    ok = rabbit_ct_broker_helpers:add_user(Config, User),
    Vhost = <<"test vhost">>,
    ok = rabbit_ct_broker_helpers:add_vhost(Config, Vhost),
    [{test_vhost, Vhost},
     {test_user, User}] ++ Config.

end_per_group(_Group, Config) ->
    ok = rabbit_ct_broker_helpers:delete_user(Config, ?config(test_user, Config)),
    ok = rabbit_ct_broker_helpers:delete_vhost(Config, ?config(test_vhost, Config)),
    rabbit_ct_helpers:run_teardown_steps(
      Config,
      rabbit_ct_client_helpers:teardown_steps() ++
      rabbit_ct_broker_helpers:teardown_steps()).

init_per_testcase(Testcase, Config) ->
    ok = set_permissions(Config, <<>>, <<>>, <<"^some vhost permission">>),
    rabbit_ct_helpers:testcase_started(Config, Testcase).

end_per_testcase(Testcase, Config) ->
    ok = clear_permissions(Config),
    rabbit_ct_helpers:testcase_finished(Config, Testcase).

amqp(Config) ->
    Port = rabbit_ct_broker_helpers:get_node_config(Config, 0, tcp_port_amqp_tls),
    Host = ?config(rmq_hostname, Config),
    Vhost = ?config(test_vhost, Config),
    CACertFile = ?config(rmq_certsdir, Config) ++ "/testca/cacert.pem",
    CertFile = ?config(rmq_certsdir, Config) ++ "/client/cert.pem",
    KeyFile = ?config(rmq_certsdir, Config) ++ "/client/key.pem",
    OpnConf = #{address => Host,
                port => Port,
                container_id => atom_to_binary(?FUNCTION_NAME),
                hostname => <<"vhost:", Vhost/binary>>,
                sasl => external,
                tls_opts => {secure_port, [{cacertfile, CACertFile},
                                           {certfile, CertFile},
                                           {keyfile, KeyFile}]}
               },
    {ok, Connection} = amqp10_client:open_connection(OpnConf),
    receive {amqp10_event, {connection, Connection, opened}} -> ok
    after 5000 -> ct:fail(missing_opened)
    end,
    ok = amqp10_client:close_connection(Connection).

set_permissions(Config, ConfigurePerm, WritePerm, ReadPerm) ->
    ok = rabbit_ct_broker_helpers:set_permissions(Config,
                                                  ?config(test_user, Config),
                                                  ?config(test_vhost, Config),
                                                  ConfigurePerm,
                                                  WritePerm,
                                                  ReadPerm).

clear_permissions(Config) ->
    User = ?config(test_user, Config),
    Vhost = ?config(test_vhost, Config),
    ok = rabbit_ct_broker_helpers:clear_permissions(Config, User, Vhost).
