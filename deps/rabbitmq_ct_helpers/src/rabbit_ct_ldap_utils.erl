%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2025 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
%%

-module(rabbit_ct_ldap_utils).

-export([vhost_access_query_base/0,
         topic_access_query_base/0,
         init_per_group/2, init_per_group/3,
         end_per_group/2, end_per_group/3,
         idle_timeout/1,
         pool_size/1,
         init_slapd/1,
         stop_slapd/1]).

-include_lib("common_test/include/ct.hrl").
-include_lib("rabbitmq_ct_helpers/include/rabbit_ldap_test.hrl").

base_conf_ldap(LdapPort, IdleTimeout, PoolSize) ->
                    {rabbitmq_auth_backend_ldap, [{servers, ["localhost"]},
                                                  {user_dn_pattern,    "cn=${username},ou=People,dc=rabbitmq,dc=com"},
                                                  {other_bind,         anon},
                                                  {use_ssl,            false},
                                                  {port,               LdapPort},
                                                  {idle_timeout,       IdleTimeout},
                                                  {pool_size,          PoolSize},
                                                  {log,                true},
                                                  {group_lookup_base,  "ou=groups,dc=rabbitmq,dc=com"},
                                                  {vhost_access_query, vhost_access_query_base()},
                                                  {resource_access_query,
                                                   {for, [{resource, exchange,
                                                           {for, [{permission, configure,
                                                                   {in_group, "cn=wheel,ou=groups,dc=rabbitmq,dc=com"}
                                                                  },
                                                                  {permission, write, {constant, true}},
                                                                  {permission, read,
                                                                   {match, {string, "${name}"},
                                                                           {string, "^xch-${username}-.*"}}
                                                                  }
                                                                 ]}},
                                                          {resource, queue,
                                                           {for, [{permission, configure,
                                                                   {match, {attribute, "${user_dn}", "description"},
                                                                           {string, "can-declare-queues"}}
                                                                  },
                                                                  {permission, write, {constant, true}},
                                                                  {permission, read,
                                                                   {'or',
                                                                    [{'and',
                                                                      [{equals, "${name}", "test1"},
                                                                       {equals, "${username}", "Alice"}]},
                                                                     {'and',
                                                                      [{equals, "${name}", "test2"},
                                                                       {'not', {equals, "${username}", "Bob"}}]}
                                                                    ]}}
                                                                 ]}}
                                                          ]}},
                                                  {topic_access_query, topic_access_query_base()},
                                                  {tag_queries, [{monitor,       {constant, true}},
                                                                 {administrator, {constant, false}},
                                                                 {management,    {constant, false}}]}
                                                ]}.

vhost_access_query_base() ->
    {exists, "ou=${vhost},ou=vhosts,dc=rabbitmq,dc=com"}.

topic_access_query_base() ->
    {constant, true}.

init_per_group(Group, Config) ->
    init_per_group(Group, Config, []).

init_per_group(Group, Config, ExtraSteps) ->
    Config1 = rabbit_ct_helpers:set_config(Config, [
        {rmq_nodename_suffix, Group}
      ]),
    LdapPort = ?config(ldap_port, Config),
    Config2 = rabbit_ct_helpers:merge_app_env(Config1, ?BASE_CONF_RABBIT),
    Config3 = rabbit_ct_helpers:merge_app_env(Config2,
                                              base_conf_ldap(LdapPort, idle_timeout(Group), pool_size(Group))),
    rabbit_ct_ldap_seed:seed({"localhost", LdapPort}),
    Config4 = rabbit_ct_helpers:set_config(Config3, {ldap_port, LdapPort}),

    Steps = rabbit_ct_broker_helpers:setup_steps() ++ ExtraSteps,
    rabbit_ct_helpers:run_steps(Config4, Steps).

end_per_group(Arg, Config) ->
    end_per_group(Arg, Config, []).

end_per_group(_Arg, Config, ExtraSteps) ->
    rabbit_ct_ldap_seed:delete({"localhost", ?config(ldap_port, Config)}),
    Steps = rabbit_ct_broker_helpers:teardown_steps() ++ ExtraSteps,
    rabbit_ct_helpers:run_steps(Config, Steps).

idle_timeout(with_idle_timeout) -> 2000;
idle_timeout(non_parallel_tests) -> infinity.

pool_size(with_idle_timeout) -> 1;
pool_size(non_parallel_tests) -> 10.

init_slapd(Config) ->
    DataDir = ?config(data_dir, Config),
    PrivDir = ?config(priv_dir, Config),
    CertsDir = ?config(rmq_certsdir, Config),
    CaCertfile = filename:join([CertsDir, "testca", "cacert.pem"]),
    ServerCertfile = filename:join([CertsDir, "server", "cert.pem"]),
    ServerKeyfile = filename:join([CertsDir, "server", "key.pem"]),
    TcpPort = 25389,
    TlsPort = 25689,
    SlapdDir = filename:join([PrivDir, "openldap"]),
    %% 
    InitSlapd = filename:join([DataDir, "init-slapd.sh"]),
    Cmd = [
        InitSlapd,
        SlapdDir,
        {"~b", [TcpPort]},
        {"~b", [TlsPort]},
        CaCertfile,
        ServerCertfile,
        ServerKeyfile
    ],
    case rabbit_ct_helpers:exec(Cmd) of
        {ok, Stdout} ->
            {match, [SlapdPid]} = re:run(
                                    Stdout,
                                    "^SLAPD_PID=([0-9]+)$",
                                    [{capture, all_but_first, list},
                                     multiline]),
            ct:pal(?LOW_IMPORTANCE,
                   "slapd(8) PID: ~ts~nslapd(8) listening on: ~b",
                   [SlapdPid, TcpPort]),
            rabbit_ct_helpers:set_config(Config,
                                         [{slapd_pid, SlapdPid},
                                          {ldap_port, TcpPort},
                                          {ldap_tls_port, TlsPort}]);
        _ ->
            _ = rabbit_ct_helpers:exec(["pkill", "-INT", "slapd"]),
            {skip, "Failed to initialize slapd(8)"}
    end.

stop_slapd(Config) ->
    SlapdPid = ?config(slapd_pid, Config),
    Cmd = ["kill", "-INT", SlapdPid],
    _ = rabbit_ct_helpers:exec(Cmd),
    Config.

