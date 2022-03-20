%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2022 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(system_SUITE).
-compile([export_all]).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("amqp_client/include/amqp_client.hrl").

-define(ALICE_NAME, "Alice").
-define(BOB_NAME, "Bob").
-define(CAROL_NAME, "Carol").
-define(PETER_NAME, "Peter").
-define(JIMMY_NAME, "Jimmy").

-define(VHOST, "test").

-define(ALICE, #amqp_params_network{username     = <<?ALICE_NAME>>,
                                    password     = <<"password">>,
                                    virtual_host = <<?VHOST>>}).

-define(BOB, #amqp_params_network{username       = <<?BOB_NAME>>,
                                  password       = <<"password">>,
                                  virtual_host   = <<?VHOST>>}).

-define(CAROL, #amqp_params_network{username     = <<?CAROL_NAME>>,
                                    password     = <<"password">>,
                                    virtual_host = <<?VHOST>>}).

-define(PETER, #amqp_params_network{username     = <<?PETER_NAME>>,
                                    password     = <<"password">>,
                                    virtual_host = <<?VHOST>>}).

-define(JIMMY, #amqp_params_network{username     = <<?JIMMY_NAME>>,
                                    password     = <<"password">>,
                                    virtual_host = <<?VHOST>>}).

-define(BASE_CONF_RABBIT, {rabbit, [{default_vhost, <<"test">>}]}).

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

%%--------------------------------------------------------------------

all() ->
    [
          {group, non_parallel_tests},
          {group, with_idle_timeout}
    ].

groups() ->
    Tests = [
        purge_connection,
        ldap_only,
        ldap_and_internal,
        internal_followed_ldap_and_internal,
        tag_attribution_ldap_only,
        tag_attribution_ldap_and_internal,
        tag_attribution_internal_followed_by_ldap_and_internal,
        invalid_or_clause_ldap_only,
        invalid_and_clause_ldap_only,
        topic_authorisation_publishing_ldap_only,
        topic_authorisation_consumption,
        match_bidirectional,
        match_bidirectional_gh_100
    ],
    [
      {non_parallel_tests, [], Tests
      },
      {with_idle_timeout, [], [connections_closed_after_timeout | Tests]
      }
    ].

suite() ->
    [{timetrap, {minutes, 2}}].

%% -------------------------------------------------------------------
%% Testsuite setup/teardown.
%% -------------------------------------------------------------------

init_per_suite(Config) ->
    rabbit_ct_helpers:log_environment(),
    rabbit_ct_helpers:run_setup_steps(Config, [fun init_slapd/1]).

end_per_suite(Config) ->
    rabbit_ct_helpers:run_teardown_steps(Config, [fun stop_slapd/1]).

init_per_group(Group, Config) ->
    Config1 = rabbit_ct_helpers:set_config(Config, [
        {rmq_nodename_suffix, Group}
      ]),
    LdapPort = ?config(ldap_port, Config),
    Config2 = rabbit_ct_helpers:merge_app_env(Config1, ?BASE_CONF_RABBIT),
    Config3 = rabbit_ct_helpers:merge_app_env(Config2,
                                              base_conf_ldap(LdapPort,
                                                             idle_timeout(Group),
                                                             pool_size(Group))),
    Logon = {"localhost", LdapPort},
    rabbit_ldap_seed:delete(Logon),
    rabbit_ldap_seed:seed(Logon),
    Config4 = rabbit_ct_helpers:set_config(Config3, {ldap_port, LdapPort}),

    rabbit_ct_helpers:run_steps(Config4,
      rabbit_ct_broker_helpers:setup_steps() ++
      rabbit_ct_client_helpers:setup_steps()).

end_per_group(_, Config) ->
    rabbit_ldap_seed:delete({"localhost", ?config(ldap_port, Config)}),
    rabbit_ct_helpers:run_steps(Config,
      rabbit_ct_client_helpers:teardown_steps() ++
      rabbit_ct_broker_helpers:teardown_steps()).

init_slapd(Config) ->
    DataDir = ?config(data_dir, Config),
    PrivDir = ?config(priv_dir, Config),
    TcpPort = 25389,
    SlapdDir = filename:join([PrivDir, "openldap"]),
    InitSlapd = filename:join([DataDir, "init-slapd.sh"]),
    Cmd = [InitSlapd, SlapdDir, {"~b", [TcpPort]}],
    case rabbit_ct_helpers:exec(Cmd) of
        {ok, Stdout} ->
            {match, [SlapdPid]} = re:run(
                                    Stdout,
                                    "^SLAPD_PID=([0-9]+)$",
                                    [{capture, all_but_first, list},
                                     multiline]),
            ct:pal(?LOW_IMPORTANCE,
                   "slapd(8) PID: ~s~nslapd(8) listening on: ~b",
                   [SlapdPid, TcpPort]),
            rabbit_ct_helpers:set_config(Config,
                                         [{slapd_pid, SlapdPid},
                                          {ldap_port, TcpPort}]);
        _ ->
            _ = rabbit_ct_helpers:exec(["pkill", "-INT", "slapd"]),
            {skip, "Failed to initialize slapd(8)"}
    end.

stop_slapd(Config) ->
    SlapdPid = ?config(slapd_pid, Config),
    Cmd = ["kill", "-INT", SlapdPid],
    _ = rabbit_ct_helpers:exec(Cmd),
    Config.

idle_timeout(with_idle_timeout) -> 2000;
idle_timeout(non_parallel_tests) -> infinity.

pool_size(with_idle_timeout) -> 1;
pool_size(non_parallel_tests) -> 10.

init_internal(Config) ->
    ok = control_action(Config, add_user, [?ALICE_NAME, ""]),
    ok = control_action(Config, set_permissions, [?ALICE_NAME, "prefix-.*", "prefix-.*", "prefix-.*"]),
    ok = control_action(Config, set_user_tags, [?ALICE_NAME, "management", "foo"]),
    ok = control_action(Config, add_user, [?BOB_NAME, ""]),
    ok = control_action(Config, set_permissions, [?BOB_NAME, "^$", "^$", "^$"]),
    ok = control_action(Config, add_user, [?PETER_NAME, ""]),
    ok = control_action(Config, set_permissions, [?PETER_NAME, "^$", "^$", "^$"]).

end_internal(Config) ->
    ok = control_action(Config, delete_user, [?ALICE_NAME]),
    ok = control_action(Config, delete_user, [?BOB_NAME]),
    ok = control_action(Config, delete_user, [?PETER_NAME]).

init_per_testcase(Testcase, Config)
    when Testcase == ldap_and_internal;
         Testcase == internal_followed_ldap_and_internal ->
    init_internal(Config),
    rabbit_ct_helpers:testcase_started(Config, Testcase);
init_per_testcase(Testcase, Config)
    when Testcase == tag_attribution_ldap_and_internal;
         Testcase == tag_attribution_internal_followed_by_ldap_and_internal ->
    % back up tag queries
    Cfg = case rabbit_ct_broker_helpers:rpc(Config, 0,
                                            application,
                                            get_env,
                                            [rabbit_auth_backend_ldap, tag_queries]) of
               undefined -> undefined;
               {ok, X} -> X
          end,
    rabbit_ct_helpers:set_config(Config, {tag_queries_config, Cfg}),
    internal_authorization_teardown(Config),
    internal_authorization_setup(Config),
    rabbit_ct_helpers:testcase_started(Config, Testcase);
init_per_testcase(Testcase, Config) ->
    rabbit_ct_helpers:testcase_started(Config, Testcase).

end_per_testcase(Testcase, Config)
    when Testcase == ldap_and_internal;
         Testcase == internal_followed_ldap_and_internal ->
    end_internal(Config),
    rabbit_ct_helpers:testcase_finished(Config, Testcase);
end_per_testcase(Testcase, Config)
    when Testcase == tag_attribution_ldap_and_internal;
         Testcase == tag_attribution_internal_followed_by_ldap_and_internal ->
    % restore tag queries
    Cfg = rabbit_ct_helpers:get_config(Config, tag_queries_config),
    ok = rabbit_ct_broker_helpers:rpc(Config, 0,
                                      application,
                                      set_env,
                                      [rabbit_auth_backend_ldap, tag_queries, Cfg]),
    internal_authorization_teardown(Config),
    rabbit_ct_helpers:testcase_finished(Config, Testcase);
end_per_testcase(connections_closed_after_timeout, Config) ->
    ok = rabbit_ct_broker_helpers:rpc(Config, 0,
                                      application,
                                      set_env,
                                      [rabbitmq_auth_backend_ldap,
                                       other_bind, anon]),
    rabbit_ct_helpers:testcase_finished(Config, connections_closed_after_timeout);
end_per_testcase(Testcase, Config)
    when Testcase == invalid_or_clause_ldap_only;
         Testcase == invalid_and_clause_ldap_only ->
    set_env(Config, vhost_access_query_base_env()),
    rabbit_ct_helpers:testcase_finished(Config, Testcase);
end_per_testcase(Testcase, Config)
    when Testcase == topic_authorisation_publishing_ldap_only;
         Testcase == topic_authorisation_consumption ->
    set_env(Config, topic_access_query_base_env()),
    rabbit_ct_helpers:testcase_finished(Config, Testcase);
end_per_testcase(Testcase, Config) ->
    rabbit_ct_helpers:testcase_finished(Config, Testcase).


%% -------------------------------------------------------------------
%% Testsuite cases
%% -------------------------------------------------------------------

purge_connection(Config) ->
    {ok, _} = rabbit_ct_broker_helpers:rpc(Config, 0,
                                           rabbit_auth_backend_ldap,
                                           user_login_authentication,
                                           [<<?ALICE_NAME>>, []]),

    ok = rabbit_ct_broker_helpers:rpc(Config, 0,
                                      rabbit_auth_backend_ldap,
                                      purge_connections, []).

connections_closed_after_timeout(Config) ->
    {ok, _} = rabbit_ct_broker_helpers:rpc(Config, 0,
                                           rabbit_auth_backend_ldap,
                                           user_login_authentication,
                                           [<<?ALICE_NAME>>, []]),

    [_] = maps:to_list(get_ldap_connections(Config)),
    ct:sleep(idle_timeout(with_idle_timeout) + 200),

    %% There should be no connections after idle timeout
    [] = maps:to_list(get_ldap_connections(Config)),

    {ok, _} = rabbit_ct_broker_helpers:rpc(Config, 0,
                                           rabbit_auth_backend_ldap,
                                           user_login_authentication,
                                           [<<?ALICE_NAME>>, []]),

    ct:sleep(round(idle_timeout(with_idle_timeout)/2)),

    %% Login with password opens different connection,
    % so unauthorized connection will be closed
    ok = rabbit_ct_broker_helpers:rpc(Config, 0,
                                      application,
                                      set_env,
                                      [rabbitmq_auth_backend_ldap,
                                       other_bind, as_user]),
    {ok, _} = rabbit_ct_broker_helpers:rpc(Config, 0,
                                           rabbit_auth_backend_ldap,
                                           user_login_authentication,
                                           [<<?ALICE_NAME>>,
                                            [{password, <<"password">>}]]),

    ct:sleep(round(idle_timeout(with_idle_timeout)/2)),

    {ok, _} = rabbit_ct_broker_helpers:rpc(Config, 0,
                                           rabbit_auth_backend_ldap,
                                           user_login_authentication,
                                           [<<?ALICE_NAME>>,
                                            [{password, <<"password">>}]]),

    ct:sleep(round(idle_timeout(with_idle_timeout)/2)),

    [{Key, _Conn}] = maps:to_list(get_ldap_connections(Config)),

    %% Key will be {IsAnon, Servers, Options}
    %% IsAnon is false for password authorization
    {false, _, _} = Key.


get_ldap_connections(Config) ->
    rabbit_ct_broker_helpers:rpc(Config, 0,
                                 rabbit_auth_backend_ldap, get_connections, []).

ldap_only(Config) ->
    ok = rabbit_ct_broker_helpers:rpc(Config, 0,
           application, set_env, [rabbit, auth_backends, [rabbit_auth_backend_ldap]]),
    login(Config),
    in_group(Config),
    const(Config),
    string_match(Config),
    boolean_logic(Config),
    tag_check(Config, [monitor]),
    tag_check_subst(Config),
    logging(Config),
    ok.

ldap_and_internal(Config) ->
    ok = rabbit_ct_broker_helpers:rpc(Config, 0,
           application, set_env, [rabbit, auth_backends,
                                  [{rabbit_auth_backend_ldap, rabbit_auth_backend_internal}]]),
    login(Config),
    permission_match(Config),
    tag_check(Config, [monitor, management, foo]),
    ok.

internal_followed_ldap_and_internal(Config) ->
    ok = rabbit_ct_broker_helpers:rpc(Config, 0,
           application, set_env, [rabbit, auth_backends,
                                  [rabbit_auth_backend_internal, {rabbit_auth_backend_ldap, rabbit_auth_backend_internal}]]),
    login(Config),
    permission_match(Config),
    tag_check(Config, [monitor, management, foo]),
    ok.

tag_attribution_ldap_only(Config) ->
    set_env(Config, tag_query_configuration()),
    ok = rabbit_ct_broker_helpers:rpc(Config, 0,
           application, set_env, [rabbit, auth_backends, [rabbit_auth_backend_ldap]]),
    tag_check(Config, <<"Edward">>, <<"password">>, [monitor, normal]).

tag_attribution_ldap_and_internal(Config) ->
    set_env(Config, tag_query_configuration()),
    ok = rabbit_ct_broker_helpers:rpc(Config, 0,
           application, set_env, [rabbit, auth_backends, [{rabbit_auth_backend_ldap,
                                                           rabbit_auth_backend_internal}]]),
    tag_check(Config, <<"Edward">>, <<"password">>,
               [monitor, normal] ++ internal_authorization_tags()).

tag_attribution_internal_followed_by_ldap_and_internal(Config) ->
    set_env(Config, tag_query_configuration()),
    ok = rabbit_ct_broker_helpers:rpc(Config, 0,
           application, set_env, [rabbit, auth_backends, [rabbit_auth_backend_internal,
                                                          {rabbit_auth_backend_ldap,
                                                           rabbit_auth_backend_internal}]]),
    tag_check(Config, <<"Edward">>, <<"password">>,
               [monitor, normal] ++ internal_authorization_tags()).

invalid_or_clause_ldap_only(Config) ->
    set_env(Config, vhost_access_query_or_in_group()),
    ok = rabbit_ct_broker_helpers:rpc(Config, 0,
           application, set_env, [rabbit, auth_backends, [rabbit_auth_backend_ldap]]),
    B = #amqp_params_network{port = rabbit_ct_broker_helpers:get_node_config(Config, 0, tcp_port_amqp)},
    {ok, C} = amqp_connection:start(B?ALICE),
    ok = amqp_connection:close(C).

invalid_and_clause_ldap_only(Config) ->
    set_env(Config, vhost_access_query_and_in_group()),
    ok = rabbit_ct_broker_helpers:rpc(Config, 0,
           application, set_env, [rabbit, auth_backends, [rabbit_auth_backend_ldap]]),
    B = #amqp_params_network{port = rabbit_ct_broker_helpers:get_node_config(Config, 0, tcp_port_amqp)},
    % NB: if the query crashes the ldap plugin it returns {error, access_refused}
    % This may not be a reliable return value assertion
    {error, not_allowed} = amqp_connection:start(B?ALICE).

topic_authorisation_publishing_ldap_only(Config) ->
    %% topic authorisation at publishing time is enforced in the AMQP channel
    %% so it can be tested by sending messages
    ok = rabbit_ct_broker_helpers:rpc(Config, 0,
        application, set_env, [rabbit, auth_backends, [rabbit_auth_backend_ldap]]),

    %% default is to let pass
    P = #amqp_params_network{port = rabbit_ct_broker_helpers:get_node_config(Config, 0, tcp_port_amqp)},
    test_publish(P?ALICE, <<"amq.topic">>, <<"a.b.c">>, ok),

    %% let pass for topic
    set_env(Config, [{topic_access_query, {constant, true}}]),

    P = #amqp_params_network{port = rabbit_ct_broker_helpers:get_node_config(Config, 0, tcp_port_amqp)},
    test_publish(P?ALICE, <<"amq.topic">>, <<"a.b.c">>, ok),

    %% check string substitution (on username)
    set_env(Config, [{topic_access_query, {for, [{permission, write, {equals, "${username}", "Alice"}},
                                                 {permission, read, {constant, false}}
    ]}}]),
    test_publish(P?ALICE, <<"amq.topic">>, <<"a.b.c">>, ok),
    test_publish(P?BOB, <<"amq.topic">>, <<"a.b.c">>, fail),

    %% check string substitution on routing key (with regex)
    set_env(Config, [{topic_access_query, {for, [{permission, write, {'and',
                [{equals, "${username}", "Alice"},
                 {match, {string, "${routing_key}"}, {string, "^a"}}]
                }},
                {permission, read, {constant, false}}
    ]}}]),
    %% user and routing key OK
    test_publish(P?ALICE, <<"amq.topic">>, <<"a.b.c">>, ok),
    %% user and routing key OK
    test_publish(P?ALICE, <<"amq.topic">>, <<"a.c">>, ok),
    %% user OK, routing key KO, should fail
    test_publish(P?ALICE, <<"amq.topic">>, <<"b.c">>, fail),
    %% user KO, routing key OK, should fail
    test_publish(P?BOB, <<"amq.topic">>, <<"a.b.c">>, fail),

    ok.

topic_authorisation_consumption(Config) ->
    %% topic authorisation for consumption isn't enforced in AMQP
    %% (it is in plugins like STOMP and MQTT, at subscription time)
    %% so we directly test the LDAP backend, inside the broker
    ok = rabbit_ct_broker_helpers:rpc(Config, 0,
        ?MODULE, topic_authorisation_consumption1, [Config]).

topic_authorisation_consumption1(Config) ->
    %% we can't use the LDAP backend record here, falling back to simple tuples
    Alice = {auth_user,<<"Alice">>, [monitor],
             {impl,"cn=Alice,ou=People,dc=rabbitmq,dc=com",<<"password">>}
    },
    Bob = {auth_user,<<"Bob">>, [monitor],
           {impl,"cn=Bob,ou=People,dc=rabbitmq,dc=com",<<"password">>}
    },
    Resource = #resource{virtual_host = <<"/">>, name = <<"amq.topic">>, kind = topic},
    Context = #{routing_key  => <<"a.b">>,
                variable_map => #{
                    <<"username">> => <<"guest">>,
                    <<"vhost">>    => <<"other-vhost">>
                }},
    %% default is to let pass
    true = rabbit_auth_backend_ldap:check_topic_access(Alice, Resource, read, Context),

    %% let pass for topic
    set_env(Config, [{topic_access_query, {for, [{permission, read, {constant, true}},
                                                 {permission, write, {constant, false}}]
    }}]),

    true = rabbit_auth_backend_ldap:check_topic_access(Alice, Resource, read, Context),

    %% check string substitution (on username)
    set_env(Config, [{topic_access_query, {for, [{permission, read, {equals, "${username}", "Alice"}},
                                                 {permission, write, {constant, false}}]
    }}]),

    true = rabbit_auth_backend_ldap:check_topic_access(Alice, Resource, read, Context),
    false = rabbit_auth_backend_ldap:check_topic_access(Bob, Resource, read, Context),

    %% check string substitution on routing key (with regex)
    set_env(Config, [{topic_access_query, {for, [{permission, read, {'and',
                [{equals, "${username}", "Alice"},
                 {match, {string, "${routing_key}"}, {string, "^a"}}]
            }},
        {permission, write, {constant, false}}]
    }}]),
    %% user and routing key OK
    true = rabbit_auth_backend_ldap:check_topic_access(Alice, Resource, read, #{routing_key => <<"a.b.c">>}),
    %% user and routing key OK
    true = rabbit_auth_backend_ldap:check_topic_access(Alice, Resource, read, #{routing_key => <<"a.c">>}),
    %% user OK, routing key KO, should fail
    false = rabbit_auth_backend_ldap:check_topic_access(Alice, Resource, read, #{routing_key => <<"b.c">>}),
    %% user KO, routing key OK, should fail
    false = rabbit_auth_backend_ldap:check_topic_access(Bob, Resource, read, #{routing_key => <<"a.b.c">>}),
    ok.

match_bidirectional(Config) ->
    ok = rabbit_ct_broker_helpers:rpc(Config, 0,
        application, set_env, [rabbit, auth_backends, [rabbit_auth_backend_ldap]]),

    Configurations = [
        fun resource_access_query_match/0,
        fun resource_access_query_match_query_is_string/0,
        fun resource_access_query_match_re_query_is_string/0,
        fun resource_access_query_match_query_and_re_query_are_strings/0
    ],

    [begin
         set_env(Config, ConfigurationFunction()),
         Q1 = [#'queue.declare'{queue = <<"Alice-queue">>}],
         Q2 = [#'queue.declare'{queue = <<"Ali">>}],
         P = #amqp_params_network{port = rabbit_ct_broker_helpers:get_node_config(Config, 0, tcp_port_amqp)},
         [test_resource(PTR) || PTR <- [{P?ALICE, Q1, ok},
                                        {P?ALICE, Q2, fail}]]
     end || ConfigurationFunction <- Configurations],
    ok.

match_bidirectional_gh_100(Config) ->
    ok = rabbit_ct_broker_helpers:rpc(Config, 0,
        application, set_env, [rabbit, auth_backends, [rabbit_auth_backend_ldap]]),

    Configurations = [
        fun resource_access_query_match_gh_100/0,
        fun resource_access_query_match_query_is_string_gh_100/0
    ],

    [begin
         set_env(Config, ConfigurationFunction()),
         Q1 = [#'queue.declare'{queue = <<"Jimmy-queue">>}],
         Q2 = [#'queue.declare'{queue = <<"Jimmy">>}],
         P = #amqp_params_network{port = rabbit_ct_broker_helpers:get_node_config(Config, 0, tcp_port_amqp)},
         [test_resource(PTR) || PTR <- [{P?JIMMY, Q1, ok},
                                        {P?JIMMY, Q2, ok}]]
     end || ConfigurationFunction <- Configurations],
    ok.

%%--------------------------------------------------------------------

test_publish(Person, Exchange, RoutingKey, ExpectedResult) ->
    {ok, Connection} = amqp_connection:start(Person),
    {ok, Channel} = amqp_connection:open_channel(Connection),
    ActualResult =
        try
            Publish = #'basic.publish'{exchange = Exchange, routing_key = RoutingKey},
            amqp_channel:cast(Channel, Publish, #amqp_msg{payload = <<"foobar">>}),
            amqp_channel:call(Channel, #'basic.qos'{prefetch_count = 0}),
            ok
        catch exit:_ -> fail
        after
            amqp_connection:close(Connection)
    end,
    ExpectedResult = ActualResult.

login(Config) ->
    lists:flatten(
      [test_login(Config, {N, Env}, L, FilterList, case {LGood, EnvGood} of
                                               {good, good} -> fun succ/1;
                                               _            -> fun fail/1
                                           end) ||
          {LGood, FilterList, L, _Tags}  <- logins(Config),
          {N, {EnvGood, Env}}            <- login_envs()]).

logins(Config) -> logins_network(Config) ++ logins_direct(Config).

%% Format for login tests, {Outcome, FilterList, Login, Tags}.
%% Tests skipped for each login_env reference in FilterList.
logins_network(Config) ->
    B = #amqp_params_network{port = rabbit_ct_broker_helpers:get_node_config(Config, 0, tcp_port_amqp)},
    [{bad,  [5, 6], B#amqp_params_network{}, []},
     {bad,  [5, 6], B#amqp_params_network{username     = <<?ALICE_NAME>>}, []},
     {bad,  [5, 6], B#amqp_params_network{username     = <<?ALICE_NAME>>,
                                          password     = <<"password">>}, []},
     {bad,  [5, 6], B#amqp_params_network{username     = <<"Alice">>,
                                          password     = <<"Alicja">>,
                                          virtual_host = <<?VHOST>>}, []},
     {bad,  [1, 2, 3, 4, 6, 7], B?CAROL, []},
     {good, [5, 6], B?ALICE, []},
     {good, [5, 6], B?BOB, []},
     {good, [1, 2, 3, 4, 6, 7, 8], B?PETER, []}].

logins_direct(Config) ->
    N = #amqp_params_direct{node = rabbit_ct_broker_helpers:get_node_config(Config, 0, nodename)},
    [{bad,  [5], N#amqp_params_direct{}, []},
     {bad,  [5], N#amqp_params_direct{username         = <<?ALICE_NAME>>}, []},
     {bad,  [5], N#amqp_params_direct{username         = <<?ALICE_NAME>>,
                                      password         = <<"password">>}, [management]},
     {good, [5], N#amqp_params_direct{username         = <<?ALICE_NAME>>,
                                      password         = <<"password">>,
                                      virtual_host     = <<?VHOST>>}, [management]}].

%% Format for login envs, {Reference, {Outcome, Env}}
login_envs() ->
    [{1, {good, base_login_env()}},
     {2, {good, dn_lookup_pre_bind_env()}},
     {3, {good, other_bind_admin_env()}},
     {4, {good, other_bind_anon_env()}},
     {5, {good, posix_vhost_access_multiattr_env()}},
     {6, {good, tag_queries_subst_env()}},
     {7, {bad,  other_bind_broken_env()}},
     {8, {good, vhost_access_query_nested_groups_env()}}].

base_login_env() ->
    [{user_dn_pattern,     "cn=${username},ou=People,dc=rabbitmq,dc=com"},
     {dn_lookup_attribute, none},
     {dn_lookup_base,      none},
     {dn_lookup_bind,      as_user},
     {other_bind,          as_user},
     {tag_queries,         [{monitor,       {constant, true}},
                            {administrator, {constant, false}},
                            {management,    {constant, false}}]},
     {vhost_access_query,  {exists, "ou=${vhost},ou=vhosts,dc=rabbitmq,dc=com"}},
     {log,                  true}].

%% TODO configure OpenLDAP to allow a dn_lookup_post_bind_env()
dn_lookup_pre_bind_env() ->
    [{user_dn_pattern,     "${username}"},
     {dn_lookup_attribute, "cn"},
     {dn_lookup_base,      "OU=People,DC=rabbitmq,DC=com"},
     {dn_lookup_bind,      {"cn=admin,dc=rabbitmq,dc=com", "admin"}}].

other_bind_admin_env() ->
    [{other_bind, {"cn=admin,dc=rabbitmq,dc=com", "admin"}}].

other_bind_anon_env() ->
    [{other_bind, anon}].

other_bind_broken_env() ->
    [{other_bind, {"cn=admin,dc=rabbitmq,dc=com", "admi"}}].

tag_queries_subst_env() ->
    [{tag_queries, [{administrator, {constant, false}},
                    {management,
                     {exists, "ou=${vhost},ou=vhosts,dc=rabbitmq,dc=com"}}]}].

posix_vhost_access_multiattr_env() ->
    [{user_dn_pattern, "uid=${username},ou=People,dc=rabbitmq,dc=com"},
     {vhost_access_query,
      {'and', [{exists, "ou=${vhost},ou=vhosts,dc=rabbitmq,dc=com"},
               {equals,
                {attribute, "${user_dn}","memberOf"},
                {string, "cn=wheel,ou=groups,dc=rabbitmq,dc=com"}},
               {equals,
                {attribute, "${user_dn}","memberOf"},
                {string, "cn=people,ou=groups,dc=rabbitmq,dc=com"}},
               {equals,
                {string, "cn=wheel,ou=groups,dc=rabbitmq,dc=com"},
                {attribute,"${user_dn}","memberOf"}},
               {equals,
                {string, "cn=people,ou=groups,dc=rabbitmq,dc=com"},
                {attribute, "${user_dn}","memberOf"}},
               {match,
                {attribute, "${user_dn}","memberOf"},
                {string, "cn=wheel,ou=groups,dc=rabbitmq,dc=com"}},
               {match,
                {attribute, "${user_dn}","memberOf"},
                {string, "cn=people,ou=groups,dc=rabbitmq,dc=com"}},
               {match,
                {string, "cn=wheel,ou=groups,dc=rabbitmq,dc=com"},
                {attribute, "${user_dn}","memberOf"}},
               {match,
                {string, "cn=people,ou=groups,dc=rabbitmq,dc=com"},
                {attribute, "${user_dn}","memberOf"}}
              ]}}].

vhost_access_query_or_in_group() ->
    [{vhost_access_query,
      {'or', [
            {in_group, "cn=bananas,ou=groups,dc=rabbitmq,dc=com"},
            {in_group, "cn=apples,ou=groups,dc=rabbitmq,dc=com"},
            {in_group, "cn=wheel,ou=groups,dc=rabbitmq,dc=com"}
             ]}}].

vhost_access_query_and_in_group() ->
    [{vhost_access_query,
      {'and', [
            {in_group, "cn=bananas,ou=groups,dc=rabbitmq,dc=com"},
            {in_group, "cn=wheel,ou=groups,dc=rabbitmq,dc=com"}
             ]}}].

vhost_access_query_nested_groups_env() ->
    [{vhost_access_query, {in_group_nested, "cn=admins,ou=groups,dc=rabbitmq,dc=com"}}].

vhost_access_query_base_env() ->
    [{vhost_access_query, vhost_access_query_base()}].

vhost_access_query_base() ->
    {exists, "ou=${vhost},ou=vhosts,dc=rabbitmq,dc=com"}.

resource_access_query_match_gh_100() ->
    [{resource_access_query,
      {match, {string, "RMQ-${vhost}"}, {attribute, "${user_dn}", "description"}}
     }].

resource_access_query_match_query_is_string_gh_100() ->
    [{resource_access_query,
      {match, "RMQ-${vhost}", {attribute, "${user_dn}", "description"}}
     }].

resource_access_query_match() ->
    [{resource_access_query, {match, {string, "${name}"},
        {string, "^${username}-"}}
    }].

resource_access_query_match_query_is_string() ->
    [{resource_access_query, {match, "${name}",
        {string, "^${username}-"}}
    }].

resource_access_query_match_re_query_is_string() ->
    [{resource_access_query, {match, {string, "${name}"},
        "^${username}-"}
    }].

resource_access_query_match_query_and_re_query_are_strings() ->
    [{resource_access_query, {match, "${name}",
        "^${username}-"}
    }].

topic_access_query_base_env() ->
    [{topic_access_query, topic_access_query_base()}].

topic_access_query_base() ->
    {constant, true}.

test_login(Config, {N, Env}, Login, FilterList, ResultFun) ->
    case lists:member(N, FilterList) of
        true -> [];
        _ ->
            try
               set_env(Config, Env),
               ResultFun(Login)
            after
               set_env(Config, base_login_env())
            end
    end.

rpc_set_env(Config, Args) ->
    rabbit_ct_broker_helpers:rpc(Config, 0, application, set_env, Args).

set_env(Config, Env) ->
    [rpc_set_env(Config, [rabbitmq_auth_backend_ldap, K, V]) || {K, V} <- Env].

succ(Login) ->
    {ok, Pid} = amqp_connection:start(Login),
    amqp_connection:close(Pid).
fail(Login) -> ?assertMatch({error, _}, amqp_connection:start(Login)).

%%--------------------------------------------------------------------

in_group(Config) ->
    X = [#'exchange.declare'{exchange = <<"test">>}],
    B = #amqp_params_network{port = rabbit_ct_broker_helpers:get_node_config(Config, 0, tcp_port_amqp)},
    test_resources([{B?ALICE, X, ok},
                        {B?BOB, X, fail}]).

const(Config) ->
    Q = [#'queue.declare'{queue = <<"test">>}],
    B = #amqp_params_network{port = rabbit_ct_broker_helpers:get_node_config(Config, 0, tcp_port_amqp)},
    test_resources([{B?ALICE, Q, ok},
                        {B?BOB, Q, fail}]).

string_match(Config) ->
    B = fun(N) ->
                [#'exchange.declare'{exchange = N},
                 #'queue.declare'{queue = <<"test">>},
                 #'queue.bind'{exchange = N, queue = <<"test">>}]
        end,
    P = #amqp_params_network{port = rabbit_ct_broker_helpers:get_node_config(Config, 0, tcp_port_amqp)},
    test_resources([{P?ALICE, B(<<"xch-Alice-abc123">>), ok},
                        {P?ALICE, B(<<"abc123">>),                     fail},
                        {P?ALICE, B(<<"xch-Someone Else-abc123">>),    fail}]).

boolean_logic(Config) ->
    Q1 = [#'queue.declare'{queue = <<"test1">>},
          #'basic.consume'{queue = <<"test1">>}],
    Q2 = [#'queue.declare'{queue = <<"test2">>},
          #'basic.consume'{queue = <<"test2">>}],
    P = #amqp_params_network{port = rabbit_ct_broker_helpers:get_node_config(Config, 0, tcp_port_amqp)},
    [test_resource(PTR) || PTR <- [{P?ALICE, Q1, ok},
                                       {P?ALICE, Q2, ok},
                                       {P?BOB, Q1, fail},
                                       {P?BOB, Q2, fail}]].

permission_match(Config) ->
    B = fun(N) ->
                [#'exchange.declare'{exchange = N},
                 #'queue.declare'{queue = <<"prefix-test">>},
                 #'queue.bind'{exchange = N, queue = <<"prefix-test">>}]
        end,
    P = #amqp_params_network{port = rabbit_ct_broker_helpers:get_node_config(Config, 0, tcp_port_amqp)},
    test_resources([{P?ALICE, B(<<"prefix-abc123">>),    ok},
                        {P?ALICE, B(<<"abc123">>),           fail},
                        {P?ALICE, B(<<"xch-Alice-abc123">>), fail}]).

%% Tag check tests, with substitution
tag_check_subst(Config) ->
    lists:flatten(
      [test_tag_check(Config, tag_queries_subst_env(),
                      fun () -> tag_check(Config, Username, Password, VHost, Outcome, Tags) end) ||
          {Outcome, _FilterList, #amqp_params_direct{username     = Username,
                                                     password     = Password,
                                                     virtual_host = VHost},
           Tags} <- logins_direct(Config)]).

%% Tag check
tag_check(Config, Tags) ->
    tag_check(Config, <<?ALICE_NAME>>, <<"password">>, Tags).

tag_check(Config, Username, Password, Tags) ->
    tag_check(Config, Username, Password, <<>>, good, Tags).

tag_check(Config, Username, Password, VHost, Outcome, Tags)
  when is_binary(Username), is_binary(Password), is_binary(VHost), is_list(Tags) ->
    {ok, User} = rabbit_ct_broker_helpers:rpc(Config, 0, rabbit_access_control, check_user_login, [Username, [{password, Password}, {vhost, VHost}]]),
    tag_check_outcome(Outcome, Tags, User);
tag_check(_, _, _, _, _, _) -> fun() -> [] end.

tag_check_outcome(good, Tags, User) -> ?assertEqual(Tags, User#user.tags);
tag_check_outcome(bad, Tags, User)  -> ?assertNotEqual(Tags, User#user.tags).

test_tag_check(Config, Env, TagCheckFun) ->
    try
       set_env(Config, Env),
       TagCheckFun()
    after
       set_env(Config, base_login_env())
    end.

tag_query_configuration() ->
    [{tag_queries,
      [{administrator, {constant, false}},
       %% Query result for tag `management` is FALSE
       %% because this object does NOT exist.
       {management,
        {exists, "cn=${username},ou=Faculty,dc=Computer Science,dc=Engineering"}},
       {monitor, {constant, true}},
       %% Query result for tag `normal` is TRUE because
       %% this object exists.
       {normal,
        {exists, "cn=${username},ou=people,dc=rabbitmq,dc=com"}}]}].

internal_authorization_setup(Config) ->
    ok = control_action(Config, add_user, ["Edward", ""]),
    ok = control_action(Config, set_user_tags, ["Edward"] ++
        [ atom_to_list(T) || T <- internal_authorization_tags() ]).

internal_authorization_teardown(Config) ->
    control_action(Config, delete_user, ["Edward"]).

internal_authorization_tags() ->
    [foo, bar].

%% Logging tests, triggered within 'test_login/4'
logging(Config) ->
    lists:flatten(
      [test_login(Config, {N, Env}, L, FilterList, case {LGood, EnvGood} of
                                               {good, good} -> fun succ/1;
                                               _            -> fun fail/1
                                           end) ||
          {LGood, FilterList, L}  <- logging_test_users(Config),
          {N, {EnvGood, Env}}     <- logging_envs()]).

%% Format for logging tests, {Outcome, FilterList, Login}.
logging_test_users(Config) ->
    P = #amqp_params_network{port = rabbit_ct_broker_helpers:get_node_config(Config, 0, tcp_port_amqp)},
    [{bad,  [], P#amqp_params_network{username = <<?ALICE_NAME>>}},
     {good, [], P?ALICE}].

logging_envs() ->
    [{1, {good, scrub_bind_creds_env()}},
     {2, {good, display_bind_creds_env()}},
     {3, {bad,  scrub_bind_single_cred_env()}},
     {4, {bad,  scrub_bind_creds_no_equals_env()}},
     {5, {bad,  scrub_bind_creds_no_seperator_env()}}].

scrub_bind_creds_env() ->
    [{log,         network},
     {other_bind,  {"cn=admin,dc=rabbitmq,dc=com", "admin"}}].

display_bind_creds_env() ->
    [{log,         network_unsafe},
     {other_bind,  {"cn=admin,dc=rabbitmq,dc=com", "admin"}}].

scrub_bind_single_cred_env() ->
    [{log,         network},
     {other_bind,  {"dc=com", "admin"}}].

scrub_bind_creds_no_equals_env() ->
    [{log,         network},
     {other_bind,  {"cn*admin,dc>rabbitmq,dc&com", "admin"}}].

scrub_bind_creds_no_seperator_env() ->
    [{log,         network},
     {other_bind,  {"cn=admindc=rabbitmqdc&com", "admin"}}].

%%--------------------------------------------------------------------

test_resources(PTRs) -> [test_resource(PTR) || PTR <- PTRs].

test_resource({Person, Things, Result}) ->
    {ok, Conn} = amqp_connection:start(Person),
    {ok, Ch} = amqp_connection:open_channel(Conn),
    ?assertEqual(Result,
                 try
                     [amqp_channel:call(Ch, T) || T <- Things],
                     ok
                 catch exit:_ -> fail
                 after
                     amqp_connection:close(Conn)
                 end).

control_action(Config, Command, Args) ->
    Node = rabbit_ct_broker_helpers:get_node_config(Config, 0, nodename),
    control_action(Config, Command, Node, Args, default_options()).

control_action(Config, Command, Args, NewOpts) ->
    Node = rabbit_ct_broker_helpers:get_node_config(Config, 0, nodename),
    control_action(Config, Command, Node, Args,
                   expand_options(default_options(), NewOpts)).

control_action(_Config, Command, Node, Args, Opts) ->
    case rabbit_control_helper:command(Command, Node, Args, Opts) of
        ok ->
            io:format("done.~n"),
            ok;
        Other ->
            io:format("failed.~n"),
            Other
    end.

default_options() -> [{"-p", ?VHOST}, {"-q", "false"}].

expand_options(As, Bs) ->
    lists:foldl(fun({K, _}=A, R) ->
                        case proplists:is_defined(K, R) of
                            true  -> R;
                            false -> [A | R]
                        end
                end, Bs, As).

