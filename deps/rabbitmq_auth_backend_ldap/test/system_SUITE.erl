%% The contents of this file are subject to the Mozilla Public License
%% Version 1.1 (the "License"); you may not use this file except in
%% compliance with the License. You may obtain a copy of the License
%% at http://www.mozilla.org/MPL/
%%
%% Software distributed under the License is distributed on an "AS IS"
%% basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See
%% the License for the specific language governing rights and
%% limitations under the License.
%%
%% The Original Code is RabbitMQ
%%
%% The Initial Developer of the Original Code is GoPivotal, Inc.
%% Copyright (c) 2007-2016 Pivotal Software, Inc.  All rights reserved.
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

-define(VHOST, "test").
-define(DEFAULT_LDAP_PORT, "3890").

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

-define(BASE_CONF_RABBIT, {rabbit, [{default_vhost, <<"test">>}]}).

base_conf_ldap(LdapPort) ->
                    {rabbitmq_auth_backend_ldap, [{servers, ["localhost"]},
                                                  {user_dn_pattern,    "cn=${username},ou=People,dc=rabbitmq,dc=com"},
                                                  {other_bind,         anon},
                                                  {use_ssl,            false},
                                                  {port,               LdapPort},
                                                  {log,                true},
                                                  {group_lookup_base,  "ou=groups,dc=rabbitmq,dc=com"},
                                                  {vhost_access_query, {exists, "ou=${vhost},ou=vhosts,dc=rabbitmq,dc=com"}},
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
                                                  {tag_queries, [{monitor,       {constant, true}},
                                                                 {administrator, {constant, false}},
                                                                 {management,    {constant, false}}]}
                                                ]}.

%%--------------------------------------------------------------------

all() ->
    [
      {group, non_parallel_tests}
    ].

groups() ->
    [
      {non_parallel_tests, [], [
                                ldap_only,
                                ldap_and_internal,
                                internal_followed_ldap_and_internal,
                                tag_attribution_ldap_only,
                                tag_attribution_ldap_and_internal,
                                tag_attribution_internal_followed_by_ldap_and_internal,
                                invalid_or_clause_ldap_only,
                                invalid_and_clause_ldap_only
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
    {LdapPort, _} = string:to_integer(os:getenv("LDAP_PORT", ?DEFAULT_LDAP_PORT)),
    Config2 = rabbit_ct_helpers:merge_app_env(Config1, ?BASE_CONF_RABBIT),
    Config3 = rabbit_ct_helpers:merge_app_env(Config2, base_conf_ldap(LdapPort)),
    Logon = {"localhost", LdapPort},
    rabbit_ldap_seed:delete(Logon),
    rabbit_ldap_seed:seed(Logon),
    Config4 = rabbit_ct_helpers:set_config(Config3, {ldap_port, LdapPort}),

    rabbit_ct_helpers:run_setup_steps(Config4,
      rabbit_ct_broker_helpers:setup_steps() ++
      rabbit_ct_client_helpers:setup_steps()).

end_per_suite(Config) ->
    rabbit_ldap_seed:delete({"localhost", ?config(ldap_port, Config)}),
    rabbit_ct_helpers:run_teardown_steps(Config,
      rabbit_ct_client_helpers:teardown_steps() ++
      rabbit_ct_broker_helpers:teardown_steps()).

init_per_group(_, Config) ->
    Config.

end_per_group(_, Config) ->
    Config.

init_internal(Config) ->
    ok = control_action(Config, add_user, [?ALICE_NAME, ""]),
    ok = control_action(Config, set_permissions, [?ALICE_NAME, "prefix-.*", "prefix-.*", "prefix-.*"]),
    ok = control_action(Config, set_user_tags, [?ALICE_NAME, "management", "foo"]),
    ok = control_action(Config, add_user, [?BOB_NAME, ""]),
    ok = control_action(Config, set_permissions, [?BOB_NAME, "", "", ""]),
    ok = control_action(Config, add_user, [?PETER_NAME, ""]),
    ok = control_action(Config, set_permissions, [?PETER_NAME, "", "", ""]).

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
end_per_testcase(Testcase, Config) ->
    rabbit_ct_helpers:testcase_finished(Config, Testcase).


%% -------------------------------------------------------------------
%% Testsuite cases
%% -------------------------------------------------------------------

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

%%--------------------------------------------------------------------

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
    case catch rabbit_control_main:action(
                 Command, Node, Args, Opts,
                 fun (Format, Args1) ->
                         io:format(Format ++ " ...~n", Args1)
                 end) of
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

