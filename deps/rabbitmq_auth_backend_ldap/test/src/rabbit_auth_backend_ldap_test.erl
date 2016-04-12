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

-module(rabbit_auth_backend_ldap_test).

-include_lib("eunit/include/eunit.hrl").
-include_lib("amqp_client/include/amqp_client.hrl").

-define(ALICE_NAME, "Alice").
-define(BOB_NAME, "Bob").
-define(CAROL_NAME, "Carol").
-define(PETER_NAME, "Peter").

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

%%--------------------------------------------------------------------

ldap_only_test_() ->
    { setup,
      fun () -> ok = application:set_env(rabbit, auth_backends,
          [rabbit_auth_backend_ldap]) end,
      fun (_) -> ok = application:unset_env(rabbit, auth_backends) end,
      [ {"LDAP Login", login()},
        {"LDAP In group", in_group()},
        {"LDAP Constant", const()},
        {"LDAP String match", string_match()},
        {"LDAP Boolean check", boolean_logic()},
        {"LDAP Tags", tag_check([monitor])}
    ]}.

ldap_and_internal_test_() ->
    { setup,
      fun () ->
          ok = application:set_env(rabbit, auth_backends,
              [{rabbit_auth_backend_ldap, rabbit_auth_backend_internal}]),
          ok = control_action(add_user, [?ALICE_NAME, ""]),
          ok = control_action(set_permissions, [?ALICE_NAME, "prefix-.*", "prefix-.*", "prefix-.*"]),
          ok = control_action(set_user_tags, [?ALICE_NAME, "management", "foo"]),
          ok = control_action(add_user, [?BOB_NAME, ""]),
          ok = control_action(set_permissions, [?BOB_NAME, "", "", ""]),
          ok = control_action(add_user, [?PETER_NAME, ""]),
          ok = control_action(set_permissions, [?PETER_NAME, "", "", ""])
      end,
      fun (_) ->
          ok = application:unset_env(rabbit, auth_backends),
          ok = control_action(delete_user, [?ALICE_NAME]),
          ok = control_action(delete_user, [?BOB_NAME]),
          ok = control_action(delete_user, [?PETER_NAME])
      end,
      [ {"LDAP&Internal Login", login()},
        {"LDAP&Internal Permissions", permission_match()},
        {"LDAP&Internal Tags", tag_check([monitor, management, foo])}
    ]}.

internal_followed_ldap_and_internal_test_() ->
    { setup,
      fun () ->
          ok = application:set_env(rabbit, auth_backends,
              [rabbit_auth_backend_internal, {rabbit_auth_backend_ldap, rabbit_auth_backend_internal}]),
          ok = control_action(add_user, [?ALICE_NAME, ""]),
          ok = control_action(set_permissions, [?ALICE_NAME, "prefix-.*", "prefix-.*", "prefix-.*"]),
          ok = control_action(set_user_tags, [?ALICE_NAME, "management", "foo"]),
          ok = control_action(add_user, [?BOB_NAME, ""]),
          ok = control_action(set_permissions, [?BOB_NAME, "", "", ""]),
          ok = control_action(add_user, [?PETER_NAME, ""]),
          ok = control_action(set_permissions, [?PETER_NAME, "", "", ""])
      end,
      fun (_) ->
          ok = application:unset_env(rabbit, auth_backends),
          ok = control_action(delete_user, [?ALICE_NAME]),
          ok = control_action(delete_user, [?BOB_NAME]),
          ok = control_action(delete_user, [?PETER_NAME])
      end,
      [ {"Internal, LDAP&Internal Login", login()},
        {"Internal, LDAP&Internal Permissions", permission_match()},
        {"Internal, LDAP&Internal Tags", tag_check([monitor, management, foo])}
    ]}.


%%--------------------------------------------------------------------

login() ->
    lists:flatten(
      [test_login({N, Env}, L, FilterList, case {LGood, EnvGood} of
                                               {good, good} -> fun succ/1;
                                               _            -> fun fail/1
                                           end) ||
          {LGood, FilterList, L}  <- logins(),
          {N, {EnvGood, Env}}     <- login_envs()]).

%% Format for login tests, {Outcome, FilterList, Login}.
%% Tests skipped for each login_env reference in FilterList.
logins() ->
    [{bad,  [5], #amqp_params_network{}},
     {bad,  [5], #amqp_params_network{username     = << ?ALICE_NAME >>}},
     {bad,  [5], #amqp_params_network{username     = << ?ALICE_NAME >>,
                                      password     = <<"password">>}},
     {bad,  [5], #amqp_params_network{username     = <<"Alice">>,
                                      password     = <<"Alicja">>,
                                      virtual_host = << ?VHOST >>}},
     {bad,  [1, 2, 3, 4, 6], ?CAROL},
     {good, [5], ?ALICE},
     {good, [5], ?BOB},
     {good, [1, 2, 3, 4, 6], ?PETER}].

%% Format for login envs, {Reference, {Outcome, Env}}
login_envs() ->
    [{1, {good, base_login_env()}},
     {2, {good, dn_lookup_pre_bind_env()}},
     {3, {good, other_bind_admin_env()}},
     {4, {good, other_bind_anon_env()}},
     {5, {good, posix_vhost_access_multiattr_env()}},
     {6, {bad,  other_bind_broken_env()}}].

base_login_env() ->
    [{user_dn_pattern,    "cn=${username},ou=People,dc=example,dc=com"},
     {dn_lookup_attribute, none},
     {dn_lookup_base,      none},
     {dn_lookup_bind,      as_user},
     {other_bind,          as_user},
     {vhost_access_query,  {exists, "ou=${vhost},ou=vhosts,dc=example,dc=com"}}].

%% TODO configure OpenLDAP to allow a dn_lookup_post_bind_env()
dn_lookup_pre_bind_env() ->
    [{user_dn_pattern,    "${username}"},
     {dn_lookup_attribute, "cn"},
     {dn_lookup_base,      "OU=People,DC=example,DC=com"},
     {dn_lookup_bind,      {"cn=admin,dc=example,dc=com", "admin"}}].

other_bind_admin_env() ->
    [{other_bind, {"cn=admin,dc=example,dc=com", "admin"}}].

other_bind_anon_env() ->
    [{other_bind, anon}].

other_bind_broken_env() ->
    [{other_bind, {"cn=admin,dc=example,dc=com", "admi"}}].

posix_vhost_access_multiattr_env() ->
  [{user_dn_pattern, "uid=${username},ou=People,dc=example,dc=com"},
   {vhost_access_query,
    {'and', [{exists, "ou=${vhost},ou=vhosts,dc=example,dc=com"},
             {equals,
              {attribute, "${user_dn}","memberOf"},
              {string, "cn=wheel,ou=groups,dc=example,dc=com"}},
             {equals,
              {attribute, "${user_dn}","memberOf"},
              {string, "cn=people,ou=groups,dc=example,dc=com"}},
             {equals,
              {string, "cn=wheel,ou=groups,dc=example,dc=com"},
              {attribute,"${user_dn}","memberOf"}},
             {equals,
              {string, "cn=people,ou=groups,dc=example,dc=com"},
              {attribute, "${user_dn}","memberOf"}},
             {match,
              {attribute, "${user_dn}","memberOf"},
              {string, "cn=wheel,ou=groups,dc=example,dc=com"}},
             {match,
              {attribute, "${user_dn}","memberOf"},
              {string, "cn=people,ou=groups,dc=example,dc=com"}},
             {match,
              {string, "cn=wheel,ou=groups,dc=example,dc=com"},
              {attribute, "${user_dn}","memberOf"}},
             {match,
              {string, "cn=people,ou=groups,dc=example,dc=com"},
              {attribute, "${user_dn}","memberOf"}}
            ]}}].

test_login({N, Env}, Login, FilterList, ResultFun) ->
    case lists:member(N, FilterList) of
        true -> [];
        _ ->
            ?_test(try
                       set_env(Env),
                       ResultFun(Login)
                   after
                       set_env(base_login_env())
                   end)
    end.

set_env(Env) ->
    [application:set_env(rabbitmq_auth_backend_ldap, K, V) || {K, V} <- Env].

succ(Login) -> ?assertMatch({ok, _}, amqp_connection:start(Login)).
fail(Login) -> ?assertMatch({error, _}, amqp_connection:start(Login)).

%%--------------------------------------------------------------------

in_group() ->
    X = [#'exchange.declare'{exchange = <<"test">>}],
    test_resource_funs([{?ALICE, X, ok},
                         {?BOB, X, fail}]).

const() ->
    Q = [#'queue.declare'{queue = <<"test">>}],
    test_resource_funs([{?ALICE, Q, ok},
                        {?BOB, Q, fail}]).

string_match() ->
    B = fun(N) ->
                [#'exchange.declare'{exchange = N},
                 #'queue.declare'{queue = <<"test">>},
                 #'queue.bind'{exchange = N, queue = <<"test">>}]
        end,
    test_resource_funs([{?ALICE, B(<<"xch-Alice-abc123">>), ok},
                        {?ALICE, B(<<"abc123">>),                     fail},
                        {?ALICE, B(<<"xch-Someone Else-abc123">>),    fail}]).

boolean_logic() ->
    Q1 = [#'queue.declare'{queue = <<"test1">>},
          #'basic.consume'{queue = <<"test1">>}],
    Q2 = [#'queue.declare'{queue = <<"test2">>},
          #'basic.consume'{queue = <<"test2">>}],
    [test_resource_fun(PTR) || PTR <- [{?ALICE, Q1, ok},
                                       {?ALICE, Q2, ok},
                                       {?BOB, Q1, fail},
                                       {?BOB, Q2, fail}]].

permission_match() ->
    B = fun(N) ->
                [#'exchange.declare'{exchange = N},
                 #'queue.declare'{queue = <<"prefix-test">>},
                 #'queue.bind'{exchange = N, queue = <<"prefix-test">>}]
        end,
    test_resource_funs([{?ALICE, B(<<"prefix-abc123">>),              ok},
                        {?ALICE, B(<<"abc123">>),                     fail},
                        {?ALICE, B(<<"xch-Alice-abc123">>), fail}]).

tag_check(Tags) ->
    fun() ->
            {ok, User} = rabbit_access_control:check_user_pass_login(
                        << ?ALICE_NAME >>, <<"password">>),
            ?assertEqual(Tags, User#user.tags)
    end.


%%--------------------------------------------------------------------

test_resource_funs(PTRs) -> [test_resource_fun(PTR) || PTR <- PTRs].

test_resource_fun({Person, Things, Result}) ->
    fun() ->
            {ok, Conn} = amqp_connection:start(Person),
            {ok, Ch} = amqp_connection:open_channel(Conn),
            ?assertEqual(Result,
                         try
                             [amqp_channel:call(Ch, T) || T <- Things],
                             amqp_connection:close(Conn),
                             ok
                         catch exit:_ -> fail
                         end)
    end.

control_action(Command, Args) ->
    control_action(Command, node(), Args, default_options()).

control_action(Command, Args, NewOpts) ->
    control_action(Command, node(), Args,
                   expand_options(default_options(), NewOpts)).

control_action(Command, Node, Args, Opts) ->
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
                            true -> R;
                            false -> [A | R]
                        end
                end, Bs, As).

