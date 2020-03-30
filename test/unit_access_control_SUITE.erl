%% The contents of this file are subject to the Mozilla Public License
%% Version 1.1 (the "License"); you may not use this file except in
%% compliance with the License. You may obtain a copy of the License at
%% https://www.mozilla.org/MPL/
%%
%% Software distributed under the License is distributed on an "AS IS"
%% basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See the
%% License for the specific language governing rights and limitations
%% under the License.
%%
%% The Original Code is RabbitMQ.
%%
%% The Initial Developer of the Original Code is GoPivotal, Inc.
%% Copyright (c) 2011-2020 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(unit_access_control_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("kernel/include/file.hrl").
-include_lib("amqp_client/include/amqp_client.hrl").
-include_lib("eunit/include/eunit.hrl").

-compile(export_all).

all() ->
    [
      {group, sequential_tests},
      {group, parallel_tests}
    ].

groups() ->
    [
      {parallel_tests, [parallel], [
          password_hashing,
          unsupported_connection_refusal
      ]},
      {sequential_tests, [], [
          login_with_credentials_but_no_password,
          login_of_passwordless_user,
          set_tags_for_passwordless_user,
          change_password,
          topic_matching,
          auth_backend_internal_expand_topic_permission,
          rabbit_direct_extract_extra_auth_props
      ]}
    ].

%% -------------------------------------------------------------------
%% Testsuite setup/teardown
%% -------------------------------------------------------------------

init_per_suite(Config) ->
    rabbit_ct_helpers:log_environment(),
    rabbit_ct_helpers:run_setup_steps(Config).

end_per_suite(Config) ->
    rabbit_ct_helpers:run_teardown_steps(Config).

init_per_group(Group, Config) ->
    Config1 = rabbit_ct_helpers:set_config(Config, [
        {rmq_nodename_suffix, Group},
        {rmq_nodes_count, 1}
      ]),
    rabbit_ct_helpers:run_steps(Config1,
      rabbit_ct_broker_helpers:setup_steps() ++
      rabbit_ct_client_helpers:setup_steps()).

end_per_group(_Group, Config) ->
    rabbit_ct_helpers:run_steps(Config,
      rabbit_ct_client_helpers:teardown_steps() ++
      rabbit_ct_broker_helpers:teardown_steps()).

init_per_testcase(Testcase, Config) ->
    rabbit_ct_helpers:testcase_started(Config, Testcase).

end_per_testcase(Testcase, Config) ->
    rabbit_ct_helpers:testcase_finished(Config, Testcase).

%% ---------------------------------------------------------------------------
%% Test Cases
%% ---------------------------------------------------------------------------

password_hashing(Config) ->
    passed = rabbit_ct_broker_helpers:rpc(Config, 0,
      ?MODULE, password_hashing1, [Config]).

password_hashing1(_Config) ->
    rabbit_password_hashing_sha256 = rabbit_password:hashing_mod(),
    application:set_env(rabbit, password_hashing_module,
                        rabbit_password_hashing_md5),
    rabbit_password_hashing_md5    = rabbit_password:hashing_mod(),
    application:set_env(rabbit, password_hashing_module,
                        rabbit_password_hashing_sha256),
    rabbit_password_hashing_sha256 = rabbit_password:hashing_mod(),

    rabbit_password_hashing_sha256 =
        rabbit_password:hashing_mod(rabbit_password_hashing_sha256),
    rabbit_password_hashing_md5    =
        rabbit_password:hashing_mod(rabbit_password_hashing_md5),
    rabbit_password_hashing_md5    =
        rabbit_password:hashing_mod(undefined),

    rabbit_password_hashing_md5    =
        rabbit_auth_backend_internal:hashing_module_for_user(
          #internal_user{}),
    rabbit_password_hashing_md5    =
        rabbit_auth_backend_internal:hashing_module_for_user(
          #internal_user{
             hashing_algorithm = undefined
            }),
    rabbit_password_hashing_md5    =
        rabbit_auth_backend_internal:hashing_module_for_user(
          #internal_user{
             hashing_algorithm = rabbit_password_hashing_md5
            }),

    rabbit_password_hashing_sha256 =
        rabbit_auth_backend_internal:hashing_module_for_user(
          #internal_user{
             hashing_algorithm = rabbit_password_hashing_sha256
            }),

    passed.

change_password(Config) ->
    passed = rabbit_ct_broker_helpers:rpc(Config, 0,
      ?MODULE, change_password1, [Config]).

change_password1(_Config) ->
    UserName = <<"test_user">>,
    Password = <<"test_password">>,
    case rabbit_auth_backend_internal:lookup_user(UserName) of
        {ok, _} -> rabbit_auth_backend_internal:delete_user(UserName, <<"acting-user">>);
        _       -> ok
    end,
    ok = application:set_env(rabbit, password_hashing_module,
                             rabbit_password_hashing_md5),
    ok = rabbit_auth_backend_internal:add_user(UserName, Password, <<"acting-user">>),
    {ok, #auth_user{username = UserName}} =
        rabbit_auth_backend_internal:user_login_authentication(
            UserName, [{password, Password}]),
    ok = application:set_env(rabbit, password_hashing_module,
                             rabbit_password_hashing_sha256),
    {ok, #auth_user{username = UserName}} =
        rabbit_auth_backend_internal:user_login_authentication(
            UserName, [{password, Password}]),

    NewPassword = <<"test_password1">>,
    ok = rabbit_auth_backend_internal:change_password(UserName, NewPassword,
                                                      <<"acting-user">>),
    {ok, #auth_user{username = UserName}} =
        rabbit_auth_backend_internal:user_login_authentication(
            UserName, [{password, NewPassword}]),

    {refused, _, [UserName]} =
        rabbit_auth_backend_internal:user_login_authentication(
            UserName, [{password, Password}]),
    passed.


login_with_credentials_but_no_password(Config) ->
    passed = rabbit_ct_broker_helpers:rpc(Config, 0,
      ?MODULE, login_with_credentials_but_no_password1, [Config]).

login_with_credentials_but_no_password1(_Config) ->
    Username = <<"login_with_credentials_but_no_password-user">>,
    Password = <<"login_with_credentials_but_no_password-password">>,
    ok = rabbit_auth_backend_internal:add_user(Username, Password, <<"acting-user">>),

    try
        rabbit_auth_backend_internal:user_login_authentication(Username,
                                                              [{key, <<"value">>}]),
        ?assert(false)
    catch exit:{unknown_auth_props, Username, [{key, <<"value">>}]} ->
            ok
    end,

    ok = rabbit_auth_backend_internal:delete_user(Username, <<"acting-user">>),

    passed.

%% passwordless users are not supposed to be used with
%% this backend (and PLAIN authentication mechanism in general)
login_of_passwordless_user(Config) ->
    passed = rabbit_ct_broker_helpers:rpc(Config, 0,
      ?MODULE, login_of_passwordless_user1, [Config]).

login_of_passwordless_user1(_Config) ->
    Username = <<"login_of_passwordless_user-user">>,
    Password = <<"">>,
    ok = rabbit_auth_backend_internal:add_user(Username, Password, <<"acting-user">>),

    ?assertMatch(
       {refused, _Message, [Username]},
       rabbit_auth_backend_internal:user_login_authentication(Username,
                                                              [{password, <<"">>}])),

    ?assertMatch(
       {refused, _Format, [Username]},
       rabbit_auth_backend_internal:user_login_authentication(Username,
                                                              [{password, ""}])),

    ok = rabbit_auth_backend_internal:delete_user(Username, <<"acting-user">>),

    passed.


set_tags_for_passwordless_user(Config) ->
    passed = rabbit_ct_broker_helpers:rpc(Config, 0,
      ?MODULE, set_tags_for_passwordless_user1, [Config]).

set_tags_for_passwordless_user1(_Config) ->
    Username = <<"set_tags_for_passwordless_user">>,
    Password = <<"set_tags_for_passwordless_user">>,
    ok = rabbit_auth_backend_internal:add_user(Username, Password,
                                               <<"acting-user">>),
    ok = rabbit_auth_backend_internal:clear_password(Username,
                                                     <<"acting-user">>),
    ok = rabbit_auth_backend_internal:set_tags(Username, [management],
                                               <<"acting-user">>),

    ?assertMatch(
       {ok, #internal_user{tags = [management]}},
       rabbit_auth_backend_internal:lookup_user(Username)),

    ok = rabbit_auth_backend_internal:set_tags(Username, [management, policymaker],
                                               <<"acting-user">>),

    ?assertMatch(
       {ok, #internal_user{tags = [management, policymaker]}},
       rabbit_auth_backend_internal:lookup_user(Username)),

    ok = rabbit_auth_backend_internal:set_tags(Username, [],
                                               <<"acting-user">>),

    ?assertMatch(
       {ok, #internal_user{tags = []}},
       rabbit_auth_backend_internal:lookup_user(Username)),

    ok = rabbit_auth_backend_internal:delete_user(Username,
                                                  <<"acting-user">>),

    passed.


rabbit_direct_extract_extra_auth_props(_Config) ->
    {ok, CSC} = code_server_cache:start_link(),
    % no protocol to extract
    [] = rabbit_direct:extract_extra_auth_props(
        {<<"guest">>, <<"guest">>}, <<"/">>, 1,
        [{name,<<"127.0.0.1:52366 -> 127.0.0.1:1883">>}]),
    % protocol to extract, but no module to call
    [] = rabbit_direct:extract_extra_auth_props(
        {<<"guest">>, <<"guest">>}, <<"/">>, 1,
        [{protocol, {'PROTOCOL_WITHOUT_MODULE', "1.0"}}]),
    % see rabbit_dummy_protocol_connection_info module
    % protocol to extract, module that returns a client ID
    [{client_id, <<"DummyClientId">>}] = rabbit_direct:extract_extra_auth_props(
        {<<"guest">>, <<"guest">>}, <<"/">>, 1,
        [{protocol, {'DUMMY_PROTOCOL', "1.0"}}]),
    % protocol to extract, but error thrown in module
    [] = rabbit_direct:extract_extra_auth_props(
        {<<"guest">>, <<"guest">>}, <<"/">>, -1,
        [{protocol, {'DUMMY_PROTOCOL', "1.0"}}]),
    gen_server:stop(CSC),
    ok.

auth_backend_internal_expand_topic_permission(_Config) ->
    ExpandMap = #{<<"username">> => <<"guest">>, <<"vhost">> => <<"default">>},
    %% simple case
    <<"services/default/accounts/guest/notifications">> =
        rabbit_auth_backend_internal:expand_topic_permission(
            <<"services/{vhost}/accounts/{username}/notifications">>,
            ExpandMap
        ),
    %% replace variable twice
    <<"services/default/accounts/default/guest/notifications">> =
        rabbit_auth_backend_internal:expand_topic_permission(
            <<"services/{vhost}/accounts/{vhost}/{username}/notifications">>,
            ExpandMap
        ),
    %% nothing to replace
    <<"services/accounts/notifications">> =
        rabbit_auth_backend_internal:expand_topic_permission(
            <<"services/accounts/notifications">>,
            ExpandMap
        ),
    %% the expand map isn't defined
    <<"services/{vhost}/accounts/{username}/notifications">> =
        rabbit_auth_backend_internal:expand_topic_permission(
            <<"services/{vhost}/accounts/{username}/notifications">>,
            undefined
        ),
    %% the expand map is empty
    <<"services/{vhost}/accounts/{username}/notifications">> =
        rabbit_auth_backend_internal:expand_topic_permission(
            <<"services/{vhost}/accounts/{username}/notifications">>,
            #{}
        ),
    ok.

unsupported_connection_refusal(Config) ->
    passed = rabbit_ct_broker_helpers:rpc(Config, 0,
      ?MODULE, unsupported_connection_refusal1, [Config]).

unsupported_connection_refusal1(Config) ->
    H = ?config(rmq_hostname, Config),
    P = rabbit_ct_broker_helpers:get_node_config(Config, 0, tcp_port_amqp),
    [passed = test_unsupported_connection_refusal(H, P, V) ||
        V <- [<<"AMQP",9,9,9,9>>, <<"AMQP",0,1,0,0>>, <<"XXXX",0,0,9,1>>]],
    passed.

test_unsupported_connection_refusal(H, P, Header) ->
    {ok, C} = gen_tcp:connect(H, P, [binary, {active, false}]),
    ok = gen_tcp:send(C, Header),
    {ok, <<"AMQP",0,0,9,1>>} = gen_tcp:recv(C, 8, 100),
    ok = gen_tcp:close(C),
    passed.


%% -------------------------------------------------------------------
%% Topic matching.
%% -------------------------------------------------------------------

topic_matching(Config) ->
    passed = rabbit_ct_broker_helpers:rpc(Config, 0,
      ?MODULE, topic_matching1, [Config]).

topic_matching1(_Config) ->
    XName = #resource{virtual_host = <<"/">>,
                      kind = exchange,
                      name = <<"topic_matching-exchange">>},
    X0 = #exchange{name = XName, type = topic, durable = false,
                   auto_delete = false, arguments = []},
    X = rabbit_exchange_decorator:set(X0),
    %% create
    rabbit_exchange_type_topic:validate(X),
    exchange_op_callback(X, create, []),

    %% add some bindings
    Bindings = [#binding{source = XName,
                         key = list_to_binary(Key),
                         destination = #resource{virtual_host = <<"/">>,
                                                 kind = queue,
                                                 name = list_to_binary(Q)},
                         args = Args} ||
                   {Key, Q, Args} <- [{"a.b.c",         "t1",  []},
                                      {"a.*.c",         "t2",  []},
                                      {"a.#.b",         "t3",  []},
                                      {"a.b.b.c",       "t4",  []},
                                      {"#",             "t5",  []},
                                      {"#.#",           "t6",  []},
                                      {"#.b",           "t7",  []},
                                      {"*.*",           "t8",  []},
                                      {"a.*",           "t9",  []},
                                      {"*.b.c",         "t10", []},
                                      {"a.#",           "t11", []},
                                      {"a.#.#",         "t12", []},
                                      {"b.b.c",         "t13", []},
                                      {"a.b.b",         "t14", []},
                                      {"a.b",           "t15", []},
                                      {"b.c",           "t16", []},
                                      {"",              "t17", []},
                                      {"*.*.*",         "t18", []},
                                      {"vodka.martini", "t19", []},
                                      {"a.b.c",         "t20", []},
                                      {"*.#",           "t21", []},
                                      {"#.*.#",         "t22", []},
                                      {"*.#.#",         "t23", []},
                                      {"#.#.#",         "t24", []},
                                      {"*",             "t25", []},
                                      {"#.b.#",         "t26", []},
                                      {"args-test",     "t27",
                                       [{<<"foo">>, longstr, <<"bar">>}]},
                                      {"args-test",     "t27", %% Note aliasing
                                       [{<<"foo">>, longstr, <<"baz">>}]}]],
    lists:foreach(fun (B) -> exchange_op_callback(X, add_binding, [B]) end,
                  Bindings),

    %% test some matches
    test_topic_expect_match(
      X, [{"a.b.c",               ["t1", "t2", "t5", "t6", "t10", "t11", "t12",
                                   "t18", "t20", "t21", "t22", "t23", "t24",
                                   "t26"]},
          {"a.b",                 ["t3", "t5", "t6", "t7", "t8", "t9", "t11",
                                   "t12", "t15", "t21", "t22", "t23", "t24",
                                   "t26"]},
          {"a.b.b",               ["t3", "t5", "t6", "t7", "t11", "t12", "t14",
                                   "t18", "t21", "t22", "t23", "t24", "t26"]},
          {"",                    ["t5", "t6", "t17", "t24"]},
          {"b.c.c",               ["t5", "t6", "t18", "t21", "t22", "t23",
                                   "t24", "t26"]},
          {"a.a.a.a.a",           ["t5", "t6", "t11", "t12", "t21", "t22",
                                   "t23", "t24"]},
          {"vodka.gin",           ["t5", "t6", "t8", "t21", "t22", "t23",
                                   "t24"]},
          {"vodka.martini",       ["t5", "t6", "t8", "t19", "t21", "t22", "t23",
                                   "t24"]},
          {"b.b.c",               ["t5", "t6", "t10", "t13", "t18", "t21",
                                   "t22", "t23", "t24", "t26"]},
          {"nothing.here.at.all", ["t5", "t6", "t21", "t22", "t23", "t24"]},
          {"oneword",             ["t5", "t6", "t21", "t22", "t23", "t24",
                                   "t25"]},
          {"args-test",           ["t5", "t6", "t21", "t22", "t23", "t24",
                                   "t25", "t27"]}]),
    %% remove some bindings
    RemovedBindings = [lists:nth(1, Bindings), lists:nth(5, Bindings),
                       lists:nth(11, Bindings), lists:nth(19, Bindings),
                       lists:nth(21, Bindings), lists:nth(28, Bindings)],
    exchange_op_callback(X, remove_bindings, [RemovedBindings]),
    RemainingBindings = ordsets:to_list(
                          ordsets:subtract(ordsets:from_list(Bindings),
                                           ordsets:from_list(RemovedBindings))),

    %% test some matches
    test_topic_expect_match(
      X,
      [{"a.b.c",               ["t2", "t6", "t10", "t12", "t18", "t20", "t22",
                                "t23", "t24", "t26"]},
       {"a.b",                 ["t3", "t6", "t7", "t8", "t9", "t12", "t15",
                                "t22", "t23", "t24", "t26"]},
       {"a.b.b",               ["t3", "t6", "t7", "t12", "t14", "t18", "t22",
                                "t23", "t24", "t26"]},
       {"",                    ["t6", "t17", "t24"]},
       {"b.c.c",               ["t6", "t18", "t22", "t23", "t24", "t26"]},
       {"a.a.a.a.a",           ["t6", "t12", "t22", "t23", "t24"]},
       {"vodka.gin",           ["t6", "t8", "t22", "t23", "t24"]},
       {"vodka.martini",       ["t6", "t8", "t22", "t23", "t24"]},
       {"b.b.c",               ["t6", "t10", "t13", "t18", "t22", "t23",
                                "t24", "t26"]},
       {"nothing.here.at.all", ["t6", "t22", "t23", "t24"]},
       {"oneword",             ["t6", "t22", "t23", "t24", "t25"]},
       {"args-test",           ["t6", "t22", "t23", "t24", "t25", "t27"]}]),

    %% remove the entire exchange
    exchange_op_callback(X, delete, [RemainingBindings]),
    %% none should match now
    test_topic_expect_match(X, [{"a.b.c", []}, {"b.b.c", []}, {"", []}]),
    passed.

exchange_op_callback(X, Fun, Args) ->
    rabbit_misc:execute_mnesia_transaction(
      fun () -> rabbit_exchange:callback(X, Fun, transaction, [X] ++ Args) end),
    rabbit_exchange:callback(X, Fun, none, [X] ++ Args).

test_topic_expect_match(X, List) ->
    lists:foreach(
      fun ({Key, Expected}) ->
              BinKey = list_to_binary(Key),
              Message = rabbit_basic:message(X#exchange.name, BinKey,
                                             #'P_basic'{}, <<>>),
              Res = rabbit_exchange_type_topic:route(
                      X, #delivery{mandatory = false,
                                   sender    = self(),
                                   message   = Message}),
              ExpectedRes = lists:map(
                              fun (Q) -> #resource{virtual_host = <<"/">>,
                                                   kind = queue,
                                                   name = list_to_binary(Q)}
                              end, Expected),
              true = (lists:usort(ExpectedRes) =:= lists:usort(Res))
      end, List).
