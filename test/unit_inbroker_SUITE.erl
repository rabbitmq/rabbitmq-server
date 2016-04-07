%% The contents of this file are subject to the Mozilla Public License
%% Version 1.1 (the "License"); you may not use this file except in
%% compliance with the License. You may obtain a copy of the License at
%% http://www.mozilla.org/MPL/
%%
%% Software distributed under the License is distributed on an "AS IS"
%% basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See the
%% License for the specific language governing rights and limitations
%% under the License.
%%
%% The Original Code is RabbitMQ.
%%
%% The Initial Developer of the Original Code is GoPivotal, Inc.
%% Copyright (c) 2011-2016 Pivotal Software, Inc.  All rights reserved.
%%

-module(unit_inbroker_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("rabbit_common/include/rabbit.hrl").

-compile(export_all).

-define(TIMEOUT_LIST_OPS_PASS, 1000).

all() ->
    [
      {group, parallel_tests}
    ].

groups() ->
    [
      {parallel_tests, [parallel], [
          {credit_flow, [parallel], [
              credit_flow_settings
            ]},
          {password_hashing, [parallel], [
              password_hashing,
              change_password
            ]},
          {rabbitmqctl, [parallel], [
              list_operations_timeout_pass
            ]}
        ]}
    ].

%% -------------------------------------------------------------------
%% Testsuite setup/teardown.
%% -------------------------------------------------------------------

init_per_suite(Config) ->
    rabbit_ct_helpers:log_environment(),
    rabbit_ct_helpers:run_setup_steps(?MODULE, Config).

end_per_suite(Config) ->
    rabbit_ct_helpers:run_teardown_steps(Config).

init_per_group(_, Config) -> Config.
end_per_group(_, Config) -> Config.

%% ---------------------------------------------------------------------------
%% Credit flow.
%% ---------------------------------------------------------------------------

credit_flow_settings(Config) ->
    passed = rabbit_ct_broker_helpers:run_on_broker(
      ?config(rmq_nodename, Config),
      ?MODULE, credit_flow_settings1, [Config]).

credit_flow_settings1(_Config) ->
    %% default values
    passed = test_proc(200, 50),

    application:set_env(rabbit, credit_flow_default_credit, {100, 20}),
    passed = test_proc(100, 20),

    application:unset_env(rabbit, credit_flow_default_credit),

    % back to defaults
    passed = test_proc(200, 50),
    passed.

test_proc(InitialCredit, MoreCreditAfter) ->
    Pid = spawn(fun dummy/0),
    Pid ! {credit, self()},
    {InitialCredit, MoreCreditAfter} =
        receive
            {credit, Val} -> Val
        end,
    passed.

dummy() ->
    credit_flow:send(self()),
    receive
        {credit, From} ->
            From ! {credit, get(credit_flow_default_credit)};
        _      ->
            dummy()
    end.

%% ---------------------------------------------------------------------------
%% Password hashing.
%% ---------------------------------------------------------------------------

password_hashing(Config) ->
    passed = rabbit_ct_broker_helpers:run_on_broker(
      ?config(rmq_nodename, Config),
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
    passed = rabbit_ct_broker_helpers:run_on_broker(
      ?config(rmq_nodename, Config),
      ?MODULE, change_password1, [Config]).

change_password1(_Config) ->
    UserName = <<"test_user">>,
    Password = <<"test_password">>,
    case rabbit_auth_backend_internal:lookup_user(UserName) of
        {ok, _} -> rabbit_auth_backend_internal:delete_user(UserName);
        _       -> ok
    end,
    ok = application:set_env(rabbit, password_hashing_module,
                             rabbit_password_hashing_md5),
    ok = rabbit_auth_backend_internal:add_user(UserName, Password),
    {ok, #auth_user{username = UserName}} =
        rabbit_auth_backend_internal:user_login_authentication(
            UserName, [{password, Password}]),
    ok = application:set_env(rabbit, password_hashing_module,
                             rabbit_password_hashing_sha256),
    {ok, #auth_user{username = UserName}} =
        rabbit_auth_backend_internal:user_login_authentication(
            UserName, [{password, Password}]),

    NewPassword = <<"test_password1">>,
    ok = rabbit_auth_backend_internal:change_password(UserName, NewPassword),
    {ok, #auth_user{username = UserName}} =
        rabbit_auth_backend_internal:user_login_authentication(
            UserName, [{password, NewPassword}]),

    {refused, _, [UserName]} =
        rabbit_auth_backend_internal:user_login_authentication(
            UserName, [{password, Password}]),
    passed.

%% -------------------------------------------------------------------
%% rabbitmqctl.
%% -------------------------------------------------------------------

list_operations_timeout_pass(Config) ->
    passed = rabbit_ct_broker_helpers:run_on_broker(
      ?config(rmq_nodename, Config),
      ?MODULE, list_operations_timeout_pass1, [Config]).

list_operations_timeout_pass1(_Config) ->
    %% create a few things so there is some useful information to list
    {_Writer1, Limiter1, Ch1} = rabbit_ct_broker_helpers:test_channel(),
    {_Writer2, Limiter2, Ch2} = rabbit_ct_broker_helpers:test_channel(),

    [Q, Q2] = [Queue || Name <- [<<"foo">>, <<"bar">>],
                        {new, Queue = #amqqueue{}} <-
                            [rabbit_amqqueue:declare(
                               rabbit_misc:r(<<"/">>, queue, Name),
                               false, false, [], none)]],

    ok = rabbit_amqqueue:basic_consume(
           Q, true, Ch1, Limiter1, false, 0, <<"ctag1">>, true, [],
           undefined),
    ok = rabbit_amqqueue:basic_consume(
           Q2, true, Ch2, Limiter2, false, 0, <<"ctag2">>, true, [],
           undefined),

    %% list users
    ok = rabbit_ct_helpers:control_action(add_user, ["foo", "bar"]),
    {error, {user_already_exists, _}} =
        rabbit_ct_helpers:control_action(add_user, ["foo", "bar"]),
    ok = rabbit_ct_helpers:control_action_t(list_users, [],
                                            ?TIMEOUT_LIST_OPS_PASS),

    %% list parameters
    ok = rabbit_runtime_parameters_test:register(),
    ok = rabbit_ct_helpers:control_action(set_parameter,
                                          ["test", "good", "123"]),
    ok = rabbit_ct_helpers:control_action_t(list_parameters, [],
                                            ?TIMEOUT_LIST_OPS_PASS),
    ok = rabbit_ct_helpers:control_action(clear_parameter,
                                          ["test", "good"]),
    rabbit_runtime_parameters_test:unregister(),

    %% list vhosts
    ok = rabbit_ct_helpers:control_action(add_vhost, ["/testhost"]),
    {error, {vhost_already_exists, _}} =
        rabbit_ct_helpers:control_action(add_vhost, ["/testhost"]),
    ok = rabbit_ct_helpers:control_action_t(list_vhosts, [],
                                            ?TIMEOUT_LIST_OPS_PASS),

    %% list permissions
    ok = rabbit_ct_helpers:control_action(set_permissions,
                                          ["foo", ".*", ".*", ".*"],
                                          [{"-p", "/testhost"}]),
    ok = rabbit_ct_helpers:control_action_t(list_permissions, [],
                                            [{"-p", "/testhost"}],
                                            ?TIMEOUT_LIST_OPS_PASS),

    %% list user permissions
    ok = rabbit_ct_helpers:control_action_t(list_user_permissions, ["foo"],
                                            ?TIMEOUT_LIST_OPS_PASS),

    %% list policies
    ok = rabbit_ct_helpers:control_action_opts(["set_policy", "name", ".*",
                                                "{\"ha-mode\":\"all\"}"]),
    ok = rabbit_ct_helpers:control_action_t(list_policies, [],
                                            ?TIMEOUT_LIST_OPS_PASS),
    ok = rabbit_ct_helpers:control_action(clear_policy, ["name"]),

    %% list queues
    ok = rabbit_ct_helpers:info_action_t(list_queues,
                                         rabbit_amqqueue:info_keys(), false,
                                         ?TIMEOUT_LIST_OPS_PASS),

    %% list exchanges
    ok = rabbit_ct_helpers:info_action_t(list_exchanges,
                                         rabbit_exchange:info_keys(), true,
                                         ?TIMEOUT_LIST_OPS_PASS),

    %% list bindings
    ok = rabbit_ct_helpers:info_action_t(list_bindings,
                                         rabbit_binding:info_keys(), true,
                                         ?TIMEOUT_LIST_OPS_PASS),

    %% list connections
    {H, P} = rabbit_ct_broker_helpers:find_listener(),
    {ok, C1} = gen_tcp:connect(H, P, [binary, {active, false}]),
    gen_tcp:send(C1, <<"AMQP", 0, 0, 9, 1>>),
    {ok, <<1,0,0>>} = gen_tcp:recv(C1, 3, 100),

    {ok, C2} = gen_tcp:connect(H, P, [binary, {active, false}]),
    gen_tcp:send(C2, <<"AMQP", 0, 0, 9, 1>>),
    {ok, <<1,0,0>>} = gen_tcp:recv(C2, 3, 100),

    ok = rabbit_ct_helpers:info_action_t(
      list_connections, rabbit_networking:connection_info_keys(), false,
      ?TIMEOUT_LIST_OPS_PASS),

    %% list consumers
    ok = rabbit_ct_helpers:info_action_t(
      list_consumers, rabbit_amqqueue:consumer_info_keys(), false,
      ?TIMEOUT_LIST_OPS_PASS),

    %% list channels
    ok = rabbit_ct_helpers:info_action_t(
      list_channels, rabbit_channel:info_keys(), false,
      ?TIMEOUT_LIST_OPS_PASS),

    %% do some cleaning up
    ok = rabbit_ct_helpers:control_action(delete_user, ["foo"]),
    {error, {no_such_user, _}} =
        rabbit_ct_helpers:control_action(delete_user, ["foo"]),

    ok = rabbit_ct_helpers:control_action(delete_vhost, ["/testhost"]),
    {error, {no_such_vhost, _}} =
        rabbit_ct_helpers:control_action(delete_vhost, ["/testhost"]),

    %% close_connection
    Conns = rabbit_networking:connections(),
    [ok = rabbit_ct_helpers:control_action(
        close_connection, [rabbit_misc:pid_to_string(ConnPid), "go away"])
     || ConnPid <- Conns],

    %% cleanup queues
    [{ok, _} = rabbit_amqqueue:delete(QR, false, false) || QR <- [Q, Q2]],

    [begin
         unlink(Chan),
         ok = rabbit_channel:shutdown(Chan)
     end || Chan <- [Ch1, Ch2]],
    passed.
