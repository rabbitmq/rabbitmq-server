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
%% The Original Code is RabbitMQ.
%%
%% The Initial Developer of the Original Code is GoPivotal, Inc.
%% Copyright (c) 2007-2015 Pivotal Software, Inc.  All rights reserved.
%%

-module(rabbit_ctl_timeout_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("rabbit_common/include/rabbit.hrl").

-compile(export_all).

-define(TIMEOUT_LIST_OPS_PASS, 1000).

all() ->
    [
      list_operations_timeout_pass
    ].

%% -------------------------------------------------------------------
%% Testsuite setup/teardown.
%% -------------------------------------------------------------------

init_per_suite(Config) ->
    rabbit_ct_helpers:log_environment(),
    rabbit_ct_helpers:run_setup_steps(?MODULE, Config).

end_per_suite(Config) ->
    rabbit_ct_helpers:run_teardown_steps(Config).

%% -------------------------------------------------------------------
%% Test cases.
%% -------------------------------------------------------------------

list_operations_timeout_pass(Config) ->
    passed = rabbit_ct_broker_helpers:run_test_on_broker(
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
