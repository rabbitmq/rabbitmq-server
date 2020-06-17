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
%% Copyright (c) 2019-2020 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(unit_access_control_authn_authz_context_propagation_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("amqp_client/include/amqp_client.hrl").

-compile(export_all).

all() ->
    [
      {group, non_parallel_tests}
    ].

groups() ->
    [
      {non_parallel_tests, [], [
          propagate_context_to_auth_backend
        ]}
    ].

%% -------------------------------------------------------------------
%% Testsuite setup/teardown.
%% -------------------------------------------------------------------

init_per_suite(Config) ->
    rabbit_ct_helpers:log_environment(),
    rabbit_ct_helpers:run_setup_steps(Config).

end_per_suite(Config) ->
    rabbit_ct_helpers:run_teardown_steps(Config).

init_per_group(_, Config) ->
    Config.

end_per_group(_, Config) ->
    Config.

init_per_testcase(Testcase, Config) ->
    AuthConfig = {rabbit, [
      {auth_backends, [rabbit_auth_backend_context_propagation_mock]}
    ]
    },
    rabbit_ct_helpers:testcase_started(Config, Testcase),
    Config1 = rabbit_ct_helpers:set_config(Config, [
        {rmq_nodename_suffix, Testcase}
      ]),
    rabbit_ct_helpers:run_setup_steps(Config1,
      [ fun(Conf) -> merge_app_env(AuthConfig, Conf) end ] ++
      rabbit_ct_broker_helpers:setup_steps() ++
      rabbit_ct_client_helpers:setup_steps()).

merge_app_env(SomeConfig, Config) ->
  rabbit_ct_helpers:merge_app_env(Config, SomeConfig).

end_per_testcase(Testcase, Config) ->
    Config1 = rabbit_ct_helpers:run_steps(Config,
      rabbit_ct_client_helpers:teardown_steps() ++
      rabbit_ct_broker_helpers:teardown_steps()),
    rabbit_ct_helpers:testcase_finished(Config1, Testcase).

%% -------------------------------------------------------------------
%% Test cases
%% -------------------------------------------------------------------

propagate_context_to_auth_backend(Config) ->
  ok = rabbit_ct_broker_helpers:add_code_path_to_all_nodes(Config,
    rabbit_auth_backend_context_propagation_mock),
  passed = rabbit_ct_broker_helpers:rpc(Config, 0,
    ?MODULE, propagate_context_to_auth_backend1, []).

propagate_context_to_auth_backend1() ->
  rabbit_auth_backend_context_propagation_mock:init(),
  AmqpParams = #amqp_params_direct{
    virtual_host = <<"/">>,
    username = <<"guest">>,
    password = <<"guest">>,
    adapter_info = #amqp_adapter_info{additional_info = [
        {variable_map, #{<<"key1">> => <<"value1">>}}
      ],
      protocol = {'FOO_PROTOCOL', '1.0'} %% this will trigger a call to rabbit_foo_protocol_connection_info
    }
  },
  {ok, Conn} = amqp_connection:start(AmqpParams),

  %% rabbit_direct will call the rabbit_foo_protocol_connection_info module to extract information
  %% this information will be propagated to the authentication backend
  [{authentication, AuthProps}] = rabbit_auth_backend_context_propagation_mock:get(authentication),
  ?assertEqual(<<"value1">>, proplists:get_value(key1, AuthProps)),

  %% variable_map is propagated from rabbit_direct to the authorization backend
  [{vhost_access, AuthzData}] = rabbit_auth_backend_context_propagation_mock:get(vhost_access),
  ?assertEqual(<<"value1">>, maps:get(<<"key1">>, AuthzData)),

  %% variable_map is extracted when the channel is created and kept in its state
  {ok, Ch} = amqp_connection:open_channel(Conn),
  QName = <<"channel_propagate_context_to_authz_backend-q">>,
  amqp_channel:call(Ch, #'queue.declare'{queue = QName}),

  check_send_receive(Ch, <<"">>, QName, QName),
  amqp_channel:call(Ch, #'queue.bind'{queue = QName, exchange = <<"amq.topic">>, routing_key = <<"a.b">>}),
  %% variable_map content is propagated from rabbit_channel to the authorization backend (resource check)
  [{resource_access, AuthzContext}] = rabbit_auth_backend_context_propagation_mock:get(resource_access),
  ?assertEqual(<<"value1">>, maps:get(<<"key1">>, AuthzContext)),

  check_send_receive(Ch, <<"amq.topic">>, <<"a.b">>, QName),
  %% variable_map is propagated from rabbit_channel to the authorization backend (topic check)
  [{topic_access, TopicContext}] = rabbit_auth_backend_context_propagation_mock:get(topic_access),
  VariableMap = maps:get(variable_map, TopicContext),
  ?assertEqual(<<"value1">>, maps:get(<<"key1">>, VariableMap)),

  passed.

check_send_receive(Ch, Exchange, RoutingKey, QName) ->
  amqp_channel:call(Ch,
    #'basic.publish'{exchange = Exchange, routing_key = RoutingKey},
    #amqp_msg{payload = <<"foo">>}),

  {#'basic.get_ok'{}, #amqp_msg{payload = <<"foo">>}} =
    amqp_channel:call(Ch, #'basic.get'{queue = QName,
      no_ack = true}).
