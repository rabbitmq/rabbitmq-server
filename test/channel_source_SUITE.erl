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
%% Copyright (c) 2007-2019 Pivotal Software, Inc.  All rights reserved.
%%

-module(channel_source_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("amqp_client/include/amqp_client.hrl").

-compile(export_all).

all() ->
    [
      {group, non_parallel_tests}
    ].

groups() ->
    [
      {non_parallel_tests, [], [
          network_channel_source_notifications,
          direct_channel_source_notifications,
          undefined_channel_source_notifications
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
    rabbit_ct_helpers:testcase_started(Config, Testcase),
    Config1 = rabbit_ct_helpers:set_config(Config, [
        {rmq_nodename_suffix, Testcase}
      ]),
    rabbit_ct_helpers:run_steps(Config1,
      rabbit_ct_broker_helpers:setup_steps() ++
      rabbit_ct_client_helpers:setup_steps()).

end_per_testcase(Testcase, Config) ->
    Config1 = rabbit_ct_helpers:run_steps(Config,
      rabbit_ct_client_helpers:teardown_steps() ++
      rabbit_ct_broker_helpers:teardown_steps()),
    rabbit_ct_helpers:testcase_finished(Config1, Testcase).

%% -------------------------------------------------------------------
%% Testcases.
%% -------------------------------------------------------------------

network_channel_source_notifications(Config) ->
    passed = rabbit_ct_broker_helpers:rpc(Config, 0,
      ?MODULE, network_channel_source_notifications1, [Config]).

network_channel_source_notifications1(Config) ->
    ExistingChannels = rabbit_channel:list(),
    Conn = rabbit_ct_client_helpers:open_unmanaged_connection(Config),
    {ok, _ClientCh} = amqp_connection:open_channel(Conn),
    [ServerCh] = rabbit_channel:list() -- ExistingChannels,
    [{channel_source, rabbit_reader}] =
        rabbit_channel:info(ServerCh, [channel_source]),
    rabbit_channel:source(ServerCh, ?MODULE),
    [{channel_source, ?MODULE}] =
        rabbit_channel:info(ServerCh, [channel_source]),
    amqp_connection:close(Conn),
    {error, channel_terminated} = rabbit_channel:source(ServerCh, ?MODULE),
    passed.

direct_channel_source_notifications(Config) ->
    passed = rabbit_ct_broker_helpers:rpc(Config, 0,
      ?MODULE, direct_channel_source_notifications1, [Config]).

direct_channel_source_notifications1(Config) ->
    ExistingChannels = rabbit_channel:list(),
    Conn = rabbit_ct_client_helpers:open_unmanaged_connection_direct(Config),
    {ok, _ClientCh} = amqp_connection:open_channel(Conn),
    [ServerCh] = rabbit_channel:list() -- ExistingChannels,
    [{channel_source, rabbit_direct}] =
        rabbit_channel:info(ServerCh, [channel_source]),
    rabbit_channel:source(ServerCh, ?MODULE),
    [{channel_source, ?MODULE}] =
        rabbit_channel:info(ServerCh, [channel_source]),
    amqp_connection:close(Conn),
    {error, channel_terminated} = rabbit_channel:source(ServerCh, ?MODULE),
    passed.

undefined_channel_source_notifications(Config) ->
    passed = rabbit_ct_broker_helpers:rpc(Config, 0,
      ?MODULE, undefined_channel_source_notifications1, [Config]).

undefined_channel_source_notifications1(_Config) ->
    ExistingChannels = rabbit_channel:list(),
    {_Writer, _Limiter, ServerCh} = rabbit_ct_broker_helpers:test_channel(),
    [ServerCh] = rabbit_channel:list() -- ExistingChannels,
    [{channel_source, undefined}] =
        rabbit_channel:info(ServerCh, [channel_source]),
    rabbit_channel:source(ServerCh, ?MODULE),
    [{channel_source, ?MODULE}] =
        rabbit_channel:info(ServerCh, [channel_source]),
    passed.
