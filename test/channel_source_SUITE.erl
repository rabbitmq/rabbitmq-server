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

-include_lib("amqp_client/include/amqp_client.hrl").

-compile(export_all).

all() ->
    [
      {group, non_parallel_tests}
    ].

groups() ->
    [
      {non_parallel_tests, [], [
          network_rabbit_reader_channel_source,
          network_arbitrary_channel_source,
          direct_channel_source,
          undefined_channel_source
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

network_rabbit_reader_channel_source(Config) ->
    passed = rabbit_ct_broker_helpers:rpc(Config, 0,
      ?MODULE, network_rabbit_reader_channel_source1, [Config]).

network_rabbit_reader_channel_source1(Config) ->
    ExistingChannels = rabbit_channel:list(),
    Conn = rabbit_ct_client_helpers:open_unmanaged_connection(Config),
    {ok, ClientCh} = amqp_connection:open_channel(Conn),
    [ServerCh] = rabbit_channel:list() -- ExistingChannels,
    [{source, rabbit_reader}] = rabbit_channel:info(ServerCh, [source]),
    _ = rabbit_channel:source(ServerCh, ?MODULE),
    [{source, ?MODULE}] = rabbit_channel:info(ServerCh, [source]),
    amqp_channel:close(ClientCh),
    amqp_connection:close(Conn),
    {error, channel_terminated} = rabbit_channel:source(ServerCh, ?MODULE),
    passed.

network_arbitrary_channel_source(Config) ->
    passed = rabbit_ct_broker_helpers:rpc(Config, 0,
      ?MODULE, network_arbitrary_channel_source1, [Config]).

network_arbitrary_channel_source1(Config) ->
    Conn = rabbit_ct_client_helpers:open_unmanaged_connection(Config),
    Writer = spawn(fun () -> rabbit_ct_broker_helpers:test_writer(self()) end),
    {ok, Limiter} = rabbit_limiter:start_link(no_limiter_id),
    {ok, Collector} = rabbit_queue_collector:start_link(no_collector_id),
    {ok, Ch} = rabbit_channel:start_link(
                 1, Conn, Writer, Conn, "", rabbit_framing_amqp_0_9_1,
                 rabbit_ct_broker_helpers:user(<<"guest">>), <<"/">>, [],
                 Collector, Limiter),
    _ = rabbit_channel:source(Ch, ?MODULE),
    [{amqp_params, #amqp_params_network{username = <<"guest">>,
        password = <<"guest">>, host = "localhost", virtual_host = <<"/">>}}] =
            rabbit_amqp_connection:amqp_params(Conn, 1000),
    [{source, ?MODULE}] = rabbit_channel:info(Ch, [source]),
    [exit(P, normal) || P <- [Writer, Limiter, Collector, Ch]],
    amqp_connection:close(Conn),
    {error, channel_terminated} = rabbit_channel:source(Ch, ?MODULE),
    passed.

direct_channel_source(Config) ->
    passed = rabbit_ct_broker_helpers:rpc(Config, 0,
      ?MODULE, direct_channel_source1, [Config]).

direct_channel_source1(Config) ->
    ExistingChannels = rabbit_channel:list(),
    Conn = rabbit_ct_client_helpers:open_unmanaged_connection_direct(Config),
    {ok, ClientCh} = amqp_connection:open_channel(Conn),
    [ServerCh] = rabbit_channel:list() -- ExistingChannels,
    [{source, rabbit_direct}] = rabbit_channel:info(ServerCh, [source]),
    _ = rabbit_channel:source(ServerCh, ?MODULE),
    [{source, ?MODULE}] = rabbit_channel:info(ServerCh, [source]),
    amqp_channel:close(ClientCh),
    amqp_connection:close(Conn),
    {error, channel_terminated} = rabbit_channel:source(ServerCh, ?MODULE),
    passed.

undefined_channel_source(Config) ->
    passed = rabbit_ct_broker_helpers:rpc(Config, 0,
      ?MODULE, undefined_channel_source1, [Config]).

undefined_channel_source1(_Config) ->
    ExistingChannels = rabbit_channel:list(),
    {_Writer, _Limiter, ServerCh} = rabbit_ct_broker_helpers:test_channel(),
    [ServerCh] = rabbit_channel:list() -- ExistingChannels,
    [{source, undefined}] = rabbit_channel:info(ServerCh, [source]),
    _ = rabbit_channel:source(ServerCh, ?MODULE),
    [{source, ?MODULE}] = rabbit_channel:info(ServerCh, [source]),
    passed.
