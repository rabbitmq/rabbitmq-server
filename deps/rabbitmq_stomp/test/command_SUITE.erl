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
%% Copyright (c) 2007-2016 Pivotal Software, Inc.  All rights reserved.


-module(command_SUITE).
-compile([export_all]).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("amqp_client/include/amqp_client.hrl").
-include("rabbit_stomp.hrl").


-define(COMMAND, 'Elixir.RabbitMQ.CLI.Ctl.Commands.ListStompConnectionsCommand').

all() ->
    [
      {group, non_parallel_tests}
    ].

groups() ->
    [
      {non_parallel_tests, [], [
                                merge_defaults,
                                run
                               ]}
    ].

init_per_suite(Config) ->
    Config1 = rabbit_ct_helpers:set_config(Config,
                                           [{rmq_nodename_suffix, ?MODULE}]),
    rabbit_ct_helpers:log_environment(),
    rabbit_ct_helpers:run_setup_steps(Config1,
      rabbit_ct_broker_helpers:setup_steps()).

end_per_suite(Config) ->
    rabbit_ct_helpers:run_teardown_steps(Config,
      rabbit_ct_broker_helpers:teardown_steps()).

init_per_group(_, Config) ->
    Config.

end_per_group(_, Config) ->
    Config.

init_per_testcase(Testcase, Config) ->
    rabbit_ct_helpers:testcase_started(Config, Testcase).

end_per_testcase(Testcase, Config) ->
    rabbit_ct_helpers:testcase_finished(Config, Testcase).

merge_defaults(_Config) ->
    {[<<"session_id">>, <<"conn_name">>], #{verbose := false}} =
        ?COMMAND:merge_defaults([], #{}),

    {[<<"other_key">>], #{verbose := true}} =
        ?COMMAND:merge_defaults([<<"other_key">>], #{verbose => true}),

    {[<<"other_key">>], #{verbose := false}} =
        ?COMMAND:merge_defaults([<<"other_key">>], #{verbose => false}).


run(Config) ->

    Node = rabbit_ct_broker_helpers:get_node_config(Config, 0, nodename),
    Opts = #{node => Node, timeout => 10000, verbose => false},

    %% No connections
    [] = 'Elixir.Enum':to_list(?COMMAND:run([], Opts)),

    StompPort = rabbit_ct_broker_helpers:get_node_config(Config, 0, tcp_port_stomp),

    {ok, _Client} = rabbit_stomp_client:connect(StompPort),
    ct:sleep(100),

    [[{session_id, _}]] =
            'Elixir.Enum':to_list(?COMMAND:run([<<"session_id">>], Opts)),


    {ok, _Client2} = rabbit_stomp_client:connect(StompPort),
    ct:sleep(100),

    [[{session_id, _}], [{session_id, _}]] =
        'Elixir.Enum':to_list(?COMMAND:run([<<"session_id">>], Opts)),

    Port = rabbit_ct_broker_helpers:get_node_config(Config, 0, tcp_port_amqp),
    start_amqp_connection(network, Node, Port),

    %% There are still just two connections
    [[{session_id, _}], [{session_id, _}]] =
        'Elixir.Enum':to_list(?COMMAND:run([<<"session_id">>], Opts)),
    
    start_amqp_connection(direct, Node, Port),

    %% Still two MQTT connections, one direct AMQP 0-9-1 connection
    [[{session_id, _}], [{session_id, _}]] =
        'Elixir.Enum':to_list(?COMMAND:run([<<"session_id">>], Opts)),

    %% Verbose returns all keys
    Infos = lists:map(fun(El) -> atom_to_binary(El, utf8) end, ?INFO_ITEMS),
    AllKeys = 'Elixir.Enum':to_list(?COMMAND:run(Infos, Opts)),
    AllKeys = 'Elixir.Enum':to_list(?COMMAND:run([], Opts#{verbose => true})),

    %% There are two connections
    [First, _Second] = AllKeys,

    %% Keys are INFO_ITEMS
    KeysCount = length(?INFO_ITEMS),
    KeysCount = length(First),

    {Keys, _} = lists:unzip(First),

    [] = Keys -- ?INFO_ITEMS,
    [] = ?INFO_ITEMS -- Keys.


start_amqp_connection(Type, Node, Port) ->
    Params = amqp_params(Type, Node, Port),
    {ok, _Connection} = amqp_connection:start(Params).

amqp_params(network, _, Port) ->
    #amqp_params_network{port = Port};
amqp_params(direct, Node, _) ->
    #amqp_params_direct{node = Node}.
