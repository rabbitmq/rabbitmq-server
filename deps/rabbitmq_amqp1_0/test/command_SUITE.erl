%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2023 VMware, Inc. or its affiliates.  All rights reserved.


-module(command_SUITE).
-compile([export_all]).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").
-include("rabbit_amqp1_0.hrl").
-include_lib("amqp_client/include/amqp_client.hrl").

-define(COMMAND, 'Elixir.RabbitMQ.CLI.Ctl.Commands.ListAmqp10ConnectionsCommand').

all() ->
    [
     {group, non_parallel_tests}
    ].

groups() ->
    [
     {non_parallel_tests, [], [
                               merge_defaults,
                               validate,
                               when_no_connections,
                               when_one_connection
                                ]}
    ].

init_per_suite(Config) ->
    application:ensure_all_started(amqp10_client),
    rabbit_ct_helpers:log_environment(),
    Config.

end_per_suite(Config) ->
    Config.

init_per_group(Group, Config) ->
    Suffix = rabbit_ct_helpers:testcase_absname(Config, "", "-"),
    Config1 = rabbit_ct_helpers:set_config(
                Config, [
                         {rmq_nodename_suffix, Suffix},
                         {amqp10_client_library, Group}
                        ]),
    rabbit_ct_helpers:run_setup_steps(
      Config1,
      rabbit_ct_broker_helpers:setup_steps() ++
      rabbit_ct_client_helpers:setup_steps()).

end_per_group(_, Config) ->
    rabbit_ct_helpers:run_teardown_steps(Config,
      rabbit_ct_client_helpers:teardown_steps() ++
      rabbit_ct_broker_helpers:teardown_steps()).

init_per_testcase(Testcase, Config) ->
    rabbit_ct_helpers:testcase_started(Config, Testcase).

end_per_testcase(Testcase, Config) ->
    rabbit_ct_helpers:testcase_finished(Config, Testcase).

merge_defaults(_Config) ->
    {[<<"pid">>], #{verbose := false}} =
        ?COMMAND:merge_defaults([], #{}),

    {[<<"other_key">>], #{verbose := true}} =
        ?COMMAND:merge_defaults([<<"other_key">>], #{verbose => true}),

    {[<<"other_key">>], #{verbose := false}} =
        ?COMMAND:merge_defaults([<<"other_key">>], #{verbose => false}).

validate(_Config) ->
    ok = ?COMMAND:validate([], #{}),
    ok = ?COMMAND:validate([<<"recv_oct">>, <<"ssl">>], #{}),
    ok = ?COMMAND:validate([atom_to_binary(K, utf8) || K <- ?INFO_ITEMS], #{}),
    {validation_failure,{bad_info_key,[other]}} =
        ?COMMAND:validate([<<"other">>], #{}).

when_no_connections(_Config) ->
    [A] = rabbit_ct_broker_helpers:get_node_configs(_Config, nodename),
    Opts = #{node => A, timeout => 2000, verbose => true},
    [] = 'Elixir.Enum':to_list(?COMMAND:run([], Opts)).

when_one_connection(_Config) ->
    [A] = rabbit_ct_broker_helpers:get_node_configs(_Config, nodename),
    Opts = #{node => A, timeout => 2000, verbose => true},

    [Connection,Sender] = open_amqp10_connection(_Config),

    [_] = 'Elixir.Enum':to_list(?COMMAND:run([], Opts)),
    close_amqp10_connection(Connection, Sender).

open_amqp10_connection(Config) ->
    Host = ?config(rmq_hostname, Config),
    Port = rabbit_ct_broker_helpers:get_node_config(Config, 0, tcp_port_amqp),
    QName  = atom_to_binary(?FUNCTION_NAME, utf8),
    Address = <<"/amq/queue/", QName/binary>>,
    %% declare a quorum queue
    Ch = rabbit_ct_client_helpers:open_channel(Config, 0),
    amqp_channel:call(Ch, #'queue.declare'{queue = QName,
                                           durable = true,
                                           arguments = [{<<"x-queue-type">>, longstr, <<"quorum">>}]}),

    % create a configuration map
    OpnConf = #{address => Host,
                port => Port,
                container_id => atom_to_binary(?FUNCTION_NAME, utf8),
                sasl => {plain, <<"guest">>, <<"guest">>}},

    % ct:pal("opening connectoin with ~tp", [OpnConf]),
    {ok, Connection} = amqp10_client:open_connection(OpnConf),
    {ok, Session} = amqp10_client:begin_session(Connection),
    SenderLinkName = <<"test-sender">>,
    {ok, Sender} = amqp10_client:attach_sender_link(Session,
                                                    SenderLinkName,
                                                    Address),

    % wait for credit to be received
    receive
        {amqp10_event, {link, Sender, credited}} -> ok
    after 2000 ->
              exit(credited_timeout)
    end,

    OutMsg = amqp10_msg:new(<<"my-tag">>, <<"my-body">>, true),
    ok = amqp10_client:send_msg(Sender, OutMsg),

    flush("pre-receive"),
    {ok, Receiver} = amqp10_client:attach_receiver_link(Session,
                                                        <<"test-receiver">>,
                                                        Address),

    % grant credit and drain
    ok = amqp10_client:flow_link_credit(Receiver, 1, never, true),

    % wait for a delivery
    receive
        {amqp10_msg, Receiver, _InMsg} -> ct:pal("Received amqp 1.0 message : ~w~n", [_InMsg]), ok
    after 2000 ->
              exit(delivery_timeout)
    end,



    [Connection, Sender].

flush(Prefix) ->
    receive
        Msg ->
            ct:pal("~ts flushed: ~w~n", [Prefix, Msg]),
            flush(Prefix)
    after 1 ->
              ok
    end.

close_amqp10_connection(Connection, Sender) ->
  flush("final"),
  ct:pal("Closing connection ~w~n", [Connection]),
  ok = amqp10_client:detach_link(Sender),
  ok = amqp10_client:close_connection(Connection),
  ok.
