%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2023 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries.  All rights reserved.
%%

-module(amqpl_consumer_ack_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("amqp_client/include/amqp_client.hrl").

-compile([nowarn_export_all,
          export_all]).

-import(rabbit_ct_broker_helpers,
        [rpc/4]).
-import(rabbit_ct_helpers,
        [eventually/3]).

all() ->
    [
     {group, tests}
    ].

groups() ->
    [
     {tests, [shuffle],
      [
       requeue_one_channel_classic_queue,
       requeue_one_channel_quorum_queue,
       requeue_two_channels_classic_queue,
       requeue_two_channels_quorum_queue
      ]}
    ].

%% -------------------------------------------------------------------
%% Testsuite setup/teardown.
%% -------------------------------------------------------------------

init_per_suite(Config) ->
    rabbit_ct_helpers:log_environment(),
    rabbit_ct_helpers:merge_app_env(
      Config, {rabbit, [{quorum_tick_interval, 1000}]}).

end_per_suite(Config) ->
    Config.

init_per_group(_Group, Config) ->
    Nodes = 1,
    Suffix = rabbit_ct_helpers:testcase_absname(Config, "", "-"),
    Config1 = rabbit_ct_helpers:set_config(
                Config, [{rmq_nodes_count, Nodes},
                         {rmq_nodename_suffix, Suffix}]),
    rabbit_ct_helpers:run_setup_steps(
      Config1,
      rabbit_ct_broker_helpers:setup_steps() ++
      rabbit_ct_client_helpers:setup_steps()).

end_per_group(_Group, Config) ->
    rabbit_ct_helpers:run_teardown_steps(
      Config,
      rabbit_ct_client_helpers:teardown_steps() ++
      rabbit_ct_broker_helpers:teardown_steps()).

init_per_testcase(Testcase, Config) ->
    rabbit_ct_helpers:testcase_started(Config, Testcase).

end_per_testcase(Testcase, Config) ->
    rabbit_ct_helpers:testcase_finished(Config, Testcase).

requeue_one_channel_classic_queue(Config) ->
    requeue_one_channel(<<"classic">>, Config).

requeue_one_channel_quorum_queue(Config) ->
    requeue_one_channel(<<"quorum">>, Config).

requeue_one_channel(QType, Config) ->
    QName = atom_to_binary(?FUNCTION_NAME),
    Ctag = <<"my consumer tag">>,
    Conn = rabbit_ct_client_helpers:open_unmanaged_connection(Config, 0),
    {ok, Ch} = amqp_connection:open_channel(Conn),

    #'queue.declare_ok'{} = amqp_channel:call(
                              Ch,
                              #'queue.declare'{
                                 queue = QName,
                                 durable = true,
                                 arguments = [{<<"x-queue-type">>, longstr, QType}]}),

    amqp_channel:subscribe(Ch,
                           #'basic.consume'{queue = QName,
                                            consumer_tag = Ctag},
                           self()),

    receive #'basic.consume_ok'{consumer_tag = Ctag} -> ok
    after 5000 -> ct:fail({missing_event, ?LINE})
    end,

    [begin
         amqp_channel:cast(
           Ch,
           #'basic.publish'{routing_key = QName},
           #amqp_msg{payload = integer_to_binary(N)})
     end || N <- lists:seq(1, 4)],

    receive {#'basic.deliver'{},
             #amqp_msg{payload = <<"1">>}} -> ok
    after 5000 -> ct:fail({missing_event, ?LINE})
    end,
    receive {#'basic.deliver'{},
             #amqp_msg{payload = <<"2">>}} -> ok
    after 5000 -> ct:fail({missing_event, ?LINE})
    end,
    D3 = receive {#'basic.deliver'{delivery_tag = Del3},
                  #amqp_msg{payload = <<"3">>}} -> Del3
         after 5000 -> ct:fail({missing_event, ?LINE})
         end,
    receive {#'basic.deliver'{},
             #amqp_msg{payload = <<"4">>}} -> ok
    after 5000 -> ct:fail({missing_event, ?LINE})
    end,
    assert_messages(QName, 4, 4, Config),

    %% Requeue the first 3 messages.
    amqp_channel:cast(Ch, #'basic.nack'{delivery_tag = D3,
                                        requeue = true,
                                        multiple = true}),

    %% First 3 messages should be redelivered.
    receive {#'basic.deliver'{},
             #amqp_msg{payload = P1}} ->
                ?assertEqual(<<"1">>, P1)
    after 5000 -> ct:fail({missing_event, ?LINE})
    end,
    receive {#'basic.deliver'{},
             #amqp_msg{payload = P2}} ->
                ?assertEqual(<<"2">>, P2)
    after 5000 -> ct:fail({missing_event, ?LINE})
    end,
    D3b = receive {#'basic.deliver'{delivery_tag = Del3b},
                   #amqp_msg{payload = P3}} ->
                      ?assertEqual(<<"3">>, P3),
                      Del3b
          after 5000 -> ct:fail({missing_event, ?LINE})
          end,
    assert_messages(QName, 4, 4, Config),

    %% Ack all 4 messages.
    amqp_channel:cast(Ch, #'basic.ack'{delivery_tag = D3b,
                                       multiple = true}),
    assert_messages(QName, 0, 0, Config),

    ?assertMatch(#'queue.delete_ok'{},
                 amqp_channel:call(Ch, #'queue.delete'{queue = QName})).

requeue_two_channels_classic_queue(Config) ->
    requeue_two_channels(<<"classic">>, Config).

requeue_two_channels_quorum_queue(Config) ->
    requeue_two_channels(<<"quorum">>, Config).

requeue_two_channels(QType, Config) ->
    QName = atom_to_binary(?FUNCTION_NAME),
    Ctag1 = <<"consumter tag 1">>,
    Ctag2 = <<"consumter tag 2">>,
    Ch1 = rabbit_ct_client_helpers:open_channel(Config),
    Ch2 = rabbit_ct_client_helpers:open_channel(Config),

    #'queue.declare_ok'{} = amqp_channel:call(
                              Ch1,
                              #'queue.declare'{
                                 queue = QName,
                                 durable = true,
                                 arguments = [{<<"x-queue-type">>, longstr, QType}]}),

    amqp_channel:subscribe(Ch1,
                           #'basic.consume'{queue = QName,
                                            consumer_tag = Ctag1},
                           self()),

    receive #'basic.consume_ok'{consumer_tag = Ctag1} -> ok
    after 5000 -> ct:fail({missing_event, ?LINE})
    end,

    amqp_channel:subscribe(Ch2,
                           #'basic.consume'{queue = QName,
                                            consumer_tag = Ctag2},
                           self()),
    receive #'basic.consume_ok'{consumer_tag = Ctag2} -> ok
    after 5000 -> ct:fail({missing_event, ?LINE})
    end,

    [begin
         amqp_channel:cast(
           Ch1,
           #'basic.publish'{routing_key = QName},
           #amqp_msg{payload = integer_to_binary(N)})
     end || N <- lists:seq(1,4)],

    %% Queue should deliver round robin.
    receive {#'basic.deliver'{consumer_tag = C1},
             #amqp_msg{payload = <<"1">>}} ->
                ?assertEqual(Ctag1, C1)
    after 5000 -> ct:fail({missing_event, ?LINE})
    end,
    receive {#'basic.deliver'{consumer_tag = C2},
             #amqp_msg{payload = <<"2">>}} ->
                ?assertEqual(Ctag2, C2)
    after 5000 -> ct:fail({missing_event, ?LINE})
    end,
    receive {#'basic.deliver'{consumer_tag = C3},
             #amqp_msg{payload = <<"3">>}} ->
                ?assertEqual(Ctag1, C3)
    after 5000 -> ct:fail({missing_event, ?LINE})
    end,
    receive {#'basic.deliver'{consumer_tag = C4},
             #amqp_msg{payload = <<"4">>}} ->
                ?assertEqual(Ctag2, C4)
    after 5000 -> ct:fail({missing_event, ?LINE})
    end,
    assert_messages(QName, 4, 4, Config),

    %% Closing Ch1 should cause both messages to be requeued and delivered to the Ch2.
    ok = rabbit_ct_client_helpers:close_channel(Ch1),

    receive {#'basic.deliver'{consumer_tag = C5},
             #amqp_msg{payload = <<"1">>}} ->
                ?assertEqual(Ctag2, C5)
    after 5000 -> ct:fail({missing_event, ?LINE})
    end,
    DelTag = receive {#'basic.deliver'{consumer_tag = C6,
                                       delivery_tag = D},
                      #amqp_msg{payload = <<"3">>}} ->
                         ?assertEqual(Ctag2, C6),
                         D
             after 5000 -> ct:fail({missing_event, ?LINE})
             end,
    assert_messages(QName, 4, 4, Config),

    %% Ch2 acks all 4 messages
    amqp_channel:cast(Ch2, #'basic.ack'{delivery_tag = DelTag,
                                        multiple = true}),
    assert_messages(QName, 0, 0, Config),

    ?assertMatch(#'queue.delete_ok'{},
                 amqp_channel:call(Ch2, #'queue.delete'{queue = QName})).

assert_messages(QNameBin, NumTotalMsgs, NumUnackedMsgs, Config) ->
    Vhost = ?config(rmq_vhost, Config),
    eventually(
      ?_assertEqual(
         lists:sort([{messages, NumTotalMsgs}, {messages_unacknowledged, NumUnackedMsgs}]),
         begin
             {ok, Q} = rpc(Config, rabbit_amqqueue, lookup, [QNameBin, Vhost]),
             Infos = rpc(Config, rabbit_amqqueue, info, [Q, [messages, messages_unacknowledged]]),
             lists:sort(Infos)
         end
        ), 500, 5).
