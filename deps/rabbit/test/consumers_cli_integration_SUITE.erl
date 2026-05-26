%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2026 Broadcom. All Rights Reserved. The term "Broadcom" refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
%%
-module(consumers_cli_integration_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("amqp_client/include/amqp_client.hrl").

-compile(export_all).

all() ->
    [
        {group, tests}
    ].

groups() ->
    [
        {tests, [], [
            list_consumers_blocked_status
        ]}
    ].

init_per_suite(Config) ->
    rabbit_ct_helpers:log_environment(),
    Config1 = rabbit_ct_helpers:set_config(
                Config, [{rmq_nodename_suffix, ?MODULE},
                         {rmq_nodes_count, 1}]),
    rabbit_ct_helpers:run_setup_steps(Config1,
                                      rabbit_ct_broker_helpers:setup_steps() ++
                                          rabbit_ct_client_helpers:setup_steps()).

end_per_suite(Config) ->
    rabbit_ct_helpers:run_teardown_steps(Config,
                                         rabbit_ct_client_helpers:teardown_steps() ++
                                             rabbit_ct_broker_helpers:teardown_steps()).

init_per_testcase(Testcase, Config) ->
    rabbit_ct_helpers:testcase_started(Config, Testcase).

end_per_testcase(Testcase, Config) ->
    rabbit_ct_helpers:testcase_finished(Config, Testcase).

list_consumers_blocked_status(Config) ->
    Ch = rabbit_ct_client_helpers:open_channel(Config, 0),
    QName = atom_to_binary(?FUNCTION_NAME),
    try
        #'queue.declare_ok'{} = amqp_channel:call(
                                   Ch, #'queue.declare'{queue = QName,
                                                        durable = true}),
        Prefetch = 5,
        #'basic.qos_ok'{} = amqp_channel:call(
                              Ch, #'basic.qos'{prefetch_count = Prefetch}),
        amqp_channel:subscribe(Ch, #'basic.consume'{queue = QName,
                                                    no_ack = false},
                               self()),
        receive #'basic.consume_ok'{} -> ok
        after 5000 -> ct:fail(no_consume_ok)
        end,
        %% Publish prefetch+1 messages: the consumer blocks at prefetch, and
        %% one ack later the queue is empty, so it does not block again.
        [amqp_channel:call(Ch,
                           #'basic.publish'{routing_key = QName},
                           #amqp_msg{payload = <<"x">>})
         || _ <- lists:seq(1, Prefetch + 1)],

        rabbit_ct_helpers:await_condition(
          fun () -> has_status(Config, QName, <<"blocked">>) end,
          30000),

        HighestTag = highest_delivery_tag(Prefetch),
        ok = amqp_channel:cast(Ch, #'basic.ack'{delivery_tag = HighestTag,
                                                multiple = true}),

        rabbit_ct_helpers:await_condition(
          fun () -> has_status(Config, QName, <<"up">>) end,
          30000)
    after
        catch amqp_channel:call(Ch, #'queue.delete'{queue = QName}),
        catch rabbit_ct_client_helpers:close_channel(Ch)
    end,
    ok.

has_status(Config, QName, Status) ->
    {ok, Out} = rabbit_ct_broker_helpers:rabbitmqctl(
                  Config, 0,
                  ["list_consumers", "--no-table-headers",
                   "queue_name", "consumer_tag", "activity_status"]),
    %% Parse through to the table, ignore log noise.
    Rows = [Row || Row <- re:split(Out, <<"\n">>, [trim]),
                   binary:match(Row, <<"\t">>) =/= nomatch],
    lists:any(
      fun (Row) ->
              case re:split(Row, <<"\t">>, [trim]) of
                  [Q, _CTag, S] when Q =:= QName, S =:= Status -> true;
                  _ -> false
              end
      end, Rows).

highest_delivery_tag(MinDeliveries) ->
    highest_delivery_tag(MinDeliveries, undefined).

highest_delivery_tag(0, Highest) ->
    receive
        {#'basic.deliver'{delivery_tag = T}, _Msg} ->
            highest_delivery_tag(0, T)
    after 200 ->
            Highest
    end;
highest_delivery_tag(N, _Highest) ->
    receive
        {#'basic.deliver'{delivery_tag = T}, _Msg} ->
            highest_delivery_tag(N - 1, T)
    after 5000 ->
            ct:fail({timeout_waiting_for_delivery, N})
    end.
