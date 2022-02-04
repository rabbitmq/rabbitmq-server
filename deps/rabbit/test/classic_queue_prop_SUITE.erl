%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2021 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(classic_queue_prop_SUITE).
-compile(export_all).

-define(NUM_TESTS, 500).

%% Set to true to get an awful lot of debug logs.
-if(false).
-define(DEBUG(X,Y), logger:debug("~0p: " ++ X, [?FUNCTION_NAME|Y])).
-else.
-define(DEBUG(X,Y), _ = X, _ = Y, ok).
-endif.

-include_lib("amqp_client/include/amqp_client.hrl").
-include_lib("proper/include/proper.hrl").

-record(cq, {
    amq = undefined :: amqqueue:amqqueue(),
    name :: atom(),
    mode :: classic | lazy,
    version :: 1 | 2,

    %% We have one queue per way of publishing messages (such as channels).
    %% We can only confirm the publish order on a per-channel level because
    %% the order is non-deterministic when multiple publishers concurrently
    %% publish.
    q = #{} :: #{pid() | internal => queue:queue()},

    %% We must account for lost acks/unconfirmed publishes after restarts.
    restarted = false :: boolean(),
    crashed = false :: boolean(),
    %% We must keep some received messages around because basic.ack is not
    %% synchronous and as a result we may end up receiving messages twice
    %% after a queue is restarted.
    acked = [] :: list(),
    %% We keep track of persistent messages that were confirmed and for
    %% which we have received the publisher confirm. This is used when
    %% restarting the queue to only move messages into 'uncertain'
    %% when we have not received confirms.
    confirmed = [] :: list(),
    %% We also keep track of persistent messages that were sent before
    %% the channel entered confirm mode. These messages will not be
    %% confirmed retroactively and must not be considered confirmed.
    unconfirmed = [] :: list(),
    %% We must also keep some published messages around because when
    %% confirms are not used, or were not received, we are uncertain
    %% of whether the message made it to the queue or not.
    uncertain = [] :: list(),
    %% We may receive any message previously received after a crash.
    received = [] :: list(),

    %% CT config necessary to open channels.
    config = no_config :: list(),

    %% Limiter pid for calling rabbit_amqqueue:basic_get.
    limiter :: pid(),

    %% Channels.
    %%
    %% We set consumers to 'true' when there had been consumers, even if they
    %% were closed or cancelled, because there can be a race condition between
    %% the cancelling and the message going back to the queue, and the test
    %% suite getting messages via basic.get.
    consumers = false :: boolean(),
    channels = #{} :: #{pid() => #{consumer := none | binary(), confirms := boolean()}}
}).

%% Common Test.

all() ->
    [{group, classic_queue_tests}].

groups() ->
    [{classic_queue_tests, [], [
%        manual%,
        classic_queue_v1,
        lazy_queue_v1,
        classic_queue_v2,
        lazy_queue_v2
    ]}].

init_per_suite(Config) ->
    rabbit_ct_helpers:log_environment(),
    rabbit_ct_helpers:run_setup_steps(Config).

end_per_suite(Config) ->
    rabbit_ct_helpers:run_teardown_steps(Config).

init_per_group(Group = classic_queue_tests, Config) ->
    Config1 = rabbit_ct_helpers:set_config(Config, [
        {rmq_nodename_suffix, Group},
        {rmq_nodes_count, 1},
        {erlang_node_config, [{rabbit, [
            %% We make sure the server can handle fast-paced forced queue crashes
            %% for the purpose of the tests.
            {amqqueue_max_restart_intensity, {1000000, 1}},
            %% We reduce the segment_entry_count values to make using multiple segments more likely.
            {queue_index_segment_entry_count, 512},
            {classic_queue_index_v2_segment_entry_count, 512}
        ]}]}
      ]),
    Config2 = rabbit_ct_helpers:run_steps(Config1,
      rabbit_ct_broker_helpers:setup_steps() ++
      rabbit_ct_client_helpers:setup_steps()),
    %% We increase the number of entries in stacktraces
    %% to ease debugging when there is a crash. The
    %% default of 8 is a bit low. We need to increase
    %% in both the CT node and the RabbitMQ node.
    erlang:system_flag(backtrace_depth, 16),
    rabbit_ct_broker_helpers:rpc(Config2, 0,
        erlang, system_flag, [backtrace_depth, 16]),
    Config2.

end_per_group(classic_queue_tests, Config) ->
    rabbit_ct_helpers:run_steps(Config,
      rabbit_ct_client_helpers:teardown_steps() ++
      rabbit_ct_broker_helpers:teardown_steps()).

%% $ make -C deps/rabbit test-build
%% $ erl -pa deps/rabbit/test
%% > classic_queue_prop_SUITE:instrs_to_manual([[{init,...},...]]).
%% Paste into do_manual/1.
%% Enable manual as the only test in groups/0.
%% $ make -C deps/rabbit ct-classic_queue
instrs_to_manual([Instrs]) ->
    io:format("~ndo_manual(Config) ->~n~n"),
    lists:foreach(fun
        ({init, CQ}) ->
            #cq{name=Name, mode=Mode, version=Version} = CQ,
            io:format("    St0 = #cq{name=~0p, mode=~0p, version=~0p,~n"
                      "              config=minimal_config(Config)},~n~n",
                      [Name, Mode, Version]);
        ({set, {var,Var}, {call, ?MODULE, cmd_setup_queue, _}}) ->
            Res = "Res" ++ integer_to_list(Var),
            PrevSt = "St" ++ integer_to_list(Var - 1),
            St = "St" ++ integer_to_list(Var),
            io:format("    ~s = cmd_setup_queue(~s),~n"
                      "    ~s = ~s#cq{amq=~s},~n~n",
                      [Res, PrevSt, St, PrevSt, Res]);
        ({set, {var,Var}, {call, ?MODULE, Cmd, [#cq{}|Args]}}) ->
            Res = "Res" ++ integer_to_list(Var),
            PrevSt = "St" ++ integer_to_list(Var - 1),
            St = "St" ++ integer_to_list(Var),
            ExtraArgs = [[", ", case A of
                                    {var,V} -> "Res" ++ integer_to_list(V);
                                    _ -> io_lib:format("~0p", [A])
                                end] || A <- Args],
            io:format("    ~s = ~s(~s~s),~n"
                      "    true = postcondition(~s, {call, undefined, ~s, [~s~s]}, ~s),~n"
                      "    ~s = next_state(~s, ~s, {call, undefined, ~s, [~s~s]}),~n~n",
                      [Res, Cmd, PrevSt, ExtraArgs,
                       PrevSt, Cmd, PrevSt, ExtraArgs, Res,
                       St, PrevSt, Res, Cmd, PrevSt, ExtraArgs]);
        ({set, {var,Var}, {call, ?MODULE, Cmd, Args}}) ->
            Res = "Res" ++ integer_to_list(Var),
            PrevSt = "St" ++ integer_to_list(Var - 1),
            St = "St" ++ integer_to_list(Var),
            ExtraArgs = case lists:flatten([[", ", case A of
                                                       {var,V} -> "Res" ++ integer_to_list(V);
                                                       _ -> io_lib:format("~0p", [A])
                                                   end] || A <- Args]) of
                "" -> "";
                ", " ++ ExtraArgs0 -> ExtraArgs0
            end,
            io:format("    ~s = ~s(~s),~n"
                      "    true = postcondition(~s, {call, undefined, ~s, [~s]}, ~s),~n"
                      "    ~s = next_state(~s, ~s, {call, undefined, ~s, [~s]}),~n~n",
                      [Res, Cmd, ExtraArgs,
                       PrevSt, Cmd, ExtraArgs, Res,
                       St, PrevSt, Res, Cmd, ExtraArgs])
    end, Instrs),
    io:format("    true.~n").

manual(Config) ->
    %% This is where tracing of client processes
    %% (amqp_channel, amqp_selective_consumer)
    %% should be added if necessary.
    true = rabbit_ct_broker_helpers:rpc(Config, 0,
        ?MODULE, do_manual, [Config]).

%% Tips to help reproduce an issue:
%% - See instrs_to_manual/1 to automatically obtain code to put in this function.
%% - Do the commands after cmd_setup_queue in a loop.
%% - Add some timer:sleep(1) or longer between commands if delays are necessary.
%% - If a shrunk set of commands isn't good enough, the original might.
%% - Removing some steps can help understand the sequence of events leading to the problem.
do_manual(Config) ->
    Config =:= Config.

classic_queue_v1(Config) ->
    true = rabbit_ct_broker_helpers:rpc(Config, 0,
        ?MODULE, do_classic_queue_v1, [Config]).

do_classic_queue_v1(Config) ->
    true = proper:quickcheck(prop_classic_queue_v1(Config),
                             [{on_output, on_output_fun()},
                              {numtests, ?NUM_TESTS}]).

lazy_queue_v1(Config) ->
    true = rabbit_ct_broker_helpers:rpc(Config, 0,
        ?MODULE, do_lazy_queue_v1, [Config]).

do_lazy_queue_v1(Config) ->
    true = proper:quickcheck(prop_lazy_queue_v1(Config),
                             [{on_output, on_output_fun()},
                              {numtests, ?NUM_TESTS}]).

classic_queue_v2(Config) ->
    true = rabbit_ct_broker_helpers:rpc(Config, 0,
        ?MODULE, do_classic_queue_v2, [Config]).

do_classic_queue_v2(Config) ->
    true = proper:quickcheck(prop_classic_queue_v2(Config),
                             [{on_output, on_output_fun()},
                              {numtests, ?NUM_TESTS}]).

lazy_queue_v2(Config) ->
    true = rabbit_ct_broker_helpers:rpc(Config, 0,
        ?MODULE, do_lazy_queue_v2, [Config]).

do_lazy_queue_v2(Config) ->
    true = proper:quickcheck(prop_lazy_queue_v2(Config),
                             [{on_output, on_output_fun()},
                              {numtests, ?NUM_TESTS}]).

on_output_fun() ->
    fun (".", _) -> ok; % don't print the '.'s on new lines
        ("!", _) -> ok;
        ("~n", _) -> ok; % don't print empty lines; CT adds many to logs already
        ("~w~n", A) -> logger:error("~p~n", [A]); % make sure this gets sent to the terminal, it's important
        (F, A) -> io:format(F, A)
    end.

%% Properties.

prop_classic_queue_v1(Config) ->
    {ok, LimiterPid} = rabbit_limiter:start_link(no_id),
    InitialState = #cq{name=?FUNCTION_NAME, mode=default, version=1,
                       config=minimal_config(Config), limiter=LimiterPid},
    prop_common(InitialState).

prop_lazy_queue_v1(Config) ->
    {ok, LimiterPid} = rabbit_limiter:start_link(no_id),
    InitialState = #cq{name=?FUNCTION_NAME, mode=lazy, version=1,
                       config=minimal_config(Config), limiter=LimiterPid},
    prop_common(InitialState).

prop_classic_queue_v2(Config) ->
    {ok, LimiterPid} = rabbit_limiter:start_link(no_id),
    InitialState = #cq{name=?FUNCTION_NAME, mode=default, version=2,
                       config=minimal_config(Config), limiter=LimiterPid},
    prop_common(InitialState).

prop_lazy_queue_v2(Config) ->
    {ok, LimiterPid} = rabbit_limiter:start_link(no_id),
    InitialState = #cq{name=?FUNCTION_NAME, mode=lazy, version=2,
                       config=minimal_config(Config), limiter=LimiterPid},
    prop_common(InitialState).

prop_common(InitialState) ->
    ?FORALL(Commands, commands(?MODULE, InitialState),
        ?TRAPEXIT(begin
            {History, State, Result} = run_commands(?MODULE, Commands),
            cmd_teardown_queue(State),
            ?WHENFAIL(logger:error("History: ~p~nState: ~p~nResult: ~p",
                                   [History, State, Result]),
                      aggregate(command_names(Commands), Result =:= ok))
        end)
    ).

minimal_config(Config) ->
    {_, [RmqNode0]} = lists:keyfind(rmq_nodes, 1, Config),
    [{rmq_nodes, [lists:filter(fun
        ({channels_manager, _}) -> true;
        ({nodename, _}) -> true;
        (_) -> false
    end, RmqNode0)]}].

%% Commands.

command(St = #cq{amq=undefined}) ->
    {call, ?MODULE, cmd_setup_queue, [St]};
command(St) ->
    ChannelCmds = case has_channels(St) of
        false -> [];
        true -> [
            {100, {call, ?MODULE, cmd_channel_confirm_mode, [channel(St)]}},
            {100, {call, ?MODULE, cmd_channel_close, [channel(St)]}},
            {900, {call, ?MODULE, cmd_channel_publish, [St, channel(St), integer(0, 1024*1024), integer(1, 2), boolean(), expiration()]}},
            {300, {call, ?MODULE, cmd_channel_publish_many, [St, channel(St), integer(2, 512), integer(0, 1024*1024), integer(1, 2), boolean(), expiration()]}},
            {900, {call, ?MODULE, cmd_channel_wait_for_confirms, [channel(St)]}},
            {300, {call, ?MODULE, cmd_channel_basic_get, [St, channel(St)]}},
            {300, {call, ?MODULE, cmd_channel_consume, [St, channel(St)]}},
            {100, {call, ?MODULE, cmd_channel_cancel, [St, channel(St)]}},
            {900, {call, ?MODULE, cmd_channel_receive_and_ack, [St, channel(St)]}},
            {900, {call, ?MODULE, cmd_channel_receive_and_requeue, [St, channel(St)]}},
            {900, {call, ?MODULE, cmd_channel_receive_and_discard, [St, channel(St)]}},
            {300, {call, ?MODULE, cmd_channel_receive_many_and_ack_ooo, [St, channel(St), integer(2, 10)]}},
            {300, {call, ?MODULE, cmd_channel_receive_many_and_requeue_ooo, [St, channel(St), integer(2, 10)]}},
            {300, {call, ?MODULE, cmd_channel_receive_many_and_discard_ooo, [St, channel(St), integer(2, 10)]}}
        ]
    end,
    weighted_union([
        %% These restart the vhost or the queue.
        {100, {call, ?MODULE, cmd_restart_vhost_clean, [St]}},
        {100, {call, ?MODULE, cmd_restart_queue_dirty, [St]}},
        %% These change internal configuration.
        { 10, {call, ?MODULE, cmd_set_v2_check_crc32, [boolean()]}},
        %% These set policies.
        {100, {call, ?MODULE, cmd_set_mode, [St, oneof([default, lazy])]}},
        {100, {call, ?MODULE, cmd_set_version, [St, oneof([1, 2])]}},
        {100, {call, ?MODULE, cmd_set_mode_version, [oneof([default, lazy]), oneof([1, 2])]}},
        %% These are direct operations using internal functions.
        {100, {call, ?MODULE, cmd_publish_msg, [St, integer(0, 1024*1024), integer(1, 2), boolean(), expiration()]}},
        {100, {call, ?MODULE, cmd_basic_get_msg, [St]}},
        {100, {call, ?MODULE, cmd_purge, [St]}},
        %% These are channel-based operations.
        {300, {call, ?MODULE, cmd_channel_open, [St]}}
        |ChannelCmds
    ]).

expiration() ->
    oneof([
        undefined,
        integer(0, 100) %% Up to 0.1s to make it more likely to trigger dropping messages.
    ]).

has_channels(#cq{channels=Channels}) ->
    map_size(Channels) > 0.

channel(#cq{channels=Channels}) ->
    elements(maps:keys(Channels)).

%% Next state.

next_state(St, AMQ, {call, _, cmd_setup_queue, _}) ->
    St#cq{amq=AMQ};
next_state(St=#cq{q=Q0, confirmed=Confirmed, uncertain=Uncertain0, channels=Channels0}, AMQ, {call, _, cmd_restart_vhost_clean, _}) ->
    %% Consider all consumers canceled when the vhost restarts.
    Channels = maps:map(fun (_, ChInfo) -> ChInfo#{consumer => none} end, Channels0),
    %% The status of persistent messages that were published before the vhost restart
    %% is uncertain, unless the channel is in confirms mode and confirms
    %% were received.
    {Uncertain, Q} = maps:fold(fun(Ch, ChQ, {UncertainAcc, QAcc}) ->
        ChQL = queue:to_list(ChQ),
        %% We keep both transient and persistent messages on clean restarts.
        ChQConfirmed = [Msg || Msg <- ChQL, lists:member(Msg, Confirmed)],
        ChQUncertain = [Msg || Msg <- ChQL, not lists:member(Msg, Confirmed)],
        {ChQUncertain ++ UncertainAcc, QAcc#{Ch => queue:from_list(ChQConfirmed)}}
    end, {Uncertain0, #{}}, Q0),
    St#cq{amq=AMQ, q=Q, restarted=true, uncertain=Uncertain, channels=Channels};
next_state(St=#cq{q=Q0, confirmed=Confirmed, uncertain=Uncertain0}, AMQ, {call, _, cmd_restart_queue_dirty, _}) ->
    %% The status of persistent messages that were published before the queue crash
    %% is uncertain, unless the channel is in confirms mode and confirms
    %% were received.
    {Uncertain, Q} = maps:fold(fun(Ch, ChQ, {UncertainAcc, QAcc}) ->
        ChQL = queue:to_list(ChQ),
        %% We only keep persistent messages because on dirty restart the queue acks the transients.
        ChQConfirmed = [Msg || Msg <- ChQL, lists:member(Msg, Confirmed)],
        %% We keep both persistent and transient in Uncertain because there might
        %% be messages in transit that will be received by consumers.
        ChQUncertain = [Msg || Msg <- ChQL, not lists:member(Msg, Confirmed)],
        {ChQUncertain ++ UncertainAcc, QAcc#{Ch => queue:from_list(ChQConfirmed)}}
    end, {Uncertain0, #{}}, Q0),
    St#cq{amq=AMQ, q=Q, restarted=true, crashed=true, uncertain=Uncertain};
next_state(St, _, {call, _, cmd_set_v2_check_crc32, _}) ->
    St;
next_state(St, _, {call, _, cmd_set_mode, [_, Mode]}) ->
    St#cq{mode=Mode};
next_state(St, _, {call, _, cmd_set_version, [_, Version]}) ->
    St#cq{version=Version};
next_state(St, _, {call, _, cmd_set_mode_version, [Mode, Version]}) ->
    St#cq{mode=Mode, version=Version};
next_state(St=#cq{q=Q}, Msg, {call, _, cmd_publish_msg, _}) ->
    IntQ = maps:get(internal, Q, queue:new()),
    St#cq{q=Q#{internal => queue:in(Msg, IntQ)}};
next_state(St, empty, {call, _, cmd_basic_get_msg, _}) ->
    St;
next_state(St=#cq{q=Q, received=Received}, Msg, {call, _, cmd_basic_get_msg, _}) ->
    %% When there are multiple active consumers we may receive
    %% messages out of order because the commands are not running
    %% in the same order as the messages sent to channels.
    %%
    %% When there are messages expired they cannot be removed
    %% (because of potential race conditions if messages expired
    %% during transit, vs in the queue) so we may receive messages
    %% seemingly out of order.
    %%
    %% For all these reasons we remove messages regardless of where
    %% they are in the queue.
    St#cq{q=delete_message(Q, Msg), received=[Msg|Received]};
next_state(St=#cq{q=Q, uncertain=Uncertain0}, _, {call, _, cmd_purge, _}) ->
    %% The status of messages that were published before a purge
    %% is uncertain in the same way as for a vhost restart or a
    %% queue crash, because the purge command does not go through
    %% channels. We therefore handle it the same way as a vhost
    %% restart, minus the AMQ change.
    Uncertain = maps:fold(fun(_, ChQ, Acc) ->
        queue:to_list(ChQ) ++ Acc
    end, Uncertain0, Q),
    St#cq{q=#{}, restarted=true, uncertain=Uncertain};
next_state(St=#cq{channels=Channels}, Ch, {call, _, cmd_channel_open, _}) ->
    St#cq{channels=Channels#{Ch => #{consumer => none, confirms => false}}};
next_state(St=#cq{channels=Channels}, _, {call, _, cmd_channel_close, [Ch]}) ->
    St#cq{channels=maps:remove(Ch, Channels)};
next_state(St=#cq{q=Q, unconfirmed=Unconfirmed, channels=Channels}, _, {call, _, cmd_channel_confirm_mode, [Ch]}) ->
    %% All persistent messages sent before we enabled confirm mode will not be
    %% confirmed retroactively. We therefore need to keep track of them to avoid
    %% marking them as confirmed later on.
    ChQ = maps:get(Ch, Q, queue:new()),
    Persistent = [Msg || Msg = #amqp_msg{props=#'P_basic'{delivery_mode=2}} <- queue:to_list(ChQ)],
    ChInfo = maps:get(Ch, Channels),
    St#cq{unconfirmed=Persistent ++ Unconfirmed,
          channels=Channels#{Ch => ChInfo#{confirms => true}}};
next_state(St=#cq{q=Q}, Msg, {call, _, cmd_channel_publish, [_, Ch|_]}) ->
    ChQ = maps:get(Ch, Q, queue:new()),
    St#cq{q=Q#{Ch => queue:in(Msg, ChQ)}};
next_state(St=#cq{q=Q}, Msgs0, {call, _, cmd_channel_publish_many, [_, Ch|_]}) ->
    %% We handle {var,_} specially here but it would work for cmd_channel_publish as well.
    Msgs = case is_list(Msgs0) of
        true -> Msgs0;
        false -> [Msgs0]
    end,
    ChQ = maps:get(Ch, Q, queue:new()),
    St#cq{q=Q#{Ch => queue:from_list(queue:to_list(ChQ) ++ Msgs)}};
next_state(St=#cq{q=Q, confirmed=Confirmed, unconfirmed=Unconfirmed}, _, {call, _, cmd_channel_wait_for_confirms, [Ch]}) ->
    %% All messages sent on the channel were confirmed. We move them
    %% to the list of confirmed messages. We might end up with
    %% duplicates in the confirmed list because we might wait
    %% for confirms multiple times before we retrieve them,
    %% but that's okay because we only need to check that the
    %% message is present when the queue gets restarted.
    ChQ = maps:get(Ch, Q, queue:new()),
    %% We only keep persistent messages. Messages in Confirmed are always persistent.
    Persistent = [Msg || Msg = #amqp_msg{props=#'P_basic'{delivery_mode=2}} <- queue:to_list(ChQ), not lists:member(Msg, Unconfirmed)],
    St#cq{confirmed=Persistent ++ Confirmed};
next_state(St, empty, {call, _, cmd_channel_basic_get, _}) ->
    St;
next_state(St=#cq{q=Q, received=Received}, Msg, {call, _, cmd_channel_basic_get, _}) ->
    %% When there are multiple active consumers we may receive
    %% messages out of order because the commands are not running
    %% in the same order as the messages sent to channels.
    %%
    %% When there are messages expired they cannot be removed
    %% (because of potential race conditions if messages expired
    %% during transit, vs in the queue) so we may receive messages
    %% seemingly out of order.
    %%
    %% For all these reasons we remove messages regardless of where
    %% they are in the queue.
    St#cq{q=delete_message(Q, Msg), received=[Msg|Received]};
next_state(St=#cq{channels=Channels}, Tag, {call, _, cmd_channel_consume, [_, Ch]}) ->
    ChInfo = maps:get(Ch, Channels),
    St#cq{consumers=true, channels=Channels#{Ch => ChInfo#{consumer => Tag}}};
next_state(St=#cq{channels=Channels}, _, {call, _, cmd_channel_cancel, [_, Ch]}) ->
    ChInfo = maps:get(Ch, Channels),
    St#cq{channels=Channels#{Ch => ChInfo#{consumer => none}}};
next_state(St, none, {call, _, cmd_channel_receive_and_ack, _}) ->
    St;
next_state(St=#cq{q=Q, acked=Acked}, Msg, {call, _, cmd_channel_receive_and_ack, _}) ->
    %% When there are multiple active consumers we may receive
    %% messages out of order because the commands are not running
    %% in the same order as the messages sent to channels.
    %%
    %% But because messages can be pending in the mailbox this can
    %% be the case also when we had two consumers and one was
    %% cancelled. So we do not verify the order of messages
    %% when using consume.
    %%
    %% We do not need to add the message to both acked and received
    %% because we always check acked even when crashes occurred.
    St#cq{q=delete_message(Q, Msg), acked=[Msg|Acked]};
next_state(St, _, {call, _, cmd_channel_receive_and_requeue, _}) ->
    St;
next_state(St=#cq{q=Q, acked=Acked}, Msg, {call, _, cmd_channel_receive_and_discard, _}) ->
    %% Rejecting without requeing ends up acking the message
    %% (and possibly dead-lettering) so we handle it the same
    %% as normal acks.
    St#cq{q=delete_message(Q, Msg), acked=[Msg|Acked]};
next_state(St=#cq{q=Q, acked=Acked}, Msgs0, {call, _, cmd_channel_receive_many_and_ack_ooo, _}) ->
    %% We handle {var,_} specially here but it would work for cmd_channel_receive_and_ack as well.
    Msgs = case is_list(Msgs0) of
        true -> Msgs0;
        false -> [Msgs0]
    end,
    St#cq{q=lists:foldl(fun(Msg, QF) -> delete_message(QF, Msg) end, Q, Msgs), acked=Msgs ++ Acked};
next_state(St, _, {call, _, cmd_channel_receive_many_and_requeue_ooo, _}) ->
    St;
next_state(St=#cq{q=Q, acked=Acked}, Msgs0, {call, _, cmd_channel_receive_many_and_discard_ooo, _}) ->
    %% We handle {var,_} specially here but it would work for cmd_channel_receive_and_discard as well.
    Msgs = case is_list(Msgs0) of
        true -> Msgs0;
        false -> [Msgs0]
    end,
    St#cq{q=lists:foldl(fun(Msg, QF) -> delete_message(QF, Msg) end, Q, Msgs), acked=Msgs ++ Acked}.

%% We remove at most one message anywhere in the queue.
delete_message(Qs0, Msg) ->
    {Qs, _} = maps:fold(fun
        (Ch, ChQ, {Qs1, true}) ->
            {Qs1#{Ch => ChQ}, true};
        (Ch, ChQ, {Qs1, false}) ->
            case queue:member(Msg, ChQ) of
                true ->
                    case queue:len(ChQ) of
                        1 ->
                            {Qs1, true};
                        _ ->
                            ChQOut = queue_delete(Msg, ChQ),
                            {Qs1#{Ch => ChQOut}, true}
                    end;
                false ->
                    {Qs1#{Ch => ChQ}, false}
            end
    end, {#{}, false}, Qs0),
    Qs.

%% Preconditions.
%%
%% We cannot rely on the data found in #cq.q here because when we are
%% in a symbolic context we cannot remove the messages from #cq.q
%% (since we are always getting a new {var,integer()}).

precondition(#cq{amq=AMQ}, {call, _, cmd_restart_vhost_clean, _}) ->
    AMQ =/= undefined;
precondition(#cq{amq=AMQ}, {call, _, cmd_restart_queue_dirty, _}) ->
    AMQ =/= undefined;
precondition(#cq{channels=Channels}, {call, _, cmd_channel_confirm_mode, [Ch]}) ->
    %% Only enabled confirms if they were not already enabled.
    %% Otherwise it is a no-op so not a big problem but this
    %% reduces the quality of the test runs.
    maps:get(confirms, maps:get(Ch, Channels)) =:= false;
precondition(#cq{channels=Channels}, {call, _, cmd_channel_wait_for_confirms, [Ch]}) ->
    %% Only wait for confirms when they were enabled.
    maps:get(confirms, maps:get(Ch, Channels)) =:= true;
precondition(#cq{channels=Channels}, {call, _, cmd_channel_basic_get, [_, Ch]}) ->
    %% Using both consume and basic_get is non-deterministic.
    maps:get(consumer, maps:get(Ch, Channels)) =:= none;
precondition(#cq{channels=Channels}, {call, _, cmd_channel_consume, [_, Ch]}) ->
    %% Don't consume if we are already consuming on this channel.
    maps:get(consumer, maps:get(Ch, Channels)) =:= none;
precondition(#cq{channels=Channels}, {call, _, cmd_channel_cancel, [_, Ch]}) ->
    %% Only cancel the consume when we are already consuming on this channel.
    maps:get(consumer, maps:get(Ch, Channels)) =/= none;
precondition(#cq{channels=Channels}, {call, _, cmd_channel_receive_and_ack, [_, Ch]}) ->
    %% Only receive and ack when we are already consuming on this channel.
    maps:get(consumer, maps:get(Ch, Channels)) =/= none;
precondition(#cq{channels=Channels}, {call, _, cmd_channel_receive_and_requeue, [_, Ch]}) ->
    %% Only receive and requeue when we are already consuming on this channel.
    maps:get(consumer, maps:get(Ch, Channels)) =/= none;
precondition(#cq{channels=Channels}, {call, _, cmd_channel_receive_and_discard, [_, Ch]}) ->
    %% Only receive and discard when we are already consuming on this channel.
    maps:get(consumer, maps:get(Ch, Channels)) =/= none;
precondition(#cq{channels=Channels}, {call, _, cmd_channel_receive_many_and_ack_ooo, [_, Ch, _]}) ->
    %% Only receive and ack when we are already consuming on this channel.
    maps:get(consumer, maps:get(Ch, Channels)) =/= none;
precondition(#cq{channels=Channels}, {call, _, cmd_channel_receive_many_and_requeue_ooo, [_, Ch, _]}) ->
    %% Only receive and requeue when we are already consuming on this channel.
    maps:get(consumer, maps:get(Ch, Channels)) =/= none;
precondition(#cq{channels=Channels}, {call, _, cmd_channel_receive_many_and_discard_ooo, [_, Ch, _]}) ->
    %% Only receive and discard when we are already consuming on this channel.
    maps:get(consumer, maps:get(Ch, Channels)) =/= none;
precondition(_, _) ->
    true.

%% Postconditions.

postcondition(_, {call, _, cmd_setup_queue, _}, Q) ->
    element(1, Q) =:= amqqueue;
postcondition(_, {call, _, cmd_restart_vhost_clean, _}, Q) ->
    element(1, Q) =:= amqqueue;
postcondition(_, {call, _, cmd_restart_queue_dirty, _}, Q) ->
    element(1, Q) =:= amqqueue;
postcondition(_, {call, _, cmd_set_v2_check_crc32, _}, Res) ->
    Res =:= ok;
postcondition(#cq{amq=AMQ}, {call, _, cmd_set_mode, [_, Mode]}, _) ->
    do_check_queue_mode(AMQ, Mode) =:= ok;
postcondition(#cq{amq=AMQ}, {call, _, cmd_set_version, [_, Version]}, _) ->
    do_check_queue_version(AMQ, Version) =:= ok;
postcondition(#cq{amq=AMQ}, {call, _, cmd_set_mode_version, [Mode, Version]}, _) ->
    (do_check_queue_mode(AMQ, Mode) =:= ok)
    andalso
    (do_check_queue_version(AMQ, Version) =:= ok);
postcondition(_, {call, _, cmd_publish_msg, _}, Msg) ->
    is_record(Msg, amqp_msg);
postcondition(St, {call, _, cmd_basic_get_msg, _}, empty) ->
    %% We may get 'empty' if there are/were consumers and the messages are
    %% in transit. We only check whether there are channels as a result,
    %% because messages may be in the process of being requeued following
    %% a consumer cancel.
    has_consumers(St) orelse
    %% Due to the asynchronous nature of publishing it may be
    %% possible to have published a message but an immediate basic.get
    %% on a separate channel cannot retrieve it. In that case we accept
    %% an empty return value only if the channel we are calling
    %% basic.get on has no messages published and not consumed.
    not queue_has_channel(St, internal) orelse
    %% When messages can expire they will never be removed from the
    %% property state because we cannot know whether the message
    %% will be received later on (it was in transit when it expired).
    %% Therefore we accept an empty response if all messages
    %% sent via this particular channel have an expiration.
    queue_part_all_expired(St, internal);
postcondition(St, {call, _, cmd_basic_get_msg, _}, Msg) ->
    %% When there are active consumers we may receive
    %% messages out of order because the commands are not running
    %% in the same order as the messages sent to channels.
    case has_consumers(St) of
        true -> queue_has_msg(St, Msg);
        false -> queue_head_has_msg(St, Msg)
    end
    orelse
    %% After a restart, unconfirmed messages and previously
    %% acked messages may or may not be received again, due
    %% to a race condition.
    restarted_and_previously_acked(St, Msg) orelse
    restarted_and_uncertain_publish_status(St, Msg) orelse
    crashed_and_previously_received(St, Msg);
postcondition(_, {call, _, cmd_purge, _}, {ok, _}) ->
    true;
postcondition(_, {call, _, cmd_channel_open, _}, _) ->
    true;
postcondition(_, {call, _, cmd_channel_confirm_mode, _}, _) ->
    true;
postcondition(_, {call, _, cmd_channel_close, _}, Res) ->
    Res =:= ok;
postcondition(_, {call, _, cmd_channel_publish, _}, Msg) ->
    is_record(Msg, amqp_msg);
postcondition(_, {call, _, cmd_channel_publish_many, _}, Msgs) ->
    lists:all(fun(Msg) -> is_record(Msg, amqp_msg) end, Msgs);
postcondition(_, {call, _, cmd_channel_wait_for_confirms, _}, Res) ->
    Res =:= true;
postcondition(St, {call, _, cmd_channel_basic_get, [_, Ch]}, empty) ->
    %% We may get 'empty' if there are consumers and the messages are
    %% in transit.
    has_consumers(St) orelse
    %% Due to the asynchronous nature of publishing it may be
    %% possible to have published a message but an immediate basic.get
    %% on a separate channel cannot retrieve it. In that case we accept
    %% an empty return value only if the channel we are calling
    %% basic.get on has no messages published and not consumed.
    not queue_has_channel(St, Ch) orelse
    %% When messages can expire they will never be removed from the
    %% property state because we cannot know whether the message
    %% will be received later on (it was in transit when it expired).
    %% Therefore we accept an empty response if all messages
    %% sent via this particular channel have an expiration.
    queue_part_all_expired(St, Ch);
postcondition(St, {call, _, cmd_channel_basic_get, _}, Msg) ->
    %% When there are active consumers we may receive
    %% messages out of order because the commands are not running
    %% in the same order as the messages sent to channels.
    case has_consumers(St) of
        true -> queue_has_msg(St, Msg);
        false -> queue_head_has_msg(St, Msg)
    end
    orelse
    %% After a restart, unconfirmed messages and previously
    %% acked messages may or may not be received again, due
    %% to a race condition.
    restarted_and_previously_acked(St, Msg) orelse
    restarted_and_uncertain_publish_status(St, Msg) orelse
    crashed_and_previously_received(St, Msg);
postcondition(_, {call, _, cmd_channel_consume, _}, _) ->
    true;
postcondition(_, {call, _, cmd_channel_cancel, _}, _) ->
    true;
postcondition(_, {call, _, cmd_channel_receive_and_ack, _}, none) ->
    true;
postcondition(St, {call, _, cmd_channel_receive_and_ack, _}, Msg) ->
    %% When there are multiple active consumers we may receive
    %% messages out of order because the commands are not running
    %% in the same order as the messages sent to channels.
    %%
    %% But because messages can be pending in the mailbox this can
    %% be the case also when we had two consumers and one was
    %% cancelled. So we do not verify the order of messages
    %% when using consume.
    queue_has_msg(St, Msg) orelse
    %% After a restart, unconfirmed messages and previously
    %% acked messages may or may not be received again, due
    %% to a race condition.
    restarted_and_previously_acked(St, Msg) orelse
    restarted_and_uncertain_publish_status(St, Msg) orelse
    crashed_and_previously_received(St, Msg);
postcondition(_, {call, _, cmd_channel_receive_and_requeue, _}, none) ->
    true;
postcondition(St, {call, _, cmd_channel_receive_and_requeue, _}, Msg) ->
    %% When there are multiple active consumers we may receive
    %% messages out of order because the commands are not running
    %% in the same order as the messages sent to channels.
    %%
    %% But because messages can be pending in the mailbox this can
    %% be the case also when we had two consumers and one was
    %% cancelled. So we do not verify the order of messages
    %% when using consume.
    queue_has_msg(St, Msg) orelse
    %% After a restart, unconfirmed messages and previously
    %% acked messages may or may not be received again, due
    %% to a race condition.
    restarted_and_previously_acked(St, Msg) orelse
    restarted_and_uncertain_publish_status(St, Msg) orelse
    crashed_and_previously_received(St, Msg);
postcondition(_, {call, _, cmd_channel_receive_and_discard, _}, none) ->
    true;
postcondition(St, {call, _, cmd_channel_receive_and_discard, _}, Msg) ->
    %% When there are multiple active consumers we may receive
    %% messages out of order because the commands are not running
    %% in the same order as the messages sent to channels.
    %%
    %% But because messages can be pending in the mailbox this can
    %% be the case also when we had two consumers and one was
    %% cancelled. So we do not verify the order of messages
    %% when using consume.
    queue_has_msg(St, Msg) orelse
    %% After a restart, unconfirmed messages and previously
    %% acked messages may or may not be received again, due
    %% to a race condition.
    restarted_and_previously_acked(St, Msg) orelse
    restarted_and_uncertain_publish_status(St, Msg) orelse
    crashed_and_previously_received(St, Msg);
postcondition(St, {call, _, cmd_channel_receive_many_and_ack_ooo, _}, Msgs) ->
    lists:all(fun(Msg) ->
        queue_has_msg(St, Msg) orelse
        restarted_and_previously_acked(St, Msg) orelse
        restarted_and_uncertain_publish_status(St, Msg) orelse
        crashed_and_previously_received(St, Msg)
    end, Msgs);
postcondition(St, {call, _, cmd_channel_receive_many_and_requeue_ooo, _}, Msgs) ->
    lists:all(fun(Msg) ->
        queue_has_msg(St, Msg) orelse
        restarted_and_previously_acked(St, Msg) orelse
        restarted_and_uncertain_publish_status(St, Msg) orelse
        crashed_and_previously_received(St, Msg)
    end, Msgs);
postcondition(St, {call, _, cmd_channel_receive_many_and_discard_ooo, _}, Msgs) ->
    lists:all(fun(Msg) ->
        queue_has_msg(St, Msg) orelse
        restarted_and_previously_acked(St, Msg) orelse
        restarted_and_uncertain_publish_status(St, Msg) orelse
        crashed_and_previously_received(St, Msg)
    end, Msgs).

%% This function returns 'true' when there was a consumer at some point
%% even if consumers were recently cancelled or closed.
has_consumers(#cq{consumers=Consumers}) ->
    Consumers.

queue_has_channel(#cq{q=Q}, Ch) ->
    maps:is_key(Ch, Q).

queue_head_has_msg(#cq{q=Qs}, Msg) ->
    maps:fold(fun
        (_, _, true) ->
            true;
        (_, ChQ, _) ->
            Res = queue_fold(fun
                (MsgInQ, false) when MsgInQ =:= Msg ->
                    true;
                (MsgInQ, false) ->
                    case MsgInQ of
                        %% We stop looking at the first message that doesn't have an expiration
                        %% as this is no longer the head of the queue.
                        #amqp_msg{props=#'P_basic'{expiration=undefined}} ->
                            end_of_head;
                        _ ->
                            false
                    end;
                (_, Res0) ->
                    Res0
            end, false, ChQ),
            Res =:= true
    end, false, Qs).

queue_has_msg(#cq{q=Qs}, Msg) ->
    maps:fold(fun
        (_, _, true) ->
            true;
        (_, ChQ, _) ->
            queue:member(Msg, ChQ)
    end, false, Qs).

queue_part_all_expired(#cq{q=Qs}, Key) ->
    queue_all(fun(#amqp_msg{props=#'P_basic'{expiration=Expiration}}) ->
        Expiration =/= undefined
    end, maps:get(Key, Qs)).

%% We may receive messages that were previously received and
%% acked, but the server never received the ack because the
%% vhost was restarting at the same time. Unfortunately the
%% current implementation means that we cannot detect
%% "receive, ack, receive again" errors after a restart.
restarted_and_previously_acked(#cq{restarted=Restarted, acked=Acked}, Msg) ->
    Restarted andalso lists:member(Msg, Acked).

%% Some messages may have been published but lost during the
%% restart (without publisher confirms). Messages we are
%% uncertain about are kept in a separate list after restart.
restarted_and_uncertain_publish_status(#cq{uncertain=Uncertain}, Msg) ->
    lists:member(Msg, Uncertain).

%% We may receive messages that were previously received
%% via basic.get but the server never wrote the ack to disk
%% before the queue process crashed. Unfortunately the
%% current implementation means that we cannot detect
%% "receive, ack, receive again" errors after a crash
%% (because that's something we have to accept when
%% crashes occur).
crashed_and_previously_received(#cq{crashed=Crashed, received=Received}, Msg) ->
    Crashed andalso lists:member(Msg, Received).

%% Helpers.

cmd_setup_queue(St=#cq{name=Name, mode=Mode, version=Version}) ->
    ?DEBUG("~0p", [St]),
    IsDurable = true, %% We want to be able to restart the queue process.
    IsAutoDelete = false,
    %% We cannot use args to set mode/version as the arguments override
    %% the policies and we also want to test policy changes.
    cmd_set_mode_version(Mode, Version),
    Args = [
%        {<<"x-queue-mode">>, longstr, atom_to_binary(Mode, utf8)},
%        {<<"x-queue-version">>, long, Version}
    ],
    QName = rabbit_misc:r(<<"/">>, queue, iolist_to_binary([atom_to_binary(Name, utf8), $_,
                                                            integer_to_binary(erlang:unique_integer([positive]))])),
    {new, AMQ} = rabbit_amqqueue:declare(QName, IsDurable, IsAutoDelete, Args, none, <<"acting-user">>),
    %% We check that the queue was creating with the right mode/version.
    ok = do_check_queue_mode(AMQ, Mode),
    ok = do_check_queue_version(AMQ, Version),
    AMQ.

cmd_teardown_queue(St=#cq{amq=undefined}) ->
    ?DEBUG("~0p", [St]),
    ok;
cmd_teardown_queue(St=#cq{amq=AMQ, channels=Channels}) ->
    ?DEBUG("~0p", [St]),
    %% We must close all channels since we will not be using them anymore.
    %% Otherwise we end up wasting resources and may hit per-(direct)-connection limits.
    %% We ignore noproc/shutdown errors at this step because the channel might have been
    %% closed but the state not updated after the property test fails.
    _ = [try cmd_channel_close(Ch) catch exit:{noproc,_} -> ok; exit:{shutdown,_} -> ok end
        || Ch <- maps:keys(Channels)],
    %% Then we can delete the queue.
    rabbit_amqqueue:delete(AMQ, false, false, <<"acting-user">>),
    rabbit_policy:delete(<<"/">>, <<"queue-mode-version-policy">>, <<"acting-user">>),
    ok.

cmd_restart_vhost_clean(St=#cq{amq=AMQ0}) ->
    ?DEBUG("~0p", [St]),
    rabbit_amqqueue:stop(<<"/">>),
    {Recovered, []} = rabbit_amqqueue:recover(<<"/">>),
    rabbit_amqqueue:start(Recovered),
    %% We must lookup the new AMQ value because the state has changed,
    %% notably the pid of the queue process is now different.
    %%
    %% We cannot use the AMQ found in Recovered directly because it is
    %% in 'stopped' state. We have to look up the most recent value.
    {ok, AMQ} = rabbit_amqqueue:lookup(amqqueue:get_name(AMQ0)),
    AMQ.

cmd_restart_queue_dirty(St=#cq{amq=AMQ}) ->
    ?DEBUG("~0p", [St]),
    Pid = rabbit_amqqueue:pid_of(AMQ),
    exit(Pid, kill),
    %% We must lookup the new AMQ value because the state has changed,
    %% notably the pid of the queue process is now different.
    Res = do_wait_updated_amqqueue(amqqueue:get_name(AMQ), Pid),
    %% We make the channels drop the pending confirms because
    %% they will be lost due to the crash.
    #cq{channels=Channels} = St,
    _ = [
        amqp_channel:wait_for_confirms(Ch, {1, second})
    || {Ch, #{confirms := true}} <- maps:to_list(Channels)],
    Res.

do_wait_updated_amqqueue(Name, Pid) ->
    timer:sleep(1),
    {ok, AMQ} = rabbit_amqqueue:lookup(Name),
    case rabbit_amqqueue:pid_of(AMQ) of
        Pid ->
            do_wait_updated_amqqueue(Name, Pid);
        _ ->
            AMQ
    end.

cmd_set_v2_check_crc32(Value) ->
    application:set_env(rabbit, classic_queue_store_v2_check_crc32, Value).

cmd_set_mode(St=#cq{version=Version}, Mode) ->
    ?DEBUG("~0p ~0p", [St, Mode]),
    do_set_policy(Mode, Version).

%% We loop until the queue has switched mode.
do_check_queue_mode(AMQ, Mode) ->
    do_check_queue_mode(AMQ, Mode, 1000).

do_check_queue_mode(_, _, 0) ->
    error;
do_check_queue_mode(AMQ, Mode, N) ->
    timer:sleep(1),
    [{backing_queue_status, Status}] = rabbit_amqqueue:info(AMQ, [backing_queue_status]),
    case proplists:get_value(mode, Status) of
        Mode -> ok;
        _ -> do_check_queue_mode(AMQ, Mode, N - 1)
    end.

cmd_set_version(St=#cq{mode=Mode}, Version) ->
    ?DEBUG("~0p ~0p", [St, Version]),
    do_set_policy(Mode, Version).

%% We loop until the queue has switched version.
do_check_queue_version(AMQ, Version) ->
    do_check_queue_version(AMQ, Version, 1000).

do_check_queue_version(_, _, 0) ->
    error;
do_check_queue_version(AMQ, Version, N) ->
    timer:sleep(1),
    [{backing_queue_status, Status}] = rabbit_amqqueue:info(AMQ, [backing_queue_status]),
    case proplists:get_value(version, Status) of
        Version -> ok;
        _ -> do_check_queue_version(AMQ, Version, N - 1)
    end.

cmd_set_mode_version(Mode, Version) ->
    ?DEBUG("~0p ~0p", [Mode, Version]),
    do_set_policy(Mode, Version).

do_set_policy(Mode, Version) ->
    rabbit_policy:set(<<"/">>, <<"queue-mode-version-policy">>, <<".*">>,
        [{<<"queue-mode">>, atom_to_binary(Mode, utf8)},
         {<<"queue-version">>, Version}],
        0, <<"queues">>, <<"acting-user">>).

cmd_publish_msg(St=#cq{amq=AMQ}, PayloadSize, DeliveryMode, Mandatory, Expiration) ->
    ?DEBUG("~0p ~0p ~0p ~0p ~0p", [St, PayloadSize, DeliveryMode, Mandatory, Expiration]),
    Payload = do_rand_payload(PayloadSize),
    Msg = rabbit_basic:message(rabbit_misc:r(<<>>, exchange, <<>>),
                               <<>>, #'P_basic'{delivery_mode = DeliveryMode,
                                                expiration = do_encode_expiration(Expiration)},
                               Payload),
    Delivery = #delivery{mandatory = Mandatory, sender = self(),
                         confirm = false, message = Msg, flow = noflow},
    ok = rabbit_amqqueue:deliver([AMQ], Delivery),
    {MsgProps, MsgPayload} = rabbit_basic_common:from_content(Msg#basic_message.content),
    #amqp_msg{props=MsgProps, payload=MsgPayload}.

cmd_basic_get_msg(St=#cq{amq=AMQ, limiter=LimiterPid}) ->
    ?DEBUG("~0p", [St]),
    %% The second argument means that we will not be sending an ack message.
    Res = rabbit_amqqueue:basic_get(AMQ, true, LimiterPid,
                                    <<"cmd_basic_get_msg">>,
                                    rabbit_queue_type:init()),
    case Res of
        {empty, _} ->
            empty;
        {ok, _CountMinusOne, {_QName, _QPid, _AckTag, _IsDelivered, Msg}, _} ->
            {MsgProps, MsgPayload} = rabbit_basic_common:from_content(Msg#basic_message.content),
            #amqp_msg{props=MsgProps, payload=MsgPayload}
    end.

cmd_purge(St=#cq{amq=AMQ}) ->
    ?DEBUG("~0p", [St]),
    rabbit_amqqueue:purge(AMQ).

cmd_channel_open(St=#cq{config=Config}) ->
    ?DEBUG("~0p", [St]),
    Server = rabbit_ct_broker_helpers:get_node_config(Config, 0, nodename),
    rabbit_ct_client_helpers:open_channel(Config, Server).

cmd_channel_confirm_mode(Ch) ->
    ?DEBUG("~0p", [Ch]),
    #'confirm.select_ok'{} = amqp_channel:call(Ch, #'confirm.select'{}),
    ok.

cmd_channel_close(Ch) ->
    ?DEBUG("~0p", [Ch]),
    %% We cannot close the channel with
    %% rabbit_ct_client_helpers:close_channel(Ch)
    %% because the pid is remote (it is in the CT node)
    %% and the helper calls is_process_alive/1.
    %% So instead we close directly.
    amqp_channel:close(Ch).

cmd_channel_publish(St=#cq{amq=AMQ}, Ch, PayloadSize, DeliveryMode, Mandatory, Expiration) ->
    ?DEBUG("~0p ~0p ~0p ~0p ~0p ~0p", [St, Ch, PayloadSize, DeliveryMode, Mandatory, Expiration]),
    #resource{name = Name} = amqqueue:get_name(AMQ),
    Payload = do_rand_payload(PayloadSize),
    Msg = #amqp_msg{props   = #'P_basic'{delivery_mode = DeliveryMode,
                                         expiration = do_encode_expiration(Expiration)},
                    payload = Payload},
    ok = amqp_channel:call(Ch,
                           #'basic.publish'{routing_key = Name,
                                            mandatory = Mandatory},
                           Msg),
    Msg.

cmd_channel_publish_many(St, Ch, Num, PayloadSize, DeliveryMode, Mandatory, Expiration) ->
    ?DEBUG("~0p ~0p ~0p ~0p ~0p ~0p ~0p", [St, Ch, Num, PayloadSize, DeliveryMode, Mandatory, Expiration]),
    [cmd_channel_publish(St, Ch, PayloadSize, DeliveryMode, Mandatory, Expiration) || _ <- lists:seq(1, Num)].

cmd_channel_wait_for_confirms(Ch) ->
    ?DEBUG("~0p", [Ch]),
    amqp_channel:wait_for_confirms(Ch, {1, second}).

cmd_channel_basic_get(St=#cq{amq=AMQ}, Ch) ->
    ?DEBUG("~0p ~0p", [St, Ch]),
    #resource{name = Name} = amqqueue:get_name(AMQ),
    case amqp_channel:call(Ch, #'basic.get'{queue = Name, no_ack = true}) of
        #'basic.get_empty'{} ->
            empty;
        {_GetOk = #'basic.get_ok'{}, Msg} ->
            Msg
    end.

cmd_channel_consume(St=#cq{amq=AMQ}, Ch) ->
    ?DEBUG("~0p ~0p", [St, Ch]),
    %% We register ourselves as a default consumer to avoid race conditions
    %% when we try to cancel the channel after the server has already canceled
    %% it, or when the server sends messages after we have canceled the channel.
    %% This works around a limitation of the current code of the Erlang client.
    ok = amqp_selective_consumer:register_default_consumer(Ch, self()),
    #resource{name = Name} = amqqueue:get_name(AMQ),
    Tag = integer_to_binary(erlang:unique_integer([positive])),
    #'basic.consume_ok'{} =
        amqp_channel:call(Ch,
                          #'basic.consume'{queue = Name, consumer_tag = Tag}),
    receive #'basic.consume_ok'{consumer_tag = Tag} -> ok end,
    Tag.

cmd_channel_cancel(St=#cq{channels=Channels}, Ch) ->
    ?DEBUG("~0p ~0p", [St, Ch]),
    #{consumer := Tag} = maps:get(Ch, Channels),
    #'basic.cancel_ok'{} =
        amqp_channel:call(Ch, #'basic.cancel'{consumer_tag = Tag}),
    receive #'basic.cancel_ok'{consumer_tag = Tag} -> ok end,
    %% We have to requeue the messages in transit to preserve ordering.
    do_receive_requeue_all(Ch, Tag).

cmd_channel_receive_and_ack(St=#cq{channels=Channels}, Ch) ->
    ?DEBUG("~0p ~0p", [St, Ch]),
    #{consumer := Tag} = maps:get(Ch, Channels),
    receive
        {#'basic.deliver'{consumer_tag = Tag,
                          delivery_tag = DeliveryTag}, Msg} ->
            amqp_channel:cast(Ch, #'basic.ack'{delivery_tag = DeliveryTag}),
            Msg
    after 0 ->
        none
    end.

cmd_channel_receive_and_requeue(St=#cq{channels=Channels}, Ch) ->
    ?DEBUG("~0p ~0p", [St, Ch]),
    #{consumer := Tag} = maps:get(Ch, Channels),
    receive
        {#'basic.deliver'{consumer_tag = Tag,
                          delivery_tag = DeliveryTag}, Msg} ->
            amqp_channel:cast(Ch, #'basic.reject'{delivery_tag = DeliveryTag}),
            Msg
    after 0 ->
        none
    end.

cmd_channel_receive_and_discard(St=#cq{channels=Channels}, Ch) ->
    ?DEBUG("~0p ~0p", [St, Ch]),
    #{consumer := Tag} = maps:get(Ch, Channels),
    receive
        {#'basic.deliver'{consumer_tag = Tag,
                          delivery_tag = DeliveryTag}, Msg} ->
            amqp_channel:cast(Ch, #'basic.reject'{delivery_tag = DeliveryTag, requeue=false}),
            Msg
    after 0 ->
        none
    end.

cmd_channel_receive_many_and_ack_ooo(St=#cq{channels=Channels}, Ch, Num) ->
    ?DEBUG("~0p ~0p ~0p", [St, Ch, Num]),
    #{consumer := Tag} = maps:get(Ch, Channels),
    do_cmd_channel_receive_many_and_ack_ooo_loop(Tag, Ch, Num, [], []).

do_cmd_channel_receive_many_and_ack_ooo_loop(_, Ch, 0, TagAcc, MsgAcc) ->
    RandomTagAcc = [T || {_, T} <- lists:sort([{rand:uniform(), T} || T <- TagAcc])],
    _ = [amqp_channel:cast(Ch, #'basic.ack'{delivery_tag = T}) || T <- RandomTagAcc],
    lists:reverse(MsgAcc);
do_cmd_channel_receive_many_and_ack_ooo_loop(Tag, Ch, Num, TagAcc, MsgAcc) ->
    receive
        {#'basic.deliver'{consumer_tag = Tag,
                          delivery_tag = DeliveryTag}, Msg} ->
            do_cmd_channel_receive_many_and_ack_ooo_loop(Tag, Ch, Num - 1, [DeliveryTag|TagAcc], [Msg|MsgAcc])
    after 0 ->
        do_cmd_channel_receive_many_and_ack_ooo_loop(Tag, Ch, 0, TagAcc, MsgAcc)
    end.

cmd_channel_receive_many_and_requeue_ooo(St=#cq{channels=Channels}, Ch, Num) ->
    ?DEBUG("~0p ~0p ~0p", [St, Ch, Num]),
    #{consumer := Tag} = maps:get(Ch, Channels),
    do_cmd_channel_receive_many_and_requeue_ooo_loop(Tag, Ch, Num, [], []).

do_cmd_channel_receive_many_and_requeue_ooo_loop(_, Ch, 0, TagAcc, MsgAcc) ->
    RandomTagAcc = [T || {_, T} <- lists:sort([{rand:uniform(), T} || T <- TagAcc])],
    _ = [amqp_channel:cast(Ch, #'basic.reject'{delivery_tag = T}) || T <- RandomTagAcc],
    lists:reverse(MsgAcc);
do_cmd_channel_receive_many_and_requeue_ooo_loop(Tag, Ch, Num, TagAcc, MsgAcc) ->
    receive
        {#'basic.deliver'{consumer_tag = Tag,
                          delivery_tag = DeliveryTag}, Msg} ->
            do_cmd_channel_receive_many_and_requeue_ooo_loop(Tag, Ch, Num - 1, [DeliveryTag|TagAcc], [Msg|MsgAcc])
    after 0 ->
        do_cmd_channel_receive_many_and_requeue_ooo_loop(Tag, Ch, 0, TagAcc, MsgAcc)
    end.

cmd_channel_receive_many_and_discard_ooo(St=#cq{channels=Channels}, Ch, Num) ->
    ?DEBUG("~0p ~0p ~0p", [St, Ch, Num]),
    #{consumer := Tag} = maps:get(Ch, Channels),
    do_cmd_channel_receive_many_and_discard_ooo_loop(Tag, Ch, Num, [], []).

do_cmd_channel_receive_many_and_discard_ooo_loop(_, Ch, 0, TagAcc, MsgAcc) ->
    RandomTagAcc = [T || {_, T} <- lists:sort([{rand:uniform(), T} || T <- TagAcc])],
    _ = [amqp_channel:cast(Ch, #'basic.reject'{delivery_tag = T, requeue=false}) || T <- RandomTagAcc],
    lists:reverse(MsgAcc);
do_cmd_channel_receive_many_and_discard_ooo_loop(Tag, Ch, Num, TagAcc, MsgAcc) ->
    receive
        {#'basic.deliver'{consumer_tag = Tag,
                          delivery_tag = DeliveryTag}, Msg} ->
            do_cmd_channel_receive_many_and_discard_ooo_loop(Tag, Ch, Num - 1, [DeliveryTag|TagAcc], [Msg|MsgAcc])
    after 0 ->
        do_cmd_channel_receive_many_and_discard_ooo_loop(Tag, Ch, 0, TagAcc, MsgAcc)
    end.

do_receive_requeue_all(Ch, Tag) ->
    receive
        {#'basic.deliver'{consumer_tag = Tag,
                          delivery_tag = DeliveryTag}, _Msg} ->
            amqp_channel:cast(Ch, #'basic.reject'{delivery_tag = DeliveryTag}),
            do_receive_requeue_all(Ch, Tag)
    after 0 ->
        ok
    end.

do_encode_expiration(undefined) -> undefined;
do_encode_expiration(Expiration) -> integer_to_binary(Expiration).

do_rand_payload(PayloadSize) ->
    Prefix = integer_to_binary(erlang:unique_integer([positive])),
    case erlang:function_exported(rand, bytes, 1) of
        true -> iolist_to_binary([Prefix, rand:bytes(PayloadSize)]);
        %% Slower failover for OTP < 24.0.
        false -> iolist_to_binary([Prefix, crypto:strong_rand_bytes(PayloadSize)])
    end.

%% This function was copied from OTP 24 and should be replaced
%% with queue:all/2 when support for OTP 23 is no longer needed.
queue_all(Pred, {R, F}) when is_function(Pred, 1), is_list(R), is_list(F) ->
    lists:all(Pred, F) andalso
    lists:all(Pred, R);
queue_all(Pred, Q) ->
    erlang:error(badarg, [Pred, Q]).

%% This function was copied from OTP 24 and should be replaced
%% with queue:delete/2 when support for OTP 23 is no longer needed.
%%
%% The helper functions delete_front, delete_rear, r2f, f2r
%% can be removed at the same time.
queue_delete(Item, {R0, F0} = Q) when is_list(R0), is_list(F0) ->
    case delete_front(Item, F0) of
        false ->
            case delete_rear(Item, R0) of
                false ->
                    Q;
                [] ->
                    f2r(F0);
                R1 ->
                    {R1, F0}
            end;
        [] ->
            r2f(R0);
        F1 ->
            {R0, F1}
    end;
queue_delete(Item, Q) ->
    erlang:error(badarg, [Item, Q]).

delete_front(Item, [Item|Rest]) ->
    Rest;
delete_front(Item, [X|Rest]) ->
    case delete_front(Item, Rest) of
        false -> false;
        F -> [X|F]
    end;
delete_front(_, []) ->
    false.

delete_rear(Item, [X|Rest]) ->
    case delete_rear(Item, Rest) of
        false when X=:=Item ->
            Rest;
        false ->
            false;
        R ->
            [X|R]
    end;
delete_rear(_, []) ->
    false.

-compile({inline, [{r2f,1},{f2r,1}]}).

%% Move half of elements from R to F, if there are at least three
r2f([]) ->
    {[],[]};
r2f([_]=R) ->
    {[],R};
r2f([X,Y]) ->
    {[X],[Y]};
r2f(List) ->
    {FF,RR} = lists:split(length(List) div 2 + 1, List),
    {FF,lists:reverse(RR, [])}.

%% Move half of elements from F to R, if there are enough
f2r([]) ->
    {[],[]};
f2r([_]=F) ->
    {F,[]};
f2r([X,Y]) ->
    {[Y],[X]};
f2r(List) ->
    {FF,RR} = lists:split(length(List) div 2 + 1, List),
    {lists:reverse(RR, []),FF}.

%% This function was copied from OTP 24 and should be replaced
%% with queue:fold/3 when support for OTP 23 is no longer needed.
queue_fold(Fun, Acc0, {R, F}) when is_function(Fun, 2), is_list(R), is_list(F) ->
    Acc1 = lists:foldl(Fun, Acc0, F),
    lists:foldr(Fun, Acc1, R);
queue_fold(Fun, Acc0, Q) ->
    erlang:error(badarg, [Fun, Acc0, Q]).
