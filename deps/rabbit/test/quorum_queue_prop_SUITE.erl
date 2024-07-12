%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2023 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(quorum_queue_prop_SUITE).
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

-record(qq, {
    amq = undefined :: amqqueue:amqqueue(),
    name :: atom(),

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
    [{group, quorum_queue_tests}].

groups() ->
    [{quorum_queue_tests, [], [
%        manual
        quorum_queue
    ]}].

init_per_suite(Config) ->
    rabbit_ct_helpers:log_environment(),
    rabbit_ct_helpers:run_setup_steps(Config).

end_per_suite(Config) ->
    rabbit_ct_helpers:run_teardown_steps(Config).

init_per_group(Group = quorum_queue_tests, Config) ->
    Config1 = rabbit_ct_helpers:set_config(Config, [
        {rmq_nodename_suffix, Group},
        {rmq_nodes_count, 1},
        {erlang_node_config, [{ra, [
            %% We want Ra to stay alive when we kill a lot of Ra systems.
            {ra_systems_sup_intensity, {1000000, 1}}
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

end_per_group(quorum_queue_tests, Config) ->
    rabbit_ct_helpers:run_steps(Config,
      rabbit_ct_client_helpers:teardown_steps() ++
      rabbit_ct_broker_helpers:teardown_steps()).

%% $ make -C deps/rabbit test-build
%% $ erl -pa deps/rabbit/test
%% > quorum_queue_prop_SUITE:instrs_to_manual([[{init,...},...]]).
%% Paste into do_manual/1.
%% Enable manual as the only test in groups/0.
%% $ make -C deps/rabbit ct-quorum_queue_prop
instrs_to_manual([Instrs]) ->
    io:format("~ndo_manual(Config) ->~n~n"),
    lists:foreach(fun
        ({init, QQ}) ->
            #qq{name=Name} = QQ,
            io:format("    St0 = #qq{name=~0p,~n"
                      "              config=minimal_config(Config)},~n~n",
                      [Name]);
        ({set, {var,Var}, {call, ?MODULE, cmd_setup_queue, _}}) ->
            Res = "Res" ++ integer_to_list(Var),
            PrevSt = "St" ++ integer_to_list(Var - 1),
            St = "St" ++ integer_to_list(Var),
            io:format("    ~s = cmd_setup_queue(~s),~n"
                      "    ~s = ~s#qq{amq=~s},~n~n",
                      [Res, PrevSt, St, PrevSt, Res]);
        ({set, {var,Var}, {call, ?MODULE, Cmd, [#qq{}|Args]}}) ->
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
%do_manual(Config) ->
%    Config =:= Config.
do_manual(Config) ->

    St0 = #qq{name=prop_quorum_queue,
              config=minimal_config(Config)},

    Res1 = cmd_setup_queue(St0),
    St1 = St0#qq{amq=Res1},

    do_manual_loop(St1).

do_manual_loop(St1) ->

    Res2 = cmd_restart_vhost_clean(St1),
    true = postcondition(St1, {call, undefined, cmd_restart_vhost_clean, [St1]}, Res2),
    St2 = next_state(St1, Res2, {call, undefined, cmd_restart_vhost_clean, [St1]}),

    Res3 = cmd_publish_msg(St2, 6, 2, false, undefined),
    true = postcondition(St2, {call, undefined, cmd_publish_msg, [St2, 6, 2, false, undefined]}, Res3),
    St3 = next_state(St2, Res3, {call, undefined, cmd_publish_msg, [St2, 6, 2, false, undefined]}),

    Res4 = cmd_basic_get_msg(St3),
    true = postcondition(St3, {call, undefined, cmd_basic_get_msg, [St3]}, Res4),
    St4 = next_state(St3, Res4, {call, undefined, cmd_basic_get_msg, [St3]}),

    Res5 = cmd_publish_msg(St4, 12, 1, true, undefined),
    true = postcondition(St4, {call, undefined, cmd_publish_msg, [St4, 12, 1, true, undefined]}, Res5),
    St5 = next_state(St4, Res5, {call, undefined, cmd_publish_msg, [St4, 12, 1, true, undefined]}),

    Res6 = cmd_basic_get_msg(St5),
    true = postcondition(St5, {call, undefined, cmd_basic_get_msg, [St5]}, Res6),
    St6 = next_state(St5, Res6, {call, undefined, cmd_basic_get_msg, [St5]}),

    do_manual_loop(St6).

quorum_queue(Config) ->
    true = rabbit_ct_broker_helpers:rpc(Config, 0,
        ?MODULE, do_quorum_queue, [Config]).

do_quorum_queue(Config) ->
    true = proper:quickcheck(prop_quorum_queue(Config),
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

prop_quorum_queue(Config) ->
    {ok, LimiterPid} = rabbit_limiter:start_link(no_id),
    InitialState = #qq{name=?FUNCTION_NAME,
                       config=minimal_config(Config), limiter=LimiterPid},
    prop_common(InitialState).

prop_common(InitialState) ->
    ?FORALL(Commands, commands(?MODULE, InitialState),
        ?TRAPEXIT(try
            {History, State, Result} = run_commands(?MODULE, Commands),
            cmd_teardown_queue(State),
            ?WHENFAIL(logger:error("History: ~p~nState: ~p~nResult: ~p",
                                   [History, State, Result]),
                      aggregate(command_names(Commands), Result =:= ok))
        catch C:E:S ->
            logger:error("Crash: ~p:~p ~p", [C, E, S])
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

command(St = #qq{amq=undefined}) ->
    {call, ?MODULE, cmd_setup_queue, [St]};
command(St) ->
    ChannelCmds = case has_channels(St) of
        false -> [];
        true -> [
            {100, {call, ?MODULE, cmd_channel_confirm_mode, [channel(St)]}},
%            {100, {call, ?MODULE, cmd_channel_close, [channel(St)]}},
            {900, {call, ?MODULE, cmd_channel_publish, [St, channel(St), integer(0, 1024*1024), integer(1, 2), boolean(), expiration()]}},
            {200, {call, ?MODULE, cmd_channel_publish_many, [St, channel(St), integer(2, 512), integer(0, 1024*1024), integer(1, 2), boolean(), expiration()]}},
            {900, {call, ?MODULE, cmd_channel_wait_for_confirms, [channel(St)]}},
            {200, {call, ?MODULE, cmd_channel_basic_get, [St, channel(St)]}},
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
        %% These are direct operations using internal functions.
        { 50, {call, ?MODULE, cmd_publish_msg, [St, integer(0, 1024*1024), integer(1, 2), boolean(), expiration()]}},
        { 50, {call, ?MODULE, cmd_basic_get_msg, [St]}},
        { 10, {call, ?MODULE, cmd_purge, [St]}},
        %% These are channel-based operations.
        {300, {call, ?MODULE, cmd_channel_open, [St]}}
        |ChannelCmds
    ]).

expiration() ->
    oneof([
        undefined,
        integer(0, 100) %% Up to 0.1s to make it more likely to trigger dropping messages.
    ]).

has_channels(#qq{channels=Channels}) ->
    map_size(Channels) > 0.

channel(#qq{channels=Channels}) ->
    elements(maps:keys(Channels)).

%% Next state.

next_state(St, AMQ, {call, _, cmd_setup_queue, _}) ->
    St#qq{amq=AMQ};
next_state(St=#qq{q=Q0, confirmed=Confirmed, uncertain=Uncertain0, channels=Channels0}, AMQ, {call, _, cmd_restart_vhost_clean, _}) ->
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
    St#qq{amq=AMQ, q=Q, restarted=true, uncertain=Uncertain, channels=Channels};
next_state(St=#qq{q=Q0, confirmed=Confirmed, uncertain=Uncertain0}, AMQ, {call, _, cmd_restart_queue_dirty, _}) ->
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
    St#qq{amq=AMQ, q=Q, restarted=true, crashed=true, uncertain=Uncertain};
next_state(St=#qq{q=Q}, Msg, {call, _, cmd_publish_msg, _}) ->
    IntQ = maps:get(internal, Q, queue:new()),
    St#qq{q=Q#{internal => queue:in(Msg, IntQ)}};
next_state(St, empty, {call, _, cmd_basic_get_msg, _}) ->
    St;
next_state(St=#qq{q=Q, received=Received}, Msg, {call, _, cmd_basic_get_msg, _}) ->
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
    St#qq{q=delete_message(Q, Msg), received=[Msg|Received]};
next_state(St=#qq{q=Q, uncertain=Uncertain0}, _, {call, _, cmd_purge, _}) ->
    %% The status of messages that were published before a purge
    %% is uncertain in the same way as for a vhost restart or a
    %% queue crash, because the purge command does not go through
    %% channels. We therefore handle it the same way as a vhost
    %% restart, minus the AMQ change.
    Uncertain = maps:fold(fun(_, ChQ, Acc) ->
        queue:to_list(ChQ) ++ Acc
    end, Uncertain0, Q),
    St#qq{q=#{}, restarted=true, uncertain=Uncertain};
next_state(St=#qq{channels=Channels}, Ch, {call, _, cmd_channel_open, _}) ->
    St#qq{channels=Channels#{Ch => #{consumer => none, confirms => false}}};
next_state(St=#qq{channels=Channels}, _, {call, _, cmd_channel_close, [Ch]}) ->
    St#qq{channels=maps:remove(Ch, Channels)};
next_state(St=#qq{q=Q, unconfirmed=Unconfirmed, channels=Channels}, _, {call, _, cmd_channel_confirm_mode, [Ch]}) ->
    %% All persistent messages sent before we enabled confirm mode will not be
    %% confirmed retroactively. We therefore need to keep track of them to avoid
    %% marking them as confirmed later on.
    ChQ = maps:get(Ch, Q, queue:new()),
    Persistent = [Msg || Msg = #amqp_msg{props=#'P_basic'{delivery_mode=2}} <- queue:to_list(ChQ)],
    ChInfo = maps:get(Ch, Channels),
    St#qq{unconfirmed=Persistent ++ Unconfirmed,
          channels=Channels#{Ch => ChInfo#{confirms => true}}};
next_state(St=#qq{q=Q}, Msg, {call, _, cmd_channel_publish, [_, Ch|_]}) ->
    ChQ = maps:get(Ch, Q, queue:new()),
    St#qq{q=Q#{Ch => queue:in(Msg, ChQ)}};
next_state(St=#qq{q=Q}, Msgs0, {call, _, cmd_channel_publish_many, [_, Ch|_]}) ->
    %% We handle {var,_} specially here but it would work for cmd_channel_publish as well.
    Msgs = case is_list(Msgs0) of
        true -> Msgs0;
        false -> [Msgs0]
    end,
    ChQ = maps:get(Ch, Q, queue:new()),
    St#qq{q=Q#{Ch => queue:from_list(queue:to_list(ChQ) ++ Msgs)}};
next_state(St=#qq{q=Q, confirmed=Confirmed, unconfirmed=Unconfirmed}, _, {call, _, cmd_channel_wait_for_confirms, [Ch]}) ->
    %% All messages sent on the channel were confirmed. We move them
    %% to the list of confirmed messages. We might end up with
    %% duplicates in the confirmed list because we might wait
    %% for confirms multiple times before we retrieve them,
    %% but that's okay because we only need to check that the
    %% message is present when the queue gets restarted.
    ChQ = maps:get(Ch, Q, queue:new()),
    %% We only keep persistent messages. Messages in Confirmed are always persistent.
    Persistent = [Msg || Msg = #amqp_msg{props=#'P_basic'{delivery_mode=2}} <- queue:to_list(ChQ), not lists:member(Msg, Unconfirmed)],
    St#qq{confirmed=Persistent ++ Confirmed};
next_state(St, empty, {call, _, cmd_channel_basic_get, _}) ->
    St;
next_state(St=#qq{q=Q, received=Received}, Msg, {call, _, cmd_channel_basic_get, _}) ->
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
    St#qq{q=delete_message(Q, Msg), received=[Msg|Received]};
next_state(St=#qq{channels=Channels}, Tag, {call, _, cmd_channel_consume, [_, Ch]}) ->
    ChInfo = maps:get(Ch, Channels),
    St#qq{consumers=true, channels=Channels#{Ch => ChInfo#{consumer => Tag}}};
next_state(St=#qq{channels=Channels}, _, {call, _, cmd_channel_cancel, [_, Ch]}) ->
    ChInfo = maps:get(Ch, Channels),
    St#qq{channels=Channels#{Ch => ChInfo#{consumer => none}}};
next_state(St, none, {call, _, cmd_channel_receive_and_ack, _}) ->
    St;
next_state(St=#qq{q=Q, acked=Acked}, Msg, {call, _, cmd_channel_receive_and_ack, _}) ->
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
    St#qq{q=delete_message(Q, Msg), acked=[Msg|Acked]};
next_state(St, _, {call, _, cmd_channel_receive_and_requeue, _}) ->
    St;
next_state(St=#qq{q=Q, acked=Acked}, Msg, {call, _, cmd_channel_receive_and_discard, _}) ->
    %% Rejecting without requeing ends up acking the message
    %% (and possibly dead-lettering) so we handle it the same
    %% as normal acks.
    St#qq{q=delete_message(Q, Msg), acked=[Msg|Acked]};
next_state(St=#qq{q=Q, acked=Acked}, Msgs0, {call, _, cmd_channel_receive_many_and_ack_ooo, _}) ->
    %% We handle {var,_} specially here but it would work for cmd_channel_receive_and_ack as well.
    Msgs = case is_list(Msgs0) of
        true -> Msgs0;
        false -> [Msgs0]
    end,
    St#qq{q=lists:foldl(fun(Msg, QF) -> delete_message(QF, Msg) end, Q, Msgs), acked=Msgs ++ Acked};
next_state(St, _, {call, _, cmd_channel_receive_many_and_requeue_ooo, _}) ->
    St;
next_state(St=#qq{q=Q, acked=Acked}, Msgs0, {call, _, cmd_channel_receive_many_and_discard_ooo, _}) ->
    %% We handle {var,_} specially here but it would work for cmd_channel_receive_and_discard as well.
    Msgs = case is_list(Msgs0) of
        true -> Msgs0;
        false -> [Msgs0]
    end,
    St#qq{q=lists:foldl(fun(Msg, QF) -> delete_message(QF, Msg) end, Q, Msgs), acked=Msgs ++ Acked}.

%% We remove at most one message anywhere in the queue.
delete_message(Qs0, Msg0) ->
    Msg = try element(1, Msg0) of
        amqp_msg -> msg_without_headers(Msg0);
        _ -> Msg0
    catch _:_ ->
        Msg0
    end,
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
%% We cannot rely on the data found in #qq.q here because when we are
%% in a symbolic context we cannot remove the messages from #qq.q
%% (since we are always getting a new {var,integer()}).

precondition(#qq{amq=AMQ}, {call, _, cmd_restart_vhost_clean, _}) ->
    AMQ =/= undefined;
precondition(#qq{amq=AMQ}, {call, _, cmd_restart_queue_dirty, _}) ->
    AMQ =/= undefined;
precondition(#qq{channels=Channels}, {call, _, cmd_channel_confirm_mode, [Ch]}) ->
    %% Only enabled confirms if they were not already enabled.
    %% Otherwise it is a no-op so not a big problem but this
    %% reduces the quality of the test runs.
    maps:get(confirms, maps:get(Ch, Channels)) =:= false;
precondition(#qq{channels=Channels}, {call, _, cmd_channel_wait_for_confirms, [Ch]}) ->
    %% Only wait for confirms when they were enabled.
    maps:get(confirms, maps:get(Ch, Channels)) =:= true;
precondition(#qq{channels=Channels}, {call, _, Cmd, [_, Ch]}) when
        %% Using both consume and basic_get is non-deterministic.
        Cmd =:= cmd_channel_basic_get;
        %% Don't consume if we are already consuming on this channel.
        Cmd =:= cmd_channel_consume ->
    maps:get(consumer, maps:get(Ch, Channels)) =:= none;
precondition(#qq{channels=Channels}, {call, _, Cmd, [_, Ch|_]}) when
        %% Only cancel/receive when we are already consuming on this channel.
        Cmd =:= cmd_channel_cancel; Cmd =:= cmd_channel_receive_and_ack;
        Cmd =:= cmd_channel_receive_and_requeue; Cmd =:= cmd_channel_receive_and_discard;
        Cmd =:= cmd_channel_receive_many_and_ack_ooo; Cmd =:= cmd_channel_receive_many_and_requeue_ooo;
        Cmd =:= cmd_channel_receive_many_and_discard_ooo ->
    maps:get(consumer, maps:get(Ch, Channels)) =/= none;
precondition(_, _) ->
    true.

%% Postconditions.

postcondition(_, {call, _, Cmd, _}, Q) when
        Cmd =:= cmd_setup_queue; Cmd =:= cmd_restart_vhost_clean; Cmd =:= cmd_restart_queue_dirty ->
    element(1, Q) =:= amqqueue;
postcondition(_, {call, _, cmd_publish_msg, _}, Msg) ->
    is_record(Msg, amqp_msg);
postcondition(_, {call, _, cmd_purge, _}, Res) ->
    element(1, Res) =:= ok;
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
    %% It is possible for nacks to be sent during restarts.
    %% This is a rare event but it is not a bug, the client
    %% is simply expected to take action. Timeouts are
    %% always a bug however (acks/nacks not sent at all).
    Res =:= true orelse Res =:= false;
postcondition(_, {call, _, cmd_channel_consume, _}, _) ->
    true;
postcondition(_, {call, _, cmd_channel_cancel, _}, _) ->
    true;
postcondition(_, {call, _, Cmd, _}, empty) when
        Cmd =:= cmd_basic_get_msg; Cmd =:= cmd_channel_basic_get ->
    %% Some command have a lower priority in QQs so it is not
    %% possible to accurately describe whether a basic.get will
    %% return results or which results it would return.
    true;
postcondition(St, {call, _, Cmd, _}, Msg) when
        Cmd =:= cmd_basic_get_msg; Cmd =:= cmd_channel_basic_get ->
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
postcondition(_, {call, _, Cmd, _}, none) when
        Cmd =:= cmd_channel_receive_and_ack;
        Cmd =:= cmd_channel_receive_and_requeue;
        Cmd =:= cmd_channel_receive_and_discard ->
    true;
postcondition(St, {call, _, Cmd, _}, Msg) when
        Cmd =:= cmd_channel_receive_and_ack;
        Cmd =:= cmd_channel_receive_and_requeue;
        Cmd =:= cmd_channel_receive_and_discard ->
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
postcondition(St, {call, _, Cmd, _}, Msgs) when
        Cmd =:= cmd_channel_receive_many_and_ack_ooo;
        Cmd =:= cmd_channel_receive_many_and_requeue_ooo;
        Cmd =:= cmd_channel_receive_many_and_discard_ooo ->
    lists:all(fun(Msg) ->
        queue_has_msg(St, Msg) orelse
        restarted_and_previously_acked(St, Msg) orelse
        restarted_and_uncertain_publish_status(St, Msg) orelse
        crashed_and_previously_received(St, Msg)
    end, Msgs).

%% This function returns 'true' when there was a consumer at some point
%% even if consumers were recently cancelled or closed.
has_consumers(#qq{consumers=Consumers}) ->
    Consumers.

queue_has_channel(#qq{q=Q}, Ch) ->
    maps:is_key(Ch, Q).

queue_head_has_msg(#qq{q=Qs}, Msg0) ->
    Msg = msg_without_headers(Msg0),
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

queue_has_msg(#qq{q=Qs}, Msg0) ->
    Msg = msg_without_headers(Msg0),
    maps:fold(fun
        (_, _, true) ->
            true;
        (_, ChQ, _) ->
            queue:member(Msg, ChQ)
    end, false, Qs).

queue_part_all_expired(#qq{q=Qs}, Key) ->
    queue_all(fun(#amqp_msg{props=#'P_basic'{expiration=Expiration}}) ->
        Expiration =/= undefined
    end, maps:get(Key, Qs)).

%% We may receive messages that were previously received and
%% acked, but the server never received the ack because the
%% vhost was restarting at the same time. Unfortunately the
%% current implementation means that we cannot detect
%% "receive, ack, receive again" errors after a restart.
restarted_and_previously_acked(#qq{restarted=Restarted, acked=Acked}, Msg0) ->
    Msg = msg_without_headers(Msg0),
    Restarted andalso lists:member(Msg, Acked).

%% Some messages may have been published but lost during the
%% restart (without publisher confirms). Messages we are
%% uncertain about are kept in a separate list after restart.
restarted_and_uncertain_publish_status(#qq{uncertain=Uncertain}, Msg0) ->
    Msg = msg_without_headers(Msg0),
    lists:member(Msg, Uncertain).

%% We may receive messages that were previously received
%% via basic.get but the server never wrote the ack to disk
%% before the queue process crashed. Unfortunately the
%% current implementation means that we cannot detect
%% "receive, ack, receive again" errors after a crash
%% (because that's something we have to accept when
%% crashes occur).
crashed_and_previously_received(#qq{crashed=Crashed, received=Received}, Msg0) ->
    Msg = msg_without_headers(Msg0),
    Crashed andalso lists:member(Msg, Received).

%% We remove headers when checking for messages because quorum queues
%% implement x-delivery-count and we cannot do a simple lists:member/2
%% of the message when messages get resent and this value is increased.
msg_without_headers(Msg=#amqp_msg{props=Props}) ->
    Msg#amqp_msg{props=Props#'P_basic'{headers=undefined}}.

%% Helpers.

cmd_setup_queue(St=#qq{name=Name}) ->
    ?DEBUG("~0p", [St]),
    IsDurable = true, %% We want to be able to restart the queue process.
    IsAutoDelete = false,
    Args = [
        {<<"x-queue-type">>, longstr, <<"quorum">>}
    ],
    QName = rabbit_misc:r(<<"/">>, queue, iolist_to_binary([atom_to_binary(Name, utf8), $_,
                                                            integer_to_binary(erlang:unique_integer([positive]))])),
    {new, AMQ} = rabbit_amqqueue:declare(QName, IsDurable, IsAutoDelete, Args, none, <<"acting-user">>),
    AMQ.

cmd_teardown_queue(St=#qq{amq=undefined}) ->
    ?DEBUG("~0p", [St]),
    ok;
cmd_teardown_queue(St=#qq{amq=AMQ, channels=Channels}) ->
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

cmd_restart_vhost_clean(St=#qq{amq=AMQ0}) ->
    ?DEBUG("~0p", [St]),
%    Pid = whereis(element(1,rabbit_amqqueue:pid_of(AMQ0))),
%    dbg:tracer(process, {fun (Msg, _) -> file:write_file("/tmp/out.txt", io_lib:format("~p~n", [Msg]), [append]) end, ok}),
%    dbg:p(Pid, [m, p, s, r]),
%    timer:sleep(1000),
    rabbit_amqqueue:stop(<<"/">>),
    {Recovered, []} = rabbit_amqqueue:recover(<<"/">>),
    %% @todo This one doesn't do anything for quorum.
    rabbit_amqqueue:start(Recovered),
    %% We must lookup the new AMQ value because the state has changed,
    %% notably the pid of the queue process is now different.
    %%
    %% We cannot use the AMQ found in Recovered directly because it is
    %% in 'stopped' state. We have to look up the most recent value.
    {ok, AMQ} = rabbit_amqqueue:lookup(amqqueue:get_name(AMQ0)),
    Pid2 = whereis(element(1,rabbit_amqqueue:pid_of(AMQ0))),
    do_wait_leader_ra_server(Pid2),
%    dbg:p(Pid2, [m, p, s, r]),
%    logger:error("cmd_restart_vhost_clean: ~p -> ~p", [Pid, Pid2]),
    AMQ. %% @todo This doesn't change. We don't need to return it.

cmd_restart_queue_dirty(St=#qq{amq=AMQ}) ->
    ?DEBUG("~0p", [St]),
    Pid = whereis(element(1, rabbit_amqqueue:pid_of(AMQ))),
    [{_, SystemSup, supervisor, _}] = [Child || Child={quorum_queues, _, _, _} <- supervisor:which_children(ra_systems_sup)],
    AllPidsToKill = [SystemSup] ++ lists:flatten(do_get_all_system_pids(SystemSup)),
    _ = [sys:suspend(P) || P <- AllPidsToKill],
    _ = [exit(P, kill) || P <- AllPidsToKill],
    do_wait_stopped_ra_server(Pid),
    do_wait_started_ra_server_sup_sup(),
    ok = ra:restart_server(quorum_queues, rabbit_amqqueue:pid_of(AMQ)),
    %% We must wait for the new ra_server_proc to restart before
    %% we can issue certain commands, like dirty restarts.
    Pid2 = do_wait_restarted_ra_server(rabbit_amqqueue:pid_of(AMQ)),
    do_wait_leader_ra_server(Pid2),
    %% We make the channels drop the pending confirms because
    %% they will be lost due to the crash.
    #qq{channels=Channels} = St,
    _ = [
        amqp_channel:wait_for_confirms(Ch, {1, second})
    || {Ch, #{confirms := true}} <- maps:to_list(Channels)],
    AMQ. %% @todo This doesn't change. We don't need to return it.

do_get_all_system_pids(Sup) ->
    [case Child of
        {_, Pid, worker, _} -> Pid;
        {_, Pid, supervisor, _} -> [Pid] ++ do_get_all_system_pids(Pid)
    end || Child <- supervisor:which_children(Sup)].

do_wait_stopped_ra_server(Pid) ->
    timer:sleep(1),
    case is_process_alive(Pid) of
        true -> do_wait_stopped_ra_server(Pid);
        false -> ok
    end.

do_wait_started_ra_server_sup_sup() ->
    timer:sleep(1),
    case whereis(ra_server_sup_sup) of
        undefined -> do_wait_started_ra_server_sup_sup();
        _ -> ok
    end.

do_wait_restarted_ra_server(Name) ->
    timer:sleep(1),
    case whereis(element(1, Name)) of
        undefined ->
            do_wait_restarted_ra_server(Name);
        RestartedPid ->
            RestartedPid
    end.

do_wait_leader_ra_server(Pid) ->
    case ra_server_proc:ping(Pid, 1000) of
        {pong, leader} ->
            ok;
        {pong, follower} ->
            do_wait_leader_ra_server(Pid)
    end.

cmd_publish_msg(St=#qq{amq=AMQ}, PayloadSize, DeliveryMode, Mandatory, Expiration) ->
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

cmd_basic_get_msg(St=#qq{amq=AMQ, limiter=LimiterPid}) ->
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

cmd_purge(St=#qq{amq=AMQ}) ->
    ?DEBUG("~0p", [St]),
    rabbit_amqqueue:purge(AMQ).

cmd_channel_open(St=#qq{config=Config}) ->
    ?DEBUG("~0p", [St]),
    Server = rabbit_ct_broker_helpers:get_node_config(Config, 0, nodename),
    Ch = rabbit_ct_client_helpers:open_channel(Config, Server),
    rpc:call(node(Ch), dbg, tracer, [process, {fun (Msg, _) -> file:write_file("/tmp/out.txt", io_lib:format("~p~n", [Msg]), [append]) end, ok}]),
    rpc:call(node(Ch), dbg, p, [Ch, [m, p, s, r]]),
    Ch.

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

cmd_channel_publish(St=#qq{amq=AMQ}, Ch, PayloadSize, DeliveryMode, Mandatory, Expiration) ->
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

cmd_channel_basic_get(St=#qq{amq=AMQ}, Ch) ->
    ?DEBUG("~0p ~0p", [St, Ch]),
    #resource{name = Name} = amqqueue:get_name(AMQ),
    case amqp_channel:call(Ch, #'basic.get'{queue = Name, no_ack = true}) of
        #'basic.get_empty'{} ->
            empty;
        {_GetOk = #'basic.get_ok'{}, Msg} ->
            Msg
    end.

cmd_channel_consume(St=#qq{amq=AMQ}, Ch) ->
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

cmd_channel_cancel(St=#qq{channels=Channels}, Ch) ->
    ?DEBUG("~0p ~0p", [St, Ch]),
    #{consumer := Tag} = maps:get(Ch, Channels),
    #'basic.cancel_ok'{} =
        amqp_channel:call(Ch, #'basic.cancel'{consumer_tag = Tag}),
    receive #'basic.cancel_ok'{consumer_tag = Tag} -> ok end,
    %% We have to requeue the messages in transit to preserve ordering.
    do_receive_requeue_all(Ch, Tag).

cmd_channel_receive_and_ack(St=#qq{channels=Channels}, Ch) ->
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

cmd_channel_receive_and_requeue(St=#qq{channels=Channels}, Ch) ->
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

cmd_channel_receive_and_discard(St=#qq{channels=Channels}, Ch) ->
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

cmd_channel_receive_many_and_ack_ooo(St=#qq{channels=Channels}, Ch, Num) ->
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

cmd_channel_receive_many_and_requeue_ooo(St=#qq{channels=Channels}, Ch, Num) ->
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

cmd_channel_receive_many_and_discard_ooo(St=#qq{channels=Channels}, Ch, Num) ->
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
