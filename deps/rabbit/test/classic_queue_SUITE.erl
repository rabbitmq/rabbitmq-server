%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2021 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(classic_queue_SUITE).
-compile(export_all).

-define(NUM_TESTS, 500).

-include_lib("amqp_client/include/amqp_client.hrl").
-include_lib("proper/include/proper.hrl").

-record(cq, {
    amq = undefined :: amqqueue:amqqueue(),
    name :: atom(),
    mode :: classic | lazy,
    version :: 1 | 2,
    %% @todo durable?
    %% @todo auto_delete?

    %% We have one queue per way of publishing messages (such as channels).
    %% We can only confirm the publish order on a per-channel level because
    %% the order is non-deterministic when multiple publishers concurrently
    %% publish.
    q = #{} :: #{pid() | internal => queue:queue()},

    %% CT config necessary to open channels.
    config = no_config :: list(),

    %% Channels.
    channels = #{} %% #{Ch => #{more info}}
}).

%% Common Test.

all() ->
    [{group, classic_queue_tests}].

groups() ->
    [{classic_queue_tests, [], [
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
        {rmq_nodes_count, 1}
      ]),
    rabbit_ct_helpers:run_steps(Config1,
      rabbit_ct_broker_helpers:setup_steps() ++
      rabbit_ct_client_helpers:setup_steps()).

end_per_group(classic_queue_tests, Config) ->
    rabbit_ct_helpers:run_steps(Config,
      rabbit_ct_client_helpers:teardown_steps() ++
      rabbit_ct_broker_helpers:teardown_steps()).
 
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
    InitialState = #cq{name=?FUNCTION_NAME, mode=default, version=1,
                       config=minimal_config(Config)},
    prop_common(InitialState).

prop_lazy_queue_v1(Config) ->
    InitialState = #cq{name=?FUNCTION_NAME, mode=lazy, version=1,
                       config=minimal_config(Config)},
    prop_common(InitialState).

prop_classic_queue_v2(Config) ->
    InitialState = #cq{name=?FUNCTION_NAME, mode=default, version=2,
                       config=minimal_config(Config)},
    prop_common(InitialState).

prop_lazy_queue_v2(Config) ->
    InitialState = #cq{name=?FUNCTION_NAME, mode=lazy, version=2,
                       config=minimal_config(Config)},
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

%commands:
%   kill
%   terminate
%   recover
%   ack
%   reject
%   consume
%   cancel
%   delete
%   requeue
%   ttl behavior: how to test this?
%   change CRC configuration

command(St = #cq{amq=undefined}) ->
    {call, ?MODULE, cmd_setup_queue, [St]};
command(St) ->
    ChannelCmds = case has_channels(St) of
        false -> [];
        true -> [
            {100, {call, ?MODULE, cmd_channel_close, [channel(St)]}},
            {900, {call, ?MODULE, cmd_channel_publish, [St, channel(St), integer(0, 1024*1024), boolean(), expiration()]}},
%           %% channel enable confirm mode
%            {300, {call, ?MODULE, cmd_channel_await_publisher_confirms, [channel(St)]}},
            {300, {call, ?MODULE, cmd_channel_basic_get, [St, channel(St)]}},
            {300, {call, ?MODULE, cmd_channel_consume, [St, channel(St)]}},
            {100, {call, ?MODULE, cmd_channel_cancel, [St, channel(St)]}},
            {900, {call, ?MODULE, cmd_channel_receive_and_ack, [St, channel(St)]}},
            {900, {call, ?MODULE, cmd_channel_receive_and_reject, [St, channel(St)]}}
            %% channel ack out of order?
        ]
    end,
    weighted_union([
        %% delete/recreate queue
        %% dirty_restart
        %% clean_restart: will have to account for channels being open!
        {100, {call, ?MODULE, cmd_set_mode, [St, oneof([default, lazy])]}},
        {100, {call, ?MODULE, cmd_set_version, [St, oneof([1, 2])]}},
        {100, {call, ?MODULE, cmd_set_mode_version, [oneof([default, lazy]), oneof([1, 2])]}},
        %% These are direct publish/basic_get(autoack)/purge.
        {100, {call, ?MODULE, cmd_publish_msg, [St, integer(0, 1024*1024), boolean(), boolean(), expiration()]}},
        {100, {call, ?MODULE, cmd_basic_get_msg, [St]}},
%        {100, {call, ?MODULE, cmd_purge, [St]}},
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
next_state(St, _, {call, _, cmd_set_mode, [_, Mode]}) ->
    St#cq{mode=Mode};
next_state(St, _, {call, _, cmd_set_version, [_, Version]}) ->
    St#cq{version=Version};
next_state(St, _, {call, _, cmd_set_mode_version, [Mode, Version]}) ->
    St#cq{mode=Mode, version=Version};
next_state(St=#cq{q=Q}, Msg, {call, _, cmd_publish_msg, _}) ->
    IntQ = maps:get(internal, Q, queue:new()),
    St#cq{q=Q#{internal => queue:in(Msg, IntQ)}};
%% @todo Special case 'empty' as an optimisation?
next_state(St=#cq{q=Q}, Msg, {call, _, cmd_basic_get_msg, _}) ->
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
    St#cq{q=queue_delete(Q, Msg)};
next_state(St, _, {call, _, cmd_purge, _}) ->
    St#cq{q=#{}};
next_state(St=#cq{channels=Channels}, Ch, {call, _, cmd_channel_open, _}) ->
    St#cq{channels=Channels#{Ch => idle}}; %% @todo A record instead of 'idle' | {'consume', Tag}?
next_state(St=#cq{channels=Channels}, _, {call, _, cmd_channel_close, [Ch]}) ->
    %% @todo What about publisher confirms?
    %% @todo What about messages we are currently in the process of receiving?
    St#cq{channels=maps:remove(Ch, Channels)};
next_state(St=#cq{q=Q}, Msg, {call, _, cmd_channel_publish, [_, Ch|_]}) ->
    %% @todo If in confirms mode, we need to keep track of things.
    %% Otherwise just queue the message as normal.
    ChQ = maps:get(Ch, Q, queue:new()),
    St#cq{q=Q#{Ch => queue:in(Msg, ChQ)}};
%% @todo Special case 'empty' as an optimisation?
next_state(St=#cq{q=Q}, Msg, {call, _, cmd_channel_basic_get, _}) ->
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
    St#cq{q=queue_delete(Q, Msg)};
next_state(St=#cq{channels=Channels}, Tag, {call, _, cmd_channel_consume, [_, Ch]}) ->
    St#cq{channels=Channels#{Ch => {consume, Tag}}};
next_state(St=#cq{channels=Channels}, _, {call, _, cmd_channel_cancel, [_, Ch]}) ->
    St#cq{channels=Channels#{Ch => idle}};
next_state(St, none, {call, _, cmd_channel_receive_and_ack, _}) ->
    St;
next_state(St=#cq{q=Q}, Msg, {call, _, cmd_channel_receive_and_ack, _}) ->
    %% When there are multiple active consumers we may receive
    %% messages out of order because the commands are not running
    %% in the same order as the messages sent to channels.
    %%
    %% But because messages can be pending in the mailbox this can
    %% be the case also when we had two consumers and one was
    %% cancelled. So we do not verify the order of messages
    %% when using consume.
    St#cq{q=queue_delete(Q, Msg)};
next_state(St, _, {call, _, cmd_channel_receive_and_reject, _}) ->
    St.

%% @todo This function is not working in a symbolic context.
%%       We cannot rely on Q for driving preconditions as a result.
%%
%% We remove at most one message anywhere in the queue.
queue_delete(Qs0, Msg) ->
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
                            ChQOut = queue:delete(Msg, ChQ),
                            {Qs1#{Ch => ChQOut}, true}
                    end;
                false ->
                    {Qs1#{Ch => ChQ}, false}
            end
    end, {#{}, false}, Qs0),
    Qs.

%% Preconditions.

precondition(St, {call, _, cmd_channel_close, _}) ->
    has_channels(St);
precondition(St, {call, _, cmd_channel_publish, _}) ->
    has_channels(St);
precondition(#cq{channels=Channels}, {call, _, cmd_channel_basic_get, [_, Ch]}) ->
    %% Using both consume and basic_get is non-deterministic.
    maps:get(Ch, Channels) =:= idle;
precondition(#cq{channels=Channels}, {call, _, cmd_channel_consume, [_, Ch]}) ->
    %% Don't consume if we are already consuming on this channel.
    maps:get(Ch, Channels) =:= idle;
precondition(#cq{channels=Channels}, {call, _, cmd_channel_cancel, [_, Ch]}) ->
    %% Only cancel the consume when we are already consuming on this channel.
    maps:get(Ch, Channels) =/= idle;
precondition(#cq{channels=Channels}, {call, _, cmd_channel_receive_and_ack, [_, Ch]}) ->
    %% Only receive and ack when we are already consuming on this channel.
    maps:get(Ch, Channels) =/= idle;
precondition(#cq{channels=Channels}, {call, _, cmd_channel_receive_and_reject, [_, Ch]}) ->
    %% Only receive and reject when we are already consuming on this channel.
    maps:get(Ch, Channels) =/= idle;
precondition(_, _) ->
    true.

%% Postconditions.

postcondition(_, {call, _, cmd_setup_queue, _}, Q) ->
    element(1, Q) =:= amqqueue;
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
postcondition(St=#cq{q=Q}, {call, _, cmd_basic_get_msg, _}, empty) ->
    %% We may get 'empty' if there are/were consumers and the messages are
    %% in transit. We only check whether there are channels as a result,
    %% because messages may be in the process of being rejected following
    %% a consumer cancel.
    has_channels(St) orelse
    %% Due to the asynchronous nature of publishing it may be
    %% possible to have published a message but an immediate basic.get
    %% on a separate channel cannot retrieve it. In that case we accept
    %% an empty return value only if the channel we are calling
    %% basic.get on has no messages published and not consumed.
    not maps:is_key(internal, Q) orelse
    %% When messages can expire they will never be removed from the
    %% property state because we cannot know whether the message
    %% will be received later on (it was in transit when it expired).
    %% Therefore we accept an empty response if all messages
    %% sent via this particular channel have an expiration.
    queue_part_all_expired(Q, internal);
postcondition(St=#cq{q=Q}, {call, _, cmd_basic_get_msg, _}, Msg) ->
    %% When there are active consumers we may receive
    %% messages out of order because the commands are not running
    %% in the same order as the messages sent to channels.
    case has_consumers(St) of
        true -> queue_has_msg(Q, Msg);
        false -> queue_head_has_msg(Q, Msg)
    end;
postcondition(_, {call, _, cmd_purge, _}, {ok, _}) ->
    true;
postcondition(_, {call, _, cmd_channel_open, _}, _) ->
    true;
postcondition(_, {call, _, cmd_channel_close, _}, Res) ->
    Res =:= ok;
postcondition(_, {call, _, cmd_channel_publish, _}, Msg) ->
    is_record(Msg, amqp_msg);
postcondition(St=#cq{q=Q}, {call, _, cmd_channel_basic_get, [_, Ch]}, empty) ->
    %% We may get 'empty' if there are consumers and the messages are
    %% in transit.
    has_consumers(St) orelse
    %% Due to the asynchronous nature of publishing it may be
    %% possible to have published a message but an immediate basic.get
    %% on a separate channel cannot retrieve it. In that case we accept
    %% an empty return value only if the channel we are calling
    %% basic.get on has no messages published and not consumed.
    not maps:is_key(Ch, Q) orelse
    %% When messages can expire they will never be removed from the
    %% property state because we cannot know whether the message
    %% will be received later on (it was in transit when it expired).
    %% Therefore we accept an empty response if all messages
    %% sent via this particular channel have an expiration.
    queue_part_all_expired(Q, Ch);
postcondition(St=#cq{q=Q}, {call, _, cmd_channel_basic_get, _}, Msg) ->
    %% When there are active consumers we may receive
    %% messages out of order because the commands are not running
    %% in the same order as the messages sent to channels.
    case has_consumers(St) of
        true -> queue_has_msg(Q, Msg);
        false -> queue_head_has_msg(Q, Msg)
    end;
postcondition(_, {call, _, cmd_channel_consume, _}, _) ->
    true;
postcondition(_, {call, _, cmd_channel_cancel, _}, _) ->
    true;
postcondition(_, {call, _, cmd_channel_receive_and_ack, _}, none) ->
    true;
postcondition(#cq{q=Q}, {call, _, cmd_channel_receive_and_ack, _}, Msg) ->
    %% When there are multiple active consumers we may receive
    %% messages out of order because the commands are not running
    %% in the same order as the messages sent to channels.
    %%
    %% But because messages can be pending in the mailbox this can
    %% be the case also when we had two consumers and one was
    %% cancelled. So we do not verify the order of messages
    %% when using consume.
    queue_has_msg(Q, Msg);
postcondition(_, {call, _, cmd_channel_receive_and_reject, _}, none) ->
    true;
postcondition(#cq{q=Q}, {call, _, cmd_channel_receive_and_reject, _}, Msg) ->
    %% When there are multiple active consumers we may receive
    %% messages out of order because the commands are not running
    %% in the same order as the messages sent to channels.
    %%
    %% But because messages can be pending in the mailbox this can
    %% be the case also when we had two consumers and one was
    %% cancelled. So we do not verify the order of messages
    %% when using consume.
    queue_has_msg(Q, Msg).

has_consumers(#cq{channels=Channels}) ->
    maps:fold(fun
        (_, {consume, _}, _) -> true;
        (_, _, Acc) -> Acc
    end, false, Channels).

queue_head_has_msg(Qs, Msg) ->
    maps:fold(fun
        (_, _, true) ->
            true;
        (_, ChQ, _) ->
            Res = queue:fold(fun
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

queue_has_msg(Qs, Msg) ->
    maps:fold(fun
        (_, _, true) ->
            true;
        (_, ChQ, _) ->
            queue:member(Msg, ChQ)
    end, false, Qs).

queue_part_all_expired(Qs, Key) ->
    queue:all(fun(#amqp_msg{props=#'P_basic'{expiration=Expiration}}) ->
        Expiration =/= undefined
    end, maps:get(Key, Qs)).

%% Helpers.

cmd_setup_queue(#cq{name=Name, mode=Mode, version=Version}) ->
    IsDurable = false,
    IsAutoDelete = false,
    %% We cannot use args to set mode/version as the arguments override
    %% the policies and we also want to test policy changes.
    cmd_set_mode_version(Mode, Version),
    %% @todo Maybe have both in-args and in-policies tested.
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

cmd_teardown_queue(#cq{amq=undefined}) ->
    ok;
cmd_teardown_queue(#cq{amq=AMQ, channels=Channels}) ->
    %% We must close all channels since we will not be using them anymore.
    %% Otherwise we end up wasting resources and may hit per-(direct)-connection limits.
    _ = [cmd_channel_close(Ch) || Ch <- maps:keys(Channels)],
    %% Then we can delete the queue.
    rabbit_amqqueue:delete(AMQ, false, false, <<"acting-user">>),
    rabbit_policy:delete(<<"/">>, <<"queue-mode-version-policy">>, <<"acting-user">>),
    ok.

cmd_set_mode(#cq{version=Version}, Mode) ->
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

cmd_set_version(#cq{mode=Mode}, Version) ->
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
    do_set_policy(Mode, Version).

do_set_policy(Mode, Version) ->
    rabbit_policy:set(<<"/">>, <<"queue-mode-version-policy">>, <<".*">>,
        [{<<"queue-mode">>, atom_to_binary(Mode, utf8)},
         {<<"queue-version">>, Version}],
        0, <<"queues">>, <<"acting-user">>).

cmd_publish_msg(#cq{amq=AMQ}, PayloadSize, Confirm, Mandatory, Expiration) ->
    Payload = do_rand_payload(PayloadSize),
    Msg = rabbit_basic:message(rabbit_misc:r(<<>>, exchange, <<>>),
                               <<>>, #'P_basic'{delivery_mode = 2,
                                                expiration = do_encode_expiration(Expiration)}, %% @todo different delivery_mode? more?
                               Payload),
    Delivery = #delivery{mandatory = Mandatory, sender = self(),
                         %% @todo Probably need to do something about Confirm?
                         confirm = Confirm, message = Msg,% msg_seq_no = Seq,
                         flow = noflow},
    ok = rabbit_amqqueue:deliver([AMQ], Delivery),
    {MsgProps, MsgPayload} = rabbit_basic_common:from_content(Msg#basic_message.content),
    #amqp_msg{props=MsgProps, payload=MsgPayload}.

cmd_basic_get_msg(#cq{amq=AMQ}) ->
    {ok, Limiter} = rabbit_limiter:start_link(no_id),
    %% The second argument means that we will not be sending
    %% a ack message. @todo Maybe handle both cases.
    Res = rabbit_amqqueue:basic_get(AMQ, true, Limiter,
                                    <<"cmd_basic_get_msg">>,
                                    rabbit_queue_type:init()),
    case Res of
        {empty, _} ->
            empty;
        {ok, _CountMinusOne, {_QName, _QPid, _AckTag, _IsDelivered, Msg}, _} ->
            {MsgProps, MsgPayload} = rabbit_basic_common:from_content(Msg#basic_message.content),
            #amqp_msg{props=MsgProps, payload=MsgPayload}
    end.

cmd_purge(#cq{amq=AMQ}) ->
    %% There may be messages in transit. We must wait for them to
    %% be processed before purging the queue.
%    timer:sleep(1000), %% @todo Something better.
    rabbit_amqqueue:purge(AMQ).

cmd_channel_open(#cq{config=Config}) ->
    Server = rabbit_ct_broker_helpers:get_node_config(Config, 0, nodename),
    Ch = rabbit_ct_client_helpers:open_channel(Config, Server),
    Ch.

cmd_channel_close(Ch) ->
    %% We cannot close the channel with
    %% rabbit_ct_client_helpers:close_channel(Ch)
    %% because the pid is remote (it is in the CT node)
    %% and the helper calls is_process_alive/1.
    %% So instead we close directly.
    amqp_channel:close(Ch).

cmd_channel_publish(#cq{amq=AMQ}, Ch, PayloadSize, Mandatory, Expiration) ->
    #resource{name = Name} = amqqueue:get_name(AMQ),
    Payload = do_rand_payload(PayloadSize),
    Msg = #amqp_msg{props   = #'P_basic'{delivery_mode = 2,
                                         expiration = do_encode_expiration(Expiration)},
                    payload = Payload},
    ok = amqp_channel:call(Ch,
                           #'basic.publish'{routing_key = Name,
                                            mandatory = Mandatory},
                           Msg),
    Msg.

cmd_channel_basic_get(#cq{amq=AMQ}, Ch) ->
    #resource{name = Name} = amqqueue:get_name(AMQ),
    case amqp_channel:call(Ch, #'basic.get'{queue = Name, no_ack = true}) of
        #'basic.get_empty'{} ->
            empty;
        {_GetOk = #'basic.get_ok'{}, Msg} ->
            Msg
    end.

cmd_channel_consume(#cq{amq=AMQ}, Ch) ->
    #resource{name = Name} = amqqueue:get_name(AMQ),
    Tag = integer_to_binary(erlang:unique_integer([positive])),
    #'basic.consume_ok'{} =
        amqp_channel:call(Ch,
                          #'basic.consume'{queue = Name, consumer_tag = Tag}),
    receive #'basic.consume_ok'{consumer_tag = Tag} -> ok end,
    Tag.

cmd_channel_cancel(#cq{channels=Channels}, Ch) ->
    {consume, Tag} = maps:get(Ch, Channels),
    #'basic.cancel_ok'{} =
        amqp_channel:call(Ch, #'basic.cancel'{consumer_tag = Tag}),
    receive #'basic.cancel_ok'{consumer_tag = Tag} -> ok end,
    %% We have to reject the messages in transit to preserve ordering.
    do_receive_reject_all(Ch, Tag).

cmd_channel_receive_and_ack(#cq{channels=Channels}, Ch) ->
    {consume, Tag} = maps:get(Ch, Channels),
    receive
        {#'basic.deliver'{consumer_tag = Tag,
                          delivery_tag = DeliveryTag}, Msg} ->
            amqp_channel:call(Ch, #'basic.ack'{delivery_tag = DeliveryTag}),
            Msg
    after 0 ->
        none
    end.

cmd_channel_receive_and_reject(#cq{channels=Channels}, Ch) ->
    {consume, Tag} = maps:get(Ch, Channels),
    receive
        {#'basic.deliver'{consumer_tag = Tag,
                          delivery_tag = DeliveryTag}, Msg} ->
            amqp_channel:call(Ch, #'basic.reject'{delivery_tag = DeliveryTag}),
            Msg
    after 0 ->
        none
    end.

do_receive_reject_all(Ch, Tag) ->
    receive
        {#'basic.deliver'{consumer_tag = Tag,
                          delivery_tag = DeliveryTag}, _Msg} ->
            amqp_channel:call(Ch, #'basic.reject'{delivery_tag = DeliveryTag}),
            do_receive_reject_all(Ch, Tag)
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
