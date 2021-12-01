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
            {900, {call, ?MODULE, cmd_channel_publish, [St, channel(St), integer(0, 1024*1024), boolean()]}},
%           %% channel enable confirm mode
%            {300, {call, ?MODULE, cmd_channel_await_publisher_confirms, [channel(St)]}},
            {900, {call, ?MODULE, cmd_channel_basic_get, [St, channel(St)]}},
            {900, {call, ?MODULE, cmd_channel_consume, [St, channel(St)]}}
            %% channel ack
            %% channel reject
            %% channel cancel
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
        {900, {call, ?MODULE, cmd_publish_msg, [St, integer(0, 1024*1024), boolean(), boolean()]}},
        {900, {call, ?MODULE, cmd_basic_get_msg, [St]}},
%        {100, {call, ?MODULE, cmd_purge, [St]}},
        %% These are channel-based operations.
        {100, {call, ?MODULE, cmd_channel_open, [St]}}
        |ChannelCmds
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
    St#cq{q=queue_out(Q, Msg)};
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
next_state(St=#cq{q=Q}, Msg, {call, _, cmd_channel_basic_get, _}) ->
    St#cq{q=queue_out(Q, Msg)};
next_state(St=#cq{channels=Channels}, Tag, {call, _, cmd_channel_consume, [_, Ch]}) ->
    St#cq{channels=Channels#{Ch => {consume, Tag}}}.

%% @todo This function is not working in a symbolic context.
%%       We cannot rely on Q for driving preconditions as a result.
%%
%% We remove at most one message.
queue_out(Qs0, Msg) ->
    {Qs, _} = maps:fold(fun
        (Ch, ChQ, {Qs1, true}) ->
            {Qs1#{Ch => ChQ}, true};
        (Ch, ChQ, {Qs1, false}) ->
            case queue:peek(ChQ) of
                {value, Msg} ->
                    case queue:len(ChQ) of
                        1 ->
                            {Qs1, true};
                        _ ->
                            {_, ChQOut} = queue:out(ChQ),
                            {Qs1#{Ch => ChQOut}, true}
                    end;
                _ ->
                    {Qs1#{Ch => ChQ}, false}
            end
    end, {#{}, false}, Qs0),
    Qs.

%% Preconditions.

precondition(St, {call, _, cmd_channel_close, _}) ->
    has_channels(St);
precondition(St, {call, _, cmd_channel_publish, _}) ->
    has_channels(St);
precondition(St=#cq{channels=Channels}, {call, _, cmd_channel_basic_get, [_, Ch]}) ->
    case has_channels(St) of
        false ->
            false;
        true ->
            %% Using both consume and basic_get is non-deterministic.
            maps:get(Ch, Channels) =:= idle
    end;
precondition(St=#cq{channels=Channels}, {call, _, cmd_channel_consume, [_, Ch]}) ->
    case has_channels(St) of
        false ->
            false;
        true ->
            %% Don't consume if we are already consuming on this channel.
            maps:get(Ch, Channels) =:= idle
    end;
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
    %% We may get 'empty' if there are consumers and the messages are
    %% in transit.
    has_consumers(St) orelse
    %% Due to the asynchronous nature of publishing it may be
    %% possible to have published a message but an immediate basic.get
    %% on a separate channel cannot retrieve it. In that case we accept
    %% an empty return value only if the channel we are calling
    %% basic.get on has no messages published and not consumed.
    not maps:is_key(internal, Q);
postcondition(#cq{q=Q}, {call, _, cmd_basic_get_msg, _}, Msg) ->
    queue_peek_has_msg(Q, Msg);
postcondition(_, {call, _, cmd_purge, _}, {ok, _}) ->
    true;
postcondition(_, {call, _, cmd_channel_open, _}, _) ->
    true;
postcondition(_, {call, _, cmd_channel_close, _}, Res) ->
    case Res of
        ok -> ok;
        _ -> logger:error("CLOSE ERROR ~p", [Res])
    end,
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
    not maps:is_key(Ch, Q);
postcondition(#cq{q=Q}, {call, _, cmd_channel_basic_get, _}, Msg) ->
    queue_peek_has_msg(Q, Msg);
postcondition(_, {call, _, cmd_channel_consume, _}, _) ->
    true.

has_consumers(#cq{channels=Channels}) ->
    maps:fold(fun
        (_, {consume, _}, _) -> true;
        (_, _, Acc) -> Acc
    end, false, Channels).

%% We want to confirm at least one of this exact message
%% was published. There might be multiple if the randomly
%% generated payload got two identical values.
queue_peek_has_msg(Qs, Msg) ->
    NumFound = maps:fold(fun
        (_, ChQ, NumFound1) ->
            case queue:peek(ChQ) of
                {value, Msg} ->
                    NumFound1 + 1;
                _ ->
                    NumFound1
            end
    end, 0, Qs),
    NumFound >= 1.

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
    QName = rabbit_misc:r(<<"/">>, queue, atom_to_binary(Name, utf8)),
    {new, AMQ} = rabbit_amqqueue:declare(QName, IsDurable, IsAutoDelete, Args, none, <<"acting-user">>),
    %% We check that the queue was creating with the right mode/version.
    ok = do_check_queue_mode(AMQ, Mode),
    ok = do_check_queue_version(AMQ, Version),
    AMQ.

cmd_teardown_queue(#cq{amq=undefined}) ->
    ok;
cmd_teardown_queue(#cq{amq=AMQ}) ->
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

cmd_publish_msg(#cq{amq=AMQ}, PayloadSize, Confirm, Mandatory) ->
    Payload = do_rand_payload(PayloadSize),
    Msg = rabbit_basic:message(rabbit_misc:r(<<>>, exchange, <<>>),
                               <<>>, #'P_basic'{delivery_mode = 2}, %% @todo expiration ; @todo different delivery_mode? more?
                               Payload),
    Delivery = #delivery{mandatory = Mandatory, sender = self(),
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

cmd_channel_publish(#cq{name=Name}, Ch, PayloadSize, _Mandatory) ->
    Payload = do_rand_payload(PayloadSize),
    Msg = #amqp_msg{props   = #'P_basic'{delivery_mode = 2},
                    payload = Payload},
    ok = amqp_channel:call(Ch,
                           #'basic.publish'{routing_key = atom_to_binary(Name, utf8)},
                           Msg),
    Msg.

cmd_channel_basic_get(#cq{name=Name}, Ch) ->
    case amqp_channel:call(Ch, #'basic.get'{queue = atom_to_binary(Name, utf8), no_ack = true}) of
        #'basic.get_empty'{} ->
            empty;
        {_GetOk = #'basic.get_ok'{}, Msg} ->
            Msg
    end.

cmd_channel_consume(#cq{name=Name}, Ch) ->
    Tag = integer_to_binary(erlang:unique_integer([positive])),
    #'basic.consume_ok'{} =
        amqp_channel:call(Ch,
                          #'basic.consume'{queue = atom_to_binary(Name, utf8), consumer_tag = Tag}),
    receive #'basic.consume_ok'{consumer_tag = Tag} -> ok end,
    Tag.

do_rand_payload(PayloadSize) ->
    case erlang:function_exported(rand, bytes, 1) of
        true -> rand:bytes(PayloadSize);
        %% Slower failover for OTP < 24.0.
        false -> crypto:strong_rand_bytes(PayloadSize)
    end.
