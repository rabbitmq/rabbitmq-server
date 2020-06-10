%% The contents of this file are subject to the Mozilla Public License
%% Version 2.0 (the "License"); you may not use this file except in
%% compliance with the License. You may obtain a copy of the License
%% at https://www.mozilla.org/en-US/MPL/2.0/
%%
%% Software distributed under the License is distributed on an "AS IS"
%% basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See
%% the License for the specific language governing rights and
%% limitations under the License.
%%
%% The Original Code is RabbitMQ.
%%
%% The Initial Developer of the Original Code is Pivotal Software, Inc.
%% Copyright (c) 2020 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(rabbit_stream_reader).

-include_lib("rabbit_common/include/rabbit.hrl").
-include("rabbit_stream.hrl").

-record(stream_connection, {
    name,
    helper_sup,
    listen_socket, socket, clusters, data, consumers,
    stream_subscriptions, credits,
    blocked,
    authentication_state, user, virtual_host,
    connection_step, % tcp_connected, peer_properties_exchanged, authenticating, authenticated, tuning, tuned, opened, failure, closing, closing_done
    frame_max,
    heartbeater,
    client_properties
}).

-record(consumer, {
    socket, member_pid, offset, subscription_id, segment, credit, stream
}).

-record(configuration, {
    initial_credits, credits_required_for_unblocking,
    frame_max, heartbeat
}).

-define(RESPONSE_FRAME_SIZE, 10). % 2 (key) + 2 (version) + 4 (correlation ID) + 2 (response code)

%% API
-export([start_link/4, init/1]).

start_link(KeepaliveSup, Transport, Ref, Opts) ->
    Pid = proc_lib:spawn_link(?MODULE, init,
        [[KeepaliveSup, Transport, Ref, Opts]]),

    {ok, Pid}.

init([KeepaliveSup, Transport, Ref, #{initial_credits := InitialCredits,
    credits_required_for_unblocking := CreditsRequiredBeforeUnblocking,
    frame_max := FrameMax,
    heartbeat := Heartbeat}]) ->
    process_flag(trap_exit, true),
    {ok, Sock} = rabbit_networking:handshake(Ref,
        application:get_env(rabbitmq_mqtt, proxy_protocol, false)),
    RealSocket = rabbit_net:unwrap_socket(Sock),
    case rabbit_net:connection_string(Sock, inbound) of
        {ok, ConnStr} ->
            rabbit_stream_manager:register(),
            Credits = atomics:new(1, [{signed, true}]),
            init_credit(Credits, InitialCredits),
            State = #stream_connection{
                name = ConnStr,
                helper_sup = KeepaliveSup,
                socket = RealSocket, data = none,
                clusters = #{},
                consumers = #{}, stream_subscriptions = #{},
                blocked = false, credits = Credits,
                authentication_state = none, user = none,
                connection_step = tcp_connected,
                frame_max = FrameMax},
            Transport:setopts(RealSocket, [{active, once}]),

            listen_loop_pre_auth(Transport, State, #configuration{
                initial_credits = InitialCredits,
                credits_required_for_unblocking = CreditsRequiredBeforeUnblocking,
                frame_max = FrameMax,
                heartbeat = Heartbeat
            });
        {Error, Reason} ->
            rabbit_net:fast_close(RealSocket),
            error_logger:warning_msg("Closing connection because of ~p ~p~n", [Error, Reason])
    end.

init_credit(CreditReference, Credits) ->
    atomics:put(CreditReference, 1, Credits).

sub_credits(CreditReference, Credits) ->
    atomics:sub(CreditReference, 1, Credits).

add_credits(CreditReference, Credits) ->

    atomics:add(CreditReference, 1, Credits).

has_credits(CreditReference) ->
    atomics:get(CreditReference, 1) > 0.

has_enough_credits_to_unblock(CreditReference, CreditsRequiredForUnblocking) ->
    atomics:get(CreditReference, 1) > CreditsRequiredForUnblocking.

listen_loop_pre_auth(Transport, #stream_connection{socket = S} = State,
    #configuration{frame_max = FrameMax, heartbeat = Heartbeat} = Configuration) ->
    {OK, Closed, Error} = Transport:messages(),
    %% FIXME introduce timeout to complete the connection opening (after block should be enough)
    receive
        {OK, S, Data} ->
            #stream_connection{connection_step = ConnectionStep0} = State,
            State1 = handle_inbound_data_pre_auth(Transport, State, Data),
            Transport:setopts(S, [{active, once}]),
            #stream_connection{connection_step = ConnectionStep} = State1,
            error_logger:info_msg("Transitioned from ~p to ~p~n", [ConnectionStep0, ConnectionStep]),
            case ConnectionStep of
                authenticated ->
                    TuneFrame = <<?COMMAND_TUNE:16, ?VERSION_0:16, FrameMax:32, Heartbeat:32>>,
                    frame(Transport, State1, TuneFrame),
                    listen_loop_pre_auth(Transport, State1#stream_connection{connection_step = tuning}, Configuration);
                opened ->
                    listen_loop_post_auth(Transport, State1, Configuration);
                failure ->
                    close(Transport, S);
                _ ->
                    listen_loop_pre_auth(Transport, State1, Configuration)
            end;
        {Closed, S} ->
            error_logger:info_msg("Socket ~w closed [~w]~n", [S, self()]),
            ok;
        {Error, S, Reason} ->
            error_logger:info_msg("Socket error ~p [~w]~n", [Reason, S, self()]);
        M ->
            error_logger:warning_msg("Unknown message ~p~n", [M]),
            close(Transport, S)
    end.

close(Transport, S) ->
    Transport:shutdown(S, write),
    Transport:close(S).

listen_loop_post_auth(Transport, #stream_connection{socket = S, consumers = Consumers,
    stream_subscriptions = StreamSubscriptions, credits = Credits, blocked = Blocked, heartbeater = Heartbeater} = State,
    #configuration{credits_required_for_unblocking = CreditsRequiredForUnblocking} = Configuration) ->
    {OK, Closed, Error} = Transport:messages(),
    receive
        {OK, S, Data} ->
            #stream_connection{connection_step = Step} = State1 = handle_inbound_data_post_auth(Transport, State, Data),
            case Step of
                closing ->
                    close(Transport, S);
                close_sent ->
                    listen_loop_post_close(Transport, State1, Configuration);
                _ ->
                    State2 = case Blocked of
                                 true ->
                                     case has_enough_credits_to_unblock(Credits, CreditsRequiredForUnblocking) of
                                         true ->
                                             Transport:setopts(S, [{active, once}]),
                                             ok = rabbit_heartbeat:resume_monitor(Heartbeater),
                                             State1#stream_connection{blocked = false};
                                         false ->
                                             State1
                                     end;
                                 false ->
                                     case has_credits(Credits) of
                                         true ->
                                             Transport:setopts(S, [{active, once}]),
                                             State1;
                                         false ->
                                             ok = rabbit_heartbeat:pause_monitor(Heartbeater),
                                             State1#stream_connection{blocked = true}
                                     end
                             end,
                    listen_loop_post_auth(Transport, State2, Configuration)
            end;
        {stream_manager, cluster_deleted, ClusterReference} ->
            State1 = case clean_state_after_stream_deletion(ClusterReference, State) of
                         {cleaned, NewState} ->
                             StreamSize = byte_size(ClusterReference),
                             FrameSize = 2 + 2 + 2 + 2 + StreamSize,
                             Transport:send(S, [<<FrameSize:32, ?COMMAND_METADATA_UPDATE:16, ?VERSION_0:16,
                                 ?RESPONSE_CODE_STREAM_DELETED:16, StreamSize:16, ClusterReference/binary>>]),
                             NewState;
                         {not_cleaned, SameState} ->
                             SameState
                     end,
            listen_loop_post_auth(Transport, State1, Configuration);
        {'$gen_cast', {queue_event, _QueueResource, {osiris_written, _QueueResource, CorrelationIdList}}} ->
            CorrelationIdBinaries = [<<CorrelationId:64>> || CorrelationId <- CorrelationIdList],
            CorrelationIdCount = length(CorrelationIdList),
            FrameSize = 2 + 2 + 4 + CorrelationIdCount * 8,
            %% FIXME enforce max frame size
            %% in practice, this should be necessary only for very large chunks and for very small frame size limits
            Transport:send(S, [<<FrameSize:32, ?COMMAND_PUBLISH_CONFIRM:16, ?VERSION_0:16>>, <<CorrelationIdCount:32>>, CorrelationIdBinaries]),
            add_credits(Credits, CorrelationIdCount),
            State1 = case Blocked of
                         true ->
                             case has_enough_credits_to_unblock(Credits, CreditsRequiredForUnblocking) of
                                 true ->
                                     Transport:setopts(S, [{active, once}]),
                                     ok = rabbit_heartbeat:resume_monitor(Heartbeater),
                                     State#stream_connection{blocked = false};
                                 false ->
                                     State
                             end;
                         false ->
                             State
                     end,
            listen_loop_post_auth(Transport, State1, Configuration);
        {'$gen_cast', {queue_event, #resource{name = StreamName}, {osiris_offset, _QueueResource, -1}}} ->
            error_logger:info_msg("received osiris offset event for ~p with offset ~p~n", [StreamName, -1]),
            listen_loop_post_auth(Transport, State, Configuration);
        {'$gen_cast', {queue_event, #resource{name = StreamName}, {osiris_offset, _QueueResource, Offset}}} when Offset > -1 ->
            State1 = case maps:get(StreamName, StreamSubscriptions, undefined) of
                         undefined ->
                             error_logger:info_msg("osiris offset event for ~p, but no subscription (leftover messages after unsubscribe?)", [StreamName]),
                             State;
                         [] ->
                             error_logger:info_msg("osiris offset event for ~p, but no registered consumers!", [StreamName]),
                             State#stream_connection{stream_subscriptions = maps:remove(StreamName, StreamSubscriptions)};
                         CorrelationIds when is_list(CorrelationIds) ->
                             Consumers1 = lists:foldl(fun(CorrelationId, ConsumersAcc) ->
                                 #{CorrelationId := Consumer} = ConsumersAcc,
                                 #consumer{credit = Credit} = Consumer,
                                 Consumer1 = case Credit of
                                                 0 ->
                                                     Consumer;
                                                 _ ->
                                                     {{segment, Segment1}, {credit, Credit1}} = send_chunks(
                                                         Transport,
                                                         Consumer
                                                     ),
                                                     Consumer#consumer{segment = Segment1, credit = Credit1}
                                             end,
                                 ConsumersAcc#{CorrelationId => Consumer1}
                                                      end,
                                 Consumers,
                                 CorrelationIds),
                             State#stream_connection{consumers = Consumers1}
                     end,
            listen_loop_post_auth(Transport, State1, Configuration);
        {heartbeat_send_error, Reason} ->
            error_logger:info_msg("Heartbeat send error ~p, closing connection~n", [Reason]),
            rabbit_stream_manager:unregister(),
            close(Transport, S);
        heartbeat_timeout ->
            error_logger:info_msg("Heartbeat timeout, closing connection~n"),
            rabbit_stream_manager:unregister(),
            close(Transport, S);
        {Closed, S} ->
            rabbit_stream_manager:unregister(),
            error_logger:info_msg("Socket ~w closed [~w]~n", [S, self()]),
            ok;
        {Error, S, Reason} ->
            rabbit_stream_manager:unregister(),
            error_logger:info_msg("Socket error ~p [~w]~n", [Reason, S, self()]);
        M ->
            error_logger:warning_msg("Unknown message ~p~n", [M]),
            %% FIXME send close
            listen_loop_post_auth(Transport, State, Configuration)
    end.

listen_loop_post_close(Transport, #stream_connection{socket = S} = State, Configuration) ->
    {OK, Closed, Error} = Transport:messages(),
    %% FIXME introduce timeout to complete the connection closing (after block should be enough)
    receive
        {OK, S, Data} ->
            #stream_connection{connection_step = Step} = State1 = handle_inbound_data_post_close(Transport, State, Data),
            case Step of
                closing_done ->
                    error_logger:info_msg("Received close confirmation from client"),
                    close(Transport, S);
                _ ->
                    Transport:setopts(S, [{active, once}]),
                    listen_loop_post_close(Transport, State1, Configuration)
            end;
        {Closed, S} ->
            error_logger:info_msg("Socket ~w closed [~w]~n", [S, self()]),
            ok;
        {Error, S, Reason} ->
            error_logger:info_msg("Socket error ~p [~w]~n", [Reason, S, self()]);
        M ->
            error_logger:warning_msg("Ignored message on closing ~p~n", [M])
    end.

handle_inbound_data_pre_auth(Transport, State, Rest) ->
    handle_inbound_data(Transport, State, Rest, fun handle_frame_pre_auth/4).

handle_inbound_data_post_auth(Transport, State, Rest) ->
    handle_inbound_data(Transport, State, Rest, fun handle_frame_post_auth/4).

handle_inbound_data_post_close(Transport, State, Rest) ->
    handle_inbound_data(Transport, State, Rest, fun handle_frame_post_close/4).

handle_inbound_data(_Transport, State, <<>>, _HandleFrameFun) ->
    State;
handle_inbound_data(Transport, #stream_connection{data = none, frame_max = FrameMax} = State, <<Size:32, _Frame:Size/binary, _Rest/bits>>, _HandleFrameFun)
    when FrameMax /= 0 andalso Size > FrameMax - 4 ->
    CloseReason = <<"frame too large">>,
    CloseReasonLength = byte_size(CloseReason),
    CloseFrame = <<?COMMAND_CLOSE:16, ?VERSION_0:16, 1:32, ?RESPONSE_CODE_FRAME_TOO_LARGE:16,
        CloseReasonLength:16, CloseReason:CloseReasonLength/binary>>,
    frame(Transport, State, CloseFrame),
    State#stream_connection{connection_step = close_sent};
handle_inbound_data(Transport, #stream_connection{data = none} = State, <<Size:32, Frame:Size/binary, Rest/bits>>, HandleFrameFun) ->
    {State1, Rest1} = HandleFrameFun(Transport, State, Frame, Rest),
    handle_inbound_data(Transport, State1, Rest1, HandleFrameFun);
handle_inbound_data(_Transport, #stream_connection{data = none} = State, Data, _HandleFrameFun) ->
    State#stream_connection{data = Data};
handle_inbound_data(Transport, #stream_connection{data = Leftover} = State, Data, HandleFrameFun) ->
    State1 = State#stream_connection{data = none},
    %% FIXME avoid concatenation to avoid a new binary allocation
    %% see osiris_replica:parse_chunk/3
    handle_inbound_data(Transport, State1, <<Leftover/binary, Data/binary>>, HandleFrameFun).

write_messages(_ClusterLeader, <<>>) ->
    ok;
write_messages(ClusterLeader, <<PublishingId:64, MessageSize:32, Message:MessageSize/binary, Rest/binary>>) ->
    % FIXME handle write error
    ok = osiris:write(ClusterLeader, PublishingId, Message),
    write_messages(ClusterLeader, Rest).

generate_publishing_error_details(Acc, <<>>) ->
    Acc;
generate_publishing_error_details(Acc, <<PublishingId:64, MessageSize:32, _Message:MessageSize/binary, Rest/binary>>) ->
    generate_publishing_error_details(
        <<Acc/binary, PublishingId:64, ?RESPONSE_CODE_STREAM_DOES_NOT_EXIST:16>>,
        Rest).

handle_frame_pre_auth(Transport, #stream_connection{socket = S} = State,
    <<?COMMAND_PEER_PROPERTIES:16, ?VERSION_0:16, CorrelationId:32,
        ClientPropertiesCount:32, ClientPropertiesFrame/binary>>, Rest) ->

    {ClientProperties, _} = parse_map(ClientPropertiesFrame, ClientPropertiesCount),

    {ok, Product} = application:get_key(rabbit, description),
    {ok, Version} = application:get_key(rabbit, vsn),

    %% Get any configuration-specified server properties
    RawConfigServerProps = application:get_env(rabbit,
        server_properties, []),

    ConfigServerProperties = lists:foldl(fun({K, V}, Acc) ->
        maps:put(rabbit_data_coercion:to_binary(K), V, Acc)
                                         end, #{}, RawConfigServerProps),

    ServerProperties = maps:merge(ConfigServerProperties, #{
        <<"product">> => Product,
        <<"version">> => Version,
        <<"cluster_name">> => rabbit_nodes:cluster_name(),
        <<"platform">> => rabbit_misc:platform_and_version(),
        <<"copyright">> => ?COPYRIGHT_MESSAGE,
        <<"information">> => ?INFORMATION_MESSAGE
    }),

    ServerPropertiesCount = map_size(ServerProperties),

    ServerPropertiesFragment = maps:fold(fun(K, V, Acc) ->
        Key = rabbit_data_coercion:to_binary(K),
        Value = rabbit_data_coercion:to_binary(V),
        KeySize = byte_size(Key),
        ValueSize = byte_size(Value),
        <<Acc/binary, KeySize:16, Key:KeySize/binary, ValueSize:16, Value:ValueSize/binary>>
                                         end, <<>>, ServerProperties),

    Frame = <<?COMMAND_PEER_PROPERTIES:16, ?VERSION_0:16, CorrelationId:32, ?RESPONSE_CODE_OK:16,
        ServerPropertiesCount:32, ServerPropertiesFragment/binary>>,
    FrameSize = byte_size(Frame),

    Transport:send(S, [<<FrameSize:32>>, <<Frame/binary>>]),
    {State#stream_connection{client_properties = ClientProperties, authentication_state = peer_properties_exchanged}, Rest};
handle_frame_pre_auth(Transport, #stream_connection{socket = S} = State,
    <<?COMMAND_SASL_HANDSHAKE:16, ?VERSION_0:16, CorrelationId:32>>, Rest) ->

    Mechanisms = auth_mechanisms(S),
    MechanismsFragment = lists:foldl(fun(M, Acc) ->
        Size = byte_size(M),
        <<Acc/binary, Size:16, M:Size/binary>>
                                     end, <<>>, Mechanisms),
    MechanismsCount = length(Mechanisms),
    Frame = <<?COMMAND_SASL_HANDSHAKE:16, ?VERSION_0:16, CorrelationId:32, ?RESPONSE_CODE_OK:16,
        MechanismsCount:32, MechanismsFragment/binary>>,
    FrameSize = byte_size(Frame),

    Transport:send(S, [<<FrameSize:32>>, <<Frame/binary>>]),
    {State, Rest};
handle_frame_pre_auth(Transport, #stream_connection{socket = S, authentication_state = AuthState0} = State0,
    <<?COMMAND_SASL_AUTHENTICATE:16, ?VERSION_0:16, CorrelationId:32,
        MechanismLength:16, Mechanism:MechanismLength/binary,
        SaslFragment/binary>>, Rest) ->

    SaslBin = case SaslFragment of
                  <<-1:32/signed>> ->
                      <<>>;
                  <<SaslBinaryLength:32, SaslBinary:SaslBinaryLength/binary>> ->
                      SaslBinary
              end,

    {State1, Rest1} = case auth_mechanism_to_module(Mechanism, S) of
                          {ok, AuthMechanism} ->
                              AuthState = case AuthState0 of
                                              none ->
                                                  AuthMechanism:init(S);
                                              AS ->
                                                  AS
                                          end,
                              {S1, FrameFragment} = case AuthMechanism:handle_response(SaslBin, AuthState) of
                                                        {refused, _Username, Msg, Args} ->
                                                            error_logger:warning_msg(Msg, Args),
                                                            {State0#stream_connection{connection_step = failure}, <<?RESPONSE_AUTHENTICATION_FAILURE:16>>};
                                                        {protocol_error, Msg, Args} ->
                                                            error_logger:warning_msg(Msg, Args),
                                                            {State0#stream_connection{connection_step = failure}, <<?RESPONSE_SASL_ERROR:16>>};
                                                        {challenge, Challenge, AuthState1} ->
                                                            ChallengeSize = byte_size(Challenge),
                                                            {State0#stream_connection{authentication_state = AuthState1, connection_step = authenticating},
                                                                <<?RESPONSE_SASL_CHALLENGE:16, ChallengeSize:32, Challenge/binary>>
                                                            };
                                                        {ok, User = #user{username = Username}} ->
                                                            case rabbit_access_control:check_user_loopback(Username, S) of
                                                                ok ->
                                                                    {State0#stream_connection{authentication_state = done, user = User, connection_step = authenticated},
                                                                        <<?RESPONSE_CODE_OK:16>>
                                                                    };
                                                                not_allowed ->
                                                                    error_logger:warning_msg("User '~s' can only connect via localhost~n", [Username]),
                                                                    {State0#stream_connection{connection_step = failure}, <<?RESPONSE_SASL_AUTHENTICATION_FAILURE_LOOPBACK:16>>}
                                                            end
                                                    end,
                              Frame = <<?COMMAND_SASL_AUTHENTICATE:16, ?VERSION_0:16, CorrelationId:32, FrameFragment/binary>>,
                              frame(Transport, S1, Frame),
                              {S1, Rest};
                          {error, _} ->
                              Frame = <<?COMMAND_SASL_AUTHENTICATE:16, ?VERSION_0:16, CorrelationId:32, ?RESPONSE_SASL_MECHANISM_NOT_SUPPORTED:16>>,
                              frame(Transport, State0, Frame),
                              {State0#stream_connection{connection_step = failure}, Rest}
                      end,

    {State1, Rest1};
handle_frame_pre_auth(Transport, #stream_connection{helper_sup = SupPid, socket = Sock, name = ConnectionName} = State,
    <<?COMMAND_TUNE:16, ?VERSION_0:16, FrameMax:32, Heartbeat:32>>, Rest) ->
    error_logger:info_msg("Tuning response ~p ~p ~n", [FrameMax, Heartbeat]),

    Frame = <<?COMMAND_HEARTBEAT:16, ?VERSION_0:16>>,
    Parent = self(),
    SendFun =
        fun() ->
            case catch frame(Transport, State, Frame) of
                ok ->
                    ok;
                {error, Reason} ->
                    Parent ! {heartbeat_send_error, Reason};
                Unexpected ->
                    Parent ! {heartbeat_send_error, Unexpected}
            end,
            ok
        end,
    ReceiveFun = fun() -> Parent ! heartbeat_timeout end,
    Heartbeater = rabbit_heartbeat:start(
        SupPid, Sock, ConnectionName,
        Heartbeat, SendFun, Heartbeat, ReceiveFun),


    {State#stream_connection{connection_step = tuned, frame_max = FrameMax, heartbeater = Heartbeater}, Rest};
handle_frame_pre_auth(Transport, #stream_connection{user = User, socket = S} = State, <<?COMMAND_OPEN:16, ?VERSION_0:16, CorrelationId:32,
    VirtualHostLength:16, VirtualHost:VirtualHostLength/binary>>, Rest) ->

    %% FIXME enforce connection limit (see rabbit_reader:is_over_connection_limit/2)

    {State1, Frame} = try
                          case rabbit_access_control:check_vhost_access(User, VirtualHost, {socket, S}, #{}) of
                              ok ->
                                  F = <<?COMMAND_OPEN:16, ?VERSION_0:16, CorrelationId:32, ?RESPONSE_CODE_OK:16>>,
                                  %% FIXME check if vhost is alive (see rabbit_reader:is_vhost_alive/2)
                                  {State#stream_connection{connection_step = opened, virtual_host = VirtualHost}, F};
                              _ ->
                                  F = <<?COMMAND_OPEN:16, ?VERSION_0:16, CorrelationId:32, ?RESPONSE_VHOST_ACCESS_FAILURE:16>>,
                                  {State#stream_connection{connection_step = failure}, F}
                          end
                      catch
                          exit:_ ->
                              Fr = <<?COMMAND_OPEN:16, ?VERSION_0:16, CorrelationId:32, ?RESPONSE_VHOST_ACCESS_FAILURE:16>>,
                              {State#stream_connection{connection_step = failure}, Fr}
                      end,

    frame(Transport, State, Frame),

    {State1, Rest};
handle_frame_pre_auth(_Transport, State, <<?COMMAND_HEARTBEAT:16, ?VERSION_0:16>>, Rest) ->
    error_logger:info_msg("Received heartbeat frame pre auth~n"),
    {State, Rest};
handle_frame_pre_auth(_Transport, State, Frame, Rest) ->
    error_logger:warning_msg("unknown frame ~p ~p, closing connection.~n", [Frame, Rest]),
    {State#stream_connection{connection_step = failure}, Rest}.

handle_frame_post_auth(Transport, #stream_connection{socket = S, credits = Credits} = State,
    <<?COMMAND_PUBLISH:16, ?VERSION_0:16,
        StreamSize:16, Stream:StreamSize/binary,
        MessageCount:32, Messages/binary>>, Rest) ->
    case lookup_leader(Stream, State) of
        cluster_not_found ->
            FrameSize = 2 + 2 + 4 + (8 + 2) * MessageCount,
            Details = generate_publishing_error_details(<<>>, Messages),
            Transport:send(S, [<<FrameSize:32, ?COMMAND_PUBLISH_ERROR:16, ?VERSION_0:16,
                MessageCount:32, Details/binary>>]),
            {State, Rest};
        {ClusterLeader, State1} ->
            write_messages(ClusterLeader, Messages),
            sub_credits(Credits, MessageCount),
            {State1, Rest}
    end;
handle_frame_post_auth(Transport, #stream_connection{socket = Socket, consumers = Consumers,
    stream_subscriptions = StreamSubscriptions, virtual_host = VirtualHost} = State,
    <<?COMMAND_SUBSCRIBE:16, ?VERSION_0:16, CorrelationId:32, SubscriptionId:32, StreamSize:16, Stream:StreamSize/binary,
        OffsetType:16/signed, OffsetAndCredit/binary>>, Rest) ->
    case rabbit_stream_manager:lookup_local_member(VirtualHost, Stream) of
        {error, not_found} ->
            response(Transport, State, ?COMMAND_SUBSCRIBE, CorrelationId, ?RESPONSE_CODE_STREAM_DOES_NOT_EXIST),
            {State, Rest};
        {ok, LocalMemberPid} ->
            case subscription_exists(StreamSubscriptions, SubscriptionId) of
                true ->
                    response(Transport, State, ?COMMAND_SUBSCRIBE, CorrelationId, ?RESPONSE_CODE_SUBSCRIPTION_ID_ALREADY_EXISTS),
                    {State, Rest};
                false ->
                    {OffsetSpec, Credit} = case OffsetType of
                                               ?OFFSET_TYPE_FIRST ->
                                                   <<Crdt:16>> = OffsetAndCredit,
                                                   {first, Crdt};
                                               ?OFFSET_TYPE_LAST ->
                                                   <<Crdt:16>> = OffsetAndCredit,
                                                   {last, Crdt};
                                               ?OFFSET_TYPE_NEXT ->
                                                   <<Crdt:16>> = OffsetAndCredit,
                                                   {next, Crdt};
                                               ?OFFSET_TYPE_OFFSET ->
                                                   <<Offset:64/unsigned, Crdt:16>> = OffsetAndCredit,
                                                   {Offset, Crdt};
                                               ?OFFSET_TYPE_TIMESTAMP ->
                                                   <<Timestamp:64/signed, Crdt:16>> = OffsetAndCredit,
                                                   {{timestamp, Timestamp}, Crdt}
                                           end,
                    {ok, Segment} = osiris:init_reader(LocalMemberPid, OffsetSpec),
                    ConsumerState = #consumer{
                        member_pid = LocalMemberPid, offset = OffsetSpec, subscription_id = SubscriptionId, socket = Socket,
                        segment = Segment,
                        credit = Credit,
                        stream = Stream
                    },
                    error_logger:info_msg("registering consumer ~p in ~p~n", [ConsumerState, self()]),

                    response_ok(Transport, State, ?COMMAND_SUBSCRIBE, CorrelationId),

                    {{segment, Segment1}, {credit, Credit1}} = send_chunks(
                        Transport,
                        ConsumerState
                    ),
                    Consumers1 = Consumers#{SubscriptionId => ConsumerState#consumer{segment = Segment1, credit = Credit1}},

                    StreamSubscriptions1 =
                        case StreamSubscriptions of
                            #{Stream := SubscriptionIds} ->
                                StreamSubscriptions#{Stream => [SubscriptionId] ++ SubscriptionIds};
                            _ ->
                                StreamSubscriptions#{Stream => [SubscriptionId]}
                        end,
                    {State#stream_connection{consumers = Consumers1, stream_subscriptions = StreamSubscriptions1}, Rest}
            end
    end;
handle_frame_post_auth(Transport, #stream_connection{consumers = Consumers, stream_subscriptions = StreamSubscriptions, clusters = Clusters} = State,
    <<?COMMAND_UNSUBSCRIBE:16, ?VERSION_0:16, CorrelationId:32, SubscriptionId:32>>, Rest) ->
    case subscription_exists(StreamSubscriptions, SubscriptionId) of
        false ->
            response(Transport, State, ?COMMAND_UNSUBSCRIBE, CorrelationId, ?RESPONSE_CODE_SUBSCRIPTION_ID_DOES_NOT_EXIST),
            {State, Rest};
        true ->
            #{SubscriptionId := Consumer} = Consumers,
            Stream = Consumer#consumer.stream,
            #{Stream := SubscriptionsForThisStream} = StreamSubscriptions,
            SubscriptionsForThisStream1 = lists:delete(SubscriptionId, SubscriptionsForThisStream),
            {StreamSubscriptions1, Clusters1} =
                case length(SubscriptionsForThisStream1) of
                    0 ->
                        %% no more subscriptions for this stream
                        {maps:remove(Stream, StreamSubscriptions),
                            maps:remove(Stream, Clusters)
                        };
                    _ ->
                        {StreamSubscriptions#{Stream => SubscriptionsForThisStream1}, Clusters}
                end,
            Consumers1 = maps:remove(SubscriptionId, Consumers),
            response_ok(Transport, State, ?COMMAND_SUBSCRIBE, CorrelationId),
            {State#stream_connection{consumers = Consumers1,
                stream_subscriptions = StreamSubscriptions1,
                clusters = Clusters1
            }, Rest}
    end;
handle_frame_post_auth(Transport, #stream_connection{consumers = Consumers} = State,
    <<?COMMAND_CREDIT:16, ?VERSION_0:16, SubscriptionId:32, Credit:16/signed>>, Rest) ->

    case Consumers of
        #{SubscriptionId := Consumer} ->
            #consumer{credit = AvailableCredit} = Consumer,

            {{segment, Segment1}, {credit, Credit1}} = send_chunks(
                Transport,
                Consumer,
                AvailableCredit + Credit
            ),

            Consumer1 = Consumer#consumer{segment = Segment1, credit = Credit1},
            {State#stream_connection{consumers = Consumers#{SubscriptionId => Consumer1}}, Rest};
        _ ->
            %% FIXME find a way to tell the client it's crediting an unknown subscription
            error_logger:warning_msg("Giving credit to unknown subscription: ~p~n", [SubscriptionId]),
            {State, Rest}
    end;
handle_frame_post_auth(Transport, #stream_connection{virtual_host = VirtualHost, user = #user{username = Username}} = State,
    <<?COMMAND_CREATE_STREAM:16, ?VERSION_0:16, CorrelationId:32, StreamSize:16, Stream:StreamSize/binary,
        ArgumentsCount:32, ArgumentsBinary/binary>>, Rest) ->
    {Arguments, _Rest} = parse_map(ArgumentsBinary, ArgumentsCount),
    case rabbit_stream_manager:create(VirtualHost, Stream, Arguments, Username) of
        {ok, #{leader_pid := LeaderPid, replica_pids := ReturnedReplicas}} ->
            error_logger:info_msg("Created cluster with leader ~p and replicas ~p~n", [LeaderPid, ReturnedReplicas]),
            response_ok(Transport, State, ?COMMAND_CREATE_STREAM, CorrelationId),
            {State, Rest};
        {error, reference_already_exists} ->
            response(Transport, State, ?COMMAND_CREATE_STREAM, CorrelationId, ?RESPONSE_CODE_STREAM_ALREADY_EXISTS),
            {State, Rest};
        {error, _} ->
            response(Transport, State, ?COMMAND_CREATE_STREAM, CorrelationId, ?RESPONSE_CODE_INTERNAL_ERROR),
            {State, Rest}
    end;
handle_frame_post_auth(Transport, #stream_connection{socket = S, virtual_host = VirtualHost,
    user = #user{username = Username}} = State,
    <<?COMMAND_DELETE_STREAM:16, ?VERSION_0:16, CorrelationId:32, StreamSize:16, Stream:StreamSize/binary>>, Rest) ->
    case rabbit_stream_manager:delete(VirtualHost, Stream, Username) of
        {ok, deleted} ->
            response_ok(Transport, State, ?COMMAND_DELETE_STREAM, CorrelationId),
            State1 = case clean_state_after_stream_deletion(Stream, State) of
                         {cleaned, NewState} ->
                             StreamSize = byte_size(Stream),
                             FrameSize = 2 + 2 + 2 + 2 + StreamSize,
                             Transport:send(S, [<<FrameSize:32, ?COMMAND_METADATA_UPDATE:16, ?VERSION_0:16,
                                 ?RESPONSE_CODE_STREAM_DELETED:16, StreamSize:16, Stream/binary>>]),
                             NewState;
                         {not_cleaned, SameState} ->
                             SameState
                     end,
            {State1, Rest};
        {error, reference_not_found} ->
            response(Transport, State, ?COMMAND_DELETE_STREAM, CorrelationId, ?RESPONSE_CODE_STREAM_DOES_NOT_EXIST),
            {State, Rest};
        {error, internal_error} ->
            response(Transport, State, ?COMMAND_DELETE_STREAM, CorrelationId, ?RESPONSE_CODE_INTERNAL_ERROR),
            {State, Rest}
    end;
handle_frame_post_auth(Transport, #stream_connection{socket = S, virtual_host = VirtualHost} = State,
    <<?COMMAND_METADATA:16, ?VERSION_0:16, CorrelationId:32, StreamCount:32, BinaryStreams/binary>>, Rest) ->
    Streams = extract_stream_list(BinaryStreams, []),

    %% get the nodes involved in the streams
    NodesMap = lists:foldl(fun(Stream, Acc) ->
        case rabbit_stream_manager:topology(VirtualHost, Stream) of
            {ok, #{leader_node := LeaderNode, replica_nodes := ReplicaNodes}} ->
                Acc1 = maps:put(LeaderNode, ok, Acc),
                lists:foldl(fun(ReplicaNode, NodesAcc) -> maps:put(ReplicaNode, ok, NodesAcc) end, Acc1, ReplicaNodes);
            {error, _} ->
                Acc
        end
                           end, #{}, Streams),

    Nodes = maps:keys(NodesMap),
    {NodesInfo, _} = lists:foldl(fun(Node, {Acc, Index}) ->
        Host = rpc:call(Node, rabbit_stream, host, []),
        Port = rpc:call(Node, rabbit_stream, port, []),
        {Acc#{Node => {{index, Index}, {host, Host}, {port, Port}}}, Index + 1}
                                 end, {#{}, 0}, Nodes),

    BrokersCount = length(Nodes),
    BrokersBin = maps:fold(fun(_K, {{index, Index}, {host, Host}, {port, Port}}, Acc) ->
        HostLength = byte_size(Host),
        <<Acc/binary, Index:16, HostLength:16, Host:HostLength/binary, Port:32>>
                           end, <<BrokersCount:32>>, NodesInfo),


    MetadataBin = lists:foldl(fun(Stream, Acc) ->
        StreamLength = byte_size(Stream),
        case rabbit_stream_manager:topology(VirtualHost, Stream) of
            {error, stream_not_found} ->
                <<Acc/binary, StreamLength:16, Stream:StreamLength/binary, ?RESPONSE_CODE_STREAM_DOES_NOT_EXIST:16,
                    -1:16, 0:32>>;
            {ok, #{leader_node := LeaderNode, replica_nodes := Replicas}} ->
                #{LeaderNode := NodeInfo} = NodesInfo,
                {{index, LeaderIndex}, {host, _}, {port, _}} = NodeInfo,
                ReplicasBinary = lists:foldl(fun(Replica, Bin) ->
                    #{Replica := NI} = NodesInfo,
                    {{index, ReplicaIndex}, {host, _}, {port, _}} = NI,
                    <<Bin/binary, ReplicaIndex:16>>
                                             end, <<>>, Replicas),
                ReplicasCount = length(Replicas),

                <<Acc/binary, StreamLength:16, Stream:StreamLength/binary, ?RESPONSE_CODE_OK:16,
                    LeaderIndex:16, ReplicasCount:32, ReplicasBinary/binary>>
        end

                              end, <<StreamCount:32>>, Streams),
    Frame = <<?COMMAND_METADATA:16, ?VERSION_0:16, CorrelationId:32, BrokersBin/binary, MetadataBin/binary>>,
    FrameSize = byte_size(Frame),
    Transport:send(S, <<FrameSize:32, Frame/binary>>),
    {State, Rest};
handle_frame_post_auth(Transport, State,
    <<?COMMAND_CLOSE:16, ?VERSION_0:16, CorrelationId:32,
        ClosingCode:16, ClosingReasonLength:16, ClosingReason:ClosingReasonLength/binary>>, _Rest) ->
    error_logger:info_msg("Received close command ~p ~p~n", [ClosingCode, ClosingReason]),
    Frame = <<?COMMAND_CLOSE:16, ?VERSION_0:16, CorrelationId:32, ?RESPONSE_CODE_OK:16>>,
    frame(Transport, State, Frame),
    {State#stream_connection{connection_step = closing}, <<>>}; %% we ignore any subsequent frames
handle_frame_post_auth(_Transport, State, <<?COMMAND_HEARTBEAT:16, ?VERSION_0:16>>, Rest) ->
    error_logger:info_msg("Received heartbeat frame post auth~n"),
    {State, Rest};
handle_frame_post_auth(Transport, State, Frame, Rest) ->
    error_logger:warning_msg("unknown frame ~p ~p, sending close command.~n", [Frame, Rest]),
    CloseReason = <<"unknown frame">>,
    CloseReasonLength = byte_size(CloseReason),
    CloseFrame = <<?COMMAND_CLOSE:16, ?VERSION_0:16, 1:32, ?RESPONSE_CODE_UNKNOWN_FRAME:16,
        CloseReasonLength:16, CloseReason:CloseReasonLength/binary>>,
    frame(Transport, State, CloseFrame),
    {State#stream_connection{connection_step = close_sent}, Rest}.

parse_map(<<>>, _Count) ->
    {#{}, <<>>};
parse_map(Content, 0) ->
    {#{}, Content};
parse_map(Arguments, Count) ->
    parse_map(#{}, Arguments, Count).

parse_map(Acc, <<>>, _Count) ->
    {Acc, <<>>};
parse_map(Acc, Content, 0) ->
    {Acc, Content};
parse_map(Acc, <<KeySize:16, Key:KeySize/binary, ValueSize:16, Value:ValueSize/binary, Rest/binary>>, Count) ->
    parse_map(maps:put(Key, Value, Acc), Rest, Count - 1).


handle_frame_post_close(_Transport, State,
    <<?COMMAND_CLOSE:16, ?VERSION_0:16, _CorrelationId:32, _ResponseCode:16>>, Rest) ->
    {State#stream_connection{connection_step = closing_done}, Rest};
handle_frame_post_close(_Transport, State, <<?COMMAND_HEARTBEAT:16, ?VERSION_0:16>>, Rest) ->
    error_logger:info_msg("Received heartbeat frame post close~n"),
    {State, Rest};
handle_frame_post_close(_Transport, State, Frame, Rest) ->
    error_logger:warning_msg("ignored frame on close ~p ~p.~n", [Frame, Rest]),
    {State, Rest}.

auth_mechanisms(Sock) ->
    {ok, Configured} = application:get_env(rabbit, auth_mechanisms),
    [rabbit_data_coercion:to_binary(Name) || {Name, Module} <- rabbit_registry:lookup_all(auth_mechanism),
        Module:should_offer(Sock), lists:member(Name, Configured)].

auth_mechanism_to_module(TypeBin, Sock) ->
    case rabbit_registry:binary_to_type(TypeBin) of
        {error, not_found} ->
            error_logger:warning_msg("Unknown authentication mechanism '~p'~n", [TypeBin]),
            {error, not_found};
        T ->
            case {lists:member(TypeBin, auth_mechanisms(Sock)),
                rabbit_registry:lookup_module(auth_mechanism, T)} of
                {true, {ok, Module}} ->
                    {ok, Module};
                _ ->
                    error_logger:warning_msg("Invalid authentication mechanism '~p'~n", [T]),
                    {error, invalid}
            end
    end.

extract_stream_list(<<>>, Streams) ->
    Streams;
extract_stream_list(<<Length:16, Stream:Length/binary, Rest/binary>>, Streams) ->
    extract_stream_list(Rest, [Stream | Streams]).

clean_state_after_stream_deletion(Stream, #stream_connection{clusters = Clusters, stream_subscriptions = StreamSubscriptions,
    consumers = Consumers} = State) ->
    case maps:is_key(Stream, StreamSubscriptions) of
        true ->
            #{Stream := SubscriptionIds} = StreamSubscriptions,
            {cleaned, State#stream_connection{
                clusters = maps:remove(Stream, Clusters),
                stream_subscriptions = maps:remove(Stream, StreamSubscriptions),
                consumers = maps:without(SubscriptionIds, Consumers)
            }};
        false ->
            {not_cleaned, State}
    end.

lookup_leader(Stream, #stream_connection{clusters = Clusters, virtual_host = VirtualHost} = State) ->
    case maps:get(Stream, Clusters, undefined) of
        undefined ->
            case lookup_leader_from_manager(VirtualHost, Stream) of
                cluster_not_found ->
                    cluster_not_found;
                ClusterPid ->
                    {ClusterPid, State#stream_connection{clusters = Clusters#{Stream => ClusterPid}}}
            end;
        ClusterPid ->
            {ClusterPid, State}
    end.

lookup_leader_from_manager(VirtualHost, Stream) ->
    rabbit_stream_manager:lookup_leader(VirtualHost, Stream).

frame(Transport, #stream_connection{socket = S}, Frame) ->
    FrameSize = byte_size(Frame),
    Transport:send(S, [<<FrameSize:32>>, Frame]).

response_ok(Transport, State, CommandId, CorrelationId) ->
    response(Transport, State, CommandId, CorrelationId, ?RESPONSE_CODE_OK).

response(Transport, #stream_connection{socket = S}, CommandId, CorrelationId, ResponseCode) ->
    Transport:send(S, [<<?RESPONSE_FRAME_SIZE:32, CommandId:16, ?VERSION_0:16>>, <<CorrelationId:32>>, <<ResponseCode:16>>]).

subscription_exists(StreamSubscriptions, SubscriptionId) ->
    SubscriptionIds = lists:flatten(maps:values(StreamSubscriptions)),
    lists:any(fun(Id) -> Id =:= SubscriptionId end, SubscriptionIds).

send_file_callback(Transport, #consumer{socket = S, subscription_id = SubscriptionId}) ->
    fun(Size) ->
        FrameSize = 2 + 2 + 4 + Size,
        FrameBeginning = <<FrameSize:32, ?COMMAND_DELIVER:16, ?VERSION_0:16, SubscriptionId:32>>,
        Transport:send(S, FrameBeginning)
    end.

send_chunks(Transport, #consumer{credit = Credit} = State) ->
    send_chunks(Transport, State, Credit).

send_chunks(_Transport, #consumer{segment = Segment}, 0) ->
    {{segment, Segment}, {credit, 0}};
send_chunks(Transport, #consumer{segment = Segment} = State, Credit) ->
    send_chunks(Transport, State, Segment, Credit, true).

send_chunks(_Transport, _State, Segment, 0 = _Credit, _Retry) ->
    {{segment, Segment}, {credit, 0}};
send_chunks(Transport, #consumer{socket = S} = State, Segment, Credit, Retry) ->
    case osiris_log:send_file(S, Segment, send_file_callback(Transport, State)) of
        {ok, Segment1} ->
            send_chunks(
                Transport,
                State,
                Segment1,
                Credit - 1,
                true
            );
        {end_of_stream, Segment1} ->
            case Retry of
                true ->
                    timer:sleep(1),
                    send_chunks(Transport, State, Segment1, Credit, false);
                false ->
                    #consumer{member_pid = Leader} = State,
                    osiris:register_offset_listener(Leader, osiris_log:next_offset(Segment1)),
                    {{segment, Segment1}, {credit, Credit}}
            end
    end.

