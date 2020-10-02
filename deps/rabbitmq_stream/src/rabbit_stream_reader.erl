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

-record(consumer, {
    socket :: rabbit_net:socket(), %% ranch_transport:socket(),
    member_pid :: pid(),
    offset :: osiris:offset(),
    subscription_id :: integer(),
    segment :: osiris_log:state(),
    credit :: integer(),
    stream :: binary()
}).

-record(stream_connection_state, {
    data :: 'none' | binary(),
    blocked :: boolean(),
    consumers :: #{integer() => #consumer{}}
}).

-record(stream_connection, {
    name :: string(),
    helper_sup :: pid(),
    socket :: rabbit_net:socket(),
    stream_leaders :: #{binary() => pid()},
    stream_subscriptions :: #{binary() => [integer()]},
    credits :: atomics:atomics_ref(),
    authentication_state :: atom(),
    user :: 'undefined' | #user{},
    virtual_host :: 'undefined' | binary(),
    connection_step :: atom(), % tcp_connected, peer_properties_exchanged, authenticating, authenticated, tuning, tuned, opened, failure, closing, closing_done
    frame_max :: integer(),
    heartbeater :: any(),
    client_properties = #{} :: #{binary() => binary()},
    monitors = #{} :: #{reference() => binary()}
}).

-record(configuration, {
    initial_credits :: integer(),
    credits_required_for_unblocking :: integer(),
    frame_max :: integer(),
    heartbeat :: integer()
}).

-define(RESPONSE_FRAME_SIZE, 10). % 2 (key) + 2 (version) + 4 (correlation ID) + 2 (response code)
-define(MAX_PERMISSION_CACHE_SIZE, 12).

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
        application:get_env(rabbitmq_stream, proxy_protocol, false)),
    RealSocket = rabbit_net:unwrap_socket(Sock),
    case rabbit_net:connection_string(Sock, inbound) of
        {ok, ConnStr} ->
            Credits = atomics:new(1, [{signed, true}]),
            init_credit(Credits, InitialCredits),
            Connection = #stream_connection{
                name = ConnStr,
                helper_sup = KeepaliveSup,
                socket = RealSocket,
                stream_leaders = #{},
                stream_subscriptions = #{},
                credits = Credits,
                authentication_state = none,
                connection_step = tcp_connected,
                frame_max = FrameMax},
            State = #stream_connection_state{
                consumers = #{}, blocked = false, data = none
            },
            Transport:setopts(RealSocket, [{active, once}]),

            listen_loop_pre_auth(Transport, Connection, State, #configuration{
                initial_credits = InitialCredits,
                credits_required_for_unblocking = CreditsRequiredBeforeUnblocking,
                frame_max = FrameMax,
                heartbeat = Heartbeat
            });
        {Error, Reason} ->
            rabbit_net:fast_close(RealSocket),
            rabbit_log:warning("Closing connection because of ~p ~p~n", [Error, Reason])
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

listen_loop_pre_auth(Transport, #stream_connection{socket = S} = Connection, State,
    #configuration{frame_max = FrameMax, heartbeat = Heartbeat} = Configuration) ->
    {OK, Closed, Error} = Transport:messages(),
    %% FIXME introduce timeout to complete the connection opening (after block should be enough)
    receive
        {OK, S, Data} ->
            #stream_connection{connection_step = ConnectionStep0} = Connection,
            {Connection1, State1} = handle_inbound_data_pre_auth(Transport, Connection, State, Data),
            Transport:setopts(S, [{active, once}]),
            #stream_connection{connection_step = ConnectionStep} = Connection1,
            rabbit_log:info("Transitioned from ~p to ~p~n", [ConnectionStep0, ConnectionStep]),
            case ConnectionStep of
                authenticated ->
                    TuneFrame = <<?COMMAND_TUNE:16, ?VERSION_0:16, FrameMax:32, Heartbeat:32>>,
                    frame(Transport, Connection1, TuneFrame),
                    listen_loop_pre_auth(Transport, Connection1#stream_connection{connection_step = tuning}, State1, Configuration);
                opened ->
                    pg_local:join(rabbit_stream_connections, self()),
                    listen_loop_post_auth(Transport, Connection1, State1, Configuration);
                failure ->
                    close(Transport, S);
                _ ->
                    listen_loop_pre_auth(Transport, Connection1, State1, Configuration)
            end;
        {Closed, S} ->
            rabbit_log:info("Socket ~w closed [~w]~n", [S, self()]),
            ok;
        {Error, S, Reason} ->
            rabbit_log:info("Socket error ~p [~w]~n", [Reason, S, self()]);
        M ->
            rabbit_log:warning("Unknown message ~p~n", [M]),
            close(Transport, S)
    end.

close(Transport, S) ->
    Transport:shutdown(S, write),
    Transport:close(S).

listen_loop_post_auth(Transport, #stream_connection{socket = S,
    stream_subscriptions = StreamSubscriptions, credits = Credits,
    heartbeater = Heartbeater, monitors = Monitors, client_properties = ClientProperties} = Connection,
    #stream_connection_state{consumers = Consumers, blocked = Blocked} = State,
    #configuration{credits_required_for_unblocking = CreditsRequiredForUnblocking} = Configuration) ->
    {OK, Closed, Error} = Transport:messages(),
    receive
        {OK, S, Data} ->
            {Connection1, State1} = handle_inbound_data_post_auth(Transport, Connection, State, Data),
            #stream_connection{connection_step = Step} = Connection1,
            case Step of
                closing ->
                    close(Transport, S);
                close_sent ->
                    rabbit_log:debug("Transitioned to close_sent ~n"),
                    Transport:setopts(S, [{active, once}]),
                    listen_loop_post_close(Transport, Connection1, State1, Configuration);
                _ ->
                    State2 = case Blocked of
                                 true ->
                                     case has_enough_credits_to_unblock(Credits, CreditsRequiredForUnblocking) of
                                         true ->
                                             Transport:setopts(S, [{active, once}]),
                                             ok = rabbit_heartbeat:resume_monitor(Heartbeater),
                                             State1#stream_connection_state{blocked = false};
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
                                             State1#stream_connection_state{blocked = true}
                                     end
                             end,
                    listen_loop_post_auth(Transport, Connection1, State2, Configuration)
            end;
        {'DOWN', MonitorRef, process, _OsirisPid, _Reason} ->
            {Connection1, State1} = case Monitors of
                                        #{MonitorRef := Stream} ->
                                            Monitors1 = maps:remove(MonitorRef, Monitors),
                                            C = Connection#stream_connection{monitors = Monitors1},
                                            case clean_state_after_stream_deletion_or_failure(Stream, C, State) of
                                                {cleaned, NewConnection, NewState} ->
                                                    StreamSize = byte_size(Stream),
                                                    FrameSize = 2 + 2 + 2 + 2 + StreamSize,
                                                    Transport:send(S, [<<FrameSize:32, ?COMMAND_METADATA_UPDATE:16, ?VERSION_0:16,
                                                        ?RESPONSE_CODE_STREAM_NOT_AVAILABLE:16, StreamSize:16, Stream/binary>>]),
                                                    {NewConnection, NewState};
                                                {not_cleaned, SameConnection, SameState} ->
                                                    {SameConnection, SameState}
                                            end;
                                        _ ->
                                            {Connection, State}
                                    end,
            listen_loop_post_auth(Transport, Connection1, State1, Configuration);
        {'$gen_cast', {queue_event, _QueueResource, {osiris_written, _QueueResource, CorrelationList}}} ->
            {FirstPublisherId, _FirstPublishingId} = lists:nth(1, CorrelationList),
            {LastPublisherId, LastPublishingIds, LastCount} = lists:foldl(fun({PublisherId, PublishingId}, {CurrentPublisherId, PublishingIds, Count}) ->
                case PublisherId of
                    CurrentPublisherId ->
                        {CurrentPublisherId, [PublishingIds, <<PublishingId:64>>], Count + 1};
                    OtherPublisherId ->
                        FrameSize = 2 + 2 + 1 + 4 + Count * 8,
                        %% FIXME enforce max frame size
                        %% in practice, this should be necessary only for very large chunks and for very small frame size limits
                        Transport:send(S, [<<FrameSize:32, ?COMMAND_PUBLISH_CONFIRM:16, ?VERSION_0:16>>,
                            <<CurrentPublisherId:8>>,
                            <<Count:32>>, PublishingIds]),
                        {OtherPublisherId, <<PublishingId:64>>, 1}
                end
                                                                          end, {FirstPublisherId, <<>>, 0}, CorrelationList),
            FrameSize = 2 + 2 + 1 + 4 + LastCount * 8,
            Transport:send(S, [<<FrameSize:32, ?COMMAND_PUBLISH_CONFIRM:16, ?VERSION_0:16>>,
                <<LastPublisherId:8>>,
                <<LastCount:32>>, LastPublishingIds]),
            CorrelationIdCount = length(CorrelationList),
            add_credits(Credits, CorrelationIdCount),
            State1 = case Blocked of
                         true ->
                             case has_enough_credits_to_unblock(Credits, CreditsRequiredForUnblocking) of
                                 true ->
                                     Transport:setopts(S, [{active, once}]),
                                     ok = rabbit_heartbeat:resume_monitor(Heartbeater),
                                     State#stream_connection_state{blocked = false};
                                 false ->
                                     State
                             end;
                         false ->
                             State
                     end,
            listen_loop_post_auth(Transport, Connection, State1, Configuration);
        {'$gen_cast', {queue_event, #resource{name = StreamName}, {osiris_offset, _QueueResource, -1}}} ->
            rabbit_log:info("received osiris offset event for ~p with offset ~p~n", [StreamName, -1]),
            listen_loop_post_auth(Transport, Connection, State, Configuration);
        {'$gen_cast', {queue_event, #resource{name = StreamName}, {osiris_offset, _QueueResource, Offset}}} when Offset > -1 ->
            {Connection1, State1} = case maps:get(StreamName, StreamSubscriptions, undefined) of
                                        undefined ->
                                            rabbit_log:info("osiris offset event for ~p, but no subscription (leftover messages after unsubscribe?)", [StreamName]),
                                            {Connection, State};
                                        [] ->
                                            rabbit_log:info("osiris offset event for ~p, but no registered consumers!", [StreamName]),
                                            {Connection#stream_connection{stream_subscriptions = maps:remove(StreamName, StreamSubscriptions)}, State};
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
                                            {Connection, State#stream_connection_state{consumers = Consumers1}}
                                    end,
            listen_loop_post_auth(Transport, Connection1, State1, Configuration);
        heartbeat_send ->
            Frame = <<?COMMAND_HEARTBEAT:16, ?VERSION_0:16>>,
            case catch frame(Transport, Connection, Frame) of
                ok ->
                    listen_loop_post_auth(Transport, Connection, State, Configuration);
                Unexpected ->
                    rabbit_log:info("Heartbeat send error ~p, closing connection~n", [Unexpected]),
                    C1 = demonitor_all_streams(Connection),
                    close(Transport, C1)
            end;
        heartbeat_timeout ->
            rabbit_log:info("Heartbeat timeout, closing connection~n"),
            C1 = demonitor_all_streams(Connection),
            close(Transport, C1);
        {infos, From} ->
            From ! {self(), ClientProperties},
            listen_loop_post_auth(Transport, Connection, State, Configuration);
        {Closed, S} ->
            demonitor_all_streams(Connection),
            rabbit_log:info("Socket ~w closed [~w]~n", [S, self()]),
            ok;
        {Error, S, Reason} ->
            demonitor_all_streams(Connection),
            rabbit_log:info("Socket error ~p [~w]~n", [Reason, S, self()]);
        M ->
            rabbit_log:warning("Unknown message ~p~n", [M]),
            %% FIXME send close
            listen_loop_post_auth(Transport, Connection, State, Configuration)
    end.

listen_loop_post_close(Transport, #stream_connection{socket = S} = Connection, State, Configuration) ->
    {OK, Closed, Error} = Transport:messages(),
    %% FIXME demonitor streams
    %% FIXME introduce timeout to complete the connection closing (after block should be enough)
    receive
        {OK, S, Data} ->
            Transport:setopts(S, [{active, once}]),
            {Connection1, State1} = handle_inbound_data_post_close(Transport, Connection, State, Data),
            #stream_connection{connection_step = Step} = Connection1,
            case Step of
                closing_done ->
                    rabbit_log:debug("Received close confirmation from client"),
                    close(Transport, S);
                _ ->
                    Transport:setopts(S, [{active, once}]),
                    listen_loop_post_close(Transport, Connection1, State1, Configuration)
            end;
        {Closed, S} ->
            rabbit_log:info("Socket ~w closed [~w]~n", [S, self()]),
            ok;
        {Error, S, Reason} ->
            rabbit_log:info("Socket error ~p [~w]~n", [Reason, S, self()]),
            close(Transport, S);
        M ->
            rabbit_log:warning("Ignored message on closing ~p~n", [M])
    end.

handle_inbound_data_pre_auth(Transport, Connection, State, Rest) ->
    handle_inbound_data(Transport, Connection, State, Rest, fun handle_frame_pre_auth/5).

handle_inbound_data_post_auth(Transport, Connection, State, Rest) ->
    handle_inbound_data(Transport, Connection, State, Rest, fun handle_frame_post_auth/5).

handle_inbound_data_post_close(Transport, Connection, State, Rest) ->
    handle_inbound_data(Transport, Connection, State, Rest, fun handle_frame_post_close/5).

handle_inbound_data(_Transport, Connection, State, <<>>, _HandleFrameFun) ->
    {Connection, State};
handle_inbound_data(Transport, #stream_connection{frame_max = FrameMax} = Connection,
    #stream_connection_state{data = none} = State, <<Size:32, _Frame:Size/binary, _Rest/bits>>, _HandleFrameFun)
    when FrameMax /= 0 andalso Size > FrameMax - 4 ->
    CloseReason = <<"frame too large">>,
    CloseReasonLength = byte_size(CloseReason),
    CloseFrame = <<?COMMAND_CLOSE:16, ?VERSION_0:16, 1:32, ?RESPONSE_CODE_FRAME_TOO_LARGE:16,
        CloseReasonLength:16, CloseReason:CloseReasonLength/binary>>,
    frame(Transport, Connection, CloseFrame),
    {Connection#stream_connection{connection_step = close_sent}, State};
handle_inbound_data(Transport, Connection,
    #stream_connection_state{data = none} = State, <<Size:32, Frame:Size/binary, Rest/bits>>, HandleFrameFun) ->
    {Connection1, State1, Rest1} = HandleFrameFun(Transport, Connection, State, Frame, Rest),
    handle_inbound_data(Transport, Connection1, State1, Rest1, HandleFrameFun);
handle_inbound_data(_Transport, Connection, #stream_connection_state{data = none} = State, Data, _HandleFrameFun) ->
    {Connection, State#stream_connection_state{data = Data}};
handle_inbound_data(Transport, Connection, #stream_connection_state{data = Leftover} = State, Data, HandleFrameFun) ->
    State1 = State#stream_connection_state{data = none},
    %% FIXME avoid concatenation to avoid a new binary allocation
    %% see osiris_replica:parse_chunk/3
    handle_inbound_data(Transport, Connection, State1, <<Leftover/binary, Data/binary>>, HandleFrameFun).

write_messages(_ClusterLeader, _PublisherId, <<>>) ->
    ok;
write_messages(ClusterLeader, PublisherId, <<PublishingId:64, 0:1, MessageSize:31, Message:MessageSize/binary, Rest/binary>>) ->
    % FIXME handle write error
    ok = osiris:write(ClusterLeader, {PublisherId, PublishingId}, Message),
    write_messages(ClusterLeader, PublisherId, Rest);
write_messages(ClusterLeader, PublisherId, <<PublishingId:64, 1:1, CompressionType:3, _Unused:4, MessageCount:16, BatchSize:32, Batch:BatchSize/binary, Rest/binary>>) ->
    % FIXME handle write error
    ok = osiris:write(ClusterLeader, {PublisherId, PublishingId}, {batch, MessageCount, CompressionType, Batch}),
    write_messages(ClusterLeader, PublisherId, Rest).

generate_publishing_error_details(Acc, _Code, <<>>) ->
    Acc;
generate_publishing_error_details(Acc, Code, <<PublishingId:64, MessageSize:32, _Message:MessageSize/binary, Rest/binary>>) ->
    generate_publishing_error_details(
        <<Acc/binary, PublishingId:64, Code:16>>,
        Code,
        Rest).

handle_frame_pre_auth(Transport, #stream_connection{socket = S} = Connection, State,
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
    {Connection#stream_connection{client_properties = ClientProperties, authentication_state = peer_properties_exchanged}, State, Rest};
handle_frame_pre_auth(Transport, #stream_connection{socket = S} = Connection, State,
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
    {Connection, State, Rest};
handle_frame_pre_auth(Transport, #stream_connection{socket = S, authentication_state = AuthState0} = Connection0, State,
    <<?COMMAND_SASL_AUTHENTICATE:16, ?VERSION_0:16, CorrelationId:32,
        MechanismLength:16, Mechanism:MechanismLength/binary,
        SaslFragment/binary>>, Rest) ->

    SaslBin = case SaslFragment of
                  <<-1:32/signed>> ->
                      <<>>;
                  <<SaslBinaryLength:32, SaslBinary:SaslBinaryLength/binary>> ->
                      SaslBinary
              end,

    {Connection1, Rest1} = case auth_mechanism_to_module(Mechanism, S) of
                               {ok, AuthMechanism} ->
                                   AuthState = case AuthState0 of
                                                   none ->
                                                       AuthMechanism:init(S);
                                                   AS ->
                                                       AS
                                               end,
                                   {S1, FrameFragment} = case AuthMechanism:handle_response(SaslBin, AuthState) of
                                                             {refused, _Username, Msg, Args} ->
                                                                 rabbit_log:warning(Msg, Args),
                                                                 {Connection0#stream_connection{connection_step = failure}, <<?RESPONSE_AUTHENTICATION_FAILURE:16>>};
                                                             {protocol_error, Msg, Args} ->
                                                                 rabbit_log:warning(Msg, Args),
                                                                 {Connection0#stream_connection{connection_step = failure}, <<?RESPONSE_SASL_ERROR:16>>};
                                                             {challenge, Challenge, AuthState1} ->
                                                                 ChallengeSize = byte_size(Challenge),
                                                                 {Connection0#stream_connection{authentication_state = AuthState1, connection_step = authenticating},
                                                                     <<?RESPONSE_SASL_CHALLENGE:16, ChallengeSize:32, Challenge/binary>>
                                                                 };
                                                             {ok, User = #user{username = Username}} ->
                                                                 case rabbit_access_control:check_user_loopback(Username, S) of
                                                                     ok ->
                                                                         {Connection0#stream_connection{authentication_state = done, user = User, connection_step = authenticated},
                                                                             <<?RESPONSE_CODE_OK:16>>
                                                                         };
                                                                     not_allowed ->
                                                                         rabbit_log:warning("User '~s' can only connect via localhost~n", [Username]),
                                                                         {Connection0#stream_connection{connection_step = failure}, <<?RESPONSE_SASL_AUTHENTICATION_FAILURE_LOOPBACK:16>>}
                                                                 end
                                                         end,
                                   Frame = <<?COMMAND_SASL_AUTHENTICATE:16, ?VERSION_0:16, CorrelationId:32, FrameFragment/binary>>,
                                   frame(Transport, S1, Frame),
                                   {S1, Rest};
                               {error, _} ->
                                   Frame = <<?COMMAND_SASL_AUTHENTICATE:16, ?VERSION_0:16, CorrelationId:32, ?RESPONSE_SASL_MECHANISM_NOT_SUPPORTED:16>>,
                                   frame(Transport, Connection0, Frame),
                                   {Connection0#stream_connection{connection_step = failure}, Rest}
                           end,

    {Connection1, State, Rest1};
handle_frame_pre_auth(_Transport, #stream_connection{helper_sup = SupPid, socket = Sock, name = ConnectionName} = Connection, State,
    <<?COMMAND_TUNE:16, ?VERSION_0:16, FrameMax:32, Heartbeat:32>>, Rest) ->
    rabbit_log:info("Tuning response ~p ~p ~n", [FrameMax, Heartbeat]),
    Parent = self(),
    %% sending a message to the main process so the heartbeat frame is sent from this main process
    %% otherwise heartbeat frames can interleave with chunk delivery
    %% (chunk delivery is made of 2 calls on the socket, one for the header and one send_file for the chunk,
    %% we don't want a heartbeat frame to sneak in in-between)
    SendFun =
        fun() ->
            Parent ! heartbeat_send,
            ok
        end,
    ReceiveFun = fun() -> Parent ! heartbeat_timeout end,
    Heartbeater = rabbit_heartbeat:start(
        SupPid, Sock, ConnectionName,
        Heartbeat, SendFun, Heartbeat, ReceiveFun),

    {Connection#stream_connection{connection_step = tuned, frame_max = FrameMax, heartbeater = Heartbeater}, State, Rest};
handle_frame_pre_auth(Transport, #stream_connection{user = User, socket = S} = Connection, State,
    <<?COMMAND_OPEN:16, ?VERSION_0:16, CorrelationId:32,
        VirtualHostLength:16, VirtualHost:VirtualHostLength/binary>>, Rest) ->

    %% FIXME enforce connection limit (see rabbit_reader:is_over_connection_limit/2)

    {Connection1, Frame} = try
                               rabbit_access_control:check_vhost_access(User, VirtualHost, {socket, S}, #{}),
                               F = <<?COMMAND_OPEN:16, ?VERSION_0:16, CorrelationId:32, ?RESPONSE_CODE_OK:16>>,
                               %% FIXME check if vhost is alive (see rabbit_reader:is_vhost_alive/2)
                               {Connection#stream_connection{connection_step = opened, virtual_host = VirtualHost}, F}
                           catch
                               exit:_ ->
                                   Fr = <<?COMMAND_OPEN:16, ?VERSION_0:16, CorrelationId:32, ?RESPONSE_VHOST_ACCESS_FAILURE:16>>,
                                   {Connection#stream_connection{connection_step = failure}, Fr}
                           end,

    frame(Transport, Connection1, Frame),

    {Connection1, State, Rest};
handle_frame_pre_auth(_Transport, Connection, State, <<?COMMAND_HEARTBEAT:16, ?VERSION_0:16>>, Rest) ->
    rabbit_log:info("Received heartbeat frame pre auth~n"),
    {Connection, State, Rest};
handle_frame_pre_auth(_Transport, Connection, State, Frame, Rest) ->
    rabbit_log:warning("unknown frame ~p ~p, closing connection.~n", [Frame, Rest]),
    {Connection#stream_connection{connection_step = failure}, State, Rest}.

handle_frame_post_auth(Transport, #stream_connection{socket = S, credits = Credits,
    virtual_host = VirtualHost, user = User} = Connection, State,
    <<?COMMAND_PUBLISH:16, ?VERSION_0:16,
        StreamSize:16, Stream:StreamSize/binary,
        PublisherId:8/unsigned,
        MessageCount:32, Messages/binary>>, Rest) ->
    case check_write_permitted(#resource{name = Stream, kind = queue, virtual_host = VirtualHost}, User, #{}) of
        ok ->
            case lookup_leader(Stream, Connection) of
                cluster_not_found ->
                    FrameSize = 2 + 2 + 1 + 4 + (8 + 2) * MessageCount,
                    Details = generate_publishing_error_details(<<>>, ?RESPONSE_CODE_STREAM_DOES_NOT_EXIST, Messages),
                    Transport:send(S, [<<FrameSize:32, ?COMMAND_PUBLISH_ERROR:16, ?VERSION_0:16,
                        PublisherId:8,
                        MessageCount:32, Details/binary>>]),
                    {Connection, State, Rest};
                {ClusterLeader, Connection1} ->
                    write_messages(ClusterLeader, PublisherId, Messages),
                    sub_credits(Credits, MessageCount),
                    {Connection1, State, Rest}
            end;
        error ->
            FrameSize = 2 + 2 + 1 + 4 + (8 + 2) * MessageCount,
            Details = generate_publishing_error_details(<<>>, ?RESPONSE_CODE_ACCESS_REFUSED, Messages),
            Transport:send(S, [<<FrameSize:32, ?COMMAND_PUBLISH_ERROR:16, ?VERSION_0:16,
                PublisherId:8,
                MessageCount:32, Details/binary>>]),
            {Connection, State, Rest}
    end;
handle_frame_post_auth(Transport, #stream_connection{socket = Socket,
    stream_subscriptions = StreamSubscriptions, virtual_host = VirtualHost, user = User} = Connection,
    #stream_connection_state{consumers = Consumers} = State,
    <<?COMMAND_SUBSCRIBE:16, ?VERSION_0:16, CorrelationId:32, SubscriptionId:8/unsigned, StreamSize:16, Stream:StreamSize/binary,
        OffsetType:16/signed, OffsetAndCredit/binary>>, Rest) ->
    case check_read_permitted(#resource{name = Stream, kind = queue, virtual_host = VirtualHost}, User, #{}) of
        ok ->
            case rabbit_stream_manager:lookup_local_member(VirtualHost, Stream) of
                {error, not_available} ->
                    response(Transport, Connection, ?COMMAND_SUBSCRIBE, CorrelationId, ?RESPONSE_CODE_STREAM_NOT_AVAILABLE),
                    {Connection, State, Rest};
                {error, not_found} ->
                    response(Transport, Connection, ?COMMAND_SUBSCRIBE, CorrelationId, ?RESPONSE_CODE_STREAM_DOES_NOT_EXIST),
                    {Connection, State, Rest};
                {ok, LocalMemberPid} ->
                    case subscription_exists(StreamSubscriptions, SubscriptionId) of
                        true ->
                            response(Transport, Connection, ?COMMAND_SUBSCRIBE, CorrelationId, ?RESPONSE_CODE_SUBSCRIPTION_ID_ALREADY_EXISTS),
                            {Connection, State, Rest};
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

                            Connection1 = maybe_monitor_stream(LocalMemberPid, Stream, Connection),

                            response_ok(Transport, Connection, ?COMMAND_SUBSCRIBE, CorrelationId),

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
                            {Connection1#stream_connection{stream_subscriptions = StreamSubscriptions1}, State#stream_connection_state{consumers = Consumers1}, Rest}
                    end
            end;
        error ->
            response(Transport, Connection, ?COMMAND_SUBSCRIBE, CorrelationId, ?RESPONSE_CODE_ACCESS_REFUSED),
            {Connection, State, Rest}
    end;
handle_frame_post_auth(Transport, #stream_connection{stream_subscriptions = StreamSubscriptions,
    stream_leaders = StreamLeaders} = Connection,
    #stream_connection_state{consumers = Consumers} = State,
    <<?COMMAND_UNSUBSCRIBE:16, ?VERSION_0:16, CorrelationId:32, SubscriptionId:8/unsigned>>, Rest) ->
    case subscription_exists(StreamSubscriptions, SubscriptionId) of
        false ->
            response(Transport, Connection, ?COMMAND_UNSUBSCRIBE, CorrelationId, ?RESPONSE_CODE_SUBSCRIPTION_ID_DOES_NOT_EXIST),
            {Connection, State, Rest};
        true ->
            #{SubscriptionId := Consumer} = Consumers,
            Stream = Consumer#consumer.stream,
            #{Stream := SubscriptionsForThisStream} = StreamSubscriptions,
            SubscriptionsForThisStream1 = lists:delete(SubscriptionId, SubscriptionsForThisStream),
            {Connection1, StreamSubscriptions1, StreamLeaders1} =
                case length(SubscriptionsForThisStream1) of
                    0 ->
                        %% no more subscriptions for this stream
                        %% we unregister even though it could affect publishing if the stream is published to
                        %% from this connection and is deleted.
                        %% to mitigate this, we remove the stream from the leaders cache
                        %% this way the stream leader will be looked up in the next publish command
                        %% and registered to.
                        C = demonitor_stream(Stream, Connection),
                        {C, maps:remove(Stream, StreamSubscriptions),
                            maps:remove(Stream, StreamLeaders)
                        };
                    _ ->
                        {Connection, StreamSubscriptions#{Stream => SubscriptionsForThisStream1}, StreamLeaders}
                end,
            Consumers1 = maps:remove(SubscriptionId, Consumers),
            response_ok(Transport, Connection, ?COMMAND_SUBSCRIBE, CorrelationId),
            {Connection1#stream_connection{
                stream_subscriptions = StreamSubscriptions1,
                stream_leaders = StreamLeaders1
            }, State#stream_connection_state{consumers = Consumers1}, Rest}
    end;
handle_frame_post_auth(Transport, #stream_connection{socket = S} = Connection,
    #stream_connection_state{consumers = Consumers} = State,
    <<?COMMAND_CREDIT:16, ?VERSION_0:16, SubscriptionId:8/unsigned, Credit:16/signed>>, Rest) ->

    case Consumers of
        #{SubscriptionId := Consumer} ->
            #consumer{credit = AvailableCredit} = Consumer,

            {{segment, Segment1}, {credit, Credit1}} = send_chunks(
                Transport,
                Consumer,
                AvailableCredit + Credit
            ),

            Consumer1 = Consumer#consumer{segment = Segment1, credit = Credit1},
            {Connection, State#stream_connection_state{consumers = Consumers#{SubscriptionId => Consumer1}}, Rest};
        _ ->
            rabbit_log:warning("Giving credit to unknown subscription: ~p~n", [SubscriptionId]),
            Frame = <<?COMMAND_CREDIT:16, ?VERSION_0:16, ?RESPONSE_CODE_SUBSCRIPTION_ID_DOES_NOT_EXIST:16, SubscriptionId:8>>,
            FrameSize = byte_size(Frame),
            Transport:send(S, [<<FrameSize:32>>, Frame]),
            {Connection, State, Rest}
    end;
handle_frame_post_auth(_Transport, #stream_connection{virtual_host = VirtualHost, user = User} = Connection,
    State,
    <<?COMMAND_COMMIT_OFFSET:16, ?VERSION_0:16, _CorrelationId:32,
      ReferenceSize:16, Reference:ReferenceSize/binary,
      StreamSize:16, Stream:StreamSize/binary, Offset:64>>, Rest) ->

    case check_write_permitted(#resource{name = Stream, kind = queue, virtual_host = VirtualHost}, User, #{}) of
        ok ->
            case lookup_leader(Stream, Connection) of
                cluster_not_found ->
                    rabbit_log:info("Could not find leader to commit offset on ~p~n", [Stream]),
                    %% FIXME commit offset is fire-and-forget, so no response even if error, change this?
                    {Connection, State, Rest};
                {ClusterLeader, Connection1} ->
                    osiris:write_tracking(ClusterLeader, Reference, Offset),
                    {Connection1, State, Rest}
            end;
        error ->
            %% FIXME commit offset is fire-and-forget, so no response even if error, change this?
            rabbit_log:info("Not authorized to commit offset on ~p~n", [Stream]),
            {Connection, State, Rest}
    end;
handle_frame_post_auth(Transport, #stream_connection{socket = S, virtual_host = VirtualHost, user = User} = Connection,
    State,
    <<?COMMAND_QUERY_OFFSET:16, ?VERSION_0:16, CorrelationId:32,
        ReferenceSize:16, Reference:ReferenceSize/binary,
        StreamSize:16, Stream:StreamSize/binary>>, Rest) ->
    FrameSize = ?RESPONSE_FRAME_SIZE + 8,
    {ResponseCode, Offset} = case check_read_permitted(#resource{name = Stream, kind = queue, virtual_host = VirtualHost}, User, #{}) of
        ok ->
            case rabbit_stream_manager:lookup_local_member(VirtualHost, Stream) of
                {error, not_found} ->
                    {?RESPONSE_CODE_STREAM_DOES_NOT_EXIST, 0};
                {ok, LocalMemberPid} ->
                    {?RESPONSE_CODE_OK, case osiris:read_tracking(LocalMemberPid, Reference) of
                        undefined ->
                            0;
                        Offt ->
                            Offt
                    end}
            end;
        error ->
            {?RESPONSE_CODE_ACCESS_REFUSED, 0}
    end,
    Transport:send(S, [<<FrameSize:32, ?COMMAND_QUERY_OFFSET:16, ?VERSION_0:16>>,
        <<CorrelationId:32>>, <<ResponseCode:16>>, <<Offset:64>>]),
    {Connection, State, Rest};
handle_frame_post_auth(Transport, #stream_connection{virtual_host = VirtualHost, user = #user{username = Username} = User} = Connection,
    State,
    <<?COMMAND_CREATE_STREAM:16, ?VERSION_0:16, CorrelationId:32, StreamSize:16, Stream:StreamSize/binary,
        ArgumentsCount:32, ArgumentsBinary/binary>>, Rest) ->
    {Arguments, _Rest} = parse_map(ArgumentsBinary, ArgumentsCount),
    case check_configure_permitted(#resource{name = Stream, kind = queue, virtual_host = VirtualHost}, User, #{}) of
        ok ->
            case rabbit_stream_manager:create(VirtualHost, Stream, Arguments, Username) of
                {ok, #{leader_pid := LeaderPid, replica_pids := ReturnedReplicas}} ->
                    rabbit_log:info("Created cluster with leader ~p and replicas ~p~n", [LeaderPid, ReturnedReplicas]),
                    response_ok(Transport, Connection, ?COMMAND_CREATE_STREAM, CorrelationId),
                    {Connection, State, Rest};
                {error, reference_already_exists} ->
                    response(Transport, Connection, ?COMMAND_CREATE_STREAM, CorrelationId, ?RESPONSE_CODE_STREAM_ALREADY_EXISTS),
                    {Connection, State, Rest};
                {error, _} ->
                    response(Transport, Connection, ?COMMAND_CREATE_STREAM, CorrelationId, ?RESPONSE_CODE_INTERNAL_ERROR),
                    {Connection, State, Rest}
            end;
        error ->
            response(Transport, Connection, ?COMMAND_CREATE_STREAM, CorrelationId, ?RESPONSE_CODE_ACCESS_REFUSED),
            {Connection, State, Rest}
    end;
handle_frame_post_auth(Transport, #stream_connection{socket = S, virtual_host = VirtualHost,
    user = #user{username = Username} = User} = Connection, State,
    <<?COMMAND_DELETE_STREAM:16, ?VERSION_0:16, CorrelationId:32, StreamSize:16, Stream:StreamSize/binary>>, Rest) ->
    case check_configure_permitted(#resource{name = Stream, kind = queue, virtual_host = VirtualHost}, User, #{}) of
        ok ->
            case rabbit_stream_manager:delete(VirtualHost, Stream, Username) of
                {ok, deleted} ->
                    response_ok(Transport, Connection, ?COMMAND_DELETE_STREAM, CorrelationId),
                    {Connection1, State1} = case clean_state_after_stream_deletion_or_failure(Stream, Connection, State) of
                                                {cleaned, NewConnection, NewState} ->
                                                    StreamSize = byte_size(Stream),
                                                    FrameSize = 2 + 2 + 2 + 2 + StreamSize,
                                                    Transport:send(S, [<<FrameSize:32, ?COMMAND_METADATA_UPDATE:16, ?VERSION_0:16,
                                                        ?RESPONSE_CODE_STREAM_NOT_AVAILABLE:16, StreamSize:16, Stream/binary>>]),
                                                    {NewConnection, NewState};
                                                {not_cleaned, SameConnection, SameState} ->
                                                    {SameConnection, SameState}
                                            end,
                    {Connection1, State1, Rest};
                {error, reference_not_found} ->
                    response(Transport, Connection, ?COMMAND_DELETE_STREAM, CorrelationId, ?RESPONSE_CODE_STREAM_DOES_NOT_EXIST),
                    {Connection, State, Rest}
            end;
        error ->
            response(Transport, Connection, ?COMMAND_DELETE_STREAM, CorrelationId, ?RESPONSE_CODE_ACCESS_REFUSED),
            {Connection, State, Rest}
    end;
handle_frame_post_auth(Transport, #stream_connection{socket = S, virtual_host = VirtualHost} = Connection, State,
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
        case {is_binary(Host), is_integer(Port)} of
            {true, true} ->
                {Acc#{Node => {{index, Index}, {host, Host}, {port, Port}}}, Index + 1};
            _ ->
                rabbit_log:warning("Error when retrieving broker metadata: ~p ~p~n", [Host, Port]),
                {Acc, Index}
        end
                                 end, {#{}, 0}, Nodes),

    BrokersCount = map_size(NodesInfo),
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
            {error, stream_not_available} ->
                <<Acc/binary, StreamLength:16, Stream:StreamLength/binary, ?RESPONSE_CODE_STREAM_NOT_AVAILABLE:16,
                    -1:16, 0:32>>;
            {ok, #{leader_node := LeaderNode, replica_nodes := Replicas}} ->
                LeaderIndex = case NodesInfo of
                                  #{LeaderNode := NodeInfo} ->
                                      {{index, LeaderIdx}, {host, _}, {port, _}} = NodeInfo,
                                      LeaderIdx;
                                  _ ->
                                      -1
                              end,
                {ReplicasBinary, ReplicasCount} = lists:foldl(fun(Replica, {Bin, Count}) ->
                    case NodesInfo of
                        #{Replica := NI} ->
                            {{index, ReplicaIndex}, {host, _}, {port, _}} = NI,
                            {<<Bin/binary, ReplicaIndex:16>>, Count + 1};
                        _ ->
                            {Bin, Count}
                    end


                                                              end, {<<>>, 0}, Replicas),
                <<Acc/binary, StreamLength:16, Stream:StreamLength/binary, ?RESPONSE_CODE_OK:16,
                    LeaderIndex:16, ReplicasCount:32, ReplicasBinary/binary>>
        end

                              end, <<StreamCount:32>>, Streams),
    Frame = <<?COMMAND_METADATA:16, ?VERSION_0:16, CorrelationId:32, BrokersBin/binary, MetadataBin/binary>>,
    FrameSize = byte_size(Frame),
    Transport:send(S, <<FrameSize:32, Frame/binary>>),
    {Connection, State, Rest};
handle_frame_post_auth(Transport, Connection, State,
    <<?COMMAND_CLOSE:16, ?VERSION_0:16, CorrelationId:32,
        ClosingCode:16, ClosingReasonLength:16, ClosingReason:ClosingReasonLength/binary>>, _Rest) ->
    rabbit_log:info("Received close command ~p ~p~n", [ClosingCode, ClosingReason]),
    Frame = <<?COMMAND_CLOSE:16, ?VERSION_0:16, CorrelationId:32, ?RESPONSE_CODE_OK:16>>,
    frame(Transport, Connection, Frame),
    {Connection#stream_connection{connection_step = closing}, State, <<>>}; %% we ignore any subsequent frames
handle_frame_post_auth(_Transport, Connection, State, <<?COMMAND_HEARTBEAT:16, ?VERSION_0:16>>, Rest) ->
    rabbit_log:info("Received heartbeat frame post auth~n"),
    {Connection, State, Rest};
handle_frame_post_auth(Transport, Connection, State, Frame, Rest) ->
    rabbit_log:warning("unknown frame ~p ~p, sending close command.~n", [Frame, Rest]),
    CloseReason = <<"unknown frame">>,
    CloseReasonLength = byte_size(CloseReason),
    CloseFrame = <<?COMMAND_CLOSE:16, ?VERSION_0:16, 1:32, ?RESPONSE_CODE_UNKNOWN_FRAME:16,
        CloseReasonLength:16, CloseReason:CloseReasonLength/binary>>,
    frame(Transport, Connection, CloseFrame),
    {Connection#stream_connection{connection_step = close_sent}, State, Rest}.

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


handle_frame_post_close(_Transport, Connection, State,
    <<?COMMAND_CLOSE:16, ?VERSION_0:16, _CorrelationId:32, _ResponseCode:16>>, Rest) ->
    rabbit_log:info("Received close confirmation~n"),
    {Connection#stream_connection{connection_step = closing_done}, State, Rest};
handle_frame_post_close(_Transport, Connection, State, <<?COMMAND_HEARTBEAT:16, ?VERSION_0:16>>, Rest) ->
    rabbit_log:info("Received heartbeat frame post close~n"),
    {Connection, State, Rest};
handle_frame_post_close(_Transport, Connection, State, Frame, Rest) ->
    rabbit_log:warning("ignored frame on close ~p ~p.~n", [Frame, Rest]),
    {Connection, State, Rest}.

auth_mechanisms(Sock) ->
    {ok, Configured} = application:get_env(rabbit, auth_mechanisms),
    [rabbit_data_coercion:to_binary(Name) || {Name, Module} <- rabbit_registry:lookup_all(auth_mechanism),
        Module:should_offer(Sock), lists:member(Name, Configured)].

auth_mechanism_to_module(TypeBin, Sock) ->
    case rabbit_registry:binary_to_type(TypeBin) of
        {error, not_found} ->
            rabbit_log:warning("Unknown authentication mechanism '~p'~n", [TypeBin]),
            {error, not_found};
        T ->
            case {lists:member(TypeBin, auth_mechanisms(Sock)),
                rabbit_registry:lookup_module(auth_mechanism, T)} of
                {true, {ok, Module}} ->
                    {ok, Module};
                _ ->
                    rabbit_log:warning("Invalid authentication mechanism '~p'~n", [T]),
                    {error, invalid}
            end
    end.

extract_stream_list(<<>>, Streams) ->
    Streams;
extract_stream_list(<<Length:16, Stream:Length/binary, Rest/binary>>, Streams) ->
    extract_stream_list(Rest, [Stream | Streams]).

clean_state_after_stream_deletion_or_failure(Stream, #stream_connection{stream_leaders = StreamLeaders, stream_subscriptions = StreamSubscriptions} = Connection,
    #stream_connection_state{consumers = Consumers} = State) ->
    case {maps:is_key(Stream, StreamSubscriptions), maps:is_key(Stream, StreamLeaders)} of
        {true, _} ->
            #{Stream := SubscriptionIds} = StreamSubscriptions,
            {cleaned, Connection#stream_connection{
                stream_leaders = maps:remove(Stream, StreamLeaders),
                stream_subscriptions = maps:remove(Stream, StreamSubscriptions)
            }, State#stream_connection_state{consumers = maps:without(SubscriptionIds, Consumers)}};
        {false, true} ->
            {cleaned, Connection#stream_connection{
                stream_leaders = maps:remove(Stream, StreamLeaders)
            }, State};
        {false, false} ->
            {not_cleaned, Connection, State}
    end.

lookup_leader(Stream, #stream_connection{stream_leaders = StreamLeaders, virtual_host = VirtualHost} = Connection) ->
    case maps:get(Stream, StreamLeaders, undefined) of
        undefined ->
            case lookup_leader_from_manager(VirtualHost, Stream) of
                cluster_not_found ->
                    cluster_not_found;
                LeaderPid ->
                    Connection1 = maybe_monitor_stream(LeaderPid, Stream, Connection),
                    {LeaderPid, Connection1#stream_connection{stream_leaders = StreamLeaders#{Stream => LeaderPid}}}
            end;
        LeaderPid ->
            {LeaderPid, Connection}
    end.

lookup_leader_from_manager(VirtualHost, Stream) ->
    rabbit_stream_manager:lookup_leader(VirtualHost, Stream).

maybe_monitor_stream(Pid, Stream, #stream_connection{monitors = Monitors} = Connection) ->
    case lists:member(Stream, maps:values(Monitors)) of
        true ->
            Connection;
        false ->
            MonitorRef = monitor(process, Pid),
            Connection#stream_connection{monitors = maps:put(MonitorRef, Stream, Monitors)}
    end.

demonitor_stream(Stream, #stream_connection{monitors = Monitors0} = Connection) ->
    Monitors = maps:fold(fun(MonitorRef, Strm, Acc) ->
        case Strm of
            Stream ->
                Acc;
            _ ->
                maps:put(MonitorRef, Strm, Acc)

        end
                         end, #{}, Monitors0),
    Connection#stream_connection{monitors = Monitors}.

demonitor_all_streams(#stream_connection{monitors = Monitors} = Connection) ->
    lists:foreach(fun(MonitorRef) ->
        demonitor(MonitorRef, [flush])
                  end, maps:keys(Monitors)),
    Connection#stream_connection{monitors = #{}}.

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
        FrameSize = 2 + 2 + 1 + Size,
        FrameBeginning = <<FrameSize:32, ?COMMAND_DELIVER:16, ?VERSION_0:16, SubscriptionId:8/unsigned>>,
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
                    #consumer{member_pid = LocalMember} = State,
                    osiris:register_offset_listener(LocalMember, osiris_log:next_offset(Segment1)),
                    {{segment, Segment1}, {credit, Credit}}
            end
    end.

check_resource_access(User, Resource, Perm, Context) ->
    V = {Resource, Context, Perm},

    Cache = case get(permission_cache) of
                undefined -> [];
                Other -> Other
            end,
    case lists:member(V, Cache) of
        true -> ok;
        false ->
            try
                rabbit_access_control:check_resource_access(
                    User, Resource, Perm, Context),
                CacheTail = lists:sublist(Cache, ?MAX_PERMISSION_CACHE_SIZE - 1),
                put(permission_cache, [V | CacheTail]),
                ok
            catch
                exit:_ ->
                    error
            end
    end.

check_configure_permitted(Resource, User, Context) ->
    check_resource_access(User, Resource, configure, Context).

check_write_permitted(Resource, User, Context) ->
    check_resource_access(User, Resource, write, Context).

check_read_permitted(Resource, User, Context) ->
    check_resource_access(User, Resource, read, Context).

%%clear_permission_cache() -> erase(permission_cache),
%%    erase(topic_permission_cache),
%%    ok.