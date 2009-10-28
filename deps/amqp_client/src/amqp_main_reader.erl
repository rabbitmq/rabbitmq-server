%%   The contents of this file are subject to the Mozilla Public License
%%   Version 1.1 (the "License"); you may not use this file except in
%%   compliance with the License. You may obtain a copy of the License at
%%   http://www.mozilla.org/MPL/
%%
%%   Software distributed under the License is distributed on an "AS IS"
%%   basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See the
%%   License for the specific language governing rights and limitations
%%   under the License.
%%
%%   The Original Code is the RabbitMQ Erlang Client.
%%
%%   The Initial Developers of the Original Code are LShift Ltd.,
%%   Cohesive Financial Technologies LLC., and Rabbit Technologies Ltd.
%%
%%   Portions created by LShift Ltd., Cohesive Financial
%%   Technologies LLC., and Rabbit Technologies Ltd. are Copyright (C)
%%   2007 LShift Ltd., Cohesive Financial Technologies LLC., and Rabbit
%%   Technologies Ltd.;
%%
%%   All Rights Reserved.
%%
%%   Contributor(s): Ben Hood <0x6e6562@gmail.com>.

%% @private
-module(amqp_main_reader).

-include("amqp_client.hrl").

-export([start/2]).

-record(mr_state, {sock,
                   message = none, %% none | {Type, Channel, Length}
                   framing_channels = amqp_channel_util:new_channel_dict()}).

start(Sock, Framing0Pid) ->
    spawn_link(
        fun() ->
            State0 = #mr_state{sock = Sock},
            State1 = register_framing_channel(0, Framing0Pid, none, State0),
            {ok, _Ref} = rabbit_net:async_recv(Sock, 7, infinity),
            main_loop(State1)
        end).

main_loop(State = #mr_state{sock = Sock}) ->
    receive
        {inet_async, Sock, _, _} = InetAsync ->
            main_loop(handle_inet_async(InetAsync, State));
        {heartbeat, Heartbeat} ->
            rabbit_heartbeat:start_heartbeat(Sock, Heartbeat),
            main_loop(State);
        {register_framing_channel, Number, Pid, Caller} ->
            main_loop(register_framing_channel(Number, Pid, Caller, State));
        timeout ->
            ?LOG_WARN("Main reader (~p) received timeout from heartbeat, "
                      "exiting~n", [self()]),
            exit(connection_timeout);
        socket_closing_timeout ->
            ?LOG_WARN("Main reader (~p) received socket_closing_timeout, "
                      "exiting~n", [self()]),
            exit(socket_closing_timeout);
        close ->
            close(State);
        {'DOWN', _MonitorRef, process, _Pid, _Info} = Down ->
            main_loop(handle_down(Down, State));
        Other ->
            ?LOG_WARN("Main reader (~p) closing: unexpected message ~p",
                      [self(), Other]),
            exit({unexpected_message, Other})
    end.

handle_inet_async({inet_async, Sock, _, Msg},
                  State = #mr_state{sock = Sock,
                                    message = CurMessage}) ->
    {Type, Channel, Length} = case CurMessage of
                                  {T, C, L} -> {T, C, L};
                                  none      -> {none, none, none}
                              end,
    case Msg of
        {ok, <<Payload:Length/binary, ?FRAME_END>>} ->
            case handle_frame(Type, Channel, Payload, State) of
                closed_ok -> close(State);
                _         -> {ok, _Ref} =
                                 rabbit_net:async_recv(Sock, 7, infinity),
                             State#mr_state{message = none}
            end;
        {ok, <<NewType:8, NewChannel:16, NewLength:32>>} ->
            {ok, _Ref} = rabbit_net:async_recv(Sock, NewLength + 1, infinity),
            State#mr_state{message = {NewType, NewChannel, NewLength}};
        {error, closed} ->
            exit(socket_closed);
        {error, Reason} ->
            ?LOG_WARN("Socket error: ~p~n", [Reason]),
            exit({socket_error, Reason})
    end.

handle_frame(Type, Channel, Payload, State) ->
    case rabbit_reader:analyze_frame(Type, Payload) of
        heartbeat when Channel /= 0 ->
            rabbit_misc:die(frame_error);
        trace when Channel /= 0 ->
            rabbit_misc:die(frame_error);
        %% Match heartbeats and trace frames, but don't do anything with them
        heartbeat ->
            heartbeat;
        trace ->
            trace;
        {method, Method = 'connection.close_ok', none} ->
            pass_frame(Channel, {method, Method}, State),
            closed_ok;
        AnalyzedFrame ->
            pass_frame(Channel, AnalyzedFrame, State)
    end.

pass_frame(Channel, Frame, #mr_state{framing_channels = Channels}) ->
    case amqp_channel_util:resolve_channel_number(Channel, Channels) of
        undefined ->
            ?LOG_INFO("Dropping frame ~p for invalid or closed channel "
                      "number ~p~n", [Frame, Channel]),
            ok;
        FramingPid ->
            rabbit_framing_channel:process(FramingPid, Frame)
    end.

register_framing_channel(Number, Pid, Caller,
                         State = #mr_state{framing_channels = Channels}) ->
    NewChannels = amqp_channel_util:register_channel(Number, Pid, Channels),
    erlang:monitor(process, Pid),
    case Caller of
        none -> ok;
        _    -> Caller ! registered_framing_channel
    end,
    State#mr_state{framing_channels = NewChannels}.

handle_down({'DOWN', _MonitorRef, process, Pid, Info},
            State = #mr_state{framing_channels = Channels}) ->
    case amqp_channel_util:is_channel_pid_registered(Pid, Channels) of
        true ->
            NewChannels =
                amqp_channel_util:unregister_channel_pid(Pid, Channels),
            State#mr_state{framing_channels = NewChannels};
        false ->
            ?LOG_WARN("Reader received unexpected DOWN signal from (~p)."
                      "Info: ~p~n", [Pid, Info]),
            exit({unexpected_down, Pid, Info})
    end.

close(#mr_state{sock = Sock}) ->
    rabbit_net:close(Sock),
    exit(normal).
